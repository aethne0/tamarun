/*
import (
    "context"
    "fmt"
    "net"
    "github.com/minio/minio-go/v7"
)

func GetMinioClient() (*minio.Client, error) {
    // 1. Resolve SRV record (e.g., from Consul/Nomad)
    // Format: _service._proto.name.
    _, addrs, err := net.LookupSRV("s3", "tcp", "minio-api.service.consul")
    if err != nil || len(addrs) == 0 {
        return nil, fmt.Errorf("could not find minio srv: %v", err)
    }

    // 2. Build the endpoint string using the target and port from the SRV record
    // Use the first result (addrs[0])
    endpoint := fmt.Sprintf("%s:%d", addrs[0].Target, addrs[0].Port)

    // 3. Initialize client with the resolved endpoint
    return minio.New(endpoint, &minio.Options{
        Creds:  credentials.NewStaticV4("minioadmin", "minioadmin", ""),
        Secure: false,
    })
}
*/

package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/lmittmann/tint"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/redis/go-redis/v9"
	"github.com/zeebo/blake3"
)

var ctx = context.Background()

type Job struct {
	Id 		uuid.UUID 	`json:"id"`
	Script	string		`json:"script"`
}

func main() {
	slog.SetDefault(slog.New(tint.NewHandler(os.Stderr, &tint.Options{
		Level:      slog.LevelDebug,
		TimeFormat: time.TimeOnly,
	})))

	redisC := redis.NewClient(&redis.Options{
		Addr: "redis.service.consul:24982",
		Password: "",
		DB: 0,
	})

	minioC, err := minio.New("minio-s3.service.consul:28360", &minio.Options{
        Creds:  credentials.NewStaticV4("minioadmin", "minioadmin", ""),
        Secure: false,
    })
	if err != nil { 
		slog.Error("minio new", "err", err) 
		os.Exit(-1)
	}

	res, err := redisC.Ping(ctx).Result()
	if err != nil { slog.Warn("err", "err", err) }

	fmt.Println(res)

	// "uploading"

	scriptIn := `print("wew, lad!"); a = 3 + 4; print("wowee zowie {}".format(a))`
	b := blake3.Sum256([]byte(scriptIn))
	bs := hex.EncodeToString(b[:])


	slog.Info("redis: hashed-to", "hash", bs)
	_, err = redisC.Get(ctx, bs).Result()
	if err != nil {
		slog.Info("redis: not found - caching", "hash", bs)
		redisC.Set(ctx, bs, "pending", time.Minute)
		_, err := minioC.PutObject(
			ctx, 
			"tamarun", 
			bs,
			bytes.NewReader([]byte(scriptIn)),
			int64(len(scriptIn)),
			minio.PutObjectOptions{},
		)
		if err != nil {
			slog.Warn("err", "err", err)
		}

		redisC.Set(ctx, hex.EncodeToString(b[:]), "cached", time.Minute)
	}

	// "running"

	retrieved := redisC.Get(ctx, hex.EncodeToString(b[:]))
	s, err := retrieved.Result()
	if err != nil {
		slog.Warn("err", "err", err)
	}

	obj, _ := minioC.GetObject(ctx, "tamarun", bs, minio.GetObjectOptions{})
	script := make([]byte, 128)
	n, _ := obj.Read(script)
	script = script[:n]

	slog.Info("redis", "hash", hex.EncodeToString(b[:]), "content", s)
	slog.Info("minio", "hash", hex.EncodeToString(b[:]), "content", string(script))
	err = run(uuid.New(), string(script))
	if err != nil {
		slog.Warn("err", "err", err)
	}
}

func run(id uuid.UUID, script string) error {
	jail := filepath.Join("/tmp", id.String())
	
	// prepare workspace
	os.MkdirAll(filepath.Join(jail, "app"), 0755)
	os.WriteFile(filepath.Join(jail, "app/main.py"), []byte(script), 0644)

	// bind mount system deps so python3 exists in the jail
	// we mount these from the host into the jail directory
	sysDirs := []string{"/bin", "/lib", "/lib64", "/usr", "/sys"}
	for _, dir := range sysDirs {
		target := filepath.Join(jail, dir)
		os.MkdirAll(target, 0755)
		if err := syscall.Mount(dir, target, "", syscall.MS_BIND|syscall.MS_RDONLY, ""); err != nil {
			return fmt.Errorf("mount %s failed: %v", dir, err)
		}
	}

	// ensure we unmount everything when the function returns
	defer func() {
		for _, dir := range sysDirs {
			syscall.Unmount(filepath.Join(jail, dir), 0)
		}
		os.RemoveAll(jail)
	}()

	// setup cgroup v2
	cgPath := filepath.Join("/sys/fs/cgroup", id.String())
	os.Mkdir(cgPath, 0755)
	defer os.Remove(cgPath) 
	os.WriteFile(filepath.Join(cgPath, "memory.max"), []byte("128M"), 0644)

	// define command & isolation
	cmd := exec.Command("python3", "/app/main.py")
	cmd.Env = append(os.Environ(), "PYTHONUNBUFFERED=1") // Prevents stdout lag
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Cloneflags: syscall.CLONE_NEWUTS | syscall.CLONE_NEWPID | 
		            syscall.CLONE_NEWNS | syscall.CLONE_NEWNET | 
		            syscall.CLONE_NEWUSER,
		UidMappings: []syscall.SysProcIDMap{{ContainerID: 0, HostID: os.Getuid(), Size: 1}},
		GidMappings: []syscall.SysProcIDMap{{ContainerID: 0, HostID: os.Getgid(), Size: 1}},
		Chroot:      jail,
		Ptrace:      true, // start paused
	}

	cmd.Dir = "/app"

	// execution
	if err := cmd.Start(); err != nil {
		return err
	}

	// move into cgroup while process is paused via ptrace
	os.WriteFile(
		filepath.Join(cgPath, "cgroup.procs"),
		fmt.Append(nil, cmd.Process.Pid),
		0644,
	)

	// use cmd.process.pid (processstate is nil until after wait)
	err := syscall.PtraceDetach(cmd.Process.Pid)
	if err != nil {
		return fmt.Errorf("ptrace detach failed: %v", err)
	}

	return cmd.Wait()
}
