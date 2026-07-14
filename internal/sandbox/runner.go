package sandbox

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"
)

type DockerRunner struct {
	Policy     Policy
	DockerPath string
}

func (d DockerRunner) Run(ctx context.Context, request RunRequest) (RunResponse, error) {
	validated, err := d.Policy.Validate(request)
	if err != nil {
		return RunResponse{}, err
	}
	for _, directory := range []string{validated.Input, validated.Output, validated.Scratch} {
		if err = validateDirectory(validated.Root, directory); err != nil {
			return RunResponse{}, err
		}
	}
	if _, err = os.Stat(filepath.Join(validated.Input, "task.json")); err != nil {
		return RunResponse{}, errors.New("approved task.json is missing")
	}
	if d.DockerPath == "" {
		d.DockerPath = "docker"
	}
	started := time.Now()
	command := exec.Command(d.DockerPath, d.Policy.DockerArgs(validated)...)
	logTail := newTailBuffer(64 << 10)
	command.Stdout, command.Stderr = logTail, logTail
	if err = command.Start(); err != nil {
		return RunResponse{}, fmt.Errorf("start chemistry sandbox: %w", err)
	}
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	wallTimer := time.NewTimer(validated.Profile.WallTime)
	meter := time.NewTicker(time.Second)
	defer wallTimer.Stop()
	defer meter.Stop()
	response := RunResponse{}
	killReason := ""
	contextDone := ctx.Done()
	wallClock := wallTimer.C
	meterTick := meter.C
	stopping := false
	stop := func(reason string) {
		if stopping {
			return
		}
		stopping = true
		killReason = reason
		contextDone = nil
		wallClock = nil
		meterTick = nil
		go d.terminate(request.AttemptID)
	}
	for {
		select {
		case waitErr := <-done:
			response.DurationMillis = time.Since(started).Milliseconds()
			response.OutputBytes, _ = directorySize(validated.Output)
			response.ScratchBytes, _ = directorySize(validated.Scratch)
			response.LogTail = logTail.String()
			if waitErr == nil {
				return response, nil
			}
			var exitErr *exec.ExitError
			if errors.As(waitErr, &exitErr) {
				response.ExitCode = exitErr.ExitCode()
			} else {
				response.ExitCode = 125
			}
			if killReason != "" {
				response.Reason = killReason
				if killReason == "wall_clock_limit" {
					response.ExitCode = 124
				}
			}
			return response, nil
		case <-contextDone:
			stop("request_cancelled")
		case <-wallClock:
			stop("wall_clock_limit")
		case <-meterTick:
			outputBytes, outputErr := directorySize(validated.Output)
			scratchBytes, scratchErr := directorySize(validated.Scratch)
			if outputErr != nil || scratchErr != nil {
				stop("metering_failure")
				continue
			}
			if outputBytes > validated.Profile.MaxOutputBytes {
				stop("output_limit")
			} else if scratchBytes > validated.Profile.MaxScratchBytes {
				stop("scratch_limit")
			}
		}
	}
}

func (d DockerRunner) terminate(attemptID interface{ String() string }) {
	name := "library-prep-" + attemptID.String()
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 35*time.Second)
	stopErr := exec.CommandContext(stopCtx, d.DockerPath, "stop", "--signal", "SIGTERM", "--time", "30", name).Run()
	stopCancel()
	if stopErr != nil {
		killCtx, killCancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = exec.CommandContext(killCtx, d.DockerPath, "kill", "--signal", "SIGKILL", name).Run()
		killCancel()
	}
	removeCtx, removeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = exec.CommandContext(removeCtx, d.DockerPath, "rm", "--force", name).Run()
	removeCancel()
}

func validateDirectory(root, path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("sandbox path %s is not a real directory", path)
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return err
	}
	rootResolved, err := filepath.EvalSymlinks(root)
	if err != nil {
		return err
	}
	relative, err := filepath.Rel(rootResolved, resolved)
	if err != nil || relative == ".." || len(relative) >= 3 && relative[:3] == ".."+string(filepath.Separator) {
		return errors.New("sandbox path escapes attempt root")
	}
	return nil
}

func directorySize(root string) (int64, error) {
	var total int64
	err := filepath.WalkDir(root, func(_ string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type().IsRegular() {
			info, infoErr := entry.Info()
			if infoErr != nil {
				return infoErr
			}
			total += info.Size()
		}
		return nil
	})
	return total, err
}

type tailBuffer struct {
	mu    sync.Mutex
	limit int
	data  []byte
}

func newTailBuffer(limit int) *tailBuffer { return &tailBuffer{limit: limit} }

func (b *tailBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.data = append(b.data, p...)
	if len(b.data) > b.limit {
		b.data = bytes.Clone(b.data[len(b.data)-b.limit:])
	}
	return len(p), nil
}

func (b *tailBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(bytes.Clone(b.data))
}
