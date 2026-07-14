package sandbox

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
)

type RunRequest struct {
	TaskID          uuid.UUID `json:"task_id"`
	AttemptID       uuid.UUID `json:"attempt_id"`
	GPUUUID         string    `json:"gpu_uuid"`
	ResourceProfile string    `json:"resource_profile"`
	ImageDigest     string    `json:"image_digest"`
	MaxOutputBytes  int64     `json:"max_output_bytes"`
	MaxScratchBytes int64     `json:"max_scratch_bytes"`
}

type RunResponse struct {
	ExitCode       int    `json:"exit_code"`
	Reason         string `json:"reason,omitempty"`
	OutputBytes    int64  `json:"output_bytes"`
	ScratchBytes   int64  `json:"scratch_bytes"`
	DurationMillis int64  `json:"duration_millis"`
	LogTail        string `json:"log_tail,omitempty"`
}

type ResourceProfile struct {
	CPUs            string
	Memory          string
	PIDs            int
	TempBytes       int64
	MaxOutputBytes  int64
	MaxScratchBytes int64
	WallTime        time.Duration
}

var ResourceProfiles = map[string]ResourceProfile{
	"rtx4090": {CPUs: "16", Memory: "44g", PIDs: 256, TempBytes: 32 << 30, MaxOutputBytes: 100 << 30, MaxScratchBytes: 60 << 30, WallTime: 4 * time.Hour},
	"rtx5090": {CPUs: "20", Memory: "56g", PIDs: 256, TempBytes: 48 << 30, MaxOutputBytes: 100 << 30, MaxScratchBytes: 80 << 30, WallTime: 4 * time.Hour},
	"cpu":     {CPUs: "16", Memory: "40g", PIDs: 256, TempBytes: 32 << 30, MaxOutputBytes: 100 << 30, MaxScratchBytes: 60 << 30, WallTime: 4 * time.Hour},
}

type Policy struct {
	AttemptRoot        string
	AllowedImageDigest string
	AllowedGPUUUIDs    map[string]bool
	SeccompProfile     string
	AppArmorProfile    string
}

type ValidatedRun struct {
	Request RunRequest
	Profile ResourceProfile
	Root    string
	Input   string
	Output  string
	Scratch string
}

func (p Policy) Validate(req RunRequest) (ValidatedRun, error) {
	if req.TaskID == uuid.Nil || req.AttemptID == uuid.Nil {
		return ValidatedRun{}, errors.New("task_id and attempt_id are required")
	}
	if req.ImageDigest != p.AllowedImageDigest || !strings.Contains(req.ImageDigest, "@sha256:") {
		return ValidatedRun{}, errors.New("image digest is not allowlisted")
	}
	profile, ok := ResourceProfiles[req.ResourceProfile]
	if !ok {
		return ValidatedRun{}, errors.New("resource profile is not allowlisted")
	}
	if req.ResourceProfile != "cpu" && !p.AllowedGPUUUIDs[req.GPUUUID] {
		return ValidatedRun{}, errors.New("GPU UUID is not allowlisted")
	}
	if req.MaxOutputBytes < 1 || req.MaxOutputBytes > profile.MaxOutputBytes || req.MaxScratchBytes < 1 || req.MaxScratchBytes > profile.MaxScratchBytes {
		return ValidatedRun{}, errors.New("attempt byte limits exceed the allowlisted resource profile")
	}
	profile.MaxOutputBytes = req.MaxOutputBytes
	profile.MaxScratchBytes = req.MaxScratchBytes
	base, err := filepath.Abs(p.AttemptRoot)
	if err != nil {
		return ValidatedRun{}, err
	}
	root := filepath.Join(base, req.AttemptID.String())
	if rel, relErr := filepath.Rel(base, root); relErr != nil || strings.HasPrefix(rel, "..") {
		return ValidatedRun{}, errors.New("attempt path escapes approved root")
	}
	return ValidatedRun{Request: req, Profile: profile, Root: root,
		Input: filepath.Join(root, "input"), Output: filepath.Join(root, "output"), Scratch: filepath.Join(root, "scratch")}, nil
}

func (p Policy) DockerArgs(run ValidatedRun) []string {
	name := "library-prep-" + run.Request.AttemptID.String()
	fileSizeLimit := run.Profile.MaxOutputBytes
	if run.Profile.MaxScratchBytes > fileSizeLimit {
		fileSizeLimit = run.Profile.MaxScratchBytes
	}
	args := []string{
		"run", "--rm", "--init", "--name", name, "--network", "none", "--read-only",
		"--user", "65532:65532", "--cap-drop", "ALL", "--security-opt", "no-new-privileges=true",
		"--security-opt", "seccomp=" + p.SeccompProfile,
		"--pids-limit", fmt.Sprint(run.Profile.PIDs), "--cpus", run.Profile.CPUs, "--memory", run.Profile.Memory,
		"--ulimit", "nofile=4096:4096", "--ulimit", fmt.Sprintf("fsize=%d:%d", fileSizeLimit, fileSizeLimit),
		"--stop-signal", "SIGTERM", "--stop-timeout", "30",
		"--mount", "type=bind,src=" + run.Input + ",dst=/work/input,readonly",
		"--mount", "type=bind,src=" + run.Output + ",dst=/work/output",
		"--mount", "type=bind,src=" + run.Scratch + ",dst=/work/scratch",
		"--tmpfs", fmt.Sprintf("/tmp:rw,noexec,nosuid,nodev,size=%d", run.Profile.TempBytes),
	}
	if p.AppArmorProfile != "" {
		args = append(args, "--security-opt", "apparmor="+p.AppArmorProfile)
	}
	if run.Request.ResourceProfile != "cpu" {
		args = append(args, "--gpus", "device="+run.Request.GPUUUID)
	}
	args = append(args, run.Request.ImageDigest, "run", "--spec", "/work/input/task.json", "--output", "/work/output", "--scratch", "/work/scratch")
	return args
}
