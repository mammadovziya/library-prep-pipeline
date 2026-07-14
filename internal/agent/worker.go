package agent

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ayvazov-i/library-prep-pipeline/internal/gpu"
	"github.com/ayvazov-i/library-prep-pipeline/internal/objectstore"
	"github.com/ayvazov-i/library-prep-pipeline/internal/platform"
	"github.com/ayvazov-i/library-prep-pipeline/internal/sandbox"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
)

type Worker struct {
	ID                uuid.UUID
	GPUUUID           string
	GPUProfile        gpu.Profile
	ImageDigest       string
	DriverVersion     string
	AttemptRoot       string
	ArtifactBucket    string
	PreflightRunner   gpu.Runner
	PreflightInterval time.Duration
	API               *APIClient
	Objects           *objectstore.Client
	Sandbox           *sandbox.Client
	Log               *slog.Logger
}

func FreeDiskBytes(path string) (int64, error) { return freeDiskBytes(path) }

type taskReference struct {
	TaskID             uuid.UUID `json:"task_id"`
	RequiredCapability string    `json:"required_capability"`
}

type taskSpec struct {
	Stage                 string `json:"stage"`
	PredictedScratchBytes int64  `json:"predicted_scratch_bytes"`
	PredictedOutputBytes  int64  `json:"predicted_output_bytes"`
	Input                 struct {
		Bucket         string `json:"bucket"`
		ObjectKey      string `json:"object_key"`
		SizeBytes      int64  `json:"size_bytes"`
		ChecksumSHA256 string `json:"checksum_sha256"`
	} `json:"input"`
}

type runnerResult struct {
	Artifacts []struct {
		Path      string          `json:"path"`
		Kind      string          `json:"kind"`
		MediaType string          `json:"media_type"`
		Manifest  json.RawMessage `json:"manifest"`
	} `json:"artifacts"`
	Metrics json.RawMessage `json:"metrics"`
}

func (w *Worker) Preflight(ctx context.Context) (gpu.Result, error) {
	return gpu.ThreeSamplePreflight(ctx, w.PreflightRunner, w.GPUUUID, w.GPUProfile, w.PreflightInterval)
}

func (w *Worker) Qualify(ctx context.Context) error {
	taskID, attemptID := uuid.New(), uuid.New()
	root := filepath.Join(w.AttemptRoot, attemptID.String())
	inputDir, outputDir, scratchDir := filepath.Join(root, "input"), filepath.Join(root, "output"), filepath.Join(root, "scratch")
	for _, directory := range []string{inputDir, outputDir, scratchDir} {
		if err := os.MkdirAll(directory, 0750); err != nil {
			return err
		}
	}
	defer func() { _ = os.RemoveAll(root) }()
	spec, _ := json.Marshal(map[string]any{
		"schema_version": "1", "stage": "qualification", "gpu_profile": w.GPUProfile,
		"image_digest": w.ImageDigest, "gpu_uuid": w.GPUUUID, "driver_version": w.DriverVersion,
	})
	if err := os.WriteFile(filepath.Join(inputDir, "task.json"), spec, 0640); err != nil {
		return err
	}
	result, err := w.Sandbox.Run(ctx, sandbox.RunRequest{
		TaskID: taskID, AttemptID: attemptID, GPUUUID: w.GPUUUID,
		ResourceProfile: w.GPUProfile.Type, ImageDigest: w.ImageDigest,
		MaxOutputBytes: 1 << 30, MaxScratchBytes: 1 << 30,
	})
	if err != nil {
		return err
	}
	if result.ExitCode != 0 {
		return fmt.Errorf("qualification sandbox exited with code %d", result.ExitCode)
	}
	if _, err = os.Stat(filepath.Join(outputDir, "qualification.json")); err != nil {
		return errors.New("qualification report is missing")
	}
	return nil
}

func (w *Worker) HandleMessage(ctx context.Context, message *nats.Msg, capability string) {
	var reference taskReference
	if err := json.Unmarshal(message.Data, &reference); err != nil || reference.TaskID == uuid.Nil {
		w.Log.Error("terminating malformed task reference")
		_ = message.Term()
		return
	}
	if capability == "gpu" {
		result, err := w.Preflight(ctx)
		if err != nil {
			w.Log.Error("GPU health preflight failed", "error", err)
			_ = message.NakWithDelay(jitter(30*time.Second, 15*time.Second))
			return
		}
		if !result.Ready {
			w.Log.Info("task deferred for external GPU activity", "reason", "blocked_external_gpu")
			if deferErr := w.API.Defer(ctx, reference.TaskID, "blocked_external_gpu"); deferErr != nil {
				w.Log.Warn("GPU deferral could not be recorded", "task_id", reference.TaskID, "error", deferErr)
				_ = message.NakWithDelay(2 * time.Minute)
				return
			}
			_ = message.AckSync()
			return
		}
	}
	claim, err := w.API.Claim(ctx, reference.TaskID)
	if err != nil {
		if errors.Is(err, platform.ErrNotFound) {
			_ = message.AckSync()
			return
		}
		if errors.Is(err, platform.ErrConflict) {
			// A duplicate can arrive while the winning attempt still owns a
			// renewable database lease (for example after a temporary NATS
			// partition). Never ACK that delivery here: doing so could strand
			// the task if the winner later crashes before committing.
			_ = message.NakWithDelay(2 * time.Minute)
			return
		}
		if errors.Is(err, platform.ErrCapacityUnavailable) || errors.Is(err, platform.ErrActiveJobLimit) || errors.Is(err, platform.ErrDailyGPUQuota) {
			reason := "worker_capacity"
			if errors.Is(err, platform.ErrActiveJobLimit) {
				reason = "account_active_limit"
			} else if errors.Is(err, platform.ErrDailyGPUQuota) {
				reason = "daily_gpu_quota"
			}
			if deferErr := w.API.Defer(ctx, reference.TaskID, reason); deferErr == nil {
				_ = message.AckSync()
				return
			}
		}
		w.Log.Warn("task claim deferred", "error", err)
		_ = message.NakWithDelay(30 * time.Second)
		return
	}
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	heartbeatDone := make(chan struct{})
	go w.heartbeat(workCtx, cancel, message, claim, heartbeatDone)
	err = w.execute(workCtx, claim, capability)
	cancel()
	<-heartbeatDone
	if err == nil {
		if ackErr := message.AckSync(); ackErr != nil {
			w.Log.Error("JetStream ACK failed after database commit", "task_id", claim.TaskID, "error", ackErr)
		}
		return
	}
	if errors.Is(err, platform.ErrLeaseLost) {
		w.Log.Warn("discarding output from stale attempt", "task_id", claim.TaskID, "attempt_id", claim.AttemptID)
		_ = message.NakWithDelay(30 * time.Second)
		return
	}
	var capacity capacityDeferral
	if errors.As(err, &capacity) {
		if deferErr := w.API.DeferClaim(ctx, claim, capacity.reason); deferErr != nil {
			w.Log.Error("claimed GPU capacity deferral could not be recorded", "task_id", claim.TaskID, "error", deferErr)
			_ = message.NakWithDelay(2 * time.Minute)
			return
		}
		w.Log.Info("claimed task deferred before sandbox launch", "reason", capacity.reason)
		_ = message.AckSync()
		return
	}
	var failure executionFailure
	if errors.Is(err, platform.ErrCapacityUnavailable) {
		failure = executionFailure{code: "reservation_exhausted", detail: "committed output would exceed the server-owned peak reservation", terminal: true}
	} else if !errors.As(err, &failure) {
		failure = executionFailure{code: "worker_infrastructure", detail: "worker execution failed", terminal: false}
	}
	disposition, failErr := w.API.Fail(ctx, claim, failure.terminal, failure.code, failure.detail)
	if failErr != nil {
		w.Log.Error("failed to persist attempt failure", "task_id", claim.TaskID, "error", failErr)
		_ = message.NakWithDelay(2 * time.Minute)
		return
	}
	if disposition == "terminal" {
		_ = message.Term()
	} else {
		_ = message.NakWithDelay(retryDelay(claim.ExecutionAttemptNumber))
	}
}

func (w *Worker) execute(ctx context.Context, claim platform.AttemptClaim, capability string) error {
	var spec taskSpec
	if err := json.Unmarshal(claim.TaskSpec, &spec); err != nil {
		return executionFailure{code: "invalid_task_spec", detail: "task specification is invalid", terminal: true}
	}
	freeBytes, err := freeDiskBytes(w.AttemptRoot)
	if err != nil {
		return executionFailure{code: "scratch_probe_failed", detail: "scratch capacity could not be measured"}
	}
	if spec.PredictedOutputBytes < 1 || spec.PredictedOutputBytes > 100<<30 {
		return executionFailure{code: "invalid_task_spec", detail: "predicted output limit is invalid", terminal: true}
	}
	if freeBytes < gpu.RequiredFreeScratch(spec.PredictedScratchBytes)+spec.PredictedOutputBytes {
		return executionFailure{code: "scratch_capacity", detail: "predicted scratch does not fit on this host"}
	}
	root := filepath.Join(w.AttemptRoot, claim.AttemptID.String())
	inputDir, outputDir, scratchDir := filepath.Join(root, "input"), filepath.Join(root, "output"), filepath.Join(root, "scratch")
	for _, directory := range []string{inputDir, outputDir, scratchDir} {
		if err = os.MkdirAll(directory, 0750); err != nil {
			return executionFailure{code: "scratch_create_failed", detail: "attempt directories could not be created"}
		}
	}
	defer func() { _ = os.RemoveAll(root) }()
	if spec.Input.ObjectKey != "" {
		inputPath := filepath.Join(inputDir, "source")
		if err = w.Objects.DownloadFile(ctx, spec.Input.Bucket, spec.Input.ObjectKey, inputPath, spec.Input.ChecksumSHA256, spec.Input.SizeBytes); err != nil {
			return executionFailure{code: "input_download_failed", detail: "input could not be downloaded or verified"}
		}
	}
	runtimeSpec := map[string]any{}
	if err = json.Unmarshal(claim.TaskSpec, &runtimeSpec); err != nil {
		return executionFailure{code: "invalid_task_spec", detail: "task specification is invalid", terminal: true}
	}
	runtimeSpec["attempt"] = map[string]any{
		"attempt_id": claim.AttemptID, "fencing_token": claim.FencingToken,
		"attempt_number": claim.AttemptNumber, "execution_attempt_number": claim.ExecutionAttemptNumber,
		"use_oom_fallback": claim.ExecutionAttemptNumber > 1,
	}
	runtimeSpec["gpu_profile"] = w.GPUProfile
	runtimeSpec["image_digest"] = w.ImageDigest
	runtimeSpec["gpu_uuid"] = w.GPUUUID
	runtimeSpec["driver_version"] = w.DriverVersion
	if spec.Input.ObjectKey != "" {
		runtimeSpec["local_input"] = "/work/input/source"
	}
	taskJSON, _ := json.Marshal(runtimeSpec)
	if err = os.WriteFile(filepath.Join(inputDir, "task.json"), taskJSON, 0640); err != nil {
		return executionFailure{code: "task_stage_failed", detail: "task specification could not be staged"}
	}
	resourceProfile := w.GPUProfile.Type
	if capability == "cpu" {
		resourceProfile = "cpu"
	} else {
		preflight, preflightErr := w.Preflight(ctx)
		if preflightErr != nil {
			return executionFailure{code: "gpu_health_probe", detail: "final GPU health preflight failed"}
		}
		if !preflight.Ready {
			return capacityDeferral{reason: "blocked_external_gpu"}
		}
	}
	sandboxResult, err := w.Sandbox.Run(ctx, sandbox.RunRequest{
		TaskID: claim.TaskID, AttemptID: claim.AttemptID, GPUUUID: w.GPUUUID,
		ResourceProfile: resourceProfile, ImageDigest: w.ImageDigest,
		MaxOutputBytes: spec.PredictedOutputBytes, MaxScratchBytes: spec.PredictedScratchBytes,
	})
	if err != nil {
		return executionFailure{code: "sandbox_unavailable", detail: "sandbox service did not complete the request"}
	}
	if sandboxResult.ExitCode != 0 {
		if sandboxResult.Reason == "output_limit" || sandboxResult.Reason == "scratch_limit" {
			return executionFailure{code: "reservation_exceeded", detail: "sandbox reached its task-specific byte reservation", terminal: true}
		}
		switch sandboxResult.ExitCode {
		case 20, 42:
			return executionFailure{code: "deterministic_chemistry_failure", detail: "chemistry task was rejected deterministically", terminal: true}
		case 70:
			return executionFailure{code: "cuda_oom", detail: "CUDA memory limit reached; retry with fallback profile"}
		case 124:
			return executionFailure{code: "sandbox_timeout", detail: "chemistry task reached its wall-clock limit"}
		default:
			return executionFailure{code: "sandbox_process_failed", detail: "chemistry process exited unsuccessfully"}
		}
	}
	resultPath := filepath.Join(outputDir, "result.json")
	resultBody, err := os.ReadFile(resultPath)
	if err != nil || len(resultBody) > 4<<20 {
		return executionFailure{code: "result_manifest_missing", detail: "sandbox result manifest is missing or too large"}
	}
	var result runnerResult
	if err = json.Unmarshal(resultBody, &result); err != nil || len(result.Artifacts) == 0 {
		return executionFailure{code: "result_manifest_invalid", detail: "sandbox result manifest is invalid"}
	}
	commits := make([]platform.ArtifactCommit, 0, len(result.Artifacts))
	prefix := fmt.Sprintf("jobs/%s/tasks/%s/attempts/%s/", claim.JobID, claim.TaskID, claim.AttemptID)
	for _, artifact := range result.Artifacts {
		relative := filepath.Clean(artifact.Path)
		if relative == "." || filepath.IsAbs(relative) || strings.HasPrefix(relative, "..") {
			return executionFailure{code: "artifact_path_invalid", detail: "runner returned an invalid artifact path", terminal: true}
		}
		localPath := filepath.Join(outputDir, relative)
		if err = validateArtifactFile(outputDir, localPath); err != nil {
			return executionFailure{code: "artifact_path_invalid", detail: "runner artifact escapes output directory", terminal: true}
		}
		objectKey := prefix + filepath.ToSlash(relative)
		size, checksum, uploadErr := w.Objects.UploadFile(ctx, w.ArtifactBucket, objectKey, localPath)
		if uploadErr != nil {
			if errors.Is(uploadErr, objectstore.ErrChecksumMismatch) {
				return executionFailure{code: "output_checksum_mismatch", detail: "attempt artifact failed independent checksum verification"}
			}
			return executionFailure{code: "artifact_upload_failed", detail: "attempt artifact could not be uploaded"}
		}
		commits = append(commits, platform.ArtifactCommit{Kind: artifact.Kind, Bucket: w.ArtifactBucket, ObjectKey: objectKey, SizeBytes: size, ChecksumSHA256: checksum, MediaType: artifact.MediaType, Manifest: artifact.Manifest})
	}
	if err = w.API.Commit(ctx, claim, commits, result.Metrics); err != nil {
		return err
	}
	return nil
}

func (w *Worker) heartbeat(ctx context.Context, cancel context.CancelFunc, message *nats.Msg, claim platform.AttemptClaim, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(platform.DefaultHeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := message.InProgress(); err != nil {
				w.Log.Warn("JetStream in-progress heartbeat failed", "task_id", claim.TaskID, "error", err)
			}
			heartbeatCtx, stop := context.WithTimeout(ctx, 10*time.Second)
			err := w.API.Heartbeat(heartbeatCtx, claim)
			stop()
			if err != nil {
				w.Log.Warn("attempt lease heartbeat failed", "task_id", claim.TaskID, "error", err)
				cancel()
				return
			}
		}
	}
}

type executionFailure struct {
	code     string
	detail   string
	terminal bool
}

func (e executionFailure) Error() string { return e.code }

type capacityDeferral struct{ reason string }

func (e capacityDeferral) Error() string { return e.reason }

func validateArtifactFile(root, path string) error {
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	pathAbs, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	rootResolved, err := filepath.EvalSymlinks(rootAbs)
	if err != nil {
		return err
	}
	pathResolved, err := filepath.EvalSymlinks(pathAbs)
	if err != nil {
		return err
	}
	relative, err := filepath.Rel(rootResolved, pathResolved)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return errors.New("path escapes root")
	}
	info, err := os.Lstat(pathAbs)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("artifact is not a regular file")
	}
	return nil
}

func fileSHA256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	_, err = io.Copy(hash, file)
	return hex.EncodeToString(hash.Sum(nil)), err
}

func jitter(base, spread time.Duration) time.Duration {
	if spread <= 0 {
		return base
	}
	var value [1]byte
	_, _ = rand.Read(value[:])
	return base + time.Duration(float64(spread)*(float64(value[0])/255.0))
}

func retryDelay(executionAttempt int) time.Duration {
	backoff := []time.Duration{30 * time.Second, 2 * time.Minute, 10 * time.Minute}
	index := executionAttempt - 1
	if index < 0 {
		index = 0
	}
	if index >= len(backoff) {
		index = len(backoff) - 1
	}
	return jitter(backoff[index], backoff[index]/5)
}
