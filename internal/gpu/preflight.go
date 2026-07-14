package gpu

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

type Profile struct {
	Type             string `json:"type"`
	EngineChunk      int    `json:"engine_chunk"`
	NVBatch          int    `json:"nvmolkit_batch"`
	BatchesPerGPU    int    `json:"batches_per_gpu"`
	RequiredFreeMiB  int64  `json:"required_free_vram_mib"`
	OOMFallbackChunk int    `json:"oom_fallback_chunk"`
	OOMFallbackBatch int    `json:"oom_fallback_batch"`
}

var Profiles = map[string]Profile{
	"rtx4090": {Type: "rtx4090", EngineChunk: 50_000, NVBatch: 250, BatchesPerGPU: 2, RequiredFreeMiB: 22_000, OOMFallbackChunk: 25_000, OOMFallbackBatch: 128},
	"rtx5090": {Type: "rtx5090", EngineChunk: 100_000, NVBatch: 500, BatchesPerGPU: 4, RequiredFreeMiB: 30_000, OOMFallbackChunk: 50_000, OOMFallbackBatch: 250},
}

type Sample struct {
	UUID          string    `json:"uuid"`
	Name          string    `json:"name"`
	DriverVersion string    `json:"driver_version"`
	TotalMiB      int64     `json:"total_vram_mib"`
	FreeMiB       int64     `json:"free_vram_mib"`
	Utilization   float64   `json:"utilization_percent"`
	ComputePIDs   []int     `json:"compute_pids"`
	SampledAt     time.Time `json:"sampled_at"`
}

type Result struct {
	Ready   bool     `json:"ready"`
	Reason  string   `json:"reason,omitempty"`
	Samples []Sample `json:"samples"`
}

type Runner interface {
	Run(context.Context, string, ...string) ([]byte, error)
}

type CommandRunner struct{}

func (CommandRunner) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, name, args...).CombinedOutput()
}

func ThreeSamplePreflight(ctx context.Context, runner Runner, expectedUUID string, profile Profile, interval time.Duration) (Result, error) {
	if expectedUUID == "" {
		return Result{}, errors.New("expected GPU UUID is required")
	}
	result := Result{Samples: make([]Sample, 0, 3)}
	for index := 0; index < 3; index++ {
		sample, err := ReadSample(ctx, runner, expectedUUID)
		if err != nil {
			return result, err
		}
		result.Samples = append(result.Samples, sample)
		switch {
		case sample.UUID != expectedUUID:
			result.Reason = "gpu_uuid_mismatch"
		case len(sample.ComputePIDs) > 0:
			result.Reason = "active_compute_process"
		case sample.FreeMiB < profile.RequiredFreeMiB:
			result.Reason = "insufficient_free_vram"
		case sample.Utilization > 5:
			result.Reason = "gpu_utilization_above_5_percent"
		}
		if result.Reason != "" {
			return result, nil
		}
		if index < 2 && interval > 0 {
			timer := time.NewTimer(interval)
			select {
			case <-ctx.Done():
				timer.Stop()
				return result, ctx.Err()
			case <-timer.C:
			}
		}
	}
	result.Ready = true
	return result, nil
}

func ReadSample(ctx context.Context, runner Runner, expectedUUID string) (Sample, error) {
	query, err := runner.Run(ctx, "nvidia-smi", "--query-gpu=uuid,name,driver_version,memory.total,memory.free,utilization.gpu", "--format=csv,noheader,nounits", "--id="+expectedUUID)
	if err != nil {
		return Sample{}, fmt.Errorf("nvidia-smi GPU health query failed: %w: %s", err, strings.TrimSpace(string(query)))
	}
	sample, err := ParseGPUQuery(string(query))
	if err != nil {
		return Sample{}, err
	}
	processOutput, processErr := runner.Run(ctx, "nvidia-smi", "--query-compute-apps=gpu_uuid,pid", "--format=csv,noheader,nounits")
	if processErr != nil {
		return Sample{}, fmt.Errorf("nvidia-smi process query failed: %w: %s", processErr, strings.TrimSpace(string(processOutput)))
	}
	sample.ComputePIDs, err = ParseComputeProcesses(string(processOutput), expectedUUID)
	if err != nil {
		return Sample{}, err
	}
	sample.SampledAt = time.Now().UTC()
	return sample, nil
}

func ParseGPUQuery(output string) (Sample, error) {
	records, err := csv.NewReader(strings.NewReader(strings.TrimSpace(output))).ReadAll()
	if err != nil || len(records) != 1 || len(records[0]) != 6 {
		return Sample{}, errors.New("unexpected nvidia-smi GPU query output")
	}
	for index := range records[0] {
		records[0][index] = strings.TrimSpace(records[0][index])
	}
	total, err := strconv.ParseInt(records[0][3], 10, 64)
	if err != nil {
		return Sample{}, fmt.Errorf("parse total VRAM: %w", err)
	}
	free, err := strconv.ParseInt(records[0][4], 10, 64)
	if err != nil {
		return Sample{}, fmt.Errorf("parse free VRAM: %w", err)
	}
	utilization, err := strconv.ParseFloat(records[0][5], 64)
	if err != nil {
		return Sample{}, fmt.Errorf("parse GPU utilization: %w", err)
	}
	return Sample{UUID: records[0][0], Name: records[0][1], DriverVersion: records[0][2], TotalMiB: total, FreeMiB: free, Utilization: utilization}, nil
}

func ParseComputeProcesses(output, expectedUUID string) ([]int, error) {
	trimmed := strings.TrimSpace(output)
	if trimmed == "" || strings.Contains(strings.ToLower(trimmed), "no running processes") {
		return nil, nil
	}
	records, err := csv.NewReader(strings.NewReader(trimmed)).ReadAll()
	if err != nil {
		return nil, err
	}
	var pids []int
	for _, record := range records {
		if len(record) != 2 || strings.TrimSpace(record[0]) != expectedUUID {
			continue
		}
		pid, parseErr := strconv.Atoi(strings.TrimSpace(record[1]))
		if parseErr != nil {
			return nil, parseErr
		}
		pids = append(pids, pid)
	}
	return pids, nil
}

func RequiredFreeScratch(predictedBytes int64) int64 {
	const gib = int64(1024 * 1024 * 1024)
	return (predictedBytes*3+1)/2 + 5*gib + 20*gib
}
