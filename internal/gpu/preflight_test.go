package gpu

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type scriptedRunner struct {
	outputs [][]byte
	index   int
}

func (s *scriptedRunner) Run(context.Context, string, ...string) ([]byte, error) {
	if s.index >= len(s.outputs) {
		return nil, errors.New("unexpected command")
	}
	value := s.outputs[s.index]
	s.index++
	return value, nil
}

func TestThreeSamplePreflightReady(t *testing.T) {
	runner := &scriptedRunner{outputs: [][]byte{
		[]byte("GPU-1, NVIDIA GeForce RTX 4090, 580.95, 24564, 23900, 1\n"), []byte(""),
		[]byte("GPU-1, NVIDIA GeForce RTX 4090, 580.95, 24564, 23890, 0\n"), []byte(""),
		[]byte("GPU-1, NVIDIA GeForce RTX 4090, 580.95, 24564, 23880, 2\n"), []byte(""),
	}}
	result, err := ThreeSamplePreflight(context.Background(), runner, "GPU-1", Profiles["rtx4090"], 0)
	if err != nil || !result.Ready || len(result.Samples) != 3 {
		t.Fatalf("expected ready result, got %#v, %v", result, err)
	}
}

func TestThreeSamplePreflightDetectsColleagueProcess(t *testing.T) {
	runner := &scriptedRunner{outputs: [][]byte{
		[]byte("GPU-1, NVIDIA GeForce RTX 4090, 580.95, 24564, 23900, 1\n"),
		[]byte("GPU-1, 4217\nGPU-2, 9000\n"),
	}}
	result, err := ThreeSamplePreflight(context.Background(), runner, "GPU-1", Profiles["rtx4090"], 0)
	if err != nil || result.Ready || result.Reason != "active_compute_process" {
		t.Fatalf("expected active process result, got %#v, %v", result, err)
	}
	if !reflect.DeepEqual(result.Samples[0].ComputePIDs, []int{4217}) {
		t.Fatalf("unexpected pids: %v", result.Samples[0].ComputePIDs)
	}
}

func TestRequiredFreeScratch(t *testing.T) {
	const gib = int64(1024 * 1024 * 1024)
	if got, want := RequiredFreeScratch(40*gib), 85*gib; got != want {
		t.Fatalf("got %d, want %d", got, want)
	}
}
