package sandbox

import (
	"path/filepath"
	"slices"
	"testing"

	"github.com/google/uuid"
)

func TestPolicyBuildsFixedDockerInvocation(t *testing.T) {
	digest := "registry.internal/chemistry@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	policy := Policy{AttemptRoot: t.TempDir(), AllowedImageDigest: digest, AllowedGPUUUIDs: map[string]bool{"GPU-1": true}, SeccompProfile: "/etc/seccomp.json", AppArmorProfile: "library-prep-chemistry"}
	req := RunRequest{TaskID: uuid.New(), AttemptID: uuid.New(), GPUUUID: "GPU-1", ResourceProfile: "rtx4090", ImageDigest: digest, MaxOutputBytes: 1 << 30, MaxScratchBytes: 1 << 30}
	run, err := policy.Validate(req)
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Base(run.Root) != req.AttemptID.String() {
		t.Fatalf("unexpected attempt root %s", run.Root)
	}
	args := policy.DockerArgs(run)
	for _, required := range []string{"--network", "none", "--read-only", "--cap-drop", "ALL", "device=GPU-1", digest} {
		if !slices.Contains(args, required) {
			t.Fatalf("docker args do not contain %q: %v", required, args)
		}
	}
}

func TestPolicyRejectsArbitraryImage(t *testing.T) {
	policy := Policy{AttemptRoot: t.TempDir(), AllowedImageDigest: "safe@sha256:abc", AllowedGPUUUIDs: map[string]bool{"GPU-1": true}}
	_, err := policy.Validate(RunRequest{TaskID: uuid.New(), AttemptID: uuid.New(), GPUUUID: "GPU-1", ResourceProfile: "rtx4090", ImageDigest: "evil:latest"})
	if err == nil {
		t.Fatal("expected arbitrary image to be rejected")
	}
}
