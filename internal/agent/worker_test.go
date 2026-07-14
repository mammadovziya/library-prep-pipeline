package agent

import (
	"os"
	"path/filepath"
	"testing"
)

func TestValidateArtifactFile(t *testing.T) {
	root := t.TempDir()
	regular := filepath.Join(root, "result.sdf")
	if err := os.WriteFile(regular, []byte("record"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := validateArtifactFile(root, regular); err != nil {
		t.Fatalf("regular artifact rejected: %v", err)
	}

	outside := filepath.Join(t.TempDir(), "worker-secret")
	if err := os.WriteFile(outside, []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	symlink := filepath.Join(root, "result-link")
	if err := os.Symlink(outside, symlink); err != nil {
		t.Skipf("symlink creation is unavailable: %v", err)
	}
	if err := validateArtifactFile(root, symlink); err == nil {
		t.Fatal("artifact symlink outside the attempt root was accepted")
	}
}
