package api

import "testing"

func TestWorkerIdentityAllowed(t *testing.T) {
	tests := []struct {
		name          string
		identity      string
		requestedName string
		allowedName   string
		want          bool
	}{
		{name: "approved single host", identity: "mscoc6", requestedName: "mscoc6", allowedName: "mscoc6", want: true},
		{name: "different host denied", identity: "worker-01", requestedName: "worker-01", allowedName: "mscoc6", want: false},
		{name: "request cannot rename certificate", identity: "mscoc6", requestedName: "worker-01", allowedName: "mscoc6", want: false},
		{name: "legacy worker prefix remains compatible without allowlist", identity: "worker-01", requestedName: "worker-01", want: true},
		{name: "unscoped name denied without allowlist", identity: "mscoc6", requestedName: "mscoc6", want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := workerIdentityAllowed(test.identity, test.requestedName, test.allowedName); got != test.want {
				t.Fatalf("workerIdentityAllowed() = %v, want %v", got, test.want)
			}
		})
	}
}
