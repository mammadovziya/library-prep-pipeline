//go:build !linux

package agent

import "math"

func freeDiskBytes(string) (int64, error) { return math.MaxInt64, nil }
