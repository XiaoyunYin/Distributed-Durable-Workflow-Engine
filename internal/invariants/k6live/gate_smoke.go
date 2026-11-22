//go:build k6g2c_smoke

package k6live

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"

	"durable-agent-execution-engine/internal/invariants/k6v1"
)

// Phase G uses a separately built checker to pause at two observer boundaries.
// The acknowledgement releases the observer. It supplies no evidence to it.
func smokeGate() observationGate {
	dir := os.Getenv("DUR053_K6_G_OBSERVATION_GATE")
	if dir == "" {
		return nil
	}
	return func(ctx context.Context, phase string, sample k6v1.QuietSample) error {
		path := filepath.Join(dir, phase)
		raw, err := k6v1.Canonical(sample)
		if err != nil || os.WriteFile(path+".pending", raw, 0o644) != nil || os.Rename(path+".pending", path+".json") != nil {
			return errors.New("K6_OBSERVATION_GATE_SIGNAL")
		}
		ctx, cancel := context.WithTimeout(ctx, 4*time.Second)
		defer cancel()
		for {
			if _, err := os.Stat(path + ".ack"); err == nil {
				return nil
			}
			if !waitPoll(ctx, 25*time.Millisecond) {
				return errors.New("K6_OBSERVATION_GATE_TIMEOUT")
			}
		}
	}
}
