package main

import (
	"durable-agent-execution-engine/internal/invariants"
	"fmt"
	"os"
)

func lifecycleMode(timelinePath, leasePath string) bool {
	var timeline invariants.LifecycleTimeline
	var leases invariants.LifecycleLeaseAudit
	for _, input := range []struct {
		path   string
		target any
	}{{timelinePath, &timeline}, {leasePath, &leases}} {
		data, err := os.ReadFile(input.path)
		if err != nil {
			fatal(err)
		}
		if err = invariants.DecodeLifecycle(data, input.target); err != nil {
			fatal(fmt.Errorf("lifecycle input: %w", err))
		}
	}
	result := invariants.CheckLifecycle(timeline, leases)
	write(result)
	return result.Valid
}
