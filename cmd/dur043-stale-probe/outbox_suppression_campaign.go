//go:build dur034_ablation

package main

import (
	"context"

	"durable-agent-execution-engine/internal/state"
)

// The AWS stale-epoch diagnostic is built with the existing campaign-only
// ablation tag so its disposable negative-control workflow emits no transport
// event. The workflow itself is retained and terminalized, not deleted.
func campaignProbeContext(ctx context.Context) context.Context {
	return state.WithTestSafeguardProfile(ctx, state.TestSafeguardProfile{
		Name:          "dur049-same-owner-epoch-probe",
		DisableOutbox: true,
	})
}

func campaignOutboxSuppressionEnabled() bool { return true }
