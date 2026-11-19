//go:build !dur053_k6

package main

import (
	"strings"
	"testing"

	"durable-agent-execution-engine/internal/engine"
)

func TestOrdinaryRuntimeHasNoLeaseTakeoverHooks(t *testing.T) {
	t.Setenv("DUR053_K4_CAMPAIGN", "1")
	t.Setenv("DUR053_K6_DEPLOYED_HOOKS", "1")
	t.Setenv("DUR053_K6_HOOK_URL", "https://127.0.0.1:1")
	t.Setenv("DUR053_K6_HOOK_CA_FILE", "unreachable")
	t.Setenv("DUR053_K6_HOOK_TOKEN", strings.Repeat("a", 64))
	t.Setenv("DUR053_K6_HOOK_WORKLOAD", "{}")
	t.Setenv("DUR053_K6_LEASE_TAKEOVER_WORKFLOW", "e2-hook-5")
	t.Setenv("POD_UID", "ordinary-runtime")
	t.Setenv("CONTAINER_ID", "ordinary-container")
	for _, role := range []string{"predecessor", "successor"} {
		t.Setenv("DUR053_K6_LEASE_TAKEOVER_ROLE", role)
		var options engine.ServeOptions
		if err := configureK6SchedulerHook(&options); err != nil ||
			options.LeaseTakeover.CreateAttempt != nil || options.LeaseTakeover.Acquire != nil {
			t.Fatalf("ordinary runtime enabled lease takeover: role=%s error=%v", role, err)
		}
	}
}
