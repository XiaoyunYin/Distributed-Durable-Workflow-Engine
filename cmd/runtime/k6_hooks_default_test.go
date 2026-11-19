//go:build !dur053_k6

package main

import (
	"strings"
	"testing"

	"durable-agent-execution-engine/internal/engine"
)

func TestOrdinaryRuntimeHasNoDeployedHooks(t *testing.T) {
	t.Setenv("DUR053_K6_DEPLOYED_HOOKS", "1")
	t.Setenv("DUR053_K6_HOOK_URL", "https://127.0.0.1:1")
	t.Setenv("DUR053_K6_HOOK_CA_FILE", "unreachable")
	t.Setenv("DUR053_K6_HOOK_TOKEN", strings.Repeat("a", 64))
	t.Setenv("DUR053_K6_HOOK_WORKLOAD", "{}")
	t.Setenv("POD_UID", "ordinary-runtime")
	var options engine.ServeOptions
	if err := configureK6SchedulerHook(&options); err != nil || options.BeforeResultConsumption != nil {
		t.Fatal("ordinary runtime enabled the deployed scheduler hook")
	}
	if hook, err := k6RelayHook("ordinary"); err != nil || hook != nil {
		t.Fatal("ordinary runtime enabled the deployed relay hooks")
	}
}
