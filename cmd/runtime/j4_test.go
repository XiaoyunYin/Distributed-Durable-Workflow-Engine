package main

import (
	"context"
	"os"
	"strings"
	"testing"

	"durable-agent-execution-engine/internal/api"
	"durable-agent-execution-engine/internal/state"
)

func TestJ4ServeRejectsInvalidModesBeforeBootstrapping(t *testing.T) {
	for _, tc := range []struct{ activity, resilience, want string }{
		{"true", "0", state.RecordedLLMMode},
		{"1", "true", state.ProviderResilienceMode},
		{"0", "1", state.ProviderResilienceMode + " requires"},
	} {
		t.Run(tc.activity+"-"+tc.resilience, func(t *testing.T) {
			t.Setenv(state.RecordedLLMMode, tc.activity)
			t.Setenv(state.ProviderResilienceMode, tc.resilience)
			err := serveLifecycle(context.Background())
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("startup error = %v, want %s", err, tc.want)
			}
		})
	}
}

func TestJ4StartupRequiresPrivateWorkerListener(t *testing.T) {
	t.Setenv(state.RecordedLLMMode, "1")
	t.Setenv(state.ProviderResilienceMode, "1")
	if validateJ4Listener(api.RoutesPublic, "", false) == nil ||
		validateJ4Listener(api.RoutesCombined, "127.0.0.1:8083", false) == nil {
		t.Fatal("enabled J4 B2 accepted an invalid listener")
	}
	if err := validateJ4Listener(api.RoutesPublic, "127.0.0.1:8083", true); err != nil {
		t.Fatal(err)
	}
	if err := os.Unsetenv(state.ProviderResilienceMode); err != nil {
		t.Fatal(err)
	}
	if validateJ4Listener(api.RoutesPublic, "127.0.0.1:8083", true) == nil {
		t.Fatal("unset B2 accepted the fixed-delay control")
	}
	t.Setenv(state.RecordedLLMMode, "0")
	if err := validateJ4Listener(api.RoutesCombined, "", false); err != nil {
		t.Fatal(err)
	}
}
