package main

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestRuntimeRelayConfigFromEnvironment(t *testing.T) {
	for _, tc := range []struct {
		name, relay, timings string
		want                 runtimeRelayConfig
	}{
		{name: "unset"},
		{name: "literal_zero", relay: "0", timings: "0"},
		{name: "instrumented_legacy_arm", relay: "0", timings: "1", want: runtimeRelayConfig{stageTimings: true}},
		{name: "instrumented_repaired_arm", relay: "1", timings: "1", want: runtimeRelayConfig{true, true}},
		{name: "repair_without_timings", relay: "1", timings: "0", want: runtimeRelayConfig{fullBatchLoop: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(relayFullBatchLoopEnv, tc.relay)
			t.Setenv(j9aStageTimingsEnv, tc.timings)
			if tc.name == "unset" {
				for _, name := range []string{relayFullBatchLoopEnv, j9aStageTimingsEnv} {
					if err := os.Unsetenv(name); err != nil {
						t.Fatal(err)
					}
				}
			}
			if got, err := runtimeRelayConfigFromEnvironment(); err != nil || got != tc.want {
				t.Fatalf("runtime relay config = %+v, err = %v, want %+v", got, err, tc.want)
			}
		})
	}
}

func TestLifecycleRejectsInvalidRelaySwitchValues(t *testing.T) {
	for _, name := range []string{relayFullBatchLoopEnv, j9aStageTimingsEnv} {
		for _, value := range []string{"", "true", "yes", " 1", "2"} {
			t.Run(name+"/"+value, func(t *testing.T) {
				t.Setenv(relayFullBatchLoopEnv, "0")
				t.Setenv(j9aStageTimingsEnv, "0")
				t.Setenv(name, value)
				if err := serveLifecycle(context.Background()); err == nil ||
					!strings.Contains(err.Error(), name+" must be 0 or 1 when set") {
					t.Fatalf("startup error = %v for %s=%q", err, name, value)
				}
			})
		}
	}
}

func TestRuntimeRejectsInvalidRelaySwitchInBothLifecycles(t *testing.T) {
	const childMarker = "DURABLE_J9A_STARTUP_CHILD"
	if os.Getenv(childMarker) == "1" {
		os.Args = []string{"runtime", "--addr", "127.0.0.1:0", "--roles", "relay"}
		main()
		return
	}
	for _, lifecycle := range []string{"drain", "legacy"} {
		t.Run(lifecycle, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			exe, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			cmd := exec.CommandContext(ctx, exe, "-test.run=^TestRuntimeRejectsInvalidRelaySwitchInBothLifecycles$")
			cmd.Env = append(cmd.Environ(), childMarker+"=1", "RUNTIME_SHUTDOWN_MODE="+lifecycle, relayFullBatchLoopEnv+"=bad")
			output, err := cmd.CombinedOutput()
			if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 1 ||
				!strings.Contains(string(output), relayFullBatchLoopEnv+" must be 0 or 1 when set") {
				t.Fatalf("startup exit = %v, output = %s", err, output)
			}
		})
	}
}
