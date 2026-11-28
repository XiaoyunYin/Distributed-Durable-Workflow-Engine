package transport

import (
	"context"
	"errors"
	"os"
	"testing"

	"durable-agent-execution-engine/internal/state"
)

const relayTestBatchSize = 10

func TestRelayUnsetModeLegacyEquivalence(t *testing.T) {
	for _, mode := range []string{"unset", "1"} {
		t.Run(mode, func(t *testing.T) {
			const env = "RUNTIME_RELAY_FULL_BATCH_LOOP"
			t.Setenv(env, "1")
			if mode == "unset" {
				if err := os.Unsetenv(env); err != nil {
					t.Fatal(err)
				}
			}
			relay := NewRelay(nil, nil, RelayConfig{BatchSize: relayTestBatchSize})
			passes := 0
			relay.runWakeup(context.Background(), func(context.Context) (RelayReport, error) {
				passes++
				if passes > 1 {
					t.Fatal("legacy wakeup ran another pass after a full batch")
				}
				return RelayReport{Claimed: relayTestBatchSize, Published: relayTestBatchSize}, nil
			})
			if relay.Config.FullBatchLoop || passes != 1 {
				t.Fatalf("full batch mode = %t, passes = %d", relay.Config.FullBatchLoop, passes)
			}
		})
	}
}

func TestRelayRepairedDrainsWithoutAnotherWakeup(t *testing.T) {
	const env = "RUNTIME_RELAY_FULL_BATCH_LOOP"
	t.Setenv(env, "0")
	if err := os.Unsetenv(env); err != nil {
		t.Fatal(err)
	}
	relay := NewRelay(nil, nil, RelayConfig{BatchSize: relayTestBatchSize, FullBatchLoop: true})
	remaining := 10*relayTestBatchSize + 3
	passes := 0
	relay.runWakeup(context.Background(), func(context.Context) (RelayReport, error) {
		passes++
		if passes > 11 {
			t.Fatal("short batch did not stop the loop")
		}
		claimed := min(remaining, relayTestBatchSize)
		remaining -= claimed
		return RelayReport{Claimed: claimed, Published: claimed}, nil
	})
	if remaining != 0 || passes != 11 {
		t.Fatalf("remaining rows = %d, passes = %d, want 0 and 11", remaining, passes)
	}
}

func TestRelayRepairedStopsAfterFailedWakeup(t *testing.T) {
	const backlog = 10 * relayTestBatchSize
	for _, tc := range []struct {
		name     string
		failures int
	}{
		{"all_publishes_fail", backlog},
		{"one_publish_fails", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			broker := &MemoryBroker{Failures: tc.failures}
			rows := make([]state.OutboxEvent, backlog)
			passes := 0
			relay := NewRelay(nil, broker, RelayConfig{BatchSize: relayTestBatchSize, FullBatchLoop: true})
			relay.runWakeup(context.Background(), func(ctx context.Context) (RelayReport, error) {
				passes++
				if passes > 1 {
					t.Fatal("failed wakeup claimed another batch")
				}
				report := RelayReport{Claimed: relayTestBatchSize}
				for i := range report.Claimed {
					rows[i].RelayAttempts++
					if err := broker.Publish(ctx, rows[i]); err != nil {
						report.Failed++
					} else {
						report.Published++
					}
				}
				if want := min(tc.failures, relayTestBatchSize); report.Failed != want {
					t.Fatalf("failed publishes = %d, want %d", report.Failed, want)
				}
				return report, nil
			})
			if passes != 1 || relay.ErrorCount() != 0 {
				t.Fatalf("passes = %d, pass errors = %d", passes, relay.ErrorCount())
			}
			for i, row := range rows {
				want := 0
				if i < relayTestBatchSize {
					want = 1
				}
				if row.RelayAttempts != want {
					t.Fatalf("row %d attempts = %d, want %d", i, row.RelayAttempts, want)
				}
			}
		})
	}
}

func TestRelayRepairedStopsOnPassError(t *testing.T) {
	passErr := errors.New("injected pass error")
	var reported []error
	relay := NewRelay(nil, nil, RelayConfig{BatchSize: relayTestBatchSize, FullBatchLoop: true,
		OnError: func(err error) { reported = append(reported, err) }})
	passes := 0
	relay.runWakeup(context.Background(), func(context.Context) (RelayReport, error) {
		passes++
		if passes > 1 {
			t.Fatal("wakeup continued after an error with a full claim")
		}
		return RelayReport{Claimed: relayTestBatchSize, Published: 3}, passErr
	})
	if relay.ErrorCount() != 1 || len(reported) != 1 || reported[0] != passErr {
		t.Fatalf("error count = %d, reported = %v", relay.ErrorCount(), reported)
	}
}

func TestRelayRepairedChecksShutdownBetweenPasses(t *testing.T) {
	for _, signal := range []string{"cancel", "drain"} {
		t.Run(signal, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			drain := make(chan struct{})
			relay := NewRelay(nil, nil, RelayConfig{BatchSize: relayTestBatchSize, Drain: drain, FullBatchLoop: true})
			passes := 0
			relay.runWakeup(ctx, func(context.Context) (RelayReport, error) {
				passes++
				if passes > 1 {
					t.Fatal("wakeup continued after shutdown signal")
				}
				if signal == "cancel" {
					cancel()
				} else {
					close(drain)
				}
				return RelayReport{Claimed: relayTestBatchSize, Published: relayTestBatchSize}, nil
			})
			relay.runWakeup(ctx, func(context.Context) (RelayReport, error) {
				t.Fatal("shutdown allowed another wakeup to start a pass")
				return RelayReport{}, nil
			})
			if passes != 1 {
				t.Fatalf("passes = %d, want 1", passes)
			}
		})
	}
}
