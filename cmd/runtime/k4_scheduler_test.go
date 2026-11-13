package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"durable-agent-execution-engine/internal/state"
)

func TestK4SchedulerAcknowledgment(t *testing.T) {
	for _, mode := range []string{"legacy", "drain"} {
		for _, flag := range []string{"", "0", "1"} {
			t.Run(mode+"/"+flag, func(t *testing.T) {
				t.Setenv("RUNTIME_SHUTDOWN_MODE", mode)
				t.Setenv("DUR053_K4_CAMPAIGN", flag)
				t.Setenv("RUNTIME_SCHEDULER_HOLD_AFTER_ACQUIRE_MS", "1")
				t.Setenv("POD_UID", "pod-uid")
				t.Setenv("CONTAINER_ID", "container-id")
				var output bytes.Buffer
				previous := slog.Default()
				slog.SetDefault(slog.New(slog.NewJSONHandler(&output, nil)))
				defer slog.SetDefault(previous)
				lease := state.Lease{PartitionID: 3, OwnerID: state.NewID(), Epoch: 7}
				options := schedulerOptions(nil)
				for i := 0; i < 2; i++ {
					if err := options.AfterLeaseAcquired(context.Background(), lease); err != nil {
						t.Fatal(err)
					}
				}
				acknowledgments := 0
				acquisitions := map[string]int{}
				for _, line := range strings.Split(strings.TrimSpace(output.String()), "\n") {
					var record map[string]any
					if err := json.Unmarshal([]byte(line), &record); err != nil {
						t.Fatal(err)
					}
					data, ok := record["data"].(string)
					if !ok {
						continue
					}
					acknowledgments++
					var ack map[string]any
					if err := json.Unmarshal([]byte(data), &ack); err != nil {
						t.Fatal(err)
					}
					acquisition, _ := ack["acquisition"].(string)
					if acquisition == "" {
						t.Fatal("K4 acquisition identity missing")
					}
					acquisitions[acquisition]++
					if ack["owner"] != lease.OwnerID || ack["epoch"] != float64(7) || ack["partition"] != float64(3) || ack["session"] != state.K4Session || ack["pod_uid"] != "pod-uid" || ack["container_id"] != "container-id" || ack["boundary"] == "" {
						t.Fatal("K4 acquisition acknowledgment identity missing", ack)
					}
				}
				if flag != "1" && acknowledgments != 0 {
					t.Fatal("K4 acknowledgment emitted with campaign flag off")
				}
				if flag == "1" && acknowledgments != 4 {
					t.Fatal("K4 per-acquisition acknowledgment missing")
				}
				if flag == "1" && len(acquisitions) != 2 {
					t.Fatal("K4 repeated acquire reused acknowledgment identity")
				}
			})
		}
	}
	t.Setenv("RUNTIME_SCHEDULER_HOLD_AFTER_ACQUIRE_MS", "0")
	t.Setenv("DUR053_K4_CAMPAIGN", "1")
	if schedulerOptions(nil).AfterLeaseAcquired != nil {
		t.Fatal("hold added when disabled")
	}
}
