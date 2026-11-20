//go:build dur053_k6

package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"durable-agent-execution-engine/internal/engine"
	"durable-agent-execution-engine/internal/k4observe"
	"durable-agent-execution-engine/internal/state"
)

func registerK6Clock(mux *http.ServeMux, role string) {
	if role != "scheduler" || os.Getenv("DUR053_K6_CLOCK") != "1" {
		return
	}
	mux.HandleFunc("GET /internal/k6/clock", func(w http.ResponseWriter, _ *http.Request) {
		start, err := k4observe.ProcessStartID()
		boot, bootErr := os.ReadFile("/proc/sys/kernel/random/boot_id")
		if err != nil || bootErr != nil {
			http.Error(w, "K6_CLOCK_PROCESS_IDENTITY", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"wall_ns": time.Now().UnixNano(), "pid": os.Getpid(), "process_start_id": start,
			"boot_id": strings.TrimSpace(string(boot)), "pod_uid": os.Getenv("POD_UID"),
			"role": role, "session": state.K4Session,
		})
	})
}

// Hold a single real scheduler lease while the observer brackets TimeChaos.
// All renewals and expiry checks still go through Store.RenewLease.
func configureK6ClockLease(ctx context.Context, options *engine.ServeOptions) {
	workflow := os.Getenv("DUR053_K6_CLOCK_WORKFLOW")
	control := os.Getenv("DUR053_K6_CLOCK_CONTROL")
	if os.Getenv("DUR053_K6_CLOCK") != "1" || workflow == "" || control == "" {
		return
	}
	options.LeaseTakeover.Acquire = func(iteration context.Context, store *state.Store, wf state.Workflow, owner string, ttl time.Duration) (state.Lease, bool, func() (bool, error), error) {
		lease, acquired, err := store.AcquireLease(iteration, wf.PartitionID, owner, ttl)
		if err != nil || !acquired || wf.WorkflowID != workflow {
			return lease, acquired, nil, err
		}
		_, armed := os.Stat(filepath.Join(control, "lease-arm"))
		_, released := os.Stat(filepath.Join(control, "lease-release"))
		if armed != nil || !errors.Is(released, os.ErrNotExist) {
			return lease, true, nil, nil
		}
		return lease, true, func() (bool, error) {
			ref := state.LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch}
			return holdK6ClockLease(ctx, filepath.Join(control, "lease-release"), store, ref, ttl)
		}, nil
	}
}

func holdK6ClockLease(ctx context.Context, release string, store *state.Store, ref state.LeaseRef, ttl time.Duration) (bool, error) {
	hold, stop := context.WithTimeout(ctx, time.Minute)
	defer stop()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(release); err == nil {
			// Engine.Run establishes a fresh iteration context on release.
			return true, nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return false, err
		}
		op, cancel := context.WithTimeout(hold, 2*time.Second)
		_, err := store.RenewLease(op, ref, ttl)
		cancel()
		if err != nil {
			return false, err
		}
		select {
		case <-ticker.C:
		case <-hold.Done():
			return false, hold.Err()
		}
	}
}
