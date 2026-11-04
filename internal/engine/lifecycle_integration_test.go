package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/state"
)

func TestK1LeaseHookReleaseAndLegacyParity(t *testing.T) {
	ctx, store := openM1Database(t)
	defer store.Close()
	for _, legacy := range []bool{false, true} {
		for _, canceled := range []bool{false, true} {
			name := "k1-hook-" + state.NewID()
			workflowID, lease := createM1Workflow(t, ctx, store, name,
				`{"entry":"root","nodes":[{"id":"root","kind":"success"}]}`, `{"root":"PURE_ACTIVITY"}`)
			func() {
				defer func() {
					_, _ = store.Pool().Exec(ctx, `DELETE FROM engine.workflow_executions WHERE workflow_id=$1`, workflowID)
					_, _ = store.Pool().Exec(ctx, `DELETE FROM engine.workflow_definitions WHERE definition_id=$1`, name)
					_ = store.ReleaseLease(ctx, state.LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch})
				}()
				runCtx, cancel := context.WithCancel(ctx)
				defer cancel()
				runner := New(store, nil)
				runner.ExternalActivities = true
				runner.OwnerID = lease.OwnerID
				runner.LegacyShutdown = legacy
				runner.AfterLeaseAcquired = func(context.Context, state.Lease) error {
					if canceled {
						cancel()
						return runCtx.Err()
					}
					return errors.New("hook failure")
				}
				if _, err := runner.Run(runCtx, workflowID); err == nil {
					t.Fatal("hook did not stop runner")
				}
				// Independent oracle: a different owner must actually commit acquisition.
				start := time.Now()
				competitor, acquired, err := store.AcquireLease(ctx, lease.PartitionID, state.NewID(), time.Second)
				if err != nil {
					t.Fatal(err)
				}
				if acquired == legacy {
					t.Fatalf("hook lease release = %v, legacy=%v (pre-K1 leaves hook lease to TTL)", acquired, legacy)
				}
				if acquired {
					defer store.ReleaseLease(ctx, state.LeaseRef{PartitionID: competitor.PartitionID, OwnerID: competitor.OwnerID, Epoch: competitor.Epoch})
					if time.Since(start) > time.Second || competitor.Epoch <= lease.Epoch {
						t.Fatal("competing acquisition was not prompt and fenced")
					}
				}
			}()
		}
	}
}

func TestK1SchedulerDrainBeforeScan(t *testing.T) {
	ctx, store := openM1Database(t)
	defer store.Close()
	drain := make(chan struct{})
	close(drain)
	runCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if err := ServeWithOptions(runCtx, store, "k1-no-new-pass", nil, ServeOptions{Drain: drain}); err != nil {
		t.Fatalf("draining scheduler started another scan: %v", err)
	}
}
