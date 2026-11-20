//go:build dur053_k6

package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/state"
)

func TestK6SchedulerRenewalDefersFencedReleaseAfterTakeover(t *testing.T) {
	ctx, store := openM1Database(t)
	defer store.Close()
	definition := "k6-partition-renewal-" + state.NewID()
	workflow, initial := createM1Workflow(t, ctx, store, definition,
		`{"entry":"root","nodes":[{"id":"root","kind":"success"}]}`,
		`{"root":"PURE_ACTIVITY"}`)
	target := New(store, nil)
	target.ExternalActivities = true
	target.OwnerID = initial.OwnerID
	target.LeaseTTL = 1200 * time.Millisecond
	target.schedulerContext = ctx
	iteration, cancelIteration := context.WithTimeout(ctx, schedulerIterationTimeout)
	defer cancelIteration()
	peer := state.NewID()
	var successor state.Lease
	defer func() {
		if successor.Epoch != 0 {
			_ = store.ReleaseLease(ctx, state.LeaseRef{PartitionID: successor.PartitionID, OwnerID: peer, Epoch: successor.Epoch})
		}
		_ = store.ReleaseLease(ctx, state.LeaseRef{PartitionID: initial.PartitionID, OwnerID: initial.OwnerID, Epoch: initial.Epoch})
		_, _ = store.Pool().Exec(ctx, `DELETE FROM engine.workflow_executions WHERE workflow_id = $1`, workflow)
		_, _ = store.Pool().Exec(ctx, `DELETE FROM engine.workflow_definitions WHERE definition_id = $1`, definition)
	}()
	var held, released state.LeaseRef
	var releaseErr error
	target.leaseTakeover.Renew = func(iteration, scheduler context.Context, store *state.Store, wf state.Workflow, ref state.LeaseRef, ttl time.Duration) (state.Lease, error) {
		if wf.WorkflowID != workflow || scheduler != ctx {
			t.Fatalf("unexpected renewal scope: workflow=%q", wf.WorkflowID)
		}
		held = ref
		resumed, cancelResumed := context.WithTimeout(scheduler, 7*time.Second)
		defer cancelResumed()
		hold := state.WithK6RenewalHold(iteration, 500*time.Millisecond, func(point state.K6RenewalPoint) (context.Context, error) {
			cancelIteration() // Simulate the pass deadline expiring while the original reference is held.
			for {
				lease, won, err := store.AcquireLease(resumed, point.Lease.PartitionID, peer, time.Minute)
				if err != nil {
					return nil, err
				}
				if won {
					successor = lease
					return resumed, nil
				}
				time.Sleep(25 * time.Millisecond)
			}
		})
		return store.RenewLease(hold, ref, ttl)
	}
	target.leaseTakeover.AfterRelease = func(ref state.LeaseRef, err error) {
		released, releaseErr = ref, err
	}
	_, err := target.Run(iteration, workflow)
	if !errors.Is(err, state.ErrK6RenewalSessionLost) {
		t.Fatalf("scheduler did not resume its original renewal session: %v", err)
	}
	if successor.Epoch != held.Epoch+1 || released != held || !errors.Is(releaseErr, state.ErrLeaseNotOwned) {
		t.Fatalf("deferred release=%+v err=%v, held=%+v successor=%+v", released, releaseErr, held, successor)
	}
	var owner string
	var epoch int64
	if err := store.Pool().QueryRow(ctx, `SELECT owner_id::text, epoch FROM engine.partition_leases WHERE partition_id = $1`, held.PartitionID).Scan(&owner, &epoch); err != nil || owner != peer || epoch != successor.Epoch {
		t.Fatalf("fenced release changed committed takeover: owner=%s epoch=%d err=%v", owner, epoch, err)
	}
}
