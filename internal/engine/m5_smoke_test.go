package engine

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/state"
	"durable-agent-execution-engine/internal/telemetry"
)

// TestM5BoundedTwoSchedulerSmoke is a small service-backed recovery/throughput
// smoke, not a performance benchmark. It exercises two independent scheduler
// owners concurrently against four durable workflows on disjoint partitions.
func TestM5BoundedTwoSchedulerSmoke(t *testing.T) {
	ctx, store := openM1Database(t)
	defer store.Close()
	metrics := telemetry.New("m5-smoke")
	store.SetTelemetry(metrics)
	// Keep the readiness timestamp distinct from the second-resolution process
	// start timestamp even on a fast local database.
	time.Sleep(1100 * time.Millisecond)
	metrics.MarkDurableReady()

	type fixture struct {
		workflowID string
		definition string
		lease      state.Lease
	}
	fixtures := make([]fixture, 0, 4)
	for i := 0; i < 4; i++ {
		definitionID := fmt.Sprintf("dur025-smoke-%d-%s", i, state.NewID())
		workflowID, lease := createM1Workflow(t, ctx, store, definitionID,
			`{"entry":"root","nodes":[{"id":"root","kind":"activity","next":"done"},{"id":"done","kind":"success"}]}`,
			`{"root":"PURE_ACTIVITY"}`)
		fixtures = append(fixtures, fixture{workflowID: workflowID, definition: definitionID, lease: lease})
	}
	defer func() {
		for _, item := range fixtures {
			_, _ = store.Pool().Exec(ctx, `DELETE FROM engine.workflow_executions WHERE workflow_id = $1`, item.workflowID)
			_, _ = store.Pool().Exec(ctx, `DELETE FROM engine.workflow_definitions WHERE definition_id = $1`, item.definition)
			_ = store.ReleaseLease(ctx, state.LeaseRef{PartitionID: item.lease.PartitionID, OwnerID: item.lease.OwnerID, Epoch: item.lease.Epoch})
		}
	}()

	seenPartitions := map[int16]bool{}
	for _, item := range fixtures {
		seenPartitions[item.lease.PartitionID] = true
		if err := store.ReleaseLease(ctx, state.LeaseRef{PartitionID: item.lease.PartitionID, OwnerID: item.lease.OwnerID, Epoch: item.lease.Epoch}); err != nil {
			t.Fatalf("release fixture lease for partition %d: %v", item.lease.PartitionID, err)
		}
	}
	if len(seenPartitions) != len(fixtures) {
		t.Fatalf("smoke fixtures share partitions: %v", seenPartitions)
	}

	driver := ActivityDriverFunc(func(context.Context, Activity) (ActivityResult, error) {
		return ActivityResult{Payload: []byte(`{"ok":true}`)}, nil
	})
	engines := []*Engine{
		New(store, driver),
		New(store, driver),
	}
	engines[0].OwnerID = state.NewID()
	engines[1].OwnerID = state.NewID()
	start := time.Now()
	results := make([]RunResult, len(fixtures))
	errors := make([]error, len(fixtures))
	var group sync.WaitGroup
	for i, item := range fixtures {
		group.Add(1)
		go func(index int, workflowID string) {
			defer group.Done()
			results[index], errors[index] = engines[index%len(engines)].Run(ctx, workflowID)
		}(i, item.workflowID)
	}
	group.Wait()

	for i, result := range results {
		if errors[i] != nil {
			t.Fatalf("workflow %d run failed: %v", i, errors[i])
		}
		if result.Blocked || result.Workflow.State != state.StateSucceeded {
			t.Fatalf("workflow %d result = %+v, want succeeded", i, result)
		}
	}
	snapshot := metrics.Snapshot()
	if snapshot.DurableReadyUnix <= snapshot.StartedUnix || snapshot.LeaseAcquires == 0 ||
		snapshot.AcceptedClaims == 0 || snapshot.AcceptedResults == 0 || snapshot.DBQueries == 0 {
		t.Fatalf("engine telemetry did not observe the smoke: %+v", snapshot)
	}
	t.Logf("two-scheduler smoke completed %d workflows in %s; preliminary only", len(fixtures), time.Since(start).Round(time.Millisecond))
}
