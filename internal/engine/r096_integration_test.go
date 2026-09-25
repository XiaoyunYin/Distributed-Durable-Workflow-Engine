package engine

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/partition"
	"durable-agent-execution-engine/internal/state"
)

func TestM2SchedulerContinuesAfterContendedPartition(t *testing.T) {
	databaseURL := m1DatabaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	store, err := state.NewFromURL(ctx, databaseURL)
	if err != nil {
		if os.Getenv("DURABLE_REQUIRE_DATABASE") == "1" {
			t.Fatalf("PostgreSQL is required but unavailable: %v", err)
		}
		t.Skipf("PostgreSQL is not available: %v", err)
	}
	defer store.Close()

	namespace := "r096-scheduler-" + state.NewID()
	definitionID := "r096-scheduler-def-" + state.NewID()
	if err := store.CreateDefinition(ctx, state.DefinitionInput{
		DefinitionID:     definitionID,
		Version:          1,
		DefinitionHash:   definitionID,
		Graph:            []byte(`{"entry":"root","nodes":[{"id":"root","kind":"success"}]}`),
		ActivityVersions: []byte(`{}`),
		EffectClasses:    []byte(`{"root":"PURE_ACTIVITY"}`),
	}); err != nil {
		t.Fatal(err)
	}
	blockedID, blockedPartition, freeID, freePartition := r096WorkflowPair(t, ctx, store)
	for _, item := range []struct {
		id        string
		partition int16
	}{
		{blockedID, blockedPartition},
		{freeID, freePartition},
	} {
		if _, err := store.CreateWorkflow(ctx, state.CreateWorkflowInput{
			WorkflowID:            item.id,
			Namespace:             namespace,
			SubmissionKey:         item.id,
			SubmissionPayloadHash: "sub-v1:" + item.id,
			DefinitionID:          definitionID,
			DefinitionVersion:     1,
			PartitionID:           item.partition,
			InitialNodeID:         "root",
			InitialInput:          []byte(`{}`),
			ActorID:               "r096-test",
		}); err != nil {
			t.Fatal(err)
		}
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = store.Pool().Exec(cleanupCtx, `DELETE FROM engine.workflow_executions WHERE namespace = $1`, namespace)
		_, _ = store.Pool().Exec(cleanupCtx, `DELETE FROM engine.workflow_definitions WHERE definition_id = $1`, definitionID)
	}()

	transaction, err := store.Pool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = transaction.Rollback(context.Background()) }()
	var heldEpoch int64
	if err := transaction.QueryRow(ctx, `SELECT epoch FROM engine.partition_leases
		WHERE partition_id = $1 FOR UPDATE`, blockedPartition).Scan(&heldEpoch); err != nil {
		t.Fatal(err)
	}

	serveCtx, serveCancel := context.WithCancel(ctx)
	defer serveCancel()
	serveDone := make(chan error, 1)
	go func() { serveDone <- Serve(serveCtx, store, namespace, make(chan struct{})) }()

	// The scan can first spend one bounded iteration on the locked partition,
	// then a second iteration completing this workflow. Keep the test budget
	// below the 15-second lease TTL while allowing both passes plus CI margin.
	deadline := time.NewTimer(2*schedulerIterationTimeout + 2*time.Second)
	defer deadline.Stop()
	for {
		workflow, err := store.GetWorkflow(ctx, freeID)
		if err != nil {
			t.Fatal(err)
		}
		if workflow.State == state.StateSucceeded {
			break
		}
		select {
		case <-deadline.C:
			t.Fatalf("scheduler did not service free partition %d while partition %d lock was held; blocked epoch=%d", freePartition, blockedPartition, heldEpoch)
		case <-time.After(50 * time.Millisecond):
		}
	}

	blocked, err := store.GetWorkflow(ctx, blockedID)
	if err != nil {
		t.Fatal(err)
	}
	if blocked.State == state.StateSucceeded {
		t.Fatalf("blocked workflow completed while its partition lock was held: %+v", blocked)
	}
	serveCancel()
	select {
	case serveErr := <-serveDone:
		if serveErr != nil && serveErr != context.Canceled {
			t.Fatalf("scheduler stopped with error: %v", serveErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("scheduler did not stop after cancellation")
	}
}

func r096WorkflowPair(t *testing.T, ctx context.Context, store *state.Store) (blockedID string, blockedPartition int16, freeID string, freePartition int16) {
	t.Helper()
	rows, err := store.Pool().Query(ctx, `SELECT partition_id FROM engine.partition_leases
		WHERE owner_id IS NULL OR lease_expires_at <= clock_timestamp()`)
	if err != nil {
		t.Fatalf("find unleased fixture partitions: %v", err)
	}
	available := map[int16]struct{}{}
	for rows.Next() {
		var partitionID int16
		if err := rows.Scan(&partitionID); err != nil {
			rows.Close()
			t.Fatalf("scan unleased fixture partition: %v", err)
		}
		available[partitionID] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		t.Fatalf("read unleased fixture partitions: %v", err)
	}
	rows.Close()
	if len(available) < 2 {
		t.Fatalf("need two unleased partitions for the R096 fixture; found %d", len(available))
	}
	for i := 0; i < 10000; i++ {
		blockedID = fmt.Sprintf("r096-blocked-%04d-%s", i, state.NewID())
		freeID = fmt.Sprintf("r096-free-%04d-%s", i, state.NewID())
		blockedMapped, err := partition.ID(blockedID)
		if err != nil {
			t.Fatal(err)
		}
		freeMapped, err := partition.ID(freeID)
		if err != nil {
			t.Fatal(err)
		}
		_, blockedAvailable := available[int16(blockedMapped)]
		_, freeAvailable := available[int16(freeMapped)]
		if blockedMapped != freeMapped && blockedAvailable && freeAvailable {
			return blockedID, int16(blockedMapped), freeID, int16(freeMapped)
		}
	}
	t.Fatal("could not find two workflow IDs on distinct partitions")
	return "", 0, "", 0
}
