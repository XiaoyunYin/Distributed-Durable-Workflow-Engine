package state

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/partition"
)

func TestRuntimeWakeupAcknowledgmentRequiresCurrentLease(t *testing.T) {
	databaseURL := integrationDatabaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	store, err := NewFromURL(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open PostgreSQL: %v", err)
	}
	defer store.Close()
	initial, acquired, err := acquireTestLease(ctx, store, NewID())
	if err != nil || !acquired {
		t.Fatalf("initial lease = %+v acquired=%v err=%v", initial, acquired, err)
	}
	current := LeaseRef{PartitionID: initial.PartitionID, OwnerID: initial.OwnerID, Epoch: initial.Epoch}
	stale := current
	definitionID, consumerID := "r091-"+NewID(), "r091-"+NewID()
	workflowID := ""
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		// Scope every cleanup to this fixture, including the current lease owner.
		for _, statement := range []struct{ query, id string }{
			{`DELETE FROM engine.workflow_executions WHERE workflow_id = $1`, workflowID},
			{`DELETE FROM engine.workflow_definitions WHERE definition_id = $1`, definitionID},
			{`DELETE FROM engine.consumer_offsets WHERE consumer_id = $1`, consumerID},
		} {
			if _, err := store.Pool().Exec(cleanupCtx, statement.query, statement.id); err != nil {
				t.Errorf("cleanup: %v", err)
			}
		}
		if err := store.ReleaseLease(cleanupCtx, current); err != nil && !errors.Is(err, ErrLeaseNotOwned) {
			t.Errorf("release fixture lease: %v", err)
		}
	}()
	for i := 0; i < 1000; i++ {
		candidate := "r091-" + NewID()
		mapped, err := partition.ID(candidate)
		if err != nil {
			t.Fatal(err)
		}
		if int16(mapped) == current.PartitionID {
			workflowID = candidate
			break
		}
	}
	if workflowID == "" {
		t.Fatal("could not generate a workflow for the fixture partition")
	}
	if err := store.CreateDefinition(ctx, DefinitionInput{DefinitionID: definitionID, Version: 1,
		DefinitionHash: definitionID, Graph: []byte(`{"entry":"root","nodes":[{"id":"root","kind":"success"}]}`),
		ActivityVersions: []byte(`{"root":"1"}`), EffectClasses: []byte(`{"root":"PURE_ACTIVITY"}`)}); err != nil {
		t.Fatal(err)
	}
	created, err := store.CreateWorkflow(ctx, CreateWorkflowInput{WorkflowID: workflowID,
		Namespace: "r091", SubmissionKey: workflowID, SubmissionPayloadHash: "sub-v1:" + workflowID,
		DefinitionID: definitionID, DefinitionVersion: 1, PartitionID: current.PartitionID,
		InitialNodeID: "root", InitialInput: []byte(`{}`), ActorID: "r091-test"})
	if err != nil {
		t.Fatal(err)
	}
	events, err := store.ListOutbox(ctx, workflowID, 10)
	if err != nil || len(events) != 1 {
		t.Fatalf("creation outbox = %+v err=%v", events, err)
	}
	event := events[0]
	inbox, err := store.RecordInbox(ctx, InboxMessage{ConsumerID: consumerID, EventID: event.EventID,
		Topic: event.Topic, Partition: 0, Offset: 0, EventType: event.EventType,
		SchemaVersion: event.SchemaVersion, Payload: event.Payload})
	if err != nil || !inbox.WakeupCreated {
		t.Fatalf("wakeup delivery = %+v err=%v", inbox, err)
	}
	wakeups, err := store.ListWakeups(ctx, workflowID, 10)
	if err != nil || len(wakeups) != 1 || wakeups[0].State != WakeupPending {
		t.Fatalf("pending wakeup = %+v err=%v", wakeups, err)
	}
	item, err := store.UpsertReconciliationItem(ctx, ReconciliationItem{WorkflowID: workflowID,
		PartitionID: current.PartitionID, Kind: ReconcileLostWakeup, Reference: "wakeup/" + wakeups[0].WakeupID})
	if err != nil || item.Status != "OPEN" {
		t.Fatalf("lost-wakeup obligation = %+v err=%v", item, err)
	}
	// Compare persisted rows, including timestamps, rather than reusing the
	// production validator as an oracle. A rejected call must change neither row.
	snapshot := func(t *testing.T) string {
		t.Helper()
		var rows string
		if err := store.Pool().QueryRow(ctx, `SELECT jsonb_build_object('wakeup', to_jsonb(w), 'item', to_jsonb(r))::text
			FROM engine.scheduler_wakeups w CROSS JOIN engine.reconciliation_items r
			WHERE w.wakeup_id = $1 AND r.item_id = $2`, wakeups[0].WakeupID, item.ItemID).Scan(&rows); err != nil {
			t.Fatal(err)
		}
		return rows
	}
	before := snapshot(t)
	assertRejected := func(t *testing.T, ref LeaseRef) {
		t.Helper()
		if err := store.AcknowledgeWorkflowWakeups(ctx, ref, workflowID, created.Workflow.Revision); !errors.Is(err, ErrLeaseNotOwned) {
			t.Errorf("wakeup acknowledgment = %v, want ErrLeaseNotOwned", err)
		}
		if after := snapshot(t); after != before {
			t.Errorf("rejected acknowledgment changed durable wakeup/reconciliation rows: before=%s after=%s", before, after)
		}
	}
	if _, err := store.Pool().Exec(ctx, `UPDATE engine.partition_leases
		SET lease_expires_at = clock_timestamp() - interval '1 second'
		WHERE partition_id = $1 AND owner_id::text = $2 AND epoch = $3`, current.PartitionID, current.OwnerID, current.Epoch); err != nil {
		t.Fatal(err)
	}
	t.Run("expired owner", func(t *testing.T) { assertRejected(t, stale) })
	taken, acquired, err := store.AcquireLease(ctx, current.PartitionID, NewID(), time.Minute)
	if err != nil || !acquired || taken.Epoch != initial.Epoch+1 {
		t.Fatalf("takeover = %+v acquired=%v err=%v", taken, acquired, err)
	}
	current = LeaseRef{PartitionID: taken.PartitionID, OwnerID: taken.OwnerID, Epoch: taken.Epoch}
	t.Run("superseded owner", func(t *testing.T) { assertRejected(t, stale) })
	t.Run("wrong owner with current epoch", func(t *testing.T) {
		wrong := current
		wrong.OwnerID = stale.OwnerID
		assertRejected(t, wrong)
	})
	t.Run("current owner with stale epoch", func(t *testing.T) {
		wrong := current
		wrong.Epoch = stale.Epoch
		assertRejected(t, wrong)
	})
	t.Run("current owner acknowledges both obligations", func(t *testing.T) {
		if err := store.AcknowledgeWorkflowWakeups(ctx, current, workflowID, created.Workflow.Revision); err != nil {
			t.Fatal(err)
		}
		var consumed, resolved bool
		if err := store.Pool().QueryRow(ctx, `SELECT w.state = 'CONSUMED' AND w.consumed_at IS NOT NULL,
			r.status = 'RESOLVED' AND r.resolved_at IS NOT NULL
			FROM engine.scheduler_wakeups w CROSS JOIN engine.reconciliation_items r
			WHERE w.wakeup_id = $1 AND r.item_id = $2`, wakeups[0].WakeupID, item.ItemID).Scan(&consumed, &resolved); err != nil {
			t.Fatal(err)
		}
		if !consumed || !resolved {
			t.Fatalf("current owner: consumed=%v resolved=%v, want both true", consumed, resolved)
		}
	})
}

func TestM2LeaseRenewalAndTakeoverFence(t *testing.T) {
	databaseURL := integrationDatabaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	store, err := NewFromURL(ctx, databaseURL)
	if err != nil {
		if os.Getenv("DURABLE_REQUIRE_DATABASE") == "1" {
			t.Fatalf("PostgreSQL is required but unavailable: %v", err)
		}
		t.Skipf("PostgreSQL is not available: %v", err)
	}
	defer store.Close()
	ownerA := NewID()
	ownerB := NewID()
	lease, acquired, err := acquireTestLease(ctx, store, ownerA)
	if err != nil || !acquired {
		t.Fatalf("owner A acquire = %+v acquired=%v err=%v", lease, acquired, err)
	}
	partitionID := lease.PartitionID
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = store.Pool().Exec(cleanupCtx, `UPDATE engine.partition_leases
			SET owner_id = NULL, lease_expires_at = NULL, updated_at = clock_timestamp()
			WHERE partition_id = $1 AND owner_id::text = $2`, partitionID, ownerB)
		_ = store.ReleaseLease(cleanupCtx, LeaseRef{PartitionID: partitionID, OwnerID: ownerA, Epoch: lease.Epoch})
	}()

	renewed, err := store.RenewLease(ctx, LeaseRef{PartitionID: partitionID, OwnerID: ownerA, Epoch: lease.Epoch}, 2*time.Minute)
	if err != nil || renewed.Epoch != lease.Epoch || !renewed.LeaseExpiresAt.After(lease.LeaseExpiresAt) {
		t.Fatalf("owner A renewal = %+v err=%v", renewed, err)
	}
	if observed, got, err := store.AcquireLease(ctx, partitionID, ownerB, time.Minute); err != nil || got || observed.OwnerID != ownerA {
		t.Fatalf("owner B acquired a live lease = %+v acquired=%v err=%v", observed, got, err)
	}
	if _, err := store.Pool().Exec(ctx, `UPDATE engine.partition_leases
		SET lease_expires_at = clock_timestamp() - interval '1 second'
		WHERE partition_id = $1`, partitionID); err != nil {
		t.Fatal(err)
	}
	taken, acquired, err := store.AcquireLease(ctx, partitionID, ownerB, time.Minute)
	if err != nil || !acquired || taken.Epoch <= lease.Epoch || taken.OwnerID != ownerB {
		t.Fatalf("owner B takeover = %+v acquired=%v err=%v", taken, acquired, err)
	}
	if _, err := store.RenewLease(ctx, LeaseRef{PartitionID: partitionID, OwnerID: ownerA, Epoch: lease.Epoch}, time.Minute); !errors.Is(err, ErrLeaseNotOwned) {
		t.Fatalf("stale owner renewal = %v, want ErrLeaseNotOwned", err)
	}
	if err := store.ReleaseLease(ctx, LeaseRef{PartitionID: partitionID, OwnerID: ownerA, Epoch: lease.Epoch}); !errors.Is(err, ErrLeaseNotOwned) {
		t.Fatalf("stale owner release = %v, want ErrLeaseNotOwned", err)
	}
	current, held, err := store.AcquireLease(ctx, partitionID, ownerB, time.Minute)
	if err != nil || !held || current.Epoch != taken.Epoch {
		t.Fatalf("owner B renewal = %+v acquired=%v err=%v", current, held, err)
	}
}

func TestM2ConcurrentExpiredLeaseHasOneWinner(t *testing.T) {
	databaseURL := integrationDatabaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	store, err := NewFromURL(ctx, databaseURL)
	if err != nil {
		if os.Getenv("DURABLE_REQUIRE_DATABASE") == "1" {
			t.Fatalf("PostgreSQL is required but unavailable: %v", err)
		}
		t.Skipf("PostgreSQL is not available: %v", err)
	}
	defer store.Close()
	initial, acquired, err := acquireTestLease(ctx, store, NewID())
	if err != nil || !acquired {
		t.Fatalf("initial lease = %+v acquired=%v err=%v", initial, acquired, err)
	}
	partitionID := initial.PartitionID
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = store.Pool().Exec(cleanupCtx, `UPDATE engine.partition_leases
			SET owner_id = NULL, lease_expires_at = NULL, updated_at = clock_timestamp()
			WHERE partition_id = $1`, partitionID)
	}()
	if _, err := store.Pool().Exec(ctx, `UPDATE engine.partition_leases
		SET lease_expires_at = clock_timestamp() - interval '1 second'
		WHERE partition_id = $1`, partitionID); err != nil {
		t.Fatal(err)
	}
	const contenders = 12
	start := make(chan struct{})
	results := make(chan struct {
		lease    Lease
		acquired bool
		err      error
	}, contenders)
	var group sync.WaitGroup
	for i := 0; i < contenders; i++ {
		ownerID := NewID()
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			lease, won, acquireErr := store.AcquireLease(ctx, partitionID, ownerID, time.Minute)
			results <- struct {
				lease    Lease
				acquired bool
				err      error
			}{lease, won, acquireErr}
		}()
	}
	close(start)
	group.Wait()
	close(results)
	winners := 0
	var winner Lease
	for result := range results {
		if result.err != nil {
			t.Fatal(result.err)
		}
		if result.acquired {
			winners++
			winner = result.lease
		}
	}
	if winners != 1 {
		t.Fatalf("expired lease winners = %d, want one", winners)
	}
	if winner.Epoch != initial.Epoch+1 {
		t.Fatalf("winner epoch = %d, want %d", winner.Epoch, initial.Epoch+1)
	}
}

func TestM2TakeoverWaitsForLockedLeaseTransaction(t *testing.T) {
	databaseURL := integrationDatabaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	store, err := NewFromURL(ctx, databaseURL)
	if err != nil {
		if os.Getenv("DURABLE_REQUIRE_DATABASE") == "1" {
			t.Fatalf("PostgreSQL is required but unavailable: %v", err)
		}
		t.Skipf("PostgreSQL is not available: %v", err)
	}
	defer store.Close()
	ownerA := NewID()
	ownerB := NewID()
	initial, acquired, err := acquireTestLease(ctx, store, ownerA)
	if err != nil || !acquired {
		t.Fatalf("initial lease = %+v acquired=%v err=%v", initial, acquired, err)
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = store.Pool().Exec(cleanupCtx, `UPDATE engine.partition_leases
			SET owner_id = NULL, lease_expires_at = NULL, updated_at = clock_timestamp()
			WHERE partition_id = $1`, initial.PartitionID)
	}()
	transaction, err := store.Pool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = transaction.Rollback(ctx) }()
	var heldEpoch int64
	if err := transaction.QueryRow(ctx, `SELECT epoch FROM engine.partition_leases
		WHERE partition_id = $1 FOR UPDATE`, initial.PartitionID).Scan(&heldEpoch); err != nil {
		t.Fatal(err)
	}
	type acquireResult struct {
		lease    Lease
		acquired bool
		err      error
	}
	result := make(chan acquireResult, 1)
	go func() {
		lease, won, acquireErr := store.AcquireLease(ctx, initial.PartitionID, ownerB, time.Minute)
		result <- acquireResult{lease: lease, acquired: won, err: acquireErr}
	}()
	select {
	case early := <-result:
		t.Fatalf("takeover bypassed the held lease row: %+v", early)
	case <-time.After(50 * time.Millisecond):
	}
	if _, err := transaction.Exec(ctx, `UPDATE engine.partition_leases
		SET lease_expires_at = clock_timestamp() - interval '1 second'
		WHERE partition_id = $1`, initial.PartitionID); err != nil {
		t.Fatal(err)
	}
	if err := transaction.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	taken := <-result
	if taken.err != nil || !taken.acquired || taken.lease.OwnerID != ownerB || taken.lease.Epoch != heldEpoch+1 {
		t.Fatalf("takeover after lock release = %+v", taken)
	}
	if err := store.ReleaseLease(ctx, LeaseRef{PartitionID: taken.lease.PartitionID, OwnerID: ownerB, Epoch: taken.lease.Epoch}); err != nil {
		t.Fatal(err)
	}
}

func TestM2ContendedLeaseAcquisitionReturnsRetryableWithinBound(t *testing.T) {
	databaseURL := integrationDatabaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	store, err := NewFromURL(ctx, databaseURL)
	if err != nil {
		if os.Getenv("DURABLE_REQUIRE_DATABASE") == "1" {
			t.Fatalf("PostgreSQL is required but unavailable: %v", err)
		}
		t.Skipf("PostgreSQL is not available: %v", err)
	}
	defer store.Close()

	initial, acquired, err := acquireTestLease(ctx, store, NewID())
	if err != nil || !acquired {
		t.Fatalf("initial lease = %+v acquired=%v err=%v", initial, acquired, err)
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = store.Pool().Exec(cleanupCtx, `UPDATE engine.partition_leases
			SET owner_id = NULL, lease_expires_at = NULL, updated_at = clock_timestamp()
			WHERE partition_id = $1`, initial.PartitionID)
	}()

	transaction, err := store.Pool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = transaction.Rollback(context.Background()) }()
	var heldEpoch int64
	if err := transaction.QueryRow(ctx, `SELECT epoch FROM engine.partition_leases
		WHERE partition_id = $1 FOR UPDATE`, initial.PartitionID).Scan(&heldEpoch); err != nil {
		t.Fatal(err)
	}

	started := time.Now()
	_, won, acquireErr := store.AcquireLease(ctx, initial.PartitionID, NewID(), time.Minute)
	elapsed := time.Since(started)
	if won {
		t.Fatal("contended acquisition unexpectedly won the lease")
	}
	if !errors.Is(acquireErr, ErrLeaseAcquisitionTimeout) {
		t.Fatalf("contended acquisition error = %v, want ErrLeaseAcquisitionTimeout", acquireErr)
	}
	if errors.Is(acquireErr, ErrLeaseNotOwned) {
		t.Fatalf("contended acquisition conflated timeout with ErrLeaseNotOwned: %v", acquireErr)
	}
	const acquisitionBound = 5 * time.Second
	if elapsed >= acquisitionBound {
		t.Fatalf("contended acquisition took %s, want less than scheduler iteration bound %s", elapsed, acquisitionBound)
	}
	t.Logf("contended lease acquisition returned retryable error after %s while epoch %d remained locked", elapsed, heldEpoch)
}
