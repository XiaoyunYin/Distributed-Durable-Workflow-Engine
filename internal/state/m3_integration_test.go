package state

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/partition"
)

func TestM3TransportPersistenceAndFencing(t *testing.T) {
	ctx, store := openM3Database(t)
	defer store.Close()
	definitionID := "dur011-state-" + NewID()
	workflowID, lease := createM3Workflow(t, ctx, store, definitionID)
	defer cleanupM3Workflow(t, ctx, store, workflowID, definitionID, lease)

	events, err := store.ListOutbox(ctx, workflowID, 20)
	if err != nil || len(events) != 1 {
		t.Fatalf("initial outbox = %v, err=%v", events, err)
	}
	firstEvent := events[0]
	owners := []string{NewID(), NewID()}
	claims := make(chan []OutboxEvent, 2)
	errorsCh := make(chan error, 2)
	var wait sync.WaitGroup
	for _, owner := range owners {
		wait.Add(1)
		go func(owner string) {
			defer wait.Done()
			claimed, claimErr := store.ClaimOutboxForPartition(ctx, owner, 1, time.Minute, lease.PartitionID)
			if claimErr != nil {
				errorsCh <- claimErr
				return
			}
			claims <- claimed
		}(owner)
	}
	wait.Wait()
	close(claims)
	close(errorsCh)
	for err := range errorsCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	var winner OutboxEvent
	winners := 0
	for claim := range claims {
		if len(claim) == 1 {
			winner = claim[0]
			winners++
		}
	}
	if winners != 1 || winner.EventID != firstEvent.EventID || winner.RelayAttempts != 1 {
		t.Fatalf("relay claims = winner=%+v count=%d", winner, winners)
	}

	if err := store.FinalizeOutboxPublication(ctx, winner.EventID, winner.ClaimOwner,
		winner.RelayAttempts, true, "", 0); err != nil {
		t.Fatalf("finalize publication: %v", err)
	}
	published, err := store.ListOutbox(ctx, workflowID, 20)
	if err != nil || published[0].PublishState != OutboxPublished {
		t.Fatalf("published outbox = %v, err=%v", published, err)
	}
	var publicationCount int
	if err := store.Pool().QueryRow(ctx, `SELECT count(*) FROM engine.outbox_publications WHERE event_id = $1`, winner.EventID).Scan(&publicationCount); err != nil {
		t.Fatal(err)
	}
	if publicationCount != 1 {
		t.Fatalf("publication rows = %d, want one", publicationCount)
	}

	consumer := "m3-consumer-" + NewID()
	if err := store.EnsureConsumerOffset(ctx, consumer, winner.Topic, 0, 0); err != nil {
		t.Fatal(err)
	}
	message := InboxMessage{ConsumerID: consumer, EventID: winner.EventID, Topic: winner.Topic,
		Partition: 0, Offset: 0, EventType: winner.EventType, Payload: winner.Payload}
	inbox, err := store.RecordInbox(ctx, message)
	if err != nil || inbox.Disposition != InboxAccepted || !inbox.CommitOffset || inbox.NextOffset != 1 {
		t.Fatalf("inbox = %+v, err=%v", inbox, err)
	}
	duplicate, err := store.RecordInbox(ctx, message)
	if err != nil || duplicate.Disposition != InboxAlreadyHandled || duplicate.WakeupCreated {
		t.Fatalf("duplicate inbox = %+v, err=%v", duplicate, err)
	}
	wakeups, err := store.ListWakeups(ctx, workflowID, 20)
	if err != nil || len(wakeups) != 1 || wakeups[0].State != WakeupPending {
		t.Fatalf("wakeups = %+v, err=%v", wakeups, err)
	}
	wakeupOwner := NewID()
	claimedWakeups, err := store.ClaimWakeups(ctx, LeaseRef{PartitionID: lease.PartitionID,
		OwnerID: lease.OwnerID, Epoch: lease.Epoch}, wakeupOwner, 10, time.Minute)
	if err != nil || len(claimedWakeups) != 1 {
		t.Fatalf("claimed wakeups = %+v, err=%v", claimedWakeups, err)
	}
	if err := store.ConsumeWakeup(ctx, LeaseRef{PartitionID: lease.PartitionID,
		OwnerID: lease.OwnerID, Epoch: lease.Epoch}, claimedWakeups[0].WakeupID, wakeupOwner); err != nil {
		t.Fatalf("consume wakeup: %v", err)
	}
}

func TestM3ConsumerOffsetsAreContiguous(t *testing.T) {
	ctx, store := openM3Database(t)
	defer store.Close()
	definitionID := "dur013-offset-" + NewID()
	workflowID, lease := createM3Workflow(t, ctx, store, definitionID)
	defer cleanupM3Workflow(t, ctx, store, workflowID, definitionID, lease)
	events, err := store.ListOutbox(ctx, workflowID, 20)
	if err != nil || len(events) != 1 {
		t.Fatalf("outbox = %v, err=%v", events, err)
	}
	secondID := NewID()
	if _, err := store.Pool().Exec(ctx, `
		INSERT INTO engine.outbox
			(event_id, workflow_id, aggregate_revision, event_type, payload)
		VALUES ($1, $2, 2, 'activity.result', '{"ok":true}')`, secondID, workflowID); err != nil {
		t.Fatal(err)
	}
	consumer := "offset-consumer-" + NewID()
	if err := store.EnsureConsumerOffset(ctx, consumer, events[0].Topic, 0, 0); err != nil {
		t.Fatal(err)
	}
	second := events[0]
	second.EventID = secondID
	second.AggregateRevision = 2
	second.EventType = "activity.result"
	second.Payload = []byte(`{"ok":true}`)
	gap, err := store.RecordInbox(ctx, InboxMessage{ConsumerID: consumer, EventID: second.EventID,
		Topic: second.Topic, Partition: 0, Offset: 1, EventType: second.EventType, Payload: second.Payload})
	if err != nil || gap.CommitOffset || gap.NextOffset != 0 {
		t.Fatalf("gap result = %+v, err=%v", gap, err)
	}
	first, err := store.RecordInbox(ctx, InboxMessage{ConsumerID: consumer, EventID: events[0].EventID,
		Topic: events[0].Topic, Partition: 0, Offset: 0, EventType: events[0].EventType, Payload: events[0].Payload})
	if err != nil || !first.CommitOffset || first.NextOffset != 2 {
		t.Fatalf("contiguous result = %+v, err=%v", first, err)
	}
	offset, err := store.GetConsumerOffset(ctx, consumer, events[0].Topic, 0)
	if err != nil || offset.NextOffset != 2 {
		t.Fatalf("stored offset = %+v, err=%v", offset, err)
	}
	poison, err := store.QuarantineMessage(ctx, InboxMessage{ConsumerID: consumer,
		EventID: "unknown-event", Topic: events[0].Topic, Partition: 0, Offset: 2,
		EventType: "unknown.event", Payload: []byte("not-json")}, "unknown outbox event")
	if err != nil || poison.Disposition != InboxQuarantined || !poison.CommitOffset || poison.NextOffset != 3 {
		t.Fatalf("poison disposition = %+v, err=%v", poison, err)
	}
	var poisonRows int
	if err := store.Pool().QueryRow(ctx, `SELECT count(*) FROM engine.transport_quarantine WHERE consumer_id = $1`, consumer).Scan(&poisonRows); err != nil {
		t.Fatal(err)
	}
	if poisonRows != 1 {
		t.Fatalf("poison rows = %d, want one", poisonRows)
	}
}

func TestM3ReconciliationAndBackpressure(t *testing.T) {
	ctx, store := openM3Database(t)
	defer store.Close()
	definitionID := "dur014-reconcile-" + NewID()
	workflowID, lease := createM3Workflow(t, ctx, store, definitionID)
	defer cleanupM3Workflow(t, ctx, store, workflowID, definitionID, lease)
	backlog, err := store.GetBacklog(ctx, lease.PartitionID)
	if err != nil || backlog.PendingOutbox != 1 {
		t.Fatalf("initial backlog = %+v, err=%v", backlog, err)
	}
	if _, err := store.CheckBackpressure(ctx, lease.PartitionID, BackpressureLimits{}); err != nil {
		t.Fatalf("unlimited backlog check = %v", err)
	}
	secondEventID := NewID()
	if _, err := store.Pool().Exec(ctx, `
		INSERT INTO engine.outbox
			(event_id, workflow_id, aggregate_revision, event_type, topic, payload)
		VALUES ($1, $2, 2, 'activity.result', $3, '{"ok":true}')`, secondEventID, workflowID, EventTopic); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CheckBackpressure(ctx, lease.PartitionID, BackpressureLimits{PendingOutbox: 1}); !errors.Is(err, ErrBacklogLimit) {
		t.Fatalf("backpressure error = %v, want ErrBacklogLimit", err)
	}
	item, err := store.UpsertReconciliationItem(ctx, ReconciliationItem{WorkflowID: workflowID,
		PartitionID: lease.PartitionID, Kind: ReconcilePendingOutbox, Reference: "outbox/" + workflowID})
	if err != nil || item.Status != "OPEN" {
		t.Fatalf("reconciliation item = %+v, err=%v", item, err)
	}
	if err := store.ResolveReconciliationItem(ctx, item.ItemID, "RESOLVED"); err != nil {
		t.Fatal(err)
	}
}

func openM3Database(t *testing.T) (context.Context, *Store) {
	t.Helper()
	if os.Getenv("DURABLE_RUN_INTEGRATION") != "1" && os.Getenv("DURABLE_REQUIRE_DATABASE") != "1" {
		t.Skip("M3 PostgreSQL integration tests require DURABLE_RUN_INTEGRATION=1 or service CI mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	store, err := NewFromURL(ctx, integrationDatabaseURL(t))
	if err != nil {
		if os.Getenv("DURABLE_REQUIRE_DATABASE") == "1" {
			t.Fatalf("PostgreSQL is required but unavailable: %v", err)
		}
		t.Skipf("PostgreSQL is not available: %v", err)
	}
	t.Cleanup(store.Close)
	var version int
	if err := store.Pool().QueryRow(ctx, `SELECT max(version) FROM engine.schema_migrations`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version < 8 {
		t.Fatalf("M3 migration is not applied: version=%d", version)
	}
	cleanupM3Fixtures(t, ctx, store)
	return ctx, store
}

func cleanupM3Fixtures(t *testing.T, ctx context.Context, store *Store) {
	t.Helper()
	// These prefixes are generated only by the committed M3 integration
	// fixtures. Cleaning them at setup makes a rerun independent of a prior
	// crash, while leaving application and developer data untouched.
	if _, err := store.Pool().Exec(ctx, `
		DELETE FROM engine.workflow_executions
		WHERE workflow_id LIKE 'dur-m3-%'`); err != nil {
		t.Fatalf("clean M3 workflow fixtures: %v", err)
	}
	if _, err := store.Pool().Exec(ctx, `
		DELETE FROM engine.workflow_definitions
		WHERE definition_id LIKE 'dur011-state-%' OR definition_id LIKE 'dur013-%' OR definition_id LIKE 'dur014-%'`); err != nil {
		t.Fatalf("clean M3 definition fixtures: %v", err)
	}
	if _, err := store.Pool().Exec(ctx, `
		DELETE FROM engine.consumer_offsets
		WHERE consumer_id LIKE 'm3-%' OR consumer_id LIKE 'offset-consumer-%'`); err != nil {
		t.Fatalf("clean M3 consumer fixtures: %v", err)
	}
	if _, err := store.Pool().Exec(ctx, `
		DELETE FROM engine.transport_quarantine
		WHERE consumer_id LIKE 'm3-%' OR consumer_id LIKE 'offset-consumer-%'`); err != nil {
		t.Fatalf("clean M3 poison fixtures: %v", err)
	}
}

func createM3Workflow(t *testing.T, ctx context.Context, store *Store, definitionID string) (string, Lease) {
	t.Helper()
	if err := store.CreateDefinition(ctx, DefinitionInput{DefinitionID: definitionID, Version: 1,
		DefinitionHash: "hash-" + definitionID, Graph: []byte(`{"entry":"root","nodes":[{"id":"root","kind":"success"}]}`),
		ActivityVersions: []byte(`{"root":"1"}`), EffectClasses: []byte(`{"root":"PURE_ACTIVITY"}`)}); err != nil {
		t.Fatal(err)
	}
	ownerID := NewID()
	var lease Lease
	var acquired bool
	var err error
	for partitionID := int16(0); partitionID < 16; partitionID++ {
		lease, acquired, err = store.AcquireLease(ctx, partitionID, ownerID, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if acquired {
			break
		}
	}
	if !acquired {
		t.Fatal("could not acquire M3 test lease")
	}
	for attempt := 0; attempt < 100; attempt++ {
		workflowID := "dur-m3-" + NewID()
		mapped, err := partition.ID(workflowID)
		if err != nil || int16(mapped) != lease.PartitionID {
			continue
		}
		created, err := store.CreateWorkflow(ctx, CreateWorkflowInput{WorkflowID: workflowID,
			Namespace: "dur-m3", SubmissionKey: "key-" + workflowID, SubmissionPayloadHash: "sub-v1:" + workflowID,
			DefinitionID: definitionID, DefinitionVersion: 1, PartitionID: lease.PartitionID,
			InitialNodeID: "root", InitialInput: []byte(`{}`), ActorID: "m3-test"})
		if err != nil {
			t.Fatal(err)
		}
		return created.Workflow.WorkflowID, lease
	}
	t.Fatal(fmt.Sprintf("could not generate workflow for partition %d", lease.PartitionID))
	return "", lease
}

func cleanupM3Workflow(t *testing.T, ctx context.Context, store *Store, workflowID, definitionID string, lease Lease) {
	t.Helper()
	if _, err := store.Pool().Exec(ctx, `DELETE FROM engine.consumer_offsets WHERE consumer_id LIKE 'm3-%' OR consumer_id LIKE 'offset-consumer-%'`); err != nil {
		t.Errorf("cleanup offsets: %v", err)
	}
	if _, err := store.Pool().Exec(ctx, `DELETE FROM engine.workflow_executions WHERE workflow_id = $1`, workflowID); err != nil {
		t.Errorf("cleanup workflow: %v", err)
	}
	if _, err := store.Pool().Exec(ctx, `DELETE FROM engine.workflow_definitions WHERE definition_id = $1`, definitionID); err != nil {
		t.Errorf("cleanup definition: %v", err)
	}
	if err := store.ReleaseLease(ctx, LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch}); err != nil && !errors.Is(err, ErrLeaseNotOwned) {
		t.Errorf("cleanup lease: %v", err)
	}
}
