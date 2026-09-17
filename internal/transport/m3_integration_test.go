package transport

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/partition"
	"durable-agent-execution-engine/internal/state"
)

func TestM3RelayCrashAfterBrokerAckRepublishesStableEvent(t *testing.T) {
	ctx, store := openM3TransportDatabase(t)
	defer store.Close()
	definitionID, workflowID, lease := createM3TransportWorkflow(t, ctx, store)
	defer cleanupM3TransportWorkflow(t, ctx, store, definitionID, workflowID, lease)

	broker := &MemoryBroker{}
	crash := true
	firstRelay := NewRelay(store, broker, RelayConfig{OwnerID: state.NewID(), BatchSize: 1,
		PartitionID: &lease.PartitionID, ClaimLease: time.Minute, AfterBoundary: func(boundary string) error {
			if crash && boundary == "after_broker_ack" {
				crash = false
				return errors.New("injected relay crash")
			}
			return nil
		}})
	if _, err := firstRelay.RunOnce(ctx); err == nil {
		t.Fatal("relay did not stop at the post-ack crash boundary")
	}
	firstMessages := broker.Events()
	if len(firstMessages) != 1 {
		t.Fatalf("broker messages after crash = %d, want one", len(firstMessages))
	}
	claimed, err := store.ListOutbox(ctx, workflowID, 10)
	if err != nil || len(claimed) != 1 || claimed[0].PublishState != state.OutboxClaimed {
		t.Fatalf("claimed outbox = %+v, err=%v", claimed, err)
	}
	if _, err := store.Pool().Exec(ctx, `UPDATE engine.outbox SET claim_expires_at = clock_timestamp() - interval '1 second' WHERE event_id = $1`, claimed[0].EventID); err != nil {
		t.Fatal(err)
	}
	secondRelay := NewRelay(store, broker, RelayConfig{OwnerID: state.NewID(), BatchSize: 1,
		PartitionID: &lease.PartitionID, ClaimLease: time.Minute})
	if report, err := secondRelay.RunOnce(ctx); err != nil || report.Published != 1 {
		t.Fatalf("recovery relay = %+v, err=%v", report, err)
	}
	secondMessages := broker.Events()
	if len(secondMessages) != 2 || secondMessages[0].EventID != secondMessages[1].EventID {
		t.Fatalf("republished events = %+v", secondMessages)
	}
	published, err := store.ListOutbox(ctx, workflowID, 10)
	if err != nil || published[0].PublishState != state.OutboxPublished || published[0].RelayAttempts != 2 {
		t.Fatalf("final outbox = %+v, err=%v", published, err)
	}
	var publications int
	if err := store.Pool().QueryRow(ctx, `SELECT count(*) FROM engine.outbox_publications WHERE event_id = $1`, published[0].EventID).Scan(&publications); err != nil {
		t.Fatal(err)
	}
	if publications != 1 {
		t.Fatalf("publication evidence rows = %d, want one durable finalization", publications)
	}

	source := NewMemorySource([]Message{{EventID: secondMessages[0].EventID, Topic: secondMessages[0].Topic,
		Partition: 0, Offset: 0, EventType: secondMessages[0].EventType, Payload: secondMessages[0].Payload},
		{EventID: secondMessages[1].EventID, Topic: secondMessages[1].Topic,
			Partition: 0, Offset: 1, EventType: secondMessages[1].EventType, Payload: secondMessages[1].Payload}})
	consumer := NewConsumer(store, source, ConsumerConfig{ConsumerID: "m3-relay-consumer-" + state.NewID()})
	for range 2 {
		message, receiveErr := source.Receive(ctx)
		if receiveErr != nil {
			t.Fatal(receiveErr)
		}
		if _, processErr := consumer.Process(ctx, message); processErr != nil {
			t.Fatal(processErr)
		}
	}
	if commits := source.Commits(); len(commits) != 2 || commits[0] != 1 || commits[1] != 2 {
		t.Fatalf("consumer commits = %v, want [1 2]", commits)
	}
	wakeups, err := store.ListWakeups(ctx, workflowID, 10)
	if err != nil || len(wakeups) != 1 {
		t.Fatalf("duplicate delivery wakeups = %+v, err=%v", wakeups, err)
	}
	poison, err := consumer.Process(ctx, Message{EventID: "unknown-event", Topic: secondMessages[0].Topic,
		Partition: 0, Offset: 2, EventType: "unknown.event", Payload: []byte("not-json")})
	if err != nil || poison.Disposition != state.InboxQuarantined || !poison.Committed {
		t.Fatalf("poison delivery = %+v, err=%v", poison, err)
	}
}

func TestM3RelayFailureIsDurablyRetryable(t *testing.T) {
	ctx, store := openM3TransportDatabase(t)
	defer store.Close()
	definitionID, workflowID, lease := createM3TransportWorkflow(t, ctx, store)
	defer cleanupM3TransportWorkflow(t, ctx, store, definitionID, workflowID, lease)

	broker := &MemoryBroker{Failures: 1}
	relay := NewRelay(store, broker, RelayConfig{OwnerID: state.NewID(), BatchSize: 1,
		PartitionID: &lease.PartitionID, RetryBackoff: time.Millisecond})
	report, err := relay.RunOnce(ctx)
	if err != nil || report.Failed != 1 {
		t.Fatalf("failed relay = %+v, err=%v", report, err)
	}
	events, err := store.ListOutbox(ctx, workflowID, 10)
	if err != nil || len(events) != 1 || events[0].PublishState != state.OutboxPending || events[0].LastError == "" {
		t.Fatalf("retryable outbox = %+v, err=%v", events, err)
	}
	if _, err := store.Pool().Exec(ctx, `UPDATE engine.outbox SET next_attempt_at = clock_timestamp() - interval '1 second' WHERE event_id = $1`, events[0].EventID); err != nil {
		t.Fatal(err)
	}
	if report, err := relay.RunOnce(ctx); err != nil || report.Published != 1 {
		t.Fatalf("retry relay = %+v, err=%v", report, err)
	}
}

func TestM3RelayPollRecoversWithoutNotification(t *testing.T) {
	ctx, store := openM3TransportDatabase(t)
	defer store.Close()
	definitionID, workflowID, lease := createM3TransportWorkflow(t, ctx, store)
	defer cleanupM3TransportWorkflow(t, ctx, store, definitionID, workflowID, lease)

	broker := &MemoryBroker{}
	relay := NewRelay(store, broker, RelayConfig{OwnerID: state.NewID(),
		PartitionID: &lease.PartitionID, BatchSize: 10, PollInterval: 20 * time.Millisecond})
	runContext, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- relay.Run(runContext) }()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for len(broker.Events()) == 0 {
		select {
		case <-deadline.C:
			t.Fatal("relay did not publish the initial event")
		case <-time.After(10 * time.Millisecond):
		}
	}
	// Insert directly so no durable-agent_outbox NOTIFY is emitted. Only the
	// relay's bounded fallback poll can discover this obligation.
	eventID := state.NewID()
	if _, err := store.Pool().Exec(ctx, `
		INSERT INTO engine.outbox
			(event_id, workflow_id, aggregate_revision, event_type, topic, payload)
		VALUES ($1, $2, 2, 'activity.result', $3, '{"ok":true}')`,
		eventID, workflowID, state.EventTopic); err != nil {
		t.Fatal(err)
	}
	for {
		found := false
		for _, event := range broker.Events() {
			if event.EventID == eventID {
				found = true
				break
			}
		}
		if found {
			break
		}
		select {
		case <-deadline.C:
			t.Fatal("relay fallback poll did not publish the silent outbox row")
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("relay shutdown error = %v", err)
	}
}

func TestM3KafkaTaskAndEventRoundTrip(t *testing.T) {
	brokerList := os.Getenv("DURABLE_KAFKA_BROKERS")
	if brokerList == "" {
		t.Skip("real Kafka integration requires DURABLE_KAFKA_BROKERS")
	}
	ctx, store := openM3TransportDatabase(t)
	defer store.Close()
	definitionID, workflowID, lease := createM3TransportWorkflow(t, ctx, store)
	defer cleanupM3TransportWorkflow(t, ctx, store, definitionID, workflowID, lease)

	events, err := store.ListOutbox(ctx, workflowID, 10)
	if err != nil || len(events) != 1 {
		t.Fatalf("initial event outbox = %+v, err=%v", events, err)
	}
	taskEventID := state.NewID()
	if _, err := store.Pool().Exec(ctx, `
		INSERT INTO engine.outbox
			(event_id, workflow_id, aggregate_revision, event_type, topic, payload)
		VALUES ($1, $2, 2, 'attempt.dispatch', $3, '{"node_id":"root"}')`,
		taskEventID, workflowID, state.TaskTopic); err != nil {
		t.Fatal(err)
	}
	broker, err := NewKafkaBroker(splitBrokers(brokerList), DefaultTopic)
	if err != nil {
		t.Fatal(err)
	}
	defer broker.Close()
	relay := NewRelay(store, broker, RelayConfig{OwnerID: state.NewID(),
		PartitionID: &lease.PartitionID, BatchSize: 10})
	if report, err := relay.RunOnce(ctx); err != nil || report.Published != 2 {
		t.Fatalf("Kafka relay = %+v, err=%v", report, err)
	}

	consume := func(topic, eventID string) {
		t.Helper()
		source, sourceErr := NewKafkaSource(splitBrokers(brokerList), topic,
			"m3-kafka-"+state.NewID())
		if sourceErr != nil {
			t.Fatal(sourceErr)
		}
		defer source.Close()
		messageContext, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		consumer := NewConsumer(store, source, ConsumerConfig{ConsumerID: "m3-kafka-consumer-" + state.NewID()})
		for {
			message, receiveErr := source.Receive(messageContext)
			if receiveErr != nil {
				t.Fatalf("receive Kafka %s message: %v", topic, receiveErr)
			}
			if message.EventID != eventID {
				continue
			}
			result, processErr := consumer.Process(messageContext, message)
			if processErr != nil {
				t.Fatalf("consume Kafka %s message: %v", topic, processErr)
			}
			if result.Disposition != state.InboxAccepted || !result.Committed {
				t.Fatalf("Kafka %s result = %+v", topic, result)
			}
			return
		}
	}
	consume(state.EventTopic, events[0].EventID)
	consume(state.TaskTopic, taskEventID)
}

func splitBrokers(value string) []string {
	var brokers []string
	for _, broker := range strings.Split(value, ",") {
		if broker = strings.TrimSpace(broker); broker != "" {
			brokers = append(brokers, broker)
		}
	}
	return brokers
}

func openM3TransportDatabase(t *testing.T) (context.Context, *state.Store) {
	t.Helper()
	if os.Getenv("DURABLE_RUN_INTEGRATION") != "1" && os.Getenv("DURABLE_REQUIRE_DATABASE") != "1" {
		t.Skip("M3 transport integration tests require DURABLE_RUN_INTEGRATION=1 or service CI mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	store, err := state.NewFromURL(ctx, transportDatabaseURL(t))
	if err != nil {
		if os.Getenv("DURABLE_REQUIRE_DATABASE") == "1" {
			t.Fatalf("PostgreSQL is required but unavailable: %v", err)
		}
		t.Skipf("PostgreSQL is not available: %v", err)
	}
	var version int
	if err := store.Pool().QueryRow(ctx, `SELECT max(version) FROM engine.schema_migrations`).Scan(&version); err != nil {
		store.Close()
		t.Fatal(err)
	}
	if version < 8 {
		store.Close()
		t.Fatalf("M3 migration is not applied: version=%d", version)
	}
	cleanupM3TransportFixtures(t, ctx, store)
	return ctx, store
}

func cleanupM3TransportFixtures(t *testing.T, ctx context.Context, store *state.Store) {
	t.Helper()
	if _, err := store.Pool().Exec(ctx, `
		DELETE FROM engine.workflow_executions
		WHERE workflow_id LIKE 'dur011-transport-%'`); err != nil {
		t.Fatalf("clean M3 transport workflow fixtures: %v", err)
	}
	if _, err := store.Pool().Exec(ctx, `
		DELETE FROM engine.workflow_definitions
		WHERE definition_id LIKE 'dur011-transport-%'`); err != nil {
		t.Fatalf("clean M3 transport definition fixtures: %v", err)
	}
	if _, err := store.Pool().Exec(ctx, `
		DELETE FROM engine.consumer_offsets
		WHERE consumer_id LIKE 'm3-relay-consumer-%'`); err != nil {
		t.Fatalf("clean M3 transport consumer fixtures: %v", err)
	}
	if _, err := store.Pool().Exec(ctx, `
		DELETE FROM engine.transport_quarantine
		WHERE consumer_id LIKE 'm3-relay-consumer-%' OR consumer_id LIKE 'm3-kafka-consumer-%'`); err != nil {
		t.Fatalf("clean M3 transport poison fixtures: %v", err)
	}
}

func transportDatabaseURL(t *testing.T) string {
	if value := os.Getenv("DURABLE_DATABASE_URL"); value != "" {
		return value
	}
	directory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		data, readErr := os.ReadFile(filepath.Join(directory, ".env"))
		if readErr == nil {
			values := map[string]string{}
			for _, line := range strings.Split(string(data), "\n") {
				parts := strings.SplitN(strings.TrimSpace(line), "=", 2)
				if len(parts) == 2 {
					values[parts[0]] = strings.Trim(parts[1], "\r\"")
				}
			}
			port := values["POSTGRES_PORT"]
			if port == "" {
				port = "5432"
			}
			if _, err := strconv.Atoi(port); err != nil {
				t.Fatal(err)
			}
			return (&url.URL{Scheme: "postgresql", User: url.UserPassword(values["POSTGRES_USER"], values["POSTGRES_PASSWORD"]),
				Host: "127.0.0.1:" + port, Path: "/" + values["POSTGRES_DB"], RawQuery: "sslmode=disable"}).String()
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			break
		}
		directory = parent
	}
	t.Fatal("set DURABLE_DATABASE_URL or provide .env for PostgreSQL integration tests")
	return ""
}

func createM3TransportWorkflow(t *testing.T, ctx context.Context, store *state.Store) (string, string, state.Lease) {
	t.Helper()
	definitionID := "dur011-transport-" + state.NewID()
	if err := store.CreateDefinition(ctx, state.DefinitionInput{DefinitionID: definitionID, Version: 1,
		DefinitionHash: "hash-" + definitionID, Graph: []byte(`{"entry":"root","nodes":[{"id":"root","kind":"success"}]}`),
		ActivityVersions: []byte(`{"root":"1"}`), EffectClasses: []byte(`{"root":"PURE_ACTIVITY"}`)}); err != nil {
		t.Fatal(err)
	}
	ownerID := state.NewID()
	var lease state.Lease
	var acquired bool
	// Keep M3 fixtures away from the foundation/M1 integration fixtures, which
	// intentionally exercise partition 0 in parallel service CI.
	for partitionID := int16(8); partitionID < 16; partitionID++ {
		var err error
		lease, acquired, err = store.AcquireLease(ctx, partitionID, ownerID, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if acquired {
			break
		}
	}
	if !acquired {
		t.Fatal("could not acquire transport test lease")
	}
	for attempt := 0; attempt < 100; attempt++ {
		workflowID := "dur011-transport-" + state.NewID()
		mapped, err := partition.ID(workflowID)
		if err != nil || int16(mapped) != lease.PartitionID {
			continue
		}
		if _, err := store.CreateWorkflow(ctx, state.CreateWorkflowInput{WorkflowID: workflowID,
			Namespace: "dur011-transport", SubmissionKey: "key-" + workflowID,
			SubmissionPayloadHash: "sub-v1:" + workflowID, DefinitionID: definitionID,
			DefinitionVersion: 1, PartitionID: lease.PartitionID, InitialNodeID: "root",
			InitialInput: []byte(`{}`), ActorID: "m3-test"}); err != nil {
			t.Fatal(err)
		}
		return definitionID, workflowID, lease
	}
	t.Fatal(fmt.Sprintf("could not generate workflow for partition %d", lease.PartitionID))
	return definitionID, "", lease
}

func cleanupM3TransportWorkflow(t *testing.T, ctx context.Context, store *state.Store, definitionID, workflowID string, lease state.Lease) {
	t.Helper()
	if _, err := store.Pool().Exec(ctx, `DELETE FROM engine.consumer_offsets WHERE consumer_id LIKE 'm3-relay-consumer-%'`); err != nil {
		t.Errorf("cleanup consumer offsets: %v", err)
	}
	if _, err := store.Pool().Exec(ctx, `DELETE FROM engine.workflow_executions WHERE workflow_id = $1`, workflowID); err != nil {
		t.Errorf("cleanup workflow: %v", err)
	}
	if _, err := store.Pool().Exec(ctx, `DELETE FROM engine.workflow_definitions WHERE definition_id = $1`, definitionID); err != nil {
		t.Errorf("cleanup definition: %v", err)
	}
	if err := store.ReleaseLease(ctx, state.LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch}); err != nil && !errors.Is(err, state.ErrLeaseNotOwned) {
		t.Errorf("cleanup lease: %v", err)
	}
}
