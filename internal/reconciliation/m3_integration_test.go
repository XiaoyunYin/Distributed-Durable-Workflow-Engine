package reconciliation

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

func TestM3ReconcilerRecoversExpiredAttempt(t *testing.T) {
	ctx, store := openM3ReconciliationDatabase(t)
	defer store.Close()
	definitionID := "dur014-reconcile-test-" + state.NewID()
	workflowID, lease := createReconciliationWorkflow(t, ctx, store, definitionID)
	defer cleanupReconciliationWorkflow(t, ctx, store, workflowID, definitionID, lease)
	ref := state.LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch}

	workflow, err := store.GetWorkflow(ctx, workflowID)
	if err != nil {
		t.Fatal(err)
	}
	nodeState := state.StateWaitingActivity
	if err := store.ApplyOwnerTransition(ctx, state.OwnerTransitionInput{Lease: ref,
		WorkflowID: workflowID, ExpectedRevision: workflow.Revision, NewState: state.StateWaitingActivity,
		ActorID: "reconciler-test", Reason: "SCHEDULE_ACTIVITY", NodeID: "root", Iteration: 0,
		NodeState: &nodeState}); err != nil {
		t.Fatal(err)
	}
	workflow, err = store.GetWorkflow(ctx, workflowID)
	if err != nil {
		t.Fatal(err)
	}
	attempt, err := store.CreateAttempt(ctx, state.AttemptInput{Lease: ref, WorkflowID: workflowID,
		NodeID: "root", Iteration: 0, ExpectedRevision: workflow.Revision,
		EffectClass: state.EffectPure, HeartbeatDeadline: time.Now().Add(time.Minute), ActorID: "reconciler-test"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimAttempt(ctx, state.ClaimInput{WorkflowID: workflowID, NodeID: "root",
		Iteration: 0, WorkerID: "reconciler-worker", RequestID: "reconciler-request",
		AttemptLease: time.Minute}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Pool().Exec(ctx, `
		UPDATE engine.activity_attempts
		SET heartbeat_deadline = clock_timestamp() - interval '1 second'
		WHERE workflow_id = $1 AND node_id = 'root' AND iteration = 0 AND attempt_number = $2`,
		workflowID, attempt.AttemptNumber); err != nil {
		t.Fatal(err)
	}

	reconciler := New(store, Config{BatchSize: 20, DispatchLease: time.Minute})
	report, err := reconciler.RunOnce(ctx, ref, "reconciler-test")
	if err != nil {
		t.Fatalf("reconciliation report = %+v, err=%v", report, err)
	}
	if report.ExpiredAttempts != 1 || report.TimedOut != 1 {
		t.Fatalf("reconciliation report = %+v", report)
	}
	attempts, err := store.ListAttempts(ctx, workflowID)
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 2 {
		t.Fatalf("attempts after recovery = %d, want replacement", len(attempts))
	}
	var redispatch bool
	for _, event := range mustListOutbox(t, ctx, store, workflowID) {
		if event.EventType == "attempt.dispatch" {
			redispatch = true
		}
	}
	if !redispatch {
		t.Fatal("reconciliation did not create a durable redispatch event")
	}
	items, err := store.ListReconciliationItemsForWorkflow(ctx, workflowID, 100)
	if err != nil {
		t.Fatal(err)
	}
	foundExpired := false
	for _, item := range items {
		if item.Kind == state.ReconcileExpiredAttempt && item.WorkflowID == workflowID {
			foundExpired = true
			if item.Status != "RESOLVED" {
				t.Fatalf("expired-attempt item = %+v, want RESOLVED", item)
			}
		}
	}
	if !foundExpired {
		t.Fatal("reconciliation did not record the expired attempt")
	}
}

func mustListOutbox(t *testing.T, ctx context.Context, store *state.Store, workflowID string) []state.OutboxEvent {
	t.Helper()
	events, err := store.ListOutbox(ctx, workflowID, 100)
	if err != nil {
		t.Fatal(err)
	}
	return events
}

func openM3ReconciliationDatabase(t *testing.T) (context.Context, *state.Store) {
	t.Helper()
	if os.Getenv("DURABLE_RUN_INTEGRATION") != "1" && os.Getenv("DURABLE_REQUIRE_DATABASE") != "1" {
		t.Skip("M3 reconciliation integration requires DURABLE_RUN_INTEGRATION=1 or service CI mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	store, err := state.NewFromURL(ctx, reconciliationDatabaseURL(t))
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
	return ctx, store
}

func reconciliationDatabaseURL(t *testing.T) string {
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

func createReconciliationWorkflow(t *testing.T, ctx context.Context, store *state.Store, definitionID string) (string, state.Lease) {
	t.Helper()
	if err := store.CreateDefinition(ctx, state.DefinitionInput{DefinitionID: definitionID, Version: 1,
		DefinitionHash: "hash-" + definitionID, Graph: []byte(`{"entry":"root","nodes":[{"id":"root","kind":"success"}]}`),
		ActivityVersions: []byte(`{"root":"1"}`), EffectClasses: []byte(`{"root":"PURE_ACTIVITY"}`)}); err != nil {
		t.Fatal(err)
	}
	ownerID := state.NewID()
	var lease state.Lease
	var acquired bool
	for partitionID := int16(0); partitionID < 16; partitionID++ {
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
		t.Fatal("could not acquire reconciliation test lease")
	}
	for attempt := 0; attempt < 100; attempt++ {
		workflowID := "dur014-reconcile-test-" + state.NewID()
		mapped, err := partition.ID(workflowID)
		if err != nil || int16(mapped) != lease.PartitionID {
			continue
		}
		if _, err := store.CreateWorkflow(ctx, state.CreateWorkflowInput{WorkflowID: workflowID,
			Namespace: "dur014-reconcile-test", SubmissionKey: "key-" + workflowID,
			SubmissionPayloadHash: "sub-v1:" + workflowID, DefinitionID: definitionID,
			DefinitionVersion: 1, PartitionID: lease.PartitionID, InitialNodeID: "root",
			InitialInput: []byte(`{}`), ActorID: "reconciler-test"}); err != nil {
			t.Fatal(err)
		}
		return workflowID, lease
	}
	t.Fatal(fmt.Sprintf("could not generate workflow for partition %d", lease.PartitionID))
	return "", lease
}

func cleanupReconciliationWorkflow(t *testing.T, ctx context.Context, store *state.Store, workflowID, definitionID string, lease state.Lease) {
	t.Helper()
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
