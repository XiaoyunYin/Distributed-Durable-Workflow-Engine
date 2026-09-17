package state

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestPostgresStateRepository(t *testing.T) {
	databaseURL := integrationDatabaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := NewFromURL(ctx, databaseURL)
	if err != nil {
		t.Skipf("PostgreSQL is not available: %v", err)
	}
	defer store.Close()

	var migrationVersion int
	if err := store.pool.QueryRow(ctx, `SELECT max(version) FROM engine.schema_migrations`).Scan(&migrationVersion); err != nil {
		t.Fatalf("read migration ledger: %v", err)
	}
	if migrationVersion < 2 {
		t.Fatalf("DUR-005 migration is not applied: version = %d", migrationVersion)
	}

	definitionID := "dur005-" + NewID()
	if err := store.CreateDefinition(ctx, DefinitionInput{
		DefinitionID: definitionID, Version: 1, DefinitionHash: "hash-1",
		Graph:            []byte(`{"nodes":["root"]}`),
		ActivityVersions: []byte(`{"root":"1"}`),
	}); err != nil {
		t.Fatalf("create definition: %v", err)
	}
	defer func() {
		_, _ = store.pool.Exec(context.Background(), `DELETE FROM engine.workflow_definitions WHERE definition_id = $1`, definitionID)
	}()

	ownerID := NewID()
	lease, acquired, err := acquireTestLease(ctx, store, ownerID)
	if err != nil {
		t.Fatal(err)
	}
	if !acquired {
		t.Fatal("test lease was not acquired")
	}
	defer func() {
		_ = store.ReleaseLease(context.Background(), LeaseRef{PartitionID: lease.PartitionID, OwnerID: ownerID, Epoch: lease.Epoch})
	}()

	var workflowIDs []string
	newWorkflow := func(t *testing.T) Workflow {
		t.Helper()
		workflowID := "dur005-" + NewID()
		workflowIDs = append(workflowIDs, workflowID)
		result, err := store.CreateWorkflow(ctx, CreateWorkflowInput{
			WorkflowID: workflowID, Namespace: "dur005", SubmissionKey: "submission-" + workflowID,
			SubmissionPayloadHash: "payload-1", DefinitionID: definitionID, DefinitionVersion: 1,
			PartitionID: lease.PartitionID, InitialNodeID: "root", ActorID: "test-client",
		})
		if err != nil {
			t.Fatalf("create workflow: %v", err)
		}
		if !result.Created || result.Workflow.Revision != 1 {
			t.Fatalf("created workflow = %+v", result)
		}
		return result.Workflow
	}

	t.Run("submission idempotency and conflict", func(t *testing.T) {
		workflow := newWorkflow(t)
		result, err := store.CreateWorkflow(ctx, CreateWorkflowInput{
			WorkflowID: "different-id", Namespace: workflow.Namespace, SubmissionKey: workflow.SubmissionKey,
			SubmissionPayloadHash: workflow.PayloadHash, DefinitionID: definitionID, DefinitionVersion: 1,
			PartitionID: lease.PartitionID, InitialNodeID: "root", ActorID: "retry-client",
		})
		if err != nil || result.Created || result.Workflow.WorkflowID != workflow.WorkflowID {
			t.Fatalf("idempotent retry = %+v, err = %v", result, err)
		}
		_, err = store.CreateWorkflow(ctx, CreateWorkflowInput{
			WorkflowID: "conflicting-id", Namespace: workflow.Namespace, SubmissionKey: workflow.SubmissionKey,
			SubmissionPayloadHash: "different", DefinitionID: definitionID, DefinitionVersion: 1,
			PartitionID: lease.PartitionID, InitialNodeID: "root", ActorID: "retry-client",
		})
		if !errors.Is(err, ErrSubmissionConflict) {
			t.Fatalf("conflicting retry error = %v, want %v", err, ErrSubmissionConflict)
		}
	})

	prepare := func(t *testing.T, class EffectClass, effectKey string) (Workflow, Attempt) {
		t.Helper()
		workflow := newWorkflow(t)
		if err := store.ApplyOwnerTransition(ctx, OwnerTransitionInput{
			Lease:      LeaseRef{PartitionID: lease.PartitionID, OwnerID: ownerID, Epoch: lease.Epoch},
			WorkflowID: workflow.WorkflowID, ExpectedRevision: 1, NewState: StateWaitingActivity,
			ActorID: "scheduler-test", Reason: "SCHEDULE_ACTIVITY", NodeID: "root",
		}); err != nil {
			t.Fatalf("schedule activity: %v", err)
		}
		attempt, err := store.CreateAttempt(ctx, AttemptInput{
			Lease:      LeaseRef{PartitionID: lease.PartitionID, OwnerID: ownerID, Epoch: lease.Epoch},
			WorkflowID: workflow.WorkflowID, NodeID: "root", ExpectedRevision: 2,
			EffectClass: class, LogicalEffectKey: effectKey, GrantScopeHash: "grant-1",
			HeartbeatDeadline: time.Now().Add(-time.Second), ActorID: "scheduler-test",
		})
		if err != nil {
			t.Fatalf("create attempt: %v", err)
		}
		return workflow, attempt
	}

	t.Run("timeout branches and late evidence", func(t *testing.T) {
		unclaimedWorkflow, unclaimed := prepare(t, EffectPure, "")
		unclaimedTimeout, err := store.TimeoutAttempt(ctx, TimeoutInput{
			Lease:      LeaseRef{PartitionID: lease.PartitionID, OwnerID: ownerID, Epoch: lease.Epoch},
			WorkflowID: unclaimedWorkflow.WorkflowID, NodeID: "root", AttemptNumber: unclaimed.AttemptNumber,
			ExpectedRevision: 3, ActorID: "scheduler-test",
		})
		if err != nil || !unclaimedTimeout.Redispatched || unclaimedTimeout.ReplacementNumber != nil {
			t.Fatalf("unclaimed timeout = %+v, err = %v", unclaimedTimeout, err)
		}

		pureWorkflow, pure := prepare(t, EffectPure, "")
		_, err = store.ClaimAttempt(ctx, ClaimInput{WorkflowID: pureWorkflow.WorkflowID, NodeID: "root", WorkerID: "worker-pure", RequestID: "request-pure-" + pureWorkflow.WorkflowID})
		if err != nil {
			t.Fatalf("claim pure attempt: %v", err)
		}
		pureTimeout, err := store.TimeoutAttempt(ctx, TimeoutInput{
			Lease:      LeaseRef{PartitionID: lease.PartitionID, OwnerID: ownerID, Epoch: lease.Epoch},
			WorkflowID: pureWorkflow.WorkflowID, NodeID: "root", AttemptNumber: pure.AttemptNumber,
			ExpectedRevision: 3, ActorID: "scheduler-test",
		})
		if err != nil || pureTimeout.ReplacementNumber == nil {
			t.Fatalf("pure timeout = %+v, err = %v", pureTimeout, err)
		}
		replacement, err := store.GetAttempt(ctx, pureWorkflow.WorkflowID, "root", 0, *pureTimeout.ReplacementNumber)
		if err != nil || replacement.EffectClass != EffectPure || !replacement.IsCurrent {
			t.Fatalf("pure replacement = %+v, err = %v", replacement, err)
		}
		if _, err := store.GetAttempt(ctx, pureWorkflow.WorkflowID, "root", 0, pure.AttemptNumber); err != nil {
			t.Fatalf("read timed-out pure attempt: %v", err)
		}

		cooperatingWorkflow, cooperating := prepare(t, EffectCooperating, "effect-"+NewID())
		_, err = store.ClaimAttempt(ctx, ClaimInput{WorkflowID: cooperatingWorkflow.WorkflowID, NodeID: "root", WorkerID: "worker-coop", RequestID: "request-coop-" + cooperatingWorkflow.WorkflowID})
		if err != nil {
			t.Fatalf("claim cooperating attempt: %v", err)
		}
		coopTimeout, err := store.TimeoutAttempt(ctx, TimeoutInput{
			Lease:      LeaseRef{PartitionID: lease.PartitionID, OwnerID: ownerID, Epoch: lease.Epoch},
			WorkflowID: cooperatingWorkflow.WorkflowID, NodeID: "root", AttemptNumber: cooperating.AttemptNumber,
			ExpectedRevision: 3, ActorID: "scheduler-test",
		})
		if err != nil || coopTimeout.ReplacementNumber == nil {
			t.Fatalf("cooperating timeout = %+v, err = %v", coopTimeout, err)
		}
		coopReplacement, err := store.GetAttempt(ctx, cooperatingWorkflow.WorkflowID, "root", 0, *coopTimeout.ReplacementNumber)
		if err != nil || coopReplacement.LogicalEffectKey != cooperating.LogicalEffectKey || coopReplacement.GrantScopeHash != cooperating.GrantScopeHash {
			t.Fatalf("cooperating replacement = %+v, err = %v", coopReplacement, err)
		}

		nonCooperatingWorkflow, nonCooperating := prepare(t, EffectNonCooperating, "")
		nonCoopClaim, err := store.ClaimAttempt(ctx, ClaimInput{WorkflowID: nonCooperatingWorkflow.WorkflowID, NodeID: "root", WorkerID: "worker-noncoop", RequestID: "request-noncoop-" + nonCooperatingWorkflow.WorkflowID})
		if err != nil {
			t.Fatalf("claim non-cooperating attempt: %v", err)
		}
		nonCoopTimeout, err := store.TimeoutAttempt(ctx, TimeoutInput{
			Lease:      LeaseRef{PartitionID: lease.PartitionID, OwnerID: ownerID, Epoch: lease.Epoch},
			WorkflowID: nonCooperatingWorkflow.WorkflowID, NodeID: "root", AttemptNumber: nonCooperating.AttemptNumber,
			ExpectedRevision: 3, ActorID: "scheduler-test",
		})
		if err != nil || !nonCoopTimeout.Reconciliation || nonCoopTimeout.ReplacementNumber != nil {
			t.Fatalf("non-cooperating timeout = %+v, err = %v", nonCoopTimeout, err)
		}
		current, err := store.GetWorkflow(ctx, nonCooperatingWorkflow.WorkflowID)
		if err != nil || current.State != StateReconciliationRequired {
			t.Fatalf("non-cooperating workflow = %+v, err = %v", current, err)
		}
		disposition, err := store.RecordResultReceipt(ctx, ResultInput{
			WorkflowID: nonCooperatingWorkflow.WorkflowID, NodeID: "root", AttemptNumber: nonCooperating.AttemptNumber,
			ClaimToken: nonCoopClaim.ClaimToken, AttemptState: AttemptSucceeded,
			Payload: []byte(`{"applied":true}`), ReconciliationRef: "reconcile-" + nonCooperatingWorkflow.WorkflowID,
		})
		if err != nil || disposition != ResultRecordedAsEvidence {
			t.Fatalf("late result disposition = %q, err = %v", disposition, err)
		}
		evidence, err := store.RecordLateEvidence(ctx, LateEvidenceInput{
			WorkflowID: nonCooperatingWorkflow.WorkflowID, NodeID: "root", AttemptNumber: nonCooperating.AttemptNumber,
			ClaimToken: nonCoopClaim.ClaimToken, ReconciliationRef: "reconcile-" + nonCooperatingWorkflow.WorkflowID,
			Payload: []byte(`{"applied":true}`),
		})
		if err != nil || evidence.EvidenceID == "" {
			t.Fatalf("late evidence = %+v, err = %v", evidence, err)
		}
		unchanged, err := store.GetWorkflow(ctx, nonCooperatingWorkflow.WorkflowID)
		if err != nil || unchanged.Revision != current.Revision || unchanged.State != StateReconciliationRequired {
			t.Fatalf("late evidence changed workflow = %+v, err = %v", unchanged, err)
		}
	})

	t.Run("invalid transition rolls back and concurrent revision conflicts", func(t *testing.T) {
		workflow := newWorkflow(t)
		before, err := store.HistoryCount(ctx, workflow.WorkflowID)
		if err != nil {
			t.Fatal(err)
		}
		err = store.ApplyOwnerTransition(ctx, OwnerTransitionInput{
			Lease:      LeaseRef{PartitionID: lease.PartitionID, OwnerID: ownerID, Epoch: lease.Epoch},
			WorkflowID: workflow.WorkflowID, ExpectedRevision: 1, NewState: StateAbandoned,
			ActorID: "scheduler-test", Reason: "INVALID_TEST",
		})
		if !errors.Is(err, ErrInvalidTransition) {
			t.Fatalf("invalid transition error = %v", err)
		}
		after, err := store.GetWorkflow(ctx, workflow.WorkflowID)
		if err != nil {
			t.Fatal(err)
		}
		history, err := store.HistoryCount(ctx, workflow.WorkflowID)
		if err != nil {
			t.Fatal(err)
		}
		if after.Revision != 1 || history != before {
			t.Fatalf("invalid transition partially committed: workflow=%+v history=%d before=%d", after, history, before)
		}

		concurrent := newWorkflow(t)
		start := make(chan struct{})
		results := make(chan error, 2)
		var group sync.WaitGroup
		for _, nextState := range []WorkflowState{StateWaitingActivity, StateWaitingApproval} {
			group.Add(1)
			go func(nextState WorkflowState) {
				defer group.Done()
				<-start
				results <- store.ApplyOwnerTransition(ctx, OwnerTransitionInput{
					Lease:      LeaseRef{PartitionID: lease.PartitionID, OwnerID: ownerID, Epoch: lease.Epoch},
					WorkflowID: concurrent.WorkflowID, ExpectedRevision: 1, NewState: nextState,
					ActorID: "scheduler-test", Reason: "CONCURRENT_TEST",
				})
			}(nextState)
		}
		close(start)
		group.Wait()
		close(results)
		successes := 0
		conflicts := 0
		for result := range results {
			if result == nil {
				successes++
			} else if errors.Is(result, ErrRevisionConflict) {
				conflicts++
			}
		}
		if successes != 1 || conflicts != 1 {
			t.Fatalf("concurrent results: successes=%d conflicts=%d", successes, conflicts)
		}
	})

	for _, workflowID := range workflowIDs {
		_, _ = store.pool.Exec(context.Background(), `DELETE FROM engine.workflow_executions WHERE workflow_id = $1`, workflowID)
	}
}

func acquireTestLease(ctx context.Context, store *Store, ownerID string) (Lease, bool, error) {
	for partition := int16(0); partition < 16; partition++ {
		lease, acquired, err := store.AcquireLease(ctx, partition, ownerID, time.Minute)
		if err != nil {
			return Lease{}, false, err
		}
		if acquired {
			return lease, true, nil
		}
	}
	return Lease{}, false, fmt.Errorf("all partitions are currently leased")
}

func integrationDatabaseURL(t *testing.T) string {
	t.Helper()
	if value := os.Getenv("DURABLE_DATABASE_URL"); value != "" {
		return value
	}
	workingDirectory, err := os.Getwd()
	if err != nil {
		t.Skipf("cannot locate repository root: %v", err)
	}
	envPath := ""
	for directory := workingDirectory; ; directory = filepath.Dir(directory) {
		candidate := filepath.Join(directory, ".env")
		if _, statErr := os.Stat(candidate); statErr == nil {
			envPath = candidate
			break
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			break
		}
	}
	data, err := os.ReadFile(envPath)
	if err != nil {
		t.Skip("set DURABLE_DATABASE_URL or provide .env for PostgreSQL integration tests")
	}
	values := map[string]string{}
	for _, line := range strings.Split(string(data), "\n") {
		parts := strings.SplitN(strings.TrimSpace(line), "=", 2)
		if len(parts) == 2 {
			values[parts[0]] = strings.Trim(parts[1], "\r\"")
		}
	}
	user := values["POSTGRES_USER"]
	database := values["POSTGRES_DB"]
	password := values["POSTGRES_PASSWORD"]
	if user == "" || database == "" || password == "" {
		t.Skip(".env lacks PostgreSQL credentials for integration tests")
	}
	port := values["POSTGRES_PORT"]
	if port == "" {
		port = "5432"
	}
	if _, err := strconv.Atoi(port); err != nil {
		t.Fatalf("invalid POSTGRES_PORT: %v", err)
	}
	return (&url.URL{
		Scheme:   "postgresql",
		User:     url.UserPassword(user, password),
		Host:     "127.0.0.1:" + port,
		Path:     "/" + database,
		RawQuery: "sslmode=disable",
	}).String()
}
