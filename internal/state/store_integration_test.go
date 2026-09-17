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

	"durable-agent-execution-engine/internal/partition"
)

func TestPostgresStateRepository(t *testing.T) {
	databaseURL := integrationDatabaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := NewFromURL(ctx, databaseURL)
	if err != nil {
		if os.Getenv("DURABLE_REQUIRE_DATABASE") == "1" {
			t.Fatalf("PostgreSQL is required but unavailable: %v", err)
		}
		t.Skipf("PostgreSQL is not available: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	var migrationVersion int
	if err := store.pool.QueryRow(ctx, `SELECT max(version) FROM engine.schema_migrations`).Scan(&migrationVersion); err != nil {
		t.Fatalf("read migration ledger: %v", err)
	}
	if migrationVersion < 3 {
		t.Fatalf("DUR-005 migrations are not applied: version = %d", migrationVersion)
	}

	definitionIDs := map[EffectClass]string{}
	for _, class := range []EffectClass{EffectPure, EffectCooperating, EffectNonCooperating} {
		definitionID := "dur005-" + NewID()
		definitionIDs[class] = definitionID
		if err := store.CreateDefinition(ctx, DefinitionInput{
			DefinitionID: definitionID, Version: 1, DefinitionHash: "hash-" + string(class),
			Graph: []byte(`{"nodes":["root"]}`), ActivityVersions: []byte(`{"root":"1"}`),
			EffectClasses: []byte(fmt.Sprintf(`{"root":%q}`, class)),
		}); err != nil {
			t.Fatalf("create definition %s: %v", class, err)
		}
	}

	ownerID := NewID()
	lease, acquired, err := acquireTestLease(ctx, store, ownerID)
	if err != nil {
		t.Fatal(err)
	}
	if !acquired {
		t.Fatal("test lease was not acquired")
	}
	renewedLease, renewed, err := store.AcquireLease(ctx, lease.PartitionID, ownerID, time.Minute)
	if err != nil || !renewed || renewedLease.Epoch != lease.Epoch {
		t.Fatalf("same-owner lease renewal = %+v, acquired=%v, err=%v", renewedLease, renewed, err)
	}
	lease = renewedLease
	var workflowIDs []string
	newWorkflow := func(t *testing.T, definitionID string) Workflow {
		t.Helper()
		var workflowID string
		var mappedPartition uint64
		for {
			workflowID = "dur005-" + NewID()
			mappedPartition, _ = partition.ID(workflowID)
			if int16(mappedPartition) == lease.PartitionID {
				break
			}
		}
		workflowIDs = append(workflowIDs, workflowID)
		result, err := store.CreateWorkflow(ctx, CreateWorkflowInput{
			WorkflowID: workflowID, Namespace: "dur005", SubmissionKey: "submission-" + workflowID,
			SubmissionPayloadHash: "payload-1", DefinitionID: definitionID, DefinitionVersion: 1,
			PartitionID: int16(mappedPartition), InitialNodeID: "root", ActorID: "test-client",
		})
		if err != nil {
			t.Fatalf("create workflow: %v", err)
		}
		if !result.Created || result.Workflow.Revision != 1 {
			t.Fatalf("created workflow = %+v", result)
		}
		return result.Workflow
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for _, workflowID := range workflowIDs {
			if _, cleanupErr := store.pool.Exec(cleanupCtx, `DELETE FROM engine.workflow_executions WHERE workflow_id = $1`, workflowID); cleanupErr != nil {
				t.Errorf("cleanup workflow %s: %v", workflowID, cleanupErr)
			}
		}
		for _, definitionID := range definitionIDs {
			if _, cleanupErr := store.pool.Exec(cleanupCtx, `DELETE FROM engine.workflow_definitions WHERE definition_id = $1`, definitionID); cleanupErr != nil {
				t.Errorf("cleanup definition %s: %v", definitionID, cleanupErr)
			}
		}
		if cleanupErr := store.ReleaseLease(cleanupCtx, LeaseRef{PartitionID: lease.PartitionID, OwnerID: ownerID, Epoch: lease.Epoch}); cleanupErr != nil && !errors.Is(cleanupErr, ErrLeaseNotOwned) {
			t.Errorf("cleanup lease: %v", cleanupErr)
		}
	})

	t.Run("submission idempotency and conflict", func(t *testing.T) {
		workflow := newWorkflow(t, definitionIDs[EffectPure])
		var createdEvents int
		if err := store.pool.QueryRow(ctx, `
			SELECT count(*) FROM engine.outbox
			WHERE workflow_id = $1 AND event_type = 'workflow.created'`, workflow.WorkflowID).Scan(&createdEvents); err != nil {
			t.Fatal(err)
		}
		if createdEvents != 1 {
			t.Fatalf("workflow.created outbox rows = %d, want 1", createdEvents)
		}
		result, err := store.CreateWorkflow(ctx, CreateWorkflowInput{
			WorkflowID: "different-id", Namespace: workflow.Namespace, SubmissionKey: workflow.SubmissionKey,
			SubmissionPayloadHash: workflow.PayloadHash, DefinitionID: definitionIDs[EffectPure], DefinitionVersion: 1,
			PartitionID: lease.PartitionID, InitialNodeID: "root", ActorID: "retry-client",
		})
		if err != nil || result.Created || result.Workflow.WorkflowID != workflow.WorkflowID {
			t.Fatalf("idempotent retry = %+v, err = %v", result, err)
		}
		_, err = store.CreateWorkflow(ctx, CreateWorkflowInput{
			WorkflowID: "conflicting-id", Namespace: workflow.Namespace, SubmissionKey: workflow.SubmissionKey,
			SubmissionPayloadHash: "different", DefinitionID: definitionIDs[EffectPure], DefinitionVersion: 1,
			PartitionID: lease.PartitionID, InitialNodeID: "root", ActorID: "retry-client",
		})
		if !errors.Is(err, ErrSubmissionConflict) {
			t.Fatalf("conflicting retry error = %v, want %v", err, ErrSubmissionConflict)
		}
	})

	prepareAt := func(t *testing.T, class EffectClass, effectKey string, deadline time.Time) (Workflow, Attempt) {
		t.Helper()
		workflow := newWorkflow(t, definitionIDs[class])
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
			HeartbeatDeadline: deadline, ActorID: "scheduler-test",
		})
		if err != nil {
			t.Fatalf("create attempt: %v", err)
		}
		return workflow, attempt
	}
	prepare := func(t *testing.T, class EffectClass, effectKey string) (Workflow, Attempt) {
		return prepareAt(t, class, effectKey, time.Now().Add(-time.Second))
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
		_, err = store.ClaimAttempt(ctx, ClaimInput{WorkflowID: pureWorkflow.WorkflowID, NodeID: "root", WorkerID: "worker-pure", RequestID: "request-pure-" + pureWorkflow.WorkflowID, AttemptLease: 10 * time.Millisecond})
		if err != nil {
			t.Fatalf("claim pure attempt: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
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
		_, err = store.ClaimAttempt(ctx, ClaimInput{WorkflowID: cooperatingWorkflow.WorkflowID, NodeID: "root", WorkerID: "worker-coop", RequestID: "request-coop-" + cooperatingWorkflow.WorkflowID, AttemptLease: 10 * time.Millisecond})
		if err != nil {
			t.Fatalf("claim cooperating attempt: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
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
		nonCoopClaim, err := store.ClaimAttempt(ctx, ClaimInput{WorkflowID: nonCooperatingWorkflow.WorkflowID, NodeID: "root", WorkerID: "worker-noncoop", RequestID: "request-noncoop-" + nonCooperatingWorkflow.WorkflowID, AttemptLease: 10 * time.Millisecond})
		if err != nil {
			t.Fatalf("claim non-cooperating attempt: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
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
		if err != nil || disposition.Disposition != ResultRecordedAsEvidence {
			t.Fatalf("late result disposition = %+v, err = %v", disposition, err)
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
		workflow := newWorkflow(t, definitionIDs[EffectPure])
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

		concurrent := newWorkflow(t, definitionIDs[EffectPure])
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

	t.Run("multi-step repository transactions roll back on outbox failure", func(t *testing.T) {
		createRollbackWorkflow := func(t *testing.T, definitionID string) Workflow {
			t.Helper()
			var workflowID string
			var mappedPartition uint64
			for {
				workflowID = "dur005-rollback-" + NewID()
				mappedPartition, _ = partition.ID(workflowID)
				if int16(mappedPartition) == lease.PartitionID {
					break
				}
			}
			workflowIDs = append(workflowIDs, workflowID)
			result, err := store.CreateWorkflow(ctx, CreateWorkflowInput{
				WorkflowID: workflowID, Namespace: "dur005", SubmissionKey: "rollback-" + workflowID,
				SubmissionPayloadHash: "payload-1", DefinitionID: definitionID, DefinitionVersion: 1,
				PartitionID: int16(mappedPartition), InitialNodeID: "root", ActorID: "test-client",
			})
			if err != nil {
				t.Fatalf("create rollback workflow: %v", err)
			}
			return result.Workflow
		}
		installOutboxFailure := func(t *testing.T) {
			t.Helper()
			if _, err := store.pool.Exec(ctx, `
				DROP TRIGGER IF EXISTS dur005_test_fail_outbox_trigger ON engine.outbox;
				CREATE OR REPLACE FUNCTION engine.dur005_test_fail_outbox() RETURNS trigger
				LANGUAGE plpgsql AS $fn$
				BEGIN
					IF NEW.workflow_id LIKE 'dur005-rollback-%' THEN
						RAISE EXCEPTION 'dur005 injected outbox failure';
					END IF;
					RETURN NEW;
				END
				$fn$;
				CREATE TRIGGER dur005_test_fail_outbox_trigger
				BEFORE INSERT ON engine.outbox
				FOR EACH ROW EXECUTE FUNCTION engine.dur005_test_fail_outbox()`); err != nil {
				t.Fatalf("install outbox failure: %v", err)
			}
			t.Cleanup(func() {
				cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				_, _ = store.pool.Exec(cleanupCtx, `DROP TRIGGER IF EXISTS dur005_test_fail_outbox_trigger ON engine.outbox`)
				_, _ = store.pool.Exec(cleanupCtx, `DROP FUNCTION IF EXISTS engine.dur005_test_fail_outbox()`)
			})
		}

		createWorkflow := createRollbackWorkflow(t, definitionIDs[EffectPure])
		if err := store.ApplyOwnerTransition(ctx, OwnerTransitionInput{
			Lease:      LeaseRef{PartitionID: lease.PartitionID, OwnerID: ownerID, Epoch: lease.Epoch},
			WorkflowID: createWorkflow.WorkflowID, ExpectedRevision: 1, NewState: StateWaitingActivity,
			ActorID: "scheduler-test", Reason: "SCHEDULE_ACTIVITY", NodeID: "root",
		}); err != nil {
			t.Fatal(err)
		}
		historyBefore, err := store.HistoryCount(ctx, createWorkflow.WorkflowID)
		if err != nil {
			t.Fatal(err)
		}
		installOutboxFailure(t)
		_, err = store.CreateAttempt(ctx, AttemptInput{
			Lease:      LeaseRef{PartitionID: lease.PartitionID, OwnerID: ownerID, Epoch: lease.Epoch},
			WorkflowID: createWorkflow.WorkflowID, NodeID: "root", ExpectedRevision: 2,
			EffectClass: EffectPure, HeartbeatDeadline: time.Now().Add(time.Minute), ActorID: "scheduler-test",
		})
		if err == nil {
			t.Fatal("CreateAttempt unexpectedly committed through injected outbox failure")
		}
		rolledBack, err := store.GetWorkflow(ctx, createWorkflow.WorkflowID)
		if err != nil {
			t.Fatal(err)
		}
		var attemptCount int
		if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM engine.activity_attempts WHERE workflow_id = $1`, createWorkflow.WorkflowID).Scan(&attemptCount); err != nil {
			t.Fatal(err)
		}
		historyAfter, err := store.HistoryCount(ctx, createWorkflow.WorkflowID)
		if err != nil {
			t.Fatal(err)
		}
		if rolledBack.Revision != 2 || attemptCount != 0 || historyAfter != historyBefore {
			t.Fatalf("CreateAttempt partially committed: workflow=%+v attempts=%d history=%d before=%d", rolledBack, attemptCount, historyAfter, historyBefore)
		}

		// Remove the first trigger before installing it for the timeout case.
		if _, err := store.pool.Exec(ctx, `DROP TRIGGER IF EXISTS dur005_test_fail_outbox_trigger ON engine.outbox`); err != nil {
			t.Fatal(err)
		}
		timeoutWorkflow := createRollbackWorkflow(t, definitionIDs[EffectPure])
		if err := store.ApplyOwnerTransition(ctx, OwnerTransitionInput{
			Lease:      LeaseRef{PartitionID: lease.PartitionID, OwnerID: ownerID, Epoch: lease.Epoch},
			WorkflowID: timeoutWorkflow.WorkflowID, ExpectedRevision: 1, NewState: StateWaitingActivity,
			ActorID: "scheduler-test", Reason: "SCHEDULE_ACTIVITY", NodeID: "root",
		}); err != nil {
			t.Fatal(err)
		}
		timeoutAttempt, err := store.CreateAttempt(ctx, AttemptInput{
			Lease:      LeaseRef{PartitionID: lease.PartitionID, OwnerID: ownerID, Epoch: lease.Epoch},
			WorkflowID: timeoutWorkflow.WorkflowID, NodeID: "root", ExpectedRevision: 2,
			EffectClass: EffectPure, HeartbeatDeadline: time.Now().Add(-time.Second), ActorID: "scheduler-test",
		})
		if err != nil {
			t.Fatal(err)
		}
		claim, err := store.ClaimAttempt(ctx, ClaimInput{
			WorkflowID: timeoutWorkflow.WorkflowID, NodeID: "root", WorkerID: "rollback-worker",
			RequestID: "rollback-timeout-" + timeoutWorkflow.WorkflowID, AttemptLease: 10 * time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
		historyBefore, err = store.HistoryCount(ctx, timeoutWorkflow.WorkflowID)
		if err != nil {
			t.Fatal(err)
		}
		installOutboxFailure(t)
		_, err = store.TimeoutAttempt(ctx, TimeoutInput{
			Lease:      LeaseRef{PartitionID: lease.PartitionID, OwnerID: ownerID, Epoch: lease.Epoch},
			WorkflowID: timeoutWorkflow.WorkflowID, NodeID: "root", AttemptNumber: timeoutAttempt.AttemptNumber,
			ExpectedRevision: 3, ActorID: "scheduler-test", DispatchLease: time.Minute,
		})
		if err == nil {
			t.Fatal("TimeoutAttempt unexpectedly committed through injected outbox failure")
		}
		rolledBack, err = store.GetWorkflow(ctx, timeoutWorkflow.WorkflowID)
		if err != nil {
			t.Fatal(err)
		}
		var currentAttemptCount int
		if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM engine.activity_attempts WHERE workflow_id = $1`, timeoutWorkflow.WorkflowID).Scan(&currentAttemptCount); err != nil {
			t.Fatal(err)
		}
		historyAfter, err = store.HistoryCount(ctx, timeoutWorkflow.WorkflowID)
		if err != nil {
			t.Fatal(err)
		}
		if rolledBack.Revision != 3 || currentAttemptCount != 1 || historyAfter != historyBefore {
			t.Fatalf("TimeoutAttempt partially committed: workflow=%+v attempts=%d history=%d before=%d", rolledBack, currentAttemptCount, historyAfter, historyBefore)
		}
		current, err := store.GetAttempt(ctx, timeoutWorkflow.WorkflowID, "root", 0, timeoutAttempt.AttemptNumber)
		if err != nil || current.State != AttemptClaimed || !current.IsCurrent || current.ClaimToken != claim.ClaimToken {
			t.Fatalf("TimeoutAttempt rollback attempt=%+v err=%v", current, err)
		}
	})

	t.Run("authoritative metadata and fresh claim deadlines", func(t *testing.T) {
		workflowID := "dur005-" + NewID()
		mapped, err := partition.ID(workflowID)
		if err != nil {
			t.Fatal(err)
		}
		_, err = store.CreateWorkflow(ctx, CreateWorkflowInput{
			WorkflowID: workflowID, Namespace: "dur005", SubmissionKey: "wrong-partition-" + workflowID,
			SubmissionPayloadHash: "payload-1", DefinitionID: definitionIDs[EffectPure], DefinitionVersion: 1,
			PartitionID: int16((mapped + 1) % partition.PartitionCount), InitialNodeID: "root", ActorID: "test-client",
		})
		if !errors.Is(err, ErrPartitionMismatch) {
			t.Fatalf("wrong partition error = %v", err)
		}

		wrongClassWorkflow := newWorkflow(t, definitionIDs[EffectPure])
		if err := store.ApplyOwnerTransition(ctx, OwnerTransitionInput{
			Lease:      LeaseRef{PartitionID: lease.PartitionID, OwnerID: ownerID, Epoch: lease.Epoch},
			WorkflowID: wrongClassWorkflow.WorkflowID, ExpectedRevision: 1, NewState: StateWaitingActivity,
			ActorID: "scheduler-test", Reason: "SCHEDULE_ACTIVITY", NodeID: "root",
		}); err != nil {
			t.Fatal(err)
		}
		_, err = store.CreateAttempt(ctx, AttemptInput{
			Lease:      LeaseRef{PartitionID: lease.PartitionID, OwnerID: ownerID, Epoch: lease.Epoch},
			WorkflowID: wrongClassWorkflow.WorkflowID, NodeID: "root", ExpectedRevision: 2,
			EffectClass: EffectNonCooperating, HeartbeatDeadline: time.Now().Add(time.Minute), ActorID: "scheduler-test",
		})
		if !errors.Is(err, ErrEffectClassMismatch) {
			t.Fatalf("wrong effect class error = %v", err)
		}

		workflow, attempt := prepareAt(t, EffectPure, "", time.Now().Add(-time.Second))
		claim, err := store.ClaimAttempt(ctx, ClaimInput{
			WorkflowID: workflow.WorkflowID, NodeID: "root", WorkerID: "worker-deadline", RequestID: "request-deadline-" + workflow.WorkflowID,
			AttemptLease: 50 * time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}
		if !claim.HeartbeatDeadline.After(time.Now()) {
			t.Fatalf("claim deadline was not re-armed: %v", claim.HeartbeatDeadline)
		}
		if _, err := store.TimeoutAttempt(ctx, TimeoutInput{
			Lease:      LeaseRef{PartitionID: lease.PartitionID, OwnerID: ownerID, Epoch: lease.Epoch},
			WorkflowID: workflow.WorkflowID, NodeID: "root", AttemptNumber: attempt.AttemptNumber,
			ExpectedRevision: 3, ActorID: "scheduler-test",
		}); err == nil {
			t.Fatal("fresh claim was immediately timeout-eligible")
		}
		time.Sleep(75 * time.Millisecond)
		timeout, err := store.TimeoutAttempt(ctx, TimeoutInput{
			Lease:      LeaseRef{PartitionID: lease.PartitionID, OwnerID: ownerID, Epoch: lease.Epoch},
			WorkflowID: workflow.WorkflowID, NodeID: "root", AttemptNumber: attempt.AttemptNumber,
			ExpectedRevision: 3, ActorID: "scheduler-test", DispatchLease: time.Minute,
		})
		if err != nil || timeout.ReplacementNumber == nil {
			t.Fatalf("expired claim timeout = %+v, err=%v", timeout, err)
		}
		replacement, err := store.GetAttempt(ctx, workflow.WorkflowID, "root", 0, *timeout.ReplacementNumber)
		if err != nil || replacement.HeartbeatDeadline == nil || !replacement.HeartbeatDeadline.After(time.Now()) {
			t.Fatalf("replacement deadline = %+v, err=%v", replacement, err)
		}
		if _, err := store.TimeoutAttempt(ctx, TimeoutInput{
			Lease:      LeaseRef{PartitionID: lease.PartitionID, OwnerID: ownerID, Epoch: lease.Epoch},
			WorkflowID: workflow.WorkflowID, NodeID: "root", AttemptNumber: *timeout.ReplacementNumber,
			ExpectedRevision: 4, ActorID: "scheduler-test",
		}); err == nil {
			t.Fatal("fresh replacement was immediately timeout-eligible")
		}

		currentWorkflow, currentAttempt := prepareAt(t, EffectPure, "", time.Now().Add(time.Minute))
		requestID := "request-current-" + currentWorkflow.WorkflowID
		firstClaim, err := store.ClaimAttempt(ctx, ClaimInput{WorkflowID: currentWorkflow.WorkflowID, NodeID: "root", WorkerID: "worker-current", RequestID: requestID})
		if err != nil {
			t.Fatal(err)
		}
		retryClaim, err := store.ClaimAttempt(ctx, ClaimInput{WorkflowID: currentWorkflow.WorkflowID, NodeID: "root", WorkerID: "worker-current", RequestID: requestID})
		if err != nil || retryClaim.ClaimToken != firstClaim.ClaimToken || retryClaim.AttemptNumber != currentAttempt.AttemptNumber {
			t.Fatalf("current claim retry = %+v, err=%v", retryClaim, err)
		}
		otherWorkflow, _ := prepareAt(t, EffectPure, "", time.Now().Add(time.Minute))
		if _, err := store.ClaimAttempt(ctx, ClaimInput{WorkflowID: otherWorkflow.WorkflowID, NodeID: "root", WorkerID: "worker-other", RequestID: requestID}); !errors.Is(err, ErrClaimRequestConflict) {
			t.Fatalf("cross-workflow request reuse error = %v", err)
		}
		if _, err := store.TimeoutAttempt(ctx, TimeoutInput{
			Lease:      LeaseRef{PartitionID: lease.PartitionID, OwnerID: ownerID, Epoch: lease.Epoch},
			WorkflowID: currentWorkflow.WorkflowID, NodeID: "root", AttemptNumber: currentAttempt.AttemptNumber,
			ExpectedRevision: 3, ActorID: "scheduler-test", DispatchLease: time.Minute,
		}); err == nil {
			t.Fatal("unexpired claim timed out")
		}
	})

	t.Run("concurrent claims and result-timeout races have one winner", func(t *testing.T) {
		for round := 0; round < 20; round++ {
			workflow, _ := prepareAt(t, EffectPure, "", time.Now().Add(time.Minute))
			start := make(chan struct{})
			claimResults := make(chan error, 2)
			var claimGroup sync.WaitGroup
			for worker := 0; worker < 2; worker++ {
				claimGroup.Add(1)
				go func(worker int) {
					defer claimGroup.Done()
					<-start
					_, err := store.ClaimAttempt(ctx, ClaimInput{
						WorkflowID: workflow.WorkflowID, NodeID: "root", WorkerID: fmt.Sprintf("race-worker-%d", worker),
						RequestID: fmt.Sprintf("race-claim-%s-%d", workflow.WorkflowID, worker),
					})
					claimResults <- err
				}(worker)
			}
			close(start)
			claimGroup.Wait()
			close(claimResults)
			claimSuccesses := 0
			claimConflicts := 0
			for err := range claimResults {
				if err == nil {
					claimSuccesses++
				} else if errors.Is(err, ErrAttemptNotCurrent) {
					claimConflicts++
				} else {
					t.Fatalf("concurrent claim error = %v", err)
				}
			}
			if claimSuccesses != 1 || claimConflicts != 1 {
				t.Fatalf("concurrent claims: successes=%d conflicts=%d", claimSuccesses, claimConflicts)
			}

			raceWorkflow, raceAttempt := prepareAt(t, EffectPure, "", time.Now().Add(time.Minute))
			raceClaim, err := store.ClaimAttempt(ctx, ClaimInput{
				WorkflowID: raceWorkflow.WorkflowID, NodeID: "root", WorkerID: "race-result-worker",
				RequestID: "race-result-claim-" + raceWorkflow.WorkflowID, AttemptLease: 10 * time.Millisecond,
			})
			if err != nil {
				t.Fatalf("race claim: %v", err)
			}
			time.Sleep(25 * time.Millisecond)
			start = make(chan struct{})
			resultErrors := make(chan error, 2)
			var raceGroup sync.WaitGroup
			raceGroup.Add(2)
			go func() {
				defer raceGroup.Done()
				<-start
				_, resultErr := store.RecordResultReceipt(ctx, ResultInput{
					WorkflowID: raceWorkflow.WorkflowID, NodeID: "root", AttemptNumber: raceAttempt.AttemptNumber,
					ClaimToken: raceClaim.ClaimToken, AttemptState: AttemptSucceeded, Payload: []byte(`{"race":true}`),
				})
				resultErrors <- resultErr
			}()
			go func() {
				defer raceGroup.Done()
				<-start
				_, timeoutErr := store.TimeoutAttempt(ctx, TimeoutInput{
					Lease:      LeaseRef{PartitionID: lease.PartitionID, OwnerID: ownerID, Epoch: lease.Epoch},
					WorkflowID: raceWorkflow.WorkflowID, NodeID: "root", AttemptNumber: raceAttempt.AttemptNumber,
					ExpectedRevision: 3, ActorID: "scheduler-test", DispatchLease: time.Minute,
				})
				resultErrors <- timeoutErr
			}()
			close(start)
			raceGroup.Wait()
			close(resultErrors)
			resultWins := 0
			timeoutWins := 0
			for raceErr := range resultErrors {
				switch {
				case raceErr == nil:
					// There is one nil result from either the worker or timeout
					// transaction; classify the winner from durable state below.
				default:
					if !errors.Is(raceErr, ErrRevisionConflict) && !errors.Is(raceErr, ErrAttemptNotCurrent) {
						t.Fatalf("result-timeout race error = %v", raceErr)
					}
				}
			}
			raceWorkflowState, err := store.GetWorkflow(ctx, raceWorkflow.WorkflowID)
			if err != nil {
				t.Fatal(err)
			}
			var currentAttemptState AttemptState
			var currentAttempt bool
			if err := store.pool.QueryRow(ctx, `
				SELECT state, is_current FROM engine.activity_attempts
				WHERE workflow_id = $1 AND node_id = 'root' AND iteration = 0
				ORDER BY attempt_number DESC LIMIT 1`, raceWorkflow.WorkflowID).Scan(&currentAttemptState, &currentAttempt); err != nil {
				t.Fatal(err)
			}
			if currentAttemptState == AttemptSucceeded && !currentAttempt {
				resultWins++
			} else if currentAttemptState == AttemptDispatchable && currentAttempt {
				timeoutWins++
			} else {
				t.Fatalf("unexpected race outcome: workflow=%+v attempt=%s current=%v", raceWorkflowState, currentAttemptState, currentAttempt)
			}
			if resultWins+timeoutWins != 1 {
				t.Fatalf("result-timeout winners=%d", resultWins+timeoutWins)
			}
		}
	})

	t.Run("result receipts and owner result consumption", func(t *testing.T) {
		workflow, attempt := prepareAt(t, EffectPure, "", time.Now().Add(time.Minute))
		claim, err := store.ClaimAttempt(ctx, ClaimInput{WorkflowID: workflow.WorkflowID, NodeID: "root", WorkerID: "worker-result", RequestID: "request-result-" + workflow.WorkflowID})
		if err != nil {
			t.Fatal(err)
		}
		payload := []byte(`{"ok":true}`)
		first, err := store.RecordResultReceipt(ctx, ResultInput{
			WorkflowID: workflow.WorkflowID, NodeID: "root", AttemptNumber: attempt.AttemptNumber,
			ClaimToken: claim.ClaimToken, AttemptState: AttemptSucceeded, Payload: payload,
		})
		if err != nil || first.Disposition != ResultAccepted {
			t.Fatalf("first result = %+v, err=%v", first, err)
		}
		var outboxBefore int
		if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM engine.outbox WHERE workflow_id = $1`, workflow.WorkflowID).Scan(&outboxBefore); err != nil {
			t.Fatal(err)
		}
		retry, err := store.RecordResultReceipt(ctx, ResultInput{
			WorkflowID: workflow.WorkflowID, NodeID: "root", AttemptNumber: attempt.AttemptNumber,
			ClaimToken: claim.ClaimToken, AttemptState: AttemptSucceeded, Payload: payload,
		})
		if err != nil || retry.Disposition != ResultAccepted || !jsonEqual(retry.Payload, first.Payload) {
			t.Fatalf("result retry = %+v, err=%v", retry, err)
		}
		var outboxAfter int
		if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM engine.outbox WHERE workflow_id = $1`, workflow.WorkflowID).Scan(&outboxAfter); err != nil {
			t.Fatal(err)
		}
		if outboxAfter != outboxBefore {
			t.Fatalf("result retry added outbox rows: before=%d after=%d", outboxBefore, outboxAfter)
		}
		_, err = store.RecordResultReceipt(ctx, ResultInput{
			WorkflowID: workflow.WorkflowID, NodeID: "root", AttemptNumber: attempt.AttemptNumber,
			ClaimToken: claim.ClaimToken, AttemptState: AttemptSucceeded, Payload: []byte(`{"ok":false}`),
		})
		if !errors.Is(err, ErrResultConflict) {
			t.Fatalf("conflicting result retry error = %v", err)
		}
		if _, err := store.ConsumeResult(ctx, ConsumeResultInput{
			Lease:      LeaseRef{PartitionID: lease.PartitionID, OwnerID: ownerID, Epoch: lease.Epoch},
			WorkflowID: workflow.WorkflowID, NodeID: "root", AttemptNumber: attempt.AttemptNumber,
			ExpectedRevision: 4, NewWorkflowState: StateFailed, ActorID: "scheduler-test",
		}); !errors.Is(err, ErrInvalidTransition) {
			t.Fatalf("invalid result consumption error = %v", err)
		}
		unchangedWorkflow, err := store.GetWorkflow(ctx, workflow.WorkflowID)
		if err != nil {
			t.Fatal(err)
		}
		if unchangedWorkflow.Revision != 4 || unchangedWorkflow.State != StateWaitingActivity {
			t.Fatalf("invalid result consumption partially committed: %+v", unchangedWorkflow)
		}
		consumed, err := store.ConsumeResult(ctx, ConsumeResultInput{
			Lease:      LeaseRef{PartitionID: lease.PartitionID, OwnerID: ownerID, Epoch: lease.Epoch},
			WorkflowID: workflow.WorkflowID, NodeID: "root", AttemptNumber: attempt.AttemptNumber,
			ExpectedRevision: 4, NewWorkflowState: StateSucceeded, ActorID: "scheduler-test",
		})
		if err != nil || consumed.Workflow.State != StateSucceeded {
			t.Fatalf("consume success = %+v, err=%v", consumed, err)
		}
		var acceptedResult []byte
		var currentAttempt *int64
		if err := store.pool.QueryRow(ctx, `SELECT accepted_result, current_attempt_number FROM engine.node_instances WHERE workflow_id = $1 AND node_id = 'root' AND iteration = 0`, workflow.WorkflowID).Scan(&acceptedResult, &currentAttempt); err != nil {
			t.Fatal(err)
		}
		if !jsonEqual(acceptedResult, payload) || currentAttempt != nil {
			t.Fatalf("node result state: accepted=%s current=%v", acceptedResult, currentAttempt)
		}

		unvalidated, unvalidatedAttempt := prepareAt(t, EffectPure, "", time.Now().Add(time.Minute))
		if err := store.ApplyOwnerTransition(ctx, OwnerTransitionInput{
			Lease:      LeaseRef{PartitionID: lease.PartitionID, OwnerID: ownerID, Epoch: lease.Epoch},
			WorkflowID: unvalidated.WorkflowID, ExpectedRevision: 3, NewState: StateSucceeded,
			ActorID: "scheduler-test", Reason: "UNVALIDATED_ADVANCE", NodeID: "root", AttemptNumber: &unvalidatedAttempt.AttemptNumber,
		}); !errors.Is(err, ErrInvalidTransition) {
			t.Fatalf("unvalidated success transition error = %v", err)
		}

		retryWorkflow, retryAttempt := prepareAt(t, EffectPure, "", time.Now().Add(time.Minute))
		retryClaim, err := store.ClaimAttempt(ctx, ClaimInput{WorkflowID: retryWorkflow.WorkflowID, NodeID: "root", WorkerID: "worker-retry", RequestID: "request-retry-" + retryWorkflow.WorkflowID})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.RecordResultReceipt(ctx, ResultInput{
			WorkflowID: retryWorkflow.WorkflowID, NodeID: "root", AttemptNumber: retryAttempt.AttemptNumber,
			ClaimToken: retryClaim.ClaimToken, AttemptState: AttemptFailedRetryable, Payload: []byte(`{"retry":true}`),
		}); err != nil {
			t.Fatal(err)
		}
		due := time.Now().Add(time.Minute)
		if _, err := store.ConsumeResult(ctx, ConsumeResultInput{
			Lease:      LeaseRef{PartitionID: lease.PartitionID, OwnerID: ownerID, Epoch: lease.Epoch},
			WorkflowID: retryWorkflow.WorkflowID, NodeID: "root", AttemptNumber: retryAttempt.AttemptNumber,
			ExpectedRevision: 4, NewWorkflowState: StateWaitingTimer, RetryDueAt: &due, ActorID: "scheduler-test",
		}); err != nil {
			t.Fatal(err)
		}
		if err := store.ApplyOwnerTransition(ctx, OwnerTransitionInput{
			Lease:      LeaseRef{PartitionID: lease.PartitionID, OwnerID: ownerID, Epoch: lease.Epoch},
			WorkflowID: retryWorkflow.WorkflowID, ExpectedRevision: 5, NewState: StateRunnable,
			ActorID: "scheduler-test", Reason: "RETRY_TIMER_DUE", NodeID: "root",
		}); err != nil {
			t.Fatal(err)
		}
		if err := store.ApplyOwnerTransition(ctx, OwnerTransitionInput{
			Lease:      LeaseRef{PartitionID: lease.PartitionID, OwnerID: ownerID, Epoch: lease.Epoch},
			WorkflowID: retryWorkflow.WorkflowID, ExpectedRevision: 6, NewState: StateWaitingActivity,
			ActorID: "scheduler-test", Reason: "SCHEDULE_RETRY", NodeID: "root",
		}); err != nil {
			t.Fatal(err)
		}
		replacement, err := store.CreateAttempt(ctx, AttemptInput{
			Lease:      LeaseRef{PartitionID: lease.PartitionID, OwnerID: ownerID, Epoch: lease.Epoch},
			WorkflowID: retryWorkflow.WorkflowID, NodeID: "root", ExpectedRevision: 7,
			EffectClass: EffectPure, HeartbeatDeadline: time.Now().Add(time.Minute), ActorID: "scheduler-test",
		})
		if err != nil || replacement.AttemptNumber != 2 {
			t.Fatalf("retry replacement = %+v, err=%v", replacement, err)
		}
	})

	t.Run("stale owner is fenced after takeover", func(t *testing.T) {
		workflow := newWorkflow(t, definitionIDs[EffectPure])
		if _, err := store.pool.Exec(ctx, `UPDATE engine.partition_leases SET lease_expires_at = clock_timestamp() - interval '1 second' WHERE partition_id = $1`, lease.PartitionID); err != nil {
			t.Fatal(err)
		}
		newOwner := NewID()
		newLease, acquired, err := store.AcquireLease(ctx, lease.PartitionID, newOwner, time.Minute)
		if err != nil || !acquired {
			t.Fatalf("takeover lease = %+v, acquired=%v, err=%v", newLease, acquired, err)
		}
		defer func() {
			_ = store.ReleaseLease(context.Background(), LeaseRef{PartitionID: newLease.PartitionID, OwnerID: newOwner, Epoch: newLease.Epoch})
		}()
		err = store.ApplyOwnerTransition(ctx, OwnerTransitionInput{
			Lease:      LeaseRef{PartitionID: lease.PartitionID, OwnerID: ownerID, Epoch: lease.Epoch},
			WorkflowID: workflow.WorkflowID, ExpectedRevision: 1, NewState: StateWaitingActivity,
			ActorID: "stale-scheduler", Reason: "STALE_TRANSITION", NodeID: "root",
		})
		if !errors.Is(err, ErrLeaseNotOwned) {
			t.Fatalf("stale owner error = %v", err)
		}
	})

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
	if os.Getenv("DURABLE_RUN_INTEGRATION") != "1" && os.Getenv("DURABLE_REQUIRE_DATABASE") != "1" {
		t.Skip("PostgreSQL integration tests require DURABLE_RUN_INTEGRATION=1 or service CI mode")
	}
	if value := os.Getenv("DURABLE_DATABASE_URL"); value != "" {
		return value
	}
	workingDirectory, err := os.Getwd()
	if err != nil {
		integrationUnavailable(t, "cannot locate repository root: "+err.Error())
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
		integrationUnavailable(t, "set DURABLE_DATABASE_URL or provide .env for PostgreSQL integration tests")
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
		integrationUnavailable(t, ".env lacks PostgreSQL credentials for integration tests")
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

func integrationUnavailable(t *testing.T, message string) {
	t.Helper()
	if os.Getenv("DURABLE_REQUIRE_DATABASE") == "1" {
		t.Fatalf("PostgreSQL integration is required: %s", message)
	}
	t.Skip(message)
}
