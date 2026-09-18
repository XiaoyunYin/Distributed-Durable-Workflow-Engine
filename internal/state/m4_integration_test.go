package state

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/partition"
)

func TestM4CheckpointRetryAndApprovalPersistence(t *testing.T) {
	ctx, store := openM3Database(t)
	workflowID, definitionID, lease := createM4Fixture(t, ctx, store, EffectPure)
	defer cleanupM4Fixture(t, ctx, store, workflowID, definitionID, lease, "")

	if err := store.ApplyOwnerTransition(ctx, OwnerTransitionInput{
		Lease: leaseRef(lease), WorkflowID: workflowID, ExpectedRevision: 1,
		NewState: StateWaitingActivity, ActorID: "m4-scheduler", Reason: "SCHEDULE_ACTIVITY",
		NodeID: "root", NodeState: ptrState(StateWaitingActivity),
	}); err != nil {
		t.Fatal(err)
	}
	attempt, err := store.CreateAttempt(ctx, AttemptInput{
		Lease: leaseRef(lease), WorkflowID: workflowID, NodeID: "root", ExpectedRevision: 2,
		EffectClass: EffectPure, HeartbeatDeadline: time.Now().Add(time.Minute), ActorID: "m4-scheduler",
	})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := store.ClaimAttempt(ctx, ClaimInput{
		WorkflowID: workflowID, NodeID: "root", WorkerID: "m4-worker", RequestID: NewID(), AttemptLease: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, err := store.RecordCheckpoint(ctx, CheckpointInput{
		WorkflowID: workflowID, NodeID: "root", AttemptNumber: attempt.AttemptNumber,
		ClaimToken: claimed.ClaimToken, Sequence: 0, SchemaVersion: 1, Payload: []byte(`{"step":1}`),
	})
	if err != nil || checkpoint.Sequence != 0 {
		t.Fatalf("first checkpoint = %+v, err=%v", checkpoint, err)
	}
	idempotent, err := store.RecordCheckpoint(ctx, CheckpointInput{
		WorkflowID: workflowID, NodeID: "root", AttemptNumber: attempt.AttemptNumber,
		ClaimToken: claimed.ClaimToken, Sequence: 0, SchemaVersion: 1, Payload: []byte(`{"step":1}`),
	})
	if err != nil || idempotent.PayloadHash != checkpoint.PayloadHash {
		t.Fatalf("idempotent checkpoint = %+v, err=%v", idempotent, err)
	}
	if _, err := store.RecordCheckpoint(ctx, CheckpointInput{
		WorkflowID: workflowID, NodeID: "root", AttemptNumber: attempt.AttemptNumber,
		ClaimToken: claimed.ClaimToken, Sequence: 2, SchemaVersion: 1, Payload: []byte(`{"step":3}`),
	}); !errors.Is(err, ErrCheckpointStale) {
		t.Fatalf("checkpoint gap error = %v, want %v", err, ErrCheckpointStale)
	}
	if _, err := store.RecordCheckpoint(ctx, CheckpointInput{
		WorkflowID: workflowID, NodeID: "root", AttemptNumber: attempt.AttemptNumber,
		ClaimToken: claimed.ClaimToken, Sequence: 0, SchemaVersion: 1, Payload: []byte(`{"step":"changed"}`),
	}); !errors.Is(err, ErrCheckpointConflict) {
		t.Fatalf("checkpoint conflict error = %v, want %v", err, ErrCheckpointConflict)
	}
	if _, err := store.RecordCheckpoint(ctx, CheckpointInput{
		WorkflowID: workflowID, NodeID: "root", AttemptNumber: attempt.AttemptNumber,
		ClaimToken: claimed.ClaimToken, Sequence: 1, SchemaVersion: 1, Payload: []byte(`{"step":2}`),
	}); err != nil {
		t.Fatal(err)
	}
	latest, err := store.GetLatestCheckpoint(ctx, workflowID, "root", 0)
	if err != nil || latest.Sequence != 1 {
		t.Fatalf("latest checkpoint = %+v, err=%v", latest, err)
	}

	policy, err := store.GetRetryPolicy(ctx, workflowID, "root", 0)
	if err != nil || policy.MaxRetries != defaultRetryBudget {
		t.Fatalf("default retry policy = %+v, err=%v", policy, err)
	}
	if err := store.SetRetryPolicy(ctx, RetryPolicyInput{
		Lease: leaseRef(lease), WorkflowID: workflowID, NodeID: "root", ExpectedRevision: 3,
		MaxRetries: 2, CheckpointSchemaVersion: 1, ActorID: "m4-scheduler",
	}); err != nil {
		t.Fatal(err)
	}
	policy, err = store.GetRetryPolicy(ctx, workflowID, "root", 0)
	if err != nil || policy.MaxRetries != 2 {
		t.Fatalf("updated retry policy = %+v, err=%v", policy, err)
	}

	approvalWorkflowID, approvalDefinitionID, approvalLease := createM4Fixture(t, ctx, store, EffectPure)
	defer cleanupM4Fixture(t, ctx, store, approvalWorkflowID, approvalDefinitionID, approvalLease, "")
	approval, err := store.CreateApprovalIntent(ctx, ApprovalIntentInput{
		Lease: leaseRef(approvalLease), WorkflowID: approvalWorkflowID, NodeID: "root", ExpectedRevision: 1,
		Target: "sandbox.write", CanonicalArguments: []byte(`{"resource":"r1","value":1}`),
		ExpectedResourceRevision: "0", ValidUntil: time.Now().Add(time.Hour), ActorID: "m4-scheduler",
	})
	if err != nil {
		t.Fatal(err)
	}
	retryApproval, err := store.CreateApprovalIntent(ctx, ApprovalIntentInput{
		Lease: leaseRef(approvalLease), WorkflowID: approvalWorkflowID, NodeID: "root", ExpectedRevision: 2,
		Target: "sandbox.write", CanonicalArguments: []byte(`{"resource":"r1","value":1}`),
		ExpectedResourceRevision: "0", ValidUntil: time.Now().Add(time.Hour), ActorID: "m4-scheduler",
	})
	if err != nil || retryApproval.IntentID != approval.IntentID {
		t.Fatalf("approval retry = %+v, err=%v", retryApproval, err)
	}
	decision, err := store.RecordApprovalDecision(ctx, ApprovalDecisionInput{
		IntentID: approval.IntentID, ApproverID: "approver-1", ProposalHash: approval.ProposalHash,
		Decision: "APPROVED", DecisionReason: "reviewed",
	})
	if err != nil || decision.Decision != "APPROVED" {
		t.Fatalf("approval decision = %+v, err=%v", decision, err)
	}
	grant, err := store.ApplyApproval(ctx, ApplyApprovalInput{
		Lease: leaseRef(approvalLease), WorkflowID: approvalWorkflowID, NodeID: "root", Iteration: 0,
		IntentID: approval.IntentID, LogicalEffectKey: "approval-effect", ExpectedRevision: 2, ActorID: "m4-scheduler",
	})
	if err != nil || grant.GrantToken == "" || grant.GrantScopeHash == "" {
		t.Fatalf("approval grant = %+v, err=%v", grant, err)
	}
	if _, err := store.ValidateApprovalGrant(ctx, GrantValidationInput{
		IntentID: grant.IntentID, GrantToken: grant.GrantToken, GrantScopeHash: grant.GrantScopeHash,
		LogicalEffectKey: "approval-effect", WorkflowID: approvalWorkflowID, NodeID: "root", Iteration: 0,
	}); err != nil {
		t.Fatalf("validate grant: %v", err)
	}
}

func TestM4EffectLedgerAndFencing(t *testing.T) {
	ctx, store := openM3Database(t)
	workflowID, definitionID, lease := createM4Fixture(t, ctx, store, EffectPure)
	resourceID := "m4-resource-" + NewID()
	defer cleanupM4Fixture(t, ctx, store, workflowID, definitionID, lease, resourceID)
	grant := createM4Grant(t, ctx, store, workflowID, lease, "effect-1", 1, "0")

	input := EffectApplyInput{WorkflowID: workflowID, LogicalEffectKey: "effect-1", ArgumentHash: "args-1",
		AttemptNumber: 1, RequestID: NewID(), ResourceID: resourceID, FenceToken: 7,
		State: []byte(`{"value":1}`), IntentID: grant.IntentID, GrantToken: grant.GrantToken, GrantScopeHash: grant.GrantScopeHash,
		ExpectedResourceRevision: "0"}
	unapproved := input
	unapproved.IntentID, unapproved.GrantToken, unapproved.GrantScopeHash = "", "", ""
	if _, err := store.ApplyEffect(ctx, unapproved); !errors.Is(err, ErrGrantInvalid) {
		t.Fatalf("unapproved effect = %v, want %v", err, ErrGrantInvalid)
	}
	receipt, err := store.ApplyEffect(ctx, input)
	if err != nil || len(receipt.Receipt) == 0 || receipt.ResourceRevision != 1 {
		t.Fatalf("first effect = %+v, err=%v", receipt, err)
	}
	duplicate := input
	duplicate.RequestID = NewID()
	duplicate.State = []byte(`{"value":999}`)
	retry, err := store.ApplyEffect(ctx, duplicate)
	if err != nil || string(retry.Receipt) != string(receipt.Receipt) {
		t.Fatalf("duplicate effect = %+v, err=%v", retry, err)
	}
	conflict := duplicate
	conflict.RequestID = NewID()
	conflict.ArgumentHash = "args-2"
	if _, err := store.ApplyEffect(ctx, conflict); !errors.Is(err, ErrEffectConflict) {
		t.Fatalf("effect conflict = %v, want %v", err, ErrEffectConflict)
	}
	grant2 := createM4Grant(t, ctx, store, workflowID, lease, "effect-2", 1, "1")
	stale := input
	stale.RequestID = NewID()
	stale.LogicalEffectKey = "effect-2"
	stale.FenceToken = 6
	stale.IntentID = grant2.IntentID
	stale.GrantToken = grant2.GrantToken
	stale.GrantScopeHash = grant2.GrantScopeHash
	stale.ExpectedResourceRevision = "1"
	if _, err := store.ApplyEffect(ctx, stale); !errors.Is(err, ErrEffectFence) {
		t.Fatalf("effect fence = %v, want %v", err, ErrEffectFence)
	}
	grant3 := createM4Grant(t, ctx, store, workflowID, lease, "effect-3", 2, "1")
	unknown, err := store.RecordUnknownEffect(ctx, workflowID, "effect-3", "args-3", grant3.GrantScopeHash, 2)
	if err != nil || unknown.Outcome != "OUTCOME_UNKNOWN" {
		t.Fatalf("unknown effect = %+v, err=%v", unknown, err)
	}
	ambiguous := input
	ambiguous.RequestID = NewID()
	ambiguous.LogicalEffectKey = "effect-3"
	ambiguous.ArgumentHash = "args-3"
	ambiguous.AttemptNumber = 2
	ambiguous.IntentID = grant3.IntentID
	ambiguous.GrantToken = grant3.GrantToken
	ambiguous.GrantScopeHash = grant3.GrantScopeHash
	ambiguous.ExpectedResourceRevision = "1"
	if _, err := store.ApplyEffect(ctx, ambiguous); !errors.Is(err, ErrAmbiguousEffect) {
		t.Fatalf("ambiguous effect = %v, want %v", err, ErrAmbiguousEffect)
	}
	if err := store.ResolveUnknownEffect(ctx, "operator-1", workflowID, "effect-3", "args-3", []byte(`{"confirmed":true}`), false); err != nil {
		t.Fatal(err)
	}
	resolved, err := store.LookupEffect(ctx, workflowID, "effect-3")
	if err != nil || resolved.Outcome != "APPLIED" || len(resolved.Receipt) == 0 {
		t.Fatalf("resolved effect = %+v, err=%v", resolved, err)
	}
	calls, err := store.ListEffectCallAttempts(ctx, workflowID)
	if err != nil || len(calls) != 5 {
		t.Fatalf("effect call evidence = %+v, err=%v", calls, err)
	}
}

func TestM4CancellationPreventsApprovalGrant(t *testing.T) {
	ctx, store := openM3Database(t)
	workflowID, definitionID, lease := createM4Fixture(t, ctx, store, EffectPure)
	defer cleanupM4Fixture(t, ctx, store, workflowID, definitionID, lease, "")

	intent, err := store.CreateApprovalIntent(ctx, ApprovalIntentInput{
		Lease: leaseRef(lease), WorkflowID: workflowID, NodeID: "root", ExpectedRevision: 1,
		Target: "sandbox.write", CanonicalArguments: []byte(`{"resource":"cancel-race"}`),
		ExpectedResourceRevision: "0", ValidUntil: time.Now().Add(time.Hour), ActorID: "scheduler",
	})
	if err != nil {
		t.Fatal(err)
	}
	request, err := store.RequestCancellation(ctx, CancellationRequestInput{
		WorkflowID: workflowID, ClientKey: "cancel-1", ObservedRevision: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordApprovalDecision(ctx, ApprovalDecisionInput{
		IntentID: intent.IntentID, ApproverID: "approver", ProposalHash: intent.ProposalHash, Decision: "APPROVED",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ApplyApproval(ctx, ApplyApprovalInput{
		Lease: leaseRef(lease), WorkflowID: workflowID, NodeID: "root", Iteration: 0,
		IntentID: intent.IntentID, LogicalEffectKey: "cancel-race-effect", ExpectedRevision: 2, ActorID: "scheduler",
	}); !errors.Is(err, ErrCancellationConflict) {
		t.Fatalf("approval after cancellation request = %v, want %v", err, ErrCancellationConflict)
	}
	workflow, err := store.ApplyCancellationRequest(ctx, leaseRef(lease), request.RequestID, "scheduler")
	if err != nil || workflow.State != StateCanceled {
		t.Fatalf("apply cancellation = %+v, err=%v", workflow, err)
	}
	var status string
	if err := store.Pool().QueryRow(ctx, `SELECT status FROM engine.cancellation_requests WHERE request_id = $1`, request.RequestID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "APPLIED" {
		t.Fatalf("cancellation status = %s, want APPLIED", status)
	}
}

func TestM4RejectedApprovalIsTerminalNoAction(t *testing.T) {
	ctx, store := openM3Database(t)
	workflowID, definitionID, lease := createM4Fixture(t, ctx, store, EffectPure)
	defer cleanupM4Fixture(t, ctx, store, workflowID, definitionID, lease, "")
	intent, err := store.CreateApprovalIntent(ctx, ApprovalIntentInput{
		Lease: leaseRef(lease), WorkflowID: workflowID, NodeID: "root", ExpectedRevision: 1,
		Target: "sandbox.write", CanonicalArguments: []byte(`{"resource":"reject"}`),
		ExpectedResourceRevision: "0", ValidUntil: time.Now().Add(time.Hour), ActorID: "scheduler",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordApprovalDecision(ctx, ApprovalDecisionInput{
		IntentID: intent.IntentID, ApproverID: "approver", ProposalHash: intent.ProposalHash, Decision: "REJECTED",
	}); err != nil {
		t.Fatal(err)
	}
	grant, err := store.ApplyApproval(ctx, ApplyApprovalInput{
		Lease: leaseRef(lease), WorkflowID: workflowID, NodeID: "root", Iteration: 0,
		IntentID: intent.IntentID, ActorID: "scheduler", ExpectedRevision: 2,
	})
	if err != nil || grant.GrantToken != "" {
		t.Fatalf("rejected approval application = %+v, err=%v", grant, err)
	}
	workflow, err := store.GetWorkflow(ctx, workflowID)
	if err != nil || workflow.State != StateRejected {
		t.Fatalf("rejected workflow = %+v, err=%v", workflow, err)
	}
}

func TestM4RetryBudgetExhaustionStopsLoop(t *testing.T) {
	ctx, store := openM3Database(t)
	workflowID, definitionID, lease := createM4Fixture(t, ctx, store, EffectPure)
	defer cleanupM4Fixture(t, ctx, store, workflowID, definitionID, lease, "")
	if err := store.ApplyOwnerTransition(ctx, OwnerTransitionInput{
		Lease: leaseRef(lease), WorkflowID: workflowID, ExpectedRevision: 1,
		NewState: StateWaitingActivity, ActorID: "scheduler", Reason: "SCHEDULE_ACTIVITY",
		NodeID: "root", NodeState: ptrState(StateWaitingActivity),
	}); err != nil {
		t.Fatal(err)
	}
	attempt, err := store.CreateAttempt(ctx, AttemptInput{
		Lease: leaseRef(lease), WorkflowID: workflowID, NodeID: "root", ExpectedRevision: 2,
		EffectClass: EffectPure, ActorID: "scheduler", HeartbeatDeadline: time.Now().Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SetRetryPolicy(ctx, RetryPolicyInput{
		Lease: leaseRef(lease), WorkflowID: workflowID, NodeID: "root", ExpectedRevision: 3,
		MaxRetries: 0, CheckpointSchemaVersion: 1, ActorID: "scheduler",
	}); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimAttempt(ctx, ClaimInput{WorkflowID: workflowID, NodeID: "root", WorkerID: "worker", RequestID: NewID(), AttemptLease: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordResultReceipt(ctx, ResultInput{WorkflowID: workflowID, NodeID: "root", Iteration: 0,
		AttemptNumber: attempt.AttemptNumber, ClaimToken: claim.ClaimToken, AttemptState: AttemptFailedRetryable,
		Payload: []byte(`{"error":"transient"}`)}); err != nil {
		t.Fatal(err)
	}
	workflow, err := store.GetWorkflow(ctx, workflowID)
	if err != nil {
		t.Fatal(err)
	}
	retryDue := time.Now().Add(time.Minute)
	consumed, err := store.ConsumeResult(ctx, ConsumeResultInput{
		Lease: leaseRef(lease), WorkflowID: workflowID, NodeID: "root", Iteration: 0,
		AttemptNumber: attempt.AttemptNumber, ExpectedRevision: workflow.Revision,
		NewWorkflowState: StateWaitingTimer, RetryDueAt: &retryDue, ActorID: "scheduler",
	})
	if err != nil || consumed.Workflow.State != StateFailed || consumed.NodeState != StateFailed {
		t.Fatalf("exhausted retry consumption = %+v, err=%v", consumed, err)
	}
}

func TestM4NonCooperatingTimeoutIsReconciliationOnly(t *testing.T) {
	ctx, store := openM3Database(t)
	workflowID, definitionID, lease := createM4Fixture(t, ctx, store, EffectNonCooperating)
	defer cleanupM4Fixture(t, ctx, store, workflowID, definitionID, lease, "")
	if err := store.ApplyOwnerTransition(ctx, OwnerTransitionInput{
		Lease: leaseRef(lease), WorkflowID: workflowID, ExpectedRevision: 1,
		NewState: StateWaitingActivity, ActorID: "scheduler", Reason: "SCHEDULE_ACTIVITY",
		NodeID: "root", NodeState: ptrState(StateWaitingActivity),
	}); err != nil {
		t.Fatal(err)
	}
	attempt, err := store.CreateAttempt(ctx, AttemptInput{
		Lease: leaseRef(lease), WorkflowID: workflowID, NodeID: "root", ExpectedRevision: 2,
		EffectClass: EffectNonCooperating, LogicalEffectKey: workflowID + ":root:0", GrantScopeHash: "scope",
		HeartbeatDeadline: time.Now().Add(time.Minute), ActorID: "scheduler",
	})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimAttempt(ctx, ClaimInput{WorkflowID: workflowID, NodeID: "root", WorkerID: "worker", RequestID: NewID(), AttemptLease: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Pool().Exec(ctx, `UPDATE engine.activity_attempts SET heartbeat_deadline = clock_timestamp() - interval '1 second' WHERE workflow_id = $1 AND node_id = 'root' AND iteration = 0 AND attempt_number = $2`, workflowID, attempt.AttemptNumber); err != nil {
		t.Fatal(err)
	}
	result, err := store.TimeoutAttempt(ctx, TimeoutInput{
		Lease: leaseRef(lease), WorkflowID: workflowID, NodeID: "root", AttemptNumber: attempt.AttemptNumber,
		ExpectedRevision: 3, ActorID: "scheduler", DispatchLease: time.Minute,
	})
	if err != nil || !result.Reconciliation || result.ReplacementNumber != nil {
		t.Fatalf("non-cooperating timeout = %+v, err=%v", result, err)
	}
	workflow, err := store.GetWorkflow(ctx, workflowID)
	if err != nil || workflow.State != StateReconciliationRequired {
		t.Fatalf("workflow after timeout = %+v, err=%v", workflow, err)
	}
	items, err := store.ListReconciliationItemsForWorkflow(ctx, workflowID, 10)
	if err != nil || len(items) != 1 || items[0].Kind != ReconcileExpiredAttempt {
		t.Fatalf("timeout obligations = %+v, err=%v", items, err)
	}
	late, err := store.RecordResultReceipt(ctx, ResultInput{
		WorkflowID: workflowID, NodeID: "root", Iteration: 0, AttemptNumber: attempt.AttemptNumber,
		ClaimToken: claim.ClaimToken, AttemptState: AttemptSucceeded, Payload: []byte(`{"applied":true}`),
	})
	if err != nil || late.Disposition != ResultRecordedAsEvidence {
		t.Fatalf("late result = %+v, err=%v", late, err)
	}
	var evidence int
	if err := store.Pool().QueryRow(ctx, `SELECT count(*) FROM engine.attempt_result_evidence WHERE workflow_id = $1`, workflowID).Scan(&evidence); err != nil {
		t.Fatal(err)
	}
	if evidence != 1 {
		t.Fatalf("late evidence rows = %d, want one", evidence)
	}
}

func createM4Grant(t *testing.T, ctx context.Context, store *Store, workflowID string, lease Lease, logicalKey string, attemptNumber int64, expectedResourceRevision string) ApprovalGrant {
	t.Helper()
	wf, err := store.GetWorkflow(ctx, workflowID)
	if err != nil {
		t.Fatal(err)
	}
	intent, err := store.CreateApprovalIntent(ctx, ApprovalIntentInput{
		Lease: leaseRef(lease), WorkflowID: workflowID, NodeID: "root", ExpectedRevision: wf.Revision,
		Target: "sandbox.write", CanonicalArguments: []byte(fmt.Sprintf(`{"logical_key":%q,"attempt":%d}`, logicalKey, attemptNumber)),
		ExpectedResourceRevision: expectedResourceRevision, ValidUntil: time.Now().Add(time.Hour), ActorID: "m4-scheduler",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordApprovalDecision(ctx, ApprovalDecisionInput{
		IntentID: intent.IntentID, ApproverID: "approver-1", ProposalHash: intent.ProposalHash, Decision: "APPROVED",
	}); err != nil {
		t.Fatal(err)
	}
	wf, err = store.GetWorkflow(ctx, workflowID)
	if err != nil {
		t.Fatal(err)
	}
	grant, err := store.ApplyApproval(ctx, ApplyApprovalInput{
		Lease: leaseRef(lease), WorkflowID: workflowID, NodeID: "root", Iteration: 0,
		IntentID: intent.IntentID, LogicalEffectKey: logicalKey, ExpectedRevision: wf.Revision, ActorID: "m4-scheduler",
	})
	if err != nil {
		t.Fatal(err)
	}
	return grant
}

func createM4Fixture(t *testing.T, ctx context.Context, store *Store, class EffectClass) (string, string, Lease) {
	t.Helper()
	var migrationVersion int
	if err := store.Pool().QueryRow(ctx, `SELECT max(version) FROM engine.schema_migrations`).Scan(&migrationVersion); err != nil {
		t.Fatal(err)
	}
	if migrationVersion < 11 {
		t.Fatalf("M4 migrations are not applied: version=%d", migrationVersion)
	}
	definitionID := "dur015-m4-" + NewID()
	if err := store.CreateDefinition(ctx, DefinitionInput{
		DefinitionID: definitionID, Version: 1, DefinitionHash: "hash-" + definitionID,
		Graph:            []byte(`{"entry":"root","nodes":[{"id":"root","kind":"activity"}]}`),
		ActivityVersions: []byte(`{"root":"1"}`), EffectClasses: []byte(fmt.Sprintf(`{"root":%q}`, class)),
	}); err != nil {
		t.Fatal(err)
	}
	ownerID := NewID()
	var lease Lease
	var acquired bool
	var err error
	for partitionID := int16(8); partitionID < 16; partitionID++ {
		lease, acquired, err = store.AcquireLease(ctx, partitionID, ownerID, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if acquired {
			break
		}
	}
	if !acquired {
		t.Fatal("could not acquire M4 fixture lease")
	}
	for attempt := 0; attempt < 200; attempt++ {
		workflowID := "dur-m4-" + NewID()
		mapped, err := partition.ID(workflowID)
		if err != nil || int16(mapped) != lease.PartitionID {
			continue
		}
		if _, err := store.CreateWorkflow(ctx, CreateWorkflowInput{
			WorkflowID: workflowID, Namespace: "dur-m4", SubmissionKey: "key-" + workflowID,
			SubmissionPayloadHash: "sub-v1:" + workflowID, DefinitionID: definitionID, DefinitionVersion: 1,
			PartitionID: lease.PartitionID, InitialNodeID: "root", InitialInput: []byte(`{}`), ActorID: "m4-client",
		}); err != nil {
			t.Fatal(err)
		}
		return workflowID, definitionID, lease
	}
	t.Fatal("could not generate workflow for M4 fixture partition")
	return "", definitionID, lease
}

func cleanupM4Fixture(t *testing.T, ctx context.Context, store *Store, workflowID, definitionID string, lease Lease, resourceID string) {
	t.Helper()
	if _, err := store.Pool().Exec(ctx, `DELETE FROM engine.workflow_executions WHERE workflow_id = $1`, workflowID); err != nil {
		t.Errorf("cleanup M4 workflow: %v", err)
	}
	if resourceID != "" {
		if _, err := store.Pool().Exec(ctx, `DELETE FROM engine.sandbox_effect_state WHERE resource_id = $1`, resourceID); err != nil {
			t.Errorf("cleanup sandbox state: %v", err)
		}
		if _, err := store.Pool().Exec(ctx, `DELETE FROM engine.effect_resource_fences WHERE resource_id = $1`, resourceID); err != nil {
			t.Errorf("cleanup effect fence: %v", err)
		}
	}
	if _, err := store.Pool().Exec(ctx, `DELETE FROM engine.workflow_definitions WHERE definition_id = $1`, definitionID); err != nil {
		t.Errorf("cleanup M4 definition: %v", err)
	}
	if err := store.ReleaseLease(ctx, leaseRef(lease)); err != nil && !errors.Is(err, ErrLeaseNotOwned) {
		t.Errorf("cleanup M4 lease: %v", err)
	}
}

func leaseRef(lease Lease) LeaseRef {
	return LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch}
}

func ptrState(state WorkflowState) *WorkflowState { return &state }
