package state

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/partition"
)

func TestDUR050AdmissionConcurrentCapsIdempotencyAndNamespaceIsolation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	store := dur050AdmissionTestStore(t, ctx, AdmissionGateConfig{MaxActiveWorkflows: 3, MaxPendingOutbox: 50000})
	namespace := "dur050-" + NewID()
	definitionID := createDur050ControlDefinition(t, ctx, store)
	var createdIDs []string
	t.Cleanup(func() { cleanupDur050Workflows(t, store, createdIDs, definitionID) })

	type outcome struct {
		input   CreateWorkflowInput
		created bool
		err     error
	}
	const submitters = 24
	results := make(chan outcome, submitters)
	var wait sync.WaitGroup
	var limitedInput CreateWorkflowInput
	for index := 0; index < submitters; index++ {
		input := dur050CreateInput(namespace, definitionID, "parallel-"+NewID())
		createdIDs = append(createdIDs, input.WorkflowID)
		wait.Add(1)
		go func(input CreateWorkflowInput) {
			defer wait.Done()
			result, err := store.CreateWorkflow(ctx, input)
			results <- outcome{input: input, created: result.Created, err: err}
		}(input)
	}
	wait.Wait()
	close(results)
	accepted := 0
	limited := 0
	var acceptedInput CreateWorkflowInput
	for result := range results {
		if result.err == nil {
			if !result.created {
				t.Fatalf("unique submission %q unexpectedly resolved as a retry", result.input.SubmissionKey)
			}
			accepted++
			acceptedInput = result.input
		} else if errors.Is(result.err, ErrAdmissionLimit) {
			limited++
			if limitedInput.WorkflowID == "" {
				limitedInput = result.input
			}
		} else {
			t.Fatalf("concurrent admission error = %v", result.err)
		}
	}
	if accepted != 3 || limited != submitters-3 {
		t.Fatalf("concurrent active cap accepted=%d limited=%d; want 3 and %d", accepted, limited, submitters-3)
	}
	duplicate, err := store.CreateWorkflow(ctx, acceptedInput)
	if err != nil || duplicate.Created || duplicate.Workflow.WorkflowID != acceptedInput.WorkflowID {
		t.Fatalf("idempotent retry at cap = %+v, err=%v; want original workflow", duplicate, err)
	}

	acceptedWorkflow, err := store.GetWorkflow(ctx, acceptedInput.WorkflowID)
	if err != nil {
		t.Fatal(err)
	}
	ownerID := NewID()
	lease, acquired, err := store.AcquireLease(ctx, acceptedWorkflow.PartitionID, ownerID, time.Minute)
	if err != nil || !acquired {
		t.Fatalf("acquire lease to free active slot: lease=%+v acquired=%t err=%v", lease, acquired, err)
	}
	defer func() {
		_ = store.ReleaseLease(context.Background(), LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch})
	}()
	if _, err := store.AdvanceGraph(ctx, AdvanceGraphInput{Lease: LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch},
		WorkflowID: acceptedWorkflow.WorkflowID, FromNodeID: "root", ExpectedRevision: acceptedWorkflow.Revision,
		FinalWorkflowState: StateSucceeded, FinalNodeState: StateSucceeded, ActorID: "dur050-test"}); err != nil {
		t.Fatalf("finish accepted workflow to free slot: %v", err)
	}
	freedRetry, err := store.CreateWorkflow(ctx, limitedInput)
	if err != nil || !freedRetry.Created {
		t.Fatalf("retry after slot freed = %+v, err=%v; want one newly accepted workflow", freedRetry, err)
	}
	freedDuplicate, err := store.CreateWorkflow(ctx, limitedInput)
	if err != nil || freedDuplicate.Created || freedDuplicate.Workflow.WorkflowID != limitedInput.WorkflowID {
		t.Fatalf("retry of newly accepted key = %+v, err=%v; want original workflow", freedDuplicate, err)
	}

	// A different namespace family does not use or consume DUR-050 slots.
	nonCampaign := dur050CreateInput("ordinary-"+NewID(), definitionID, "outside-gate")
	createdIDs = append(createdIDs, nonCampaign.WorkflowID)
	if result, err := store.CreateWorkflow(ctx, nonCampaign); err != nil || !result.Created {
		t.Fatalf("non-DUR-050 submission was affected by campaign cap: result=%+v err=%v", result, err)
	}
}

func TestDUR050AdmissionPendingOutboxCapIsConcurrentAndScoped(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	store := dur050AdmissionTestStore(t, ctx, AdmissionGateConfig{MaxActiveWorkflows: 100, MaxPendingOutbox: 2})
	namespace := "dur050-" + NewID()
	definitionID := createDur050ControlDefinition(t, ctx, store)
	var createdIDs []string
	t.Cleanup(func() { cleanupDur050Workflows(t, store, createdIDs, definitionID) })
	type outcome struct {
		input CreateWorkflowInput
		err   error
	}
	const submitters = 12
	results := make(chan outcome, submitters)
	var wait sync.WaitGroup
	for index := 0; index < submitters; index++ {
		input := dur050CreateInput(namespace, definitionID, "outbox-"+NewID())
		createdIDs = append(createdIDs, input.WorkflowID)
		wait.Add(1)
		go func(input CreateWorkflowInput) {
			defer wait.Done()
			result, err := store.CreateWorkflow(ctx, input)
			if err == nil && !result.Created {
				err = errors.New("unique submission unexpectedly became a retry")
			}
			results <- outcome{input: input, err: err}
		}(input)
	}
	wait.Wait()
	close(results)
	accepted, backpressured := 0, 0
	var acceptedInputs []CreateWorkflowInput
	for result := range results {
		if result.err == nil {
			accepted++
			acceptedInputs = append(acceptedInputs, result.input)
		} else if errors.Is(result.err, ErrAdmissionBackpressure) {
			backpressured++
		} else {
			t.Fatalf("concurrent outbox cap error = %v", result.err)
		}
	}
	if accepted != 2 || backpressured != submitters-2 {
		t.Fatalf("concurrent pending-event cap accepted=%d backpressured=%d; want 2 and %d", accepted, backpressured, submitters-2)
	}
	duplicate, err := store.CreateWorkflow(ctx, acceptedInputs[0])
	if err != nil || duplicate.Created || duplicate.Workflow.WorkflowID != acceptedInputs[0].WorkflowID {
		t.Fatalf("idempotent retry at pending-event cap = %+v, err=%v; want original workflow", duplicate, err)
	}
	lease, acquired, err := store.AcquireLease(ctx, acceptedInputs[0].PartitionID, NewID(), time.Minute)
	if err != nil || !acquired {
		t.Fatalf("acquire lease for full-outbox transition: lease=%+v acquired=%t err=%v", lease, acquired, err)
	}
	defer func() {
		_ = store.ReleaseLease(context.Background(), LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch})
	}()
	waiting := StateWaitingActivity
	if err := store.ApplyOwnerTransition(ctx, OwnerTransitionInput{Lease: LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch},
		WorkflowID: acceptedInputs[0].WorkflowID, ExpectedRevision: 1, NewState: StateWaitingActivity,
		NodeID: "root", NodeState: &waiting, ActorID: "dur050-test", Reason: "TEST_PENDING_OUTBOX_CAP"}); err != nil {
		t.Fatalf("move accepted workflow to activity wait: %v", err)
	}
	if _, err := store.CreateAttempt(ctx, AttemptInput{Lease: LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch},
		WorkflowID: acceptedInputs[0].WorkflowID, NodeID: "root", ExpectedRevision: 2, EffectClass: EffectPure,
		HeartbeatDeadline: time.Now().Add(time.Minute), ActorID: "dur050-test"}); !errors.Is(err, ErrAdmissionBackpressure) {
		t.Fatalf("event insertion at pending-outbox cap = %v, want ErrAdmissionBackpressure", err)
	}
	unchanged, err := store.GetWorkflow(ctx, acceptedInputs[0].WorkflowID)
	if err != nil || unchanged.State != StateWaitingActivity || unchanged.Revision != 2 {
		t.Fatalf("workflow changed despite rejected outbox insertion: %+v err=%v", unchanged, err)
	}
	var attempts, pending int
	if err := store.Pool().QueryRow(ctx, `SELECT count(*) FROM engine.activity_attempts WHERE workflow_id = $1`, acceptedInputs[0].WorkflowID).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if err := store.Pool().QueryRow(ctx, `SELECT count(*) FROM engine.outbox o JOIN engine.workflow_executions w USING (workflow_id)
		WHERE w.namespace = $1 AND o.publish_state IN ('PENDING','CLAIMED')`, namespace).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if attempts != 0 || pending != 2 {
		t.Fatalf("backpressured event was partially committed: attempts=%d pending=%d; want 0 and 2", attempts, pending)
	}

	// The same pending-event pressure in another namespace cannot block an
	// ordinary workflow or include its row in DUR-050's namespace count.
	outside := dur050CreateInput("ordinary-"+NewID(), definitionID, "outside-pending-cap")
	createdIDs = append(createdIDs, outside.WorkflowID)
	if result, err := store.CreateWorkflow(ctx, outside); err != nil || !result.Created {
		t.Fatalf("non-DUR-050 namespace was affected by pending cap: result=%+v err=%v", result, err)
	}
}

func TestDUR050AdmissionSlotsReleaseOnTerminalPathsAndRemainDuringReconciliation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	store := dur050AdmissionTestStore(t, ctx, AdmissionGateConfig{MaxActiveWorkflows: 1, MaxPendingOutbox: 50000})
	definitionID := createDur050ControlDefinition(t, ctx, store)
	lease, acquired, err := acquireTestLease(ctx, store, NewID())
	if err != nil || !acquired {
		t.Fatalf("acquire test lease = %+v acquired=%t err=%v", lease, acquired, err)
	}
	defer func() {
		_ = store.ReleaseLease(context.Background(), LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch})
	}()
	namespace := "dur050-" + NewID()
	var createdIDs []string
	t.Cleanup(func() { cleanupDur050Workflows(t, store, createdIDs, definitionID) })

	createOnLease := func(key string) Workflow {
		t.Helper()
		input := dur050CreateInputForPartition(t, namespace, definitionID, key, lease.PartitionID)
		createdIDs = append(createdIDs, input.WorkflowID)
		result, err := store.CreateWorkflow(ctx, input)
		if err != nil || !result.Created {
			t.Fatalf("create workflow %q: result=%+v err=%v", key, result, err)
		}
		return result.Workflow
	}
	assertActiveSlots := func(want int) {
		t.Helper()
		var got int
		if err := store.Pool().QueryRow(ctx, `SELECT count(*) FROM engine.dur050_admission_slots
			WHERE namespace = $1`, namespace).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("active admission slots = %d, want %d", got, want)
		}
	}
	assertTerminal := func(workflow Workflow, expected WorkflowState) {
		t.Helper()
		stored, err := store.GetWorkflow(ctx, workflow.WorkflowID)
		if err != nil || stored.State != expected {
			t.Fatalf("terminal workflow = %+v, err=%v; want %s", stored, err, expected)
		}
		var slotExists bool
		if err := store.Pool().QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM engine.dur050_admission_slots WHERE workflow_id = $1)`, workflow.WorkflowID).Scan(&slotExists); err != nil {
			t.Fatal(err)
		}
		if slotExists {
			t.Fatalf("%s workflow retained its admission slot after terminal transition", expected)
		}
		assertActiveSlots(0)
	}

	succeeded := createOnLease("succeeded-result")
	if err := store.ApplyOwnerTransition(ctx, OwnerTransitionInput{Lease: LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch},
		WorkflowID: succeeded.WorkflowID, ExpectedRevision: succeeded.Revision, NewState: StateWaitingActivity,
		NodeID: "root", NodeState: ptrState(StateWaitingActivity), ActorID: "dur050-test", Reason: "TEST_SCHEDULE_ACTIVITY"}); err != nil {
		t.Fatalf("schedule succeeded activity: %v", err)
	}
	attempt, err := store.CreateAttempt(ctx, AttemptInput{Lease: LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch},
		WorkflowID: succeeded.WorkflowID, NodeID: "root", ExpectedRevision: succeeded.Revision + 1,
		EffectClass: EffectPure, HeartbeatDeadline: time.Now().Add(time.Minute), ActorID: "dur050-test"})
	if err != nil {
		t.Fatalf("create succeeded activity attempt: %v", err)
	}
	claim, err := store.ClaimAttempt(ctx, ClaimInput{WorkflowID: succeeded.WorkflowID, NodeID: "root", WorkerID: "dur050-worker", RequestID: NewID()})
	if err != nil {
		t.Fatalf("claim succeeded activity attempt: %v", err)
	}
	if _, err := store.RecordResultReceipt(ctx, ResultInput{WorkflowID: succeeded.WorkflowID, NodeID: "root",
		AttemptNumber: attempt.AttemptNumber, ClaimToken: claim.ClaimToken, AttemptState: AttemptSucceeded,
		Payload: []byte(`{"ok":true}`)}); err != nil {
		t.Fatalf("record succeeded activity result: %v", err)
	}
	if _, err := store.ConsumeResult(ctx, ConsumeResultInput{Lease: LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch},
		WorkflowID: succeeded.WorkflowID, NodeID: "root", AttemptNumber: attempt.AttemptNumber,
		ExpectedRevision: succeeded.Revision + 3, NewWorkflowState: StateSucceeded, ActorID: "dur050-test"}); err != nil {
		t.Fatalf("consume succeeded activity result: %v", err)
	}
	assertTerminal(succeeded, StateSucceeded)

	graphSucceeded := createOnLease("succeeded-graph")
	if _, err := store.AdvanceGraph(ctx, AdvanceGraphInput{Lease: LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch}, WorkflowID: graphSucceeded.WorkflowID,
		FromNodeID: "root", ExpectedRevision: graphSucceeded.Revision, FinalWorkflowState: StateSucceeded,
		FinalNodeState: StateSucceeded, ActorID: "dur050-test"}); err != nil {
		t.Fatalf("advance graph to SUCCEEDED: %v", err)
	}
	assertTerminal(graphSucceeded, StateSucceeded)

	failed := createOnLease("failed")
	failedNode := StateFailed
	if err := store.ApplyOwnerTransition(ctx, OwnerTransitionInput{Lease: LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch}, WorkflowID: failed.WorkflowID,
		ExpectedRevision: failed.Revision, NewState: StateFailed, NodeID: "root", NodeState: &failedNode,
		ActorID: "dur050-test", Reason: "TEST_FINAL_FAILURE"}); err != nil {
		t.Fatalf("transition FAILED workflow: %v", err)
	}
	assertTerminal(failed, StateFailed)

	canceled := createOnLease("canceled")
	if _, err := store.CancelWorkflow(ctx, CancelWorkflowInput{Lease: LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch}, WorkflowID: canceled.WorkflowID,
		ExpectedRevision: canceled.Revision, ActorID: "dur050-test"}); err != nil {
		t.Fatalf("cancel workflow: %v", err)
	}
	assertTerminal(canceled, StateCanceled)

	recon := createOnLease("reconciliation")
	waiting := StateWaitingActivity
	if err := store.ApplyOwnerTransition(ctx, OwnerTransitionInput{Lease: LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch}, WorkflowID: recon.WorkflowID,
		ExpectedRevision: 1, NewState: StateWaitingActivity, NodeID: "root", NodeState: &waiting,
		ActorID: "dur050-test", Reason: "TEST_ACTIVITY_STARTED"}); err != nil {
		t.Fatal(err)
	}
	reconciliation := StateReconciliationRequired
	if err := store.ApplyOwnerTransition(ctx, OwnerTransitionInput{Lease: LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch}, WorkflowID: recon.WorkflowID,
		ExpectedRevision: 2, NewState: StateReconciliationRequired, NodeID: "root", NodeState: &reconciliation,
		ActorID: "dur050-test", Reason: "TEST_UNCERTAIN_EFFECT"}); err != nil {
		t.Fatal(err)
	}
	assertActiveSlots(1)
	blocked := dur050CreateInputForPartition(t, namespace, definitionID, "blocked-while-reconciling", lease.PartitionID)
	if _, err := store.CreateWorkflow(ctx, blocked); !errors.Is(err, ErrAdmissionLimit) {
		t.Fatalf("submission while RECONCILIATION_REQUIRED holds slot = %v, want ErrAdmissionLimit", err)
	}
	if _, err := store.CancelWorkflow(ctx, CancelWorkflowInput{Lease: LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch}, WorkflowID: recon.WorkflowID,
		ExpectedRevision: 3, ActorID: "dur050-test"}); err != nil {
		t.Fatalf("terminal operator resolution from reconciliation: %v", err)
	}
	assertTerminal(recon, StateCanceled)

	rejected := createOnLease("rejected")
	intent, err := store.CreateApprovalIntent(ctx, ApprovalIntentInput{
		Lease:      LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch},
		WorkflowID: rejected.WorkflowID, NodeID: "root", ExpectedRevision: rejected.Revision,
		Target: "sandbox.write", CanonicalArguments: []byte(`{"resource":"dur050-reject"}`),
		ExpectedResourceRevision: "0", ValidUntil: time.Now().Add(time.Hour), ActorID: "dur050-test",
	})
	if err != nil {
		t.Fatalf("create rejected approval intent: %v", err)
	}
	if _, err := store.RecordApprovalDecision(ctx, ApprovalDecisionInput{
		IntentID: intent.IntentID, ApproverID: "dur050-approver", ProposalHash: intent.ProposalHash,
		Decision: "REJECTED",
	}); err != nil {
		t.Fatalf("record rejected approval decision: %v", err)
	}
	if _, err := store.ApplyApproval(ctx, ApplyApprovalInput{
		Lease:      LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch},
		WorkflowID: rejected.WorkflowID, NodeID: "root", Iteration: 0,
		IntentID: intent.IntentID, ExpectedRevision: rejected.Revision + 1, ActorID: "dur050-test",
	}); err != nil {
		t.Fatalf("apply rejected approval: %v", err)
	}
	assertTerminal(rejected, StateRejected)

	cancelRequested := createOnLease("cancel-request")
	request, err := store.RequestCancellation(ctx, CancellationRequestInput{WorkflowID: cancelRequested.WorkflowID,
		ClientKey: "dur050-cancel-" + NewID(), ObservedRevision: cancelRequested.Revision})
	if err != nil {
		t.Fatalf("request workflow cancellation: %v", err)
	}
	if _, err := store.ApplyCancellationRequest(ctx, LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch}, request.RequestID, "dur050-test"); err != nil {
		t.Fatalf("apply cancellation request: %v", err)
	}
	assertTerminal(cancelRequested, StateCanceled)

	abandoned := createOnLease("abandoned")
	if err := store.ApplyOwnerTransition(ctx, OwnerTransitionInput{Lease: LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch}, WorkflowID: abandoned.WorkflowID,
		ExpectedRevision: abandoned.Revision, NewState: StateWaitingActivity, NodeID: "root", NodeState: &waiting,
		ActorID: "dur050-test", Reason: "TEST_ABANDONMENT_ACTIVITY"}); err != nil {
		t.Fatal(err)
	}
	if err := store.ApplyOwnerTransition(ctx, OwnerTransitionInput{Lease: LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch}, WorkflowID: abandoned.WorkflowID,
		ExpectedRevision: abandoned.Revision + 1, NewState: StateReconciliationRequired, NodeID: "root", NodeState: &reconciliation,
		ActorID: "dur050-test", Reason: "TEST_ABANDONMENT_UNCERTAIN_EFFECT"}); err != nil {
		t.Fatal(err)
	}
	assertActiveSlots(1)
	if err := store.ApplyOwnerTransition(ctx, OwnerTransitionInput{Lease: LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch}, WorkflowID: abandoned.WorkflowID,
		ExpectedRevision: abandoned.Revision + 2, NewState: StateAbandoned,
		ActorID: "dur050-test", Reason: "TEST_OPERATOR_ABANDONMENT"}); err != nil {
		t.Fatalf("abandon reconciled workflow: %v", err)
	}
	assertTerminal(abandoned, StateAbandoned)
}

func TestDUR050AdmissionTerminalStateAndSlotReleaseRollbackTogether(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	store := dur050AdmissionTestStore(t, ctx, AdmissionGateConfig{MaxActiveWorkflows: 2, MaxPendingOutbox: 50000})
	definitionID := createDur050ControlDefinition(t, ctx, store)
	lease, acquired, err := acquireTestLease(ctx, store, NewID())
	if err != nil || !acquired {
		t.Fatalf("acquire test lease = %+v acquired=%t err=%v", lease, acquired, err)
	}
	defer func() {
		_ = store.ReleaseLease(context.Background(), LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch})
	}()
	namespace := "dur050-" + NewID()
	workflowInput := dur050CreateInputForPartition(t, namespace, definitionID, "fault-boundary", lease.PartitionID)
	workflowResult, err := store.CreateWorkflow(ctx, workflowInput)
	if err != nil {
		t.Fatal(err)
	}
	workflowID := workflowResult.Workflow.WorkflowID
	triggerSuffix := strings.ReplaceAll(NewID(), "-", "")
	triggerName, functionName := "dur050_fail_release_"+triggerSuffix, "dur050_fail_fn_"+triggerSuffix
	triggerSQL := fmt.Sprintf(`CREATE FUNCTION engine.%s() RETURNS trigger LANGUAGE plpgsql AS $dur050$
BEGIN
  IF OLD.workflow_id = '%s' THEN
    RAISE EXCEPTION 'DUR050 injected slot-release fault';
  END IF;
  RETURN OLD;
END
$dur050$;
CREATE TRIGGER %s BEFORE DELETE ON engine.dur050_admission_slots
FOR EACH ROW EXECUTE FUNCTION engine.%s()`, functionName, workflowID, triggerName, functionName)
	if _, err := store.Pool().Exec(ctx, triggerSQL); err != nil {
		t.Fatalf("install terminal-boundary fault trigger: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = store.Pool().Exec(cleanupCtx, fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON engine.dur050_admission_slots; DROP FUNCTION IF EXISTS engine.%s()", triggerName, functionName))
	})
	failedNode := StateFailed
	err = store.ApplyOwnerTransition(ctx, OwnerTransitionInput{Lease: LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch}, WorkflowID: workflowID,
		ExpectedRevision: 1, NewState: StateFailed, NodeID: "root", NodeState: &failedNode,
		ActorID: "dur050-test", Reason: "INJECTED_TERMINAL_RELEASE_FAILURE"})
	if err == nil || !strings.Contains(err.Error(), "DUR050 injected slot-release fault") {
		t.Fatalf("faulted terminal transition error = %v, want injected release failure", err)
	}
	stored, err := store.GetWorkflow(ctx, workflowID)
	if err != nil || stored.State != StateRunnable || stored.Revision != 1 {
		t.Fatalf("workflow did not roll back with slot release: %+v err=%v", stored, err)
	}
	var slotExists bool
	if err := store.Pool().QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM engine.dur050_admission_slots WHERE workflow_id = $1)`, workflowID).Scan(&slotExists); err != nil {
		t.Fatal(err)
	}
	if !slotExists {
		t.Fatal("slot deletion survived failed terminal transition")
	}
	if _, err := store.Pool().Exec(ctx, fmt.Sprintf("DROP TRIGGER %s ON engine.dur050_admission_slots; DROP FUNCTION engine.%s()", triggerName, functionName)); err != nil {
		t.Fatal(err)
	}
	if err := store.ApplyOwnerTransition(ctx, OwnerTransitionInput{Lease: LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch}, WorkflowID: workflowID,
		ExpectedRevision: 1, NewState: StateFailed, NodeID: "root", NodeState: &failedNode,
		ActorID: "dur050-test", Reason: "RETRY_AFTER_INJECTED_FAULT"}); err != nil {
		t.Fatalf("terminal retry after fault removal: %v", err)
	}
	stored, err = store.GetWorkflow(ctx, workflowID)
	if err != nil || stored.State != StateFailed || stored.Revision != 2 {
		t.Fatalf("retried terminal transition = %+v err=%v", stored, err)
	}
	if err := store.Pool().QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM engine.dur050_admission_slots WHERE workflow_id = $1)`, workflowID).Scan(&slotExists); err != nil || slotExists {
		t.Fatalf("successful retry retained admission slot: exists=%t err=%v", slotExists, err)
	}
	cleanupDur050Workflows(t, store, []string{workflowID}, definitionID)
}

func dur050AdmissionTestStore(t *testing.T, ctx context.Context, config AdmissionGateConfig) *Store {
	t.Helper()
	store, err := NewFromURL(ctx, integrationDatabaseURL(t))
	if err != nil {
		t.Fatalf("open PostgreSQL: %v", err)
	}
	store.dur050Admission = config
	t.Cleanup(func() { store.Close() })
	var migration int
	if err := store.Pool().QueryRow(ctx, `SELECT max(version) FROM engine.schema_migrations`).Scan(&migration); err != nil {
		t.Fatal(err)
	}
	if migration < 17 {
		t.Fatalf("DUR-050 admission migration not applied: database version=%d", migration)
	}
	return store
}

func createDur050ControlDefinition(t *testing.T, ctx context.Context, store *Store) string {
	t.Helper()
	definitionID := "dur050-def-" + NewID()
	err := store.CreateDefinition(ctx, DefinitionInput{DefinitionID: definitionID, Version: 1,
		DefinitionHash: "dur050-control-v1", Graph: []byte(`{"entry":"root","nodes":[{"id":"root","kind":"control"}]}`),
		ActivityVersions: []byte(`{}`), EffectClasses: []byte(`{"root":"PURE_ACTIVITY"}`)})
	if err != nil {
		t.Fatalf("create DUR-050 definition: %v", err)
	}
	return definitionID
}

func dur050CreateInput(namespace, definitionID, key string) CreateWorkflowInput {
	workflowID := "dur050-wf-" + NewID()
	partitionID, _ := partition.ID(workflowID)
	return CreateWorkflowInput{WorkflowID: workflowID, Namespace: namespace,
		SubmissionKey: key, SubmissionPayloadHash: "payload-v1", DefinitionID: definitionID,
		DefinitionVersion: 1, PartitionID: int16(partitionID), InitialNodeID: "root",
		InitialInput: []byte(`{}`), ActorID: "dur050-test"}
}

func dur050CreateInputForPartition(t *testing.T, namespace, definitionID, key string, want int16) CreateWorkflowInput {
	t.Helper()
	for attempt := 0; attempt < 10000; attempt++ {
		input := dur050CreateInput(namespace, definitionID, key+"-"+NewID())
		if input.PartitionID == want {
			return input
		}
	}
	t.Fatal("could not generate workflow ID for lease partition")
	return CreateWorkflowInput{}
}

func cleanupDur050Workflows(t *testing.T, store *Store, workflowIDs []string, definitionID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, workflowID := range workflowIDs {
		if _, err := store.Pool().Exec(ctx, `DELETE FROM engine.workflow_executions WHERE workflow_id = $1`, workflowID); err != nil {
			t.Errorf("cleanup DUR-050 workflow %s: %v", workflowID, err)
		}
	}
	if definitionID != "" {
		if _, err := store.Pool().Exec(ctx, `DELETE FROM engine.workflow_definitions WHERE definition_id = $1`, definitionID); err != nil {
			t.Errorf("cleanup DUR-050 definition %s: %v", definitionID, err)
		}
	}
}
