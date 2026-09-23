// dur043-stale-probe is a campaign-only diagnostic. It attempts one durable
// owner transition with a lease identity captured before a takeover and exits
// successfully only when the repository rejects that superseded identity
// without changing the workflow revision.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"durable-agent-execution-engine/internal/partition"
	"durable-agent-execution-engine/internal/state"
)

type probeResult struct {
	WorkflowID          string `json:"workflow_id"`
	PartitionID         int16  `json:"partition_id"`
	OwnerID             string `json:"owner_id"`
	Epoch               int64  `json:"epoch"`
	BeforeRevision      int64  `json:"before_revision"`
	AfterRevision       int64  `json:"after_revision"`
	Error               string `json:"error"`
	StaleRejected       bool   `json:"stale_rejected"`
	ResultAttemptNumber int64  `json:"result_attempt_number,omitempty"`
	ResultError         string `json:"result_error,omitempty"`
	ResultRejected      bool   `json:"result_rejected,omitempty"`
}

type sameOwnerEpochResult struct {
	Status                      string `json:"status"`
	WorkflowID                  string `json:"workflow_id"`
	PartitionID                 int16  `json:"partition_id"`
	OwnerID                     string `json:"owner_id"`
	StaleEpoch                  int64  `json:"stale_epoch"`
	CurrentEpoch                int64  `json:"current_epoch"`
	SameOwner                   bool   `json:"same_owner"`
	BeforeRevision              int64  `json:"before_revision"`
	AfterRevision               int64  `json:"after_revision"`
	ProbeWorkflowRetained       bool   `json:"probe_workflow_retained"`
	ProbeWorkflowFinalState     string `json:"probe_workflow_final_state,omitempty"`
	ProbeOutboxSuppressed       bool   `json:"probe_outbox_suppressed"`
	ProbeOutboxRowsObserved     int64  `json:"probe_outbox_rows_observed"`
	ProbeOutboxRowsAfterCleanup int64  `json:"probe_outbox_rows_after_cleanup"`
	Error                       string `json:"error"`
}

func main() {
	sameOwnerEpoch := flag.Bool("same-owner-stale-epoch", false, "create a disposable workflow and verify a prior epoch from the same current owner is fenced")
	workflowID := flag.String("workflow-id", "", "workflow to probe")
	partitionID := flag.Int("partition-id", -1, "partition captured before takeover")
	ownerID := flag.String("owner-id", "", "owner captured before takeover")
	epoch := flag.Int64("epoch", 0, "epoch captured before takeover")
	nodeID := flag.String("node-id", "", "superseded attempt node for an optional late-result probe")
	iteration := flag.Int("iteration", 0, "superseded attempt iteration for an optional late-result probe")
	attemptNumber := flag.Int64("attempt-number", 0, "superseded attempt number for an optional late-result probe")
	claimToken := flag.String("claim-token", "", "superseded attempt claim token for an optional late-result probe")
	flag.Parse()
	if *workflowID == "" {
		fatal("workflow-id is required")
	}
	if *sameOwnerEpoch && !campaignOutboxSuppressionEnabled() {
		fatal("same-owner stale-epoch probe requires the campaign-only outbox-suppression build")
	}
	if !*sameOwnerEpoch && (*partitionID < 0 || *ownerID == "" || *epoch <= 0) {
		fatal("partition-id, owner-id, and a positive epoch are required")
	}
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		fatal("DATABASE_URL is required")
	}
	timeout := 15 * time.Second
	if *sameOwnerEpoch {
		timeout = 3 * time.Minute
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	store, err := state.NewFromURL(ctx, databaseURL)
	if err != nil {
		fatal(err.Error())
	}
	defer store.Close()
	if *sameOwnerEpoch {
		result, err := runSameOwnerStaleEpoch(ctx, store, *workflowID)
		if err != nil {
			_ = json.NewEncoder(os.Stdout).Encode(sameOwnerEpochResult{Status: "FAIL", WorkflowID: *workflowID, Error: err.Error()})
			fatal(err.Error())
		}
		if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
			fatal(err.Error())
		}
		if result.Status != "PASS" {
			store.Close()
			os.Exit(1)
		}
		return
	}

	workflow, err := store.GetWorkflow(ctx, *workflowID)
	if err != nil {
		fatal(err.Error())
	}
	result := probeResult{
		WorkflowID:     *workflowID,
		PartitionID:    int16(*partitionID),
		OwnerID:        *ownerID,
		Epoch:          *epoch,
		BeforeRevision: workflow.Revision,
	}
	err = store.ApplyOwnerTransition(ctx, state.OwnerTransitionInput{
		Lease:            state.LeaseRef{PartitionID: int16(*partitionID), OwnerID: *ownerID, Epoch: *epoch},
		WorkflowID:       workflow.WorkflowID,
		ExpectedRevision: workflow.Revision,
		NewState:         state.StateCanceled,
		ActorID:          "dur043-stale-probe",
		Reason:           "DUR043_STALE_OWNER_PROBE",
	})
	result.Error = fmt.Sprint(err)
	result.StaleRejected = errors.Is(err, state.ErrLeaseNotOwned)
	workflowAfter, readErr := store.GetWorkflow(ctx, *workflowID)
	if readErr != nil {
		fatal(readErr.Error())
	}
	result.AfterRevision = workflowAfter.Revision
	if !result.StaleRejected || result.BeforeRevision != result.AfterRevision {
		fatal("stale owner transition was not rejected without a durable revision change")
	}
	if *nodeID != "" || *attemptNumber != 0 || *claimToken != "" {
		if *nodeID == "" || *attemptNumber <= 0 || *claimToken == "" {
			fatal("node-id, positive attempt-number, and claim-token are required together")
		}
		result.ResultAttemptNumber = *attemptNumber
		_, resultErr := store.RecordResultReceipt(ctx, state.ResultInput{
			WorkflowID: workflow.WorkflowID, NodeID: *nodeID, Iteration: *iteration,
			AttemptNumber: *attemptNumber, ClaimToken: *claimToken,
			AttemptState: state.AttemptSucceeded, Payload: json.RawMessage(`{"dur043":"late-result"}`),
		})
		result.ResultError = fmt.Sprint(resultErr)
		result.ResultRejected = errors.Is(resultErr, state.ErrAttemptNotCurrent) || errors.Is(resultErr, state.ErrLeaseNotOwned)
		if !result.ResultRejected {
			fatal("superseded attempt result was not rejected: " + result.ResultError)
		}
	}
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		fatal(err.Error())
	}
}

func runSameOwnerStaleEpoch(ctx context.Context, store *state.Store, workflowID string) (result sameOwnerEpochResult, returnErr error) {
	partitionValue, err := partition.ID(workflowID)
	if err != nil {
		return sameOwnerEpochResult{}, fmt.Errorf("map probe workflow partition: %w", err)
	}
	if partitionValue >= partition.PartitionCount {
		return sameOwnerEpochResult{}, fmt.Errorf("probe workflow mapped outside the frozen %d-partition map: %d", partition.PartitionCount, partitionValue)
	}
	partitionNumber := int16(partitionValue)
	result = sameOwnerEpochResult{Status: "FAIL", WorkflowID: workflowID, PartitionID: partitionNumber}
	ownerID := state.NewID()
	first, err := acquireProbeLease(ctx, store, partitionNumber, ownerID)
	if err != nil {
		return result, fmt.Errorf("acquire initial probe lease: %w", err)
	}
	staleRef := state.LeaseRef{PartitionID: first.PartitionID, OwnerID: first.OwnerID, Epoch: first.Epoch}
	if err := store.ReleaseLease(ctx, staleRef); err != nil {
		return result, fmt.Errorf("release initial probe lease: %w", err)
	}
	current, err := acquireProbeLease(ctx, store, partitionNumber, ownerID)
	if err != nil {
		return result, fmt.Errorf("reacquire probe lease: %w", err)
	}
	currentRef := state.LeaseRef{PartitionID: current.PartitionID, OwnerID: current.OwnerID, Epoch: current.Epoch}
	result.OwnerID = ownerID
	result.StaleEpoch = first.Epoch
	result.CurrentEpoch = current.Epoch
	result.SameOwner = first.OwnerID == current.OwnerID
	if !result.SameOwner || current.Epoch <= first.Epoch {
		_ = store.ReleaseLease(ctx, currentRef)
		return result, fmt.Errorf("probe did not establish the same owner at a strictly newer epoch: first=%+v current=%+v", first, current)
	}

	workflowCreated := false
	probeCtx := campaignProbeContext(ctx)
	cleanup := func() error {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		cleanupProbeCtx := campaignProbeContext(cleanupCtx)
		if workflowCreated {
			workflow, err := store.GetWorkflow(cleanupCtx, workflowID)
			if err != nil {
				return fmt.Errorf("read probe workflow for terminal cleanup: %w", err)
			}
			if !isTerminalProbeState(workflow.State) {
				if err := store.ApplyOwnerTransition(cleanupProbeCtx, state.OwnerTransitionInput{
					Lease: currentRef, WorkflowID: workflowID, ExpectedRevision: workflow.Revision,
					NewState: state.StateCanceled, ActorID: "dur049-epoch-probe",
					Reason: "DUR049_SAME_OWNER_STALE_EPOCH_PROBE_CLEANUP",
				}); err != nil {
					return fmt.Errorf("terminalize probe workflow without deleting its durable identity: %w", err)
				}
			}
			workflow, err = store.GetWorkflow(cleanupCtx, workflowID)
			if err != nil {
				return fmt.Errorf("verify terminal probe workflow: %w", err)
			}
			if workflow.State != state.StateCanceled {
				return fmt.Errorf("probe workflow final state = %s, want CANCELED", workflow.State)
			}
			if err := store.Pool().QueryRow(cleanupCtx, `SELECT count(*) FROM engine.outbox WHERE workflow_id = $1`, workflowID).Scan(&result.ProbeOutboxRowsAfterCleanup); err != nil {
				return fmt.Errorf("verify cleanup emitted no outbox rows: %w", err)
			}
			if result.ProbeOutboxRowsAfterCleanup != 0 {
				return fmt.Errorf("terminal probe cleanup left %d outbox rows, want 0", result.ProbeOutboxRowsAfterCleanup)
			}
			result.ProbeWorkflowRetained = true
			result.ProbeWorkflowFinalState = string(workflow.State)
			result.ProbeOutboxSuppressed = true
		}
		if err := store.ReleaseLease(cleanupCtx, currentRef); err != nil && !errors.Is(err, state.ErrLeaseNotOwned) {
			return fmt.Errorf("release current probe lease: %w", err)
		}
		return nil
	}
	defer func() {
		if err := cleanup(); err != nil && returnErr == nil {
			returnErr = err
		}
	}()

	digest := sha256.Sum256([]byte(workflowID))
	created, err := store.CreateWorkflow(probeCtx, state.CreateWorkflowInput{
		WorkflowID: workflowID, Namespace: "dur049-epoch-probe", SubmissionKey: workflowID,
		SubmissionPayloadHash: "sub-v1:" + hex.EncodeToString(digest[:]),
		DefinitionID:          "dur049-fault-fixture-v1", DefinitionVersion: 1,
		PartitionID: partitionNumber, InitialNodeID: "dur048.sleep",
		InitialInput: json.RawMessage(`{"probe":"same-owner-stale-epoch"}`), ActorID: "dur049-epoch-probe",
	})
	if err != nil {
		return result, fmt.Errorf("create isolated probe workflow: %w", err)
	}
	workflowCreated = true
	result.BeforeRevision = created.Workflow.Revision
	if err := store.Pool().QueryRow(probeCtx, `SELECT count(*) FROM engine.outbox WHERE workflow_id = $1`, workflowID).Scan(&result.ProbeOutboxRowsObserved); err != nil {
		return result, fmt.Errorf("verify probe workflow did not create outbox work: %w", err)
	}
	if result.ProbeOutboxRowsObserved != 0 {
		return result, fmt.Errorf("probe workflow created %d outbox rows; campaign negative control requires suppression", result.ProbeOutboxRowsObserved)
	}
	transitionErr := store.ApplyOwnerTransition(probeCtx, state.OwnerTransitionInput{
		Lease: staleRef, WorkflowID: workflowID, ExpectedRevision: created.Workflow.Revision,
		NewState: state.StateCanceled, ActorID: "dur049-epoch-probe",
		Reason: "DUR049_SAME_OWNER_STALE_EPOCH_NEGATIVE_CONTROL",
	})
	result.Error = fmt.Sprint(transitionErr)
	workflowAfter, err := store.GetWorkflow(ctx, workflowID)
	if err != nil {
		return result, fmt.Errorf("read probe workflow after stale transition: %w", err)
	}
	result.AfterRevision = workflowAfter.Revision
	result.Status = "PASS"
	if !errors.Is(transitionErr, state.ErrLeaseNotOwned) || result.AfterRevision != result.BeforeRevision || !result.ProbeWorkflowRetained || result.ProbeWorkflowFinalState != string(state.StateCanceled) || !result.ProbeOutboxSuppressed || result.ProbeOutboxRowsObserved != 0 || result.ProbeOutboxRowsAfterCleanup != 0 {
		result.Status = "FAIL"
	}
	return result, nil
}

func isTerminalProbeState(value state.WorkflowState) bool {
	switch value {
	case state.StateSucceeded, state.StateFailed, state.StateRejected, state.StateCanceled, state.StateAbandoned:
		return true
	default:
		return false
	}
}

func acquireProbeLease(ctx context.Context, store *state.Store, partitionID int16, ownerID string) (state.Lease, error) {
	for {
		lease, acquired, err := store.AcquireLease(ctx, partitionID, ownerID, 2*time.Minute)
		if err != nil {
			return state.Lease{}, err
		}
		if acquired {
			return lease, nil
		}
		select {
		case <-ctx.Done():
			return state.Lease{}, ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func fatal(message string) {
	fmt.Fprintln(os.Stderr, message)
	os.Exit(1)
}
