// dur043-stale-probe is a campaign-only diagnostic. It attempts one durable
// owner transition with a lease identity captured before a takeover and exits
// successfully only when the repository rejects that superseded identity
// without changing the workflow revision.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

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

func main() {
	workflowID := flag.String("workflow-id", "", "workflow to probe")
	partitionID := flag.Int("partition-id", -1, "partition captured before takeover")
	ownerID := flag.String("owner-id", "", "owner captured before takeover")
	epoch := flag.Int64("epoch", 0, "epoch captured before takeover")
	nodeID := flag.String("node-id", "", "superseded attempt node for an optional late-result probe")
	iteration := flag.Int("iteration", 0, "superseded attempt iteration for an optional late-result probe")
	attemptNumber := flag.Int64("attempt-number", 0, "superseded attempt number for an optional late-result probe")
	claimToken := flag.String("claim-token", "", "superseded attempt claim token for an optional late-result probe")
	flag.Parse()
	if *workflowID == "" || *partitionID < 0 || *ownerID == "" || *epoch <= 0 {
		fatal("workflow-id, partition-id, owner-id, and a positive epoch are required")
	}
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		fatal("DATABASE_URL is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	store, err := state.NewFromURL(ctx, databaseURL)
	if err != nil {
		fatal(err.Error())
	}
	defer store.Close()

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

func fatal(message string) {
	fmt.Fprintln(os.Stderr, message)
	os.Exit(1)
}
