// Package invariants contains an independent, evidence-based checker for M1.
// It intentionally does not call state transition validators: its verdict is
// derived from persisted history and submission records using its own rules.
package invariants

import (
	"context"
	"fmt"
	"sort"

	"durable-agent-execution-engine/internal/state"
)

type HistoryRecord struct {
	WorkflowID     string
	Revision       int64
	ActorKind      string
	SchedulerEpoch *int64
	OldState       string
	NewState       string
}

type AcceptedResult struct {
	WorkflowID string
	NodeID     string
	Iteration  int
	Accepted   bool
}

type SubmissionRecord struct {
	Namespace  string
	Key        string
	Hash       string
	WorkflowID string
}

type AttemptRecord struct {
	WorkflowID      string
	NodeID          string
	Iteration       int
	AttemptNumber   int64
	State           string
	ClaimToken      string
	WorkerID        string
	WorkerRequestID string
	Outcome         string
	IsCurrent       bool
	ResultRecorded  bool
}

type Trace struct {
	History     []HistoryRecord
	Results     []AcceptedResult
	Submissions []SubmissionRecord
	Attempts    []AttemptRecord
}

type Verdict struct {
	Valid      bool
	Violations []string
}

// Load reads the evidence used by the M1 checker. It intentionally maps rows
// into checker-owned structs before validation, so Check cannot accidentally
// reuse a production transition decision.
func Load(ctx context.Context, store *state.Store, workflowIDs []string) (Trace, error) {
	var trace Trace
	for _, workflowID := range workflowIDs {
		workflow, err := store.GetWorkflow(ctx, workflowID)
		if err != nil {
			return Trace{}, err
		}
		history, err := store.ListHistory(ctx, workflowID, 0, 1000)
		if err != nil {
			return Trace{}, err
		}
		for _, record := range history {
			oldState := ""
			if record.OldState != nil {
				oldState = string(*record.OldState)
			}
			trace.History = append(trace.History, HistoryRecord{WorkflowID: workflowID,
				Revision: record.Revision, ActorKind: record.ActorKind, SchedulerEpoch: record.SchedulerEpoch,
				OldState: oldState, NewState: string(record.NewState)})
		}
		nodes, err := store.ListNodes(ctx, workflowID)
		if err != nil {
			return Trace{}, err
		}
		for _, node := range nodes {
			if len(node.AcceptedResult) != 0 {
				trace.Results = append(trace.Results, AcceptedResult{WorkflowID: workflowID,
					NodeID: node.NodeID, Iteration: node.Iteration, Accepted: true})
			}
		}
		attempts, err := store.ListAttempts(ctx, workflowID)
		if err != nil {
			return Trace{}, err
		}
		for _, attempt := range attempts {
			trace.Attempts = append(trace.Attempts, AttemptRecord{
				WorkflowID: attempt.WorkflowID, NodeID: attempt.NodeID, Iteration: attempt.Iteration,
				AttemptNumber: attempt.AttemptNumber, State: string(attempt.State), ClaimToken: attempt.ClaimToken,
				WorkerID: attempt.WorkerID, WorkerRequestID: attempt.WorkerRequestID,
				Outcome: attempt.OutcomeDisposition, IsCurrent: attempt.IsCurrent,
				ResultRecorded: len(attempt.Result) != 0,
			})
		}
		trace.Submissions = append(trace.Submissions, SubmissionRecord{Namespace: workflow.Namespace,
			Key: workflow.SubmissionKey, Hash: workflow.PayloadHash, WorkflowID: workflowID})
	}
	return trace, nil
}

func Check(trace Trace) Verdict {
	var violations []string
	checkHistory(trace.History, &violations)
	checkResults(trace.Results, &violations)
	checkSubmissions(trace.Submissions, &violations)
	checkOwnershipAndAttempts(trace.History, trace.Attempts, &violations)
	return Verdict{Valid: len(violations) == 0, Violations: violations}
}

func checkOwnershipAndAttempts(history []HistoryRecord, attempts []AttemptRecord, violations *[]string) {
	byWorkflow := make(map[string][]HistoryRecord)
	for _, record := range history {
		byWorkflow[record.WorkflowID] = append(byWorkflow[record.WorkflowID], record)
	}
	for workflowID, records := range byWorkflow {
		sort.Slice(records, func(i, j int) bool { return records[i].Revision < records[j].Revision })
		var previousEpoch int64
		for _, record := range records {
			if record.ActorKind == "scheduler" {
				if record.SchedulerEpoch == nil || *record.SchedulerEpoch <= 0 {
					*violations = append(*violations, "scheduler history is missing an ownership epoch: "+workflowID)
				} else if previousEpoch != 0 && *record.SchedulerEpoch < previousEpoch {
					*violations = append(*violations, "scheduler epoch moved backwards: "+workflowID)
				} else {
					previousEpoch = *record.SchedulerEpoch
				}
			} else if record.SchedulerEpoch != nil {
				*violations = append(*violations, "non-scheduler history carries a scheduler epoch: "+workflowID)
			}
		}
	}
	byNode := make(map[string][]AttemptRecord)
	for _, attempt := range attempts {
		if attempt.WorkflowID == "" || attempt.NodeID == "" || attempt.Iteration < 0 || attempt.AttemptNumber <= 0 {
			*violations = append(*violations, "attempt identity is incomplete")
			continue
		}
		key := fmt.Sprintf("%s\x00%s\x00%d", attempt.WorkflowID, attempt.NodeID, attempt.Iteration)
		byNode[key] = append(byNode[key], attempt)
		if attempt.IsCurrent && isTerminalAttempt(attempt.State) {
			*violations = append(*violations, "terminal attempt remains current: "+key)
		}
		if attempt.State == "CLAIMED" && (attempt.ClaimToken == "" || attempt.WorkerID == "" || attempt.WorkerRequestID == "") {
			*violations = append(*violations, "claimed attempt is missing worker ownership: "+key)
		}
		if attempt.ResultRecorded && (attempt.State == "TIMED_OUT" || attempt.State == "REPLACED" || attempt.State == "CANCELED" || attempt.State == "OUTCOME_UNKNOWN") {
			*violations = append(*violations, "stale worker result recorded on settled attempt: "+key)
		}
	}
	for key, records := range byNode {
		sort.Slice(records, func(i, j int) bool { return records[i].AttemptNumber < records[j].AttemptNumber })
		current := 0
		for index, record := range records {
			want := int64(index + 1)
			if record.AttemptNumber != want {
				*violations = append(*violations, fmt.Sprintf("attempt generation is not contiguous for %s", key))
				break
			}
			if record.IsCurrent {
				current++
			}
		}
		if current > 1 {
			*violations = append(*violations, "more than one current attempt: "+key)
		}
	}
}

func isTerminalAttempt(state string) bool {
	switch state {
	case "SUCCEEDED", "FAILED_RETRYABLE", "FAILED_FINAL", "TIMED_OUT", "REPLACED", "OUTCOME_UNKNOWN", "CANCELED":
		return true
	default:
		return false
	}
}

func checkHistory(history []HistoryRecord, violations *[]string) {
	byWorkflow := make(map[string][]HistoryRecord)
	for _, record := range history {
		if record.WorkflowID == "" {
			*violations = append(*violations, "history record has no workflow ID")
			continue
		}
		byWorkflow[record.WorkflowID] = append(byWorkflow[record.WorkflowID], record)
	}
	for workflowID, records := range byWorkflow {
		sort.Slice(records, func(i, j int) bool { return records[i].Revision < records[j].Revision })
		if len(records) == 0 || records[0].Revision != 1 || records[0].OldState != "" || records[0].NewState != "RUNNABLE" {
			*violations = append(*violations, "workflow history does not start with revision 1 creation: "+workflowID)
		}
		var previousRevision int64
		previousState := ""
		terminalSeen := false
		for index, record := range records {
			if record.Revision <= 0 || (index > 0 && record.Revision != previousRevision+1) {
				*violations = append(*violations, fmt.Sprintf("history revision %d is not contiguous for %s", record.Revision, workflowID))
			}
			if index > 0 && record.OldState != previousState {
				*violations = append(*violations, fmt.Sprintf("history old state %q does not follow %q for %s", record.OldState, previousState, workflowID))
			}
			if index > 0 && !allowedTransition(record.OldState, record.NewState) {
				*violations = append(*violations, fmt.Sprintf("illegal workflow transition %s -> %s for %s", record.OldState, record.NewState, workflowID))
			}
			if terminalSeen {
				*violations = append(*violations, "workflow has history after its terminal decision: "+workflowID)
			}
			if isTerminal(record.NewState) {
				terminalSeen = true
			}
			previousRevision = record.Revision
			previousState = record.NewState
		}
	}
}

func allowedTransition(from, to string) bool {
	if isTerminal(from) {
		return false
	}
	allowed := map[string]map[string]bool{
		"RUNNABLE": {
			"RUNNABLE": true, "WAITING_ACTIVITY": true, "WAITING_APPROVAL": true,
			"WAITING_TIMER": true, "PAUSED_UNSUPPORTED_VERSION": true,
			"SUCCEEDED": true, "FAILED": true, "CANCELED": true,
		},
		"WAITING_ACTIVITY": {
			"WAITING_ACTIVITY": true, "RUNNABLE": true, "WAITING_TIMER": true,
			"RECONCILIATION_REQUIRED": true, "FAILED": true, "CANCELED": true,
		},
		"WAITING_TIMER":              {"WAITING_TIMER": true, "RUNNABLE": true, "CANCELED": true},
		"WAITING_APPROVAL":           {"WAITING_APPROVAL": true, "RUNNABLE": true, "REJECTED": true, "CANCELED": true},
		"PAUSED_UNSUPPORTED_VERSION": {"PAUSED_UNSUPPORTED_VERSION": true, "RUNNABLE": true, "CANCELED": true, "ABANDONED": true},
		"RECONCILIATION_REQUIRED":    {"RECONCILIATION_REQUIRED": true, "RUNNABLE": true, "CANCELED": true, "ABANDONED": true},
	}
	return allowed[from][to]
}

func checkResults(results []AcceptedResult, violations *[]string) {
	seen := make(map[string]struct{})
	for _, result := range results {
		if result.WorkflowID == "" || result.NodeID == "" || result.Iteration < 0 {
			*violations = append(*violations, "accepted result identity is incomplete")
		}
		if !result.Accepted {
			continue
		}
		key := fmt.Sprintf("%s\x00%s\x00%d", result.WorkflowID, result.NodeID, result.Iteration)
		if _, exists := seen[key]; exists {
			*violations = append(*violations, "node has more than one accepted result: "+key)
		}
		seen[key] = struct{}{}
	}
}

func checkSubmissions(submissions []SubmissionRecord, violations *[]string) {
	byKey := make(map[string]SubmissionRecord)
	byWorkflow := make(map[string]SubmissionRecord)
	for _, submission := range submissions {
		if submission.Namespace == "" || submission.Key == "" || submission.Hash == "" || submission.WorkflowID == "" {
			*violations = append(*violations, "submission identity is incomplete")
			continue
		}
		key := submission.Namespace + "\x00" + submission.Key
		if prior, exists := byKey[key]; exists && (prior.Hash != submission.Hash || prior.WorkflowID != submission.WorkflowID) {
			*violations = append(*violations, "submission key maps to conflicting identity: "+key)
		}
		byKey[key] = submission
		if prior, exists := byWorkflow[submission.WorkflowID]; exists && (prior.Namespace != submission.Namespace || prior.Key != submission.Key) {
			*violations = append(*violations, "workflow ID maps to multiple submissions: "+submission.WorkflowID)
		}
		byWorkflow[submission.WorkflowID] = submission
	}
}

func isTerminal(state string) bool {
	switch state {
	case "SUCCEEDED", "FAILED", "REJECTED", "CANCELED", "ABANDONED":
		return true
	default:
		return false
	}
}
