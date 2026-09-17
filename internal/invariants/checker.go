// Package invariants contains an independent, evidence-based checker for M1.
// It intentionally does not call state transition validators: its verdict is
// derived from persisted history and submission records using its own rules.
package invariants

import "fmt"

type HistoryRecord struct {
	Revision int64
	OldState string
	NewState string
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

type Trace struct {
	History     []HistoryRecord
	Results     []AcceptedResult
	Submissions []SubmissionRecord
}

type Verdict struct {
	Valid      bool
	Violations []string
}

func Check(trace Trace) Verdict {
	var violations []string
	checkHistory(trace.History, &violations)
	checkResults(trace.Results, &violations)
	checkSubmissions(trace.Submissions, &violations)
	return Verdict{Valid: len(violations) == 0, Violations: violations}
}

func checkHistory(history []HistoryRecord, violations *[]string) {
	seenTerminal := false
	var previous int64
	for index, record := range history {
		if record.Revision <= 0 || (index > 0 && record.Revision != previous+1) {
			*violations = append(*violations, fmt.Sprintf("history revision %d is not contiguous", record.Revision))
		}
		if seenTerminal && !isTerminal(record.NewState) {
			*violations = append(*violations, "workflow leaves a terminal state")
		}
		if isTerminal(record.NewState) {
			seenTerminal = true
		}
		previous = record.Revision
	}
}

func checkResults(results []AcceptedResult, violations *[]string) {
	seen := make(map[string]struct{})
	for _, result := range results {
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
