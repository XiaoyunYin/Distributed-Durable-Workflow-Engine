package invariants

import (
	"strings"
	"testing"
)

func validTrace() Trace {
	return Trace{
		History: []HistoryRecord{
			{WorkflowID: "wf", Revision: 1, NewState: "RUNNABLE"},
			{WorkflowID: "wf", Revision: 2, OldState: "RUNNABLE", NewState: "SUCCEEDED"},
		},
		Results:     []AcceptedResult{{WorkflowID: "wf", NodeID: "root", Iteration: 0, Accepted: true}},
		Submissions: []SubmissionRecord{{Namespace: "default", Key: "k", Hash: "sub-v1:x", WorkflowID: "wf"}},
	}
}

func TestCheckAcceptsDurableTrace(t *testing.T) {
	verdict := Check(validTrace())
	if !verdict.Valid {
		t.Fatalf("valid trace rejected: %v", verdict.Violations)
	}
}

func TestCheckRejectsEachIndependentInvariantViolation(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*Trace)
		message string
	}{
		{name: "history starts at revision one", mutate: func(trace *Trace) {
			trace.History[0].Revision = 5
		}, message: "does not start with revision 1"},
		{name: "history old state follows previous state", mutate: func(trace *Trace) {
			trace.History[1].OldState = "WAITING_ACTIVITY"
		}, message: "old state"},
		{name: "terminal state cannot change", mutate: func(trace *Trace) {
			trace.History = append(trace.History, HistoryRecord{WorkflowID: "wf", Revision: 3,
				OldState: "SUCCEEDED", NewState: "CANCELED"})
		}, message: "terminal"},
		{name: "accepted result is unique", mutate: func(trace *Trace) {
			trace.Results = append(trace.Results, trace.Results[0])
		}, message: "more than one accepted result"},
		{name: "submission identity is stable", mutate: func(trace *Trace) {
			trace.Submissions = append(trace.Submissions, SubmissionRecord{Namespace: "default", Key: "k",
				Hash: "sub-v1:changed", WorkflowID: "wf-2"})
		}, message: "conflicting identity"},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			trace := validTrace()
			testCase.mutate(&trace)
			verdict := Check(trace)
			if verdict.Valid {
				t.Fatalf("expected invalid trace")
			}
			joined := strings.Join(verdict.Violations, "\n")
			if !strings.Contains(joined, testCase.message) {
				t.Fatalf("expected %q in violations, got %v", testCase.message, verdict.Violations)
			}
		})
	}
}

func TestCheckRejectsOwnershipAndAttemptViolations(t *testing.T) {
	epochOne := int64(1)
	epochTwo := int64(2)
	validAttempt := AttemptRecord{WorkflowID: "wf", NodeID: "root", Iteration: 0,
		AttemptNumber: 1, State: "CLAIMED", ClaimToken: "token", WorkerID: "worker",
		WorkerRequestID: "request", IsCurrent: true}
	cases := []struct {
		name    string
		trace   Trace
		message string
	}{
		{name: "scheduler epoch is required", trace: Trace{History: []HistoryRecord{
			{WorkflowID: "wf", Revision: 1, NewState: "RUNNABLE"},
			{WorkflowID: "wf", Revision: 2, ActorKind: "scheduler", NewState: "WAITING_ACTIVITY"},
		}}, message: "missing an ownership epoch"},
		{name: "scheduler epoch cannot move backward", trace: Trace{History: []HistoryRecord{
			{WorkflowID: "wf", Revision: 1, NewState: "RUNNABLE"},
			{WorkflowID: "wf", Revision: 2, ActorKind: "scheduler", SchedulerEpoch: &epochTwo, NewState: "WAITING_ACTIVITY"},
			{WorkflowID: "wf", Revision: 3, ActorKind: "scheduler", SchedulerEpoch: &epochOne, NewState: "WAITING_ACTIVITY"},
		}}, message: "moved backwards"},
		{name: "claimed attempt has ownership", trace: Trace{Attempts: []AttemptRecord{
			{WorkflowID: "wf", NodeID: "root", Iteration: 0, AttemptNumber: 1, State: "CLAIMED", IsCurrent: true},
		}}, message: "missing worker ownership"},
		{name: "only one current attempt", trace: Trace{Attempts: []AttemptRecord{
			validAttempt, {WorkflowID: "wf", NodeID: "root", Iteration: 0, AttemptNumber: 2,
				State: "DISPATCHABLE", IsCurrent: true},
		}}, message: "more than one current attempt"},
		{name: "stale result cannot be accepted", trace: Trace{Attempts: []AttemptRecord{
			{WorkflowID: "wf", NodeID: "root", Iteration: 0, AttemptNumber: 1, State: "REPLACED", ResultRecorded: true},
		}}, message: "stale worker result"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			verdict := Check(testCase.trace)
			if verdict.Valid || !strings.Contains(strings.Join(verdict.Violations, "\n"), testCase.message) {
				t.Fatalf("expected %q, got valid=%v violations=%v", testCase.message, verdict.Valid, verdict.Violations)
			}
		})
	}
}
