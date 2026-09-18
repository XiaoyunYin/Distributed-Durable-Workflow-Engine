package invariants

import (
	"strings"
	"testing"
)

func validM4Trace() Trace {
	return Trace{
		Checkpoints: []CheckpointRecord{{WorkflowID: "wf", NodeID: "root", Iteration: 0,
			Sequence: 0, SchemaVersion: 1, SourceAttempt: 1, PayloadValid: true, PayloadHash: "hash-0"},
			{WorkflowID: "wf", NodeID: "root", Iteration: 0,
				Sequence: 1, SchemaVersion: 1, SourceAttempt: 1, PayloadValid: true, PayloadHash: "hash-1"}},
		Effects: []EffectRecordSnapshot{{WorkflowID: "wf", LogicalEffectKey: "effect-1",
			ArgumentHash: "args-1", AttemptNumber: 1, Outcome: "APPLIED", ReceiptPresent: true}},
		EffectCalls: []EffectCallRecord{{WorkflowID: "wf", LogicalEffectKey: "effect-1",
			ArgumentHash: "args-1", AttemptNumber: 1, RequestID: "request-1", Outcome: "APPLIED"}},
		Approvals: []ApprovalRecord{{IntentID: "intent-1", WorkflowID: "wf", NodeID: "root", Iteration: 0,
			ProposalHash: "proposal-1", Target: "sandbox.write", ArgumentsValid: true,
			Decision: "APPROVED", DispatchStatus: "GRANTED", GrantScopeHash: "scope-1", GrantToken: "token-1"}},
	}
}

func TestCheckAcceptsM4Evidence(t *testing.T) {
	if verdict := Check(validM4Trace()); !verdict.Valid {
		t.Fatalf("valid M4 trace rejected: %v", verdict.Violations)
	}
}

func TestCheckRejectsM4EvidenceIndependently(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*Trace)
		message string
	}{
		{name: "checkpoint gap", mutate: func(trace *Trace) {
			trace.Checkpoints[1].Sequence = 3
		}, message: "checkpoint sequence is not contiguous"},
		{name: "missing effect receipt", mutate: func(trace *Trace) {
			trace.Effects[0].ReceiptPresent = false
		}, message: "lacks a receipt"},
		{name: "duplicate effect", mutate: func(trace *Trace) {
			trace.Effects = append(trace.Effects, trace.Effects[0])
		}, message: "duplicate records"},
		{name: "unknown call outcome", mutate: func(trace *Trace) {
			trace.EffectCalls[0].Outcome = "NOT_A_RESULT"
		}, message: "unknown outcome"},
		{name: "grant without approval", mutate: func(trace *Trace) {
			trace.Approvals[0].Decision = "REJECTED"
		}, message: "matching approved decision"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			trace := validM4Trace()
			testCase.mutate(&trace)
			verdict := Check(trace)
			if verdict.Valid || !strings.Contains(strings.Join(verdict.Violations, "\n"), testCase.message) {
				t.Fatalf("expected %q, got valid=%v violations=%v", testCase.message, verdict.Valid, verdict.Violations)
			}
		})
	}
}
