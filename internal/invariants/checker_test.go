package invariants

import "testing"

func TestCheckAcceptsDurableTrace(t *testing.T) {
	verdict := Check(Trace{
		History:     []HistoryRecord{{Revision: 1, NewState: "RUNNABLE"}, {Revision: 2, OldState: "RUNNABLE", NewState: "SUCCEEDED"}},
		Results:     []AcceptedResult{{WorkflowID: "wf", NodeID: "root", Accepted: true}},
		Submissions: []SubmissionRecord{{Namespace: "default", Key: "k", Hash: "sub-v1:x", WorkflowID: "wf"}, {Namespace: "default", Key: "k", Hash: "sub-v1:x", WorkflowID: "wf"}},
	})
	if !verdict.Valid {
		t.Fatalf("valid trace rejected: %v", verdict.Violations)
	}
}

func TestCheckRejectsIndependentInvariantViolations(t *testing.T) {
	verdict := Check(Trace{
		History:     []HistoryRecord{{Revision: 1, NewState: "SUCCEEDED"}, {Revision: 3, OldState: "SUCCEEDED", NewState: "RUNNABLE"}},
		Results:     []AcceptedResult{{WorkflowID: "wf", NodeID: "root", Accepted: true}, {WorkflowID: "wf", NodeID: "root", Accepted: true}},
		Submissions: []SubmissionRecord{{Namespace: "default", Key: "k", Hash: "a", WorkflowID: "wf"}, {Namespace: "default", Key: "k", Hash: "b", WorkflowID: "wf-2"}},
	})
	if verdict.Valid || len(verdict.Violations) < 4 {
		t.Fatalf("expected independent violations, got %+v", verdict)
	}
}
