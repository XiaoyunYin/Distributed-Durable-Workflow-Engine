package invariants

import (
	"strings"
	"testing"
)

func validFaultEvidence() FaultEvidence {
	code := 0
	return FaultEvidence{RunID: "run-1", Seed: 23, Boundary: "after-effect",
		BoundaryReached: true, RequestedAction: "release", ObservedAction: "released",
		ProcessExitKnown: true, ProcessReturnCode: &code, CleanupCompleted: true}
}

func linkedFaultEvidence() FaultEvidence {
	evidence := validFaultEvidence()
	evidence.Boundary = "attempt_claimed"
	evidence.WorkflowID = "wf-1"
	evidence.NodeID = "root"
	evidence.AttemptNumber = 1
	return evidence
}

func TestCheckWithFaultEvidenceAcceptsObservedRun(t *testing.T) {
	if verdict := CheckWithFaults(validTrace(), []FaultEvidence{validFaultEvidence()}); !verdict.Valid {
		t.Fatalf("valid fault evidence rejected: %v", verdict.Violations)
	}
}

func TestCheckWithFaultEvidenceRejectsMutations(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*FaultEvidence)
		want   string
	}{
		{"missing cleanup", func(e *FaultEvidence) { e.CleanupCompleted = false }, "bounded cleanup"},
		{"release before boundary", func(e *FaultEvidence) { e.BoundaryReached = false }, "before the boundary"},
		{"missing observed action", func(e *FaultEvidence) { e.ObservedAction = "" }, "observed action"},
		{"missing process outcome", func(e *FaultEvidence) { e.ProcessExitKnown = false }, "process outcome"},
		{"contradictory boundary", func(e *FaultEvidence) { e.ObservedAction = "boundary_not_reached" }, "contradicts"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			evidence := validFaultEvidence()
			testCase.mutate(&evidence)
			verdict := CheckWithFaults(validTrace(), []FaultEvidence{evidence})
			if verdict.Valid || !strings.Contains(strings.Join(verdict.Violations, "\n"), testCase.want) {
				t.Fatalf("expected %q, got valid=%v violations=%v", testCase.want, verdict.Valid, verdict.Violations)
			}
		})
	}
}

func TestCheckWithFaultEvidenceJoinsDurableAttempt(t *testing.T) {
	trace := Trace{History: []HistoryRecord{
		{WorkflowID: "wf-1", Revision: 1, NewState: "RUNNABLE"},
		{WorkflowID: "wf-1", Revision: 2, OldState: "RUNNABLE", NewState: "SUCCEEDED"},
	}, Results: []AcceptedResult{{WorkflowID: "wf-1", NodeID: "root", Iteration: 0, Accepted: true}},
		Submissions: []SubmissionRecord{{Namespace: "default", Key: "k", Hash: "sub-v1:x", WorkflowID: "wf-1"}}, Attempts: []AttemptRecord{{
			WorkflowID: "wf-1", NodeID: "root", Iteration: 0, AttemptNumber: 1, State: "CLAIMED",
			ClaimToken: "token", WorkerID: "worker", WorkerRequestID: "request", IsCurrent: true,
		}}}
	if verdict := CheckWithFaults(trace, []FaultEvidence{linkedFaultEvidence()}); !verdict.Valid {
		t.Fatalf("linked fault evidence rejected: %v", verdict.Violations)
	}
	missing := trace
	missing.Attempts = nil
	verdict := CheckWithFaults(missing, []FaultEvidence{linkedFaultEvidence()})
	if verdict.Valid || !strings.Contains(strings.Join(verdict.Violations, "\n"), "no durable attempt") {
		t.Fatalf("missing durable attempt accepted: %+v", verdict)
	}
}

func TestParseFaultTraceBuildsObservedSummary(t *testing.T) {
	trace := strings.NewReader(`{"schema_version":"fault-trace.v1","run_id":"run-1","sequence":1,"event":"boundary_reached","seed":23,"boundary":"after-effect"}
{"schema_version":"fault-trace.v1","run_id":"run-1","sequence":2,"event":"process_started","seed":23,"boundary":"after-effect","command":["python","-m","faults.target"]}
{"schema_version":"fault-trace.v1","run_id":"run-1","sequence":3,"event":"command","seed":23,"boundary":"after-effect","command":"release"}
{"schema_version":"fault-trace.v1","run_id":"run-1","sequence":4,"event":"fault_observed","seed":23,"boundary":"after-effect","action":"released"}
{"schema_version":"fault-trace.v1","run_id":"run-1","sequence":5,"event":"process_exited","seed":23,"boundary":"after-effect","return_code":0}
{"schema_version":"fault-trace.v1","run_id":"run-1","sequence":6,"event":"cleanup_completed","seed":23,"boundary":"after-effect","bounded":true}
`)
	evidence, err := ParseFaultTrace(trace)
	if err != nil || len(evidence) != 1 {
		t.Fatalf("parse fault trace = %v, %v", evidence, err)
	}
	if verdict := CheckWithFaults(validTrace(), evidence); !verdict.Valid {
		t.Fatalf("parsed fault evidence rejected: %v", verdict.Violations)
	}
}

func TestParseFaultTraceRejectsCleanupTimeoutAndSchemaDrift(t *testing.T) {
	trace := strings.NewReader(`{"schema_version":"fault-trace.v1","run_id":"run-1","sequence":1,"event":"cleanup_timeout","seed":23,"boundary":"after-effect"}
{"schema_version":"fault-trace.v1","run_id":"run-1","sequence":2,"event":"cleanup_completed","seed":23,"boundary":"after-effect","bounded":true}
`)
	evidence, err := ParseFaultTrace(trace)
	if err != nil {
		t.Fatal(err)
	}
	if verdict := CheckWithFaults(validTrace(), evidence); verdict.Valid {
		t.Fatal("cleanup timeout was accepted")
	}
	_, err = ParseFaultTrace(strings.NewReader(`{"schema_version":"fault-trace.v0","run_id":"run-1","sequence":1,"event":"process_started"}`))
	if err == nil || !strings.Contains(err.Error(), "unsupported fault trace schema") {
		t.Fatalf("schema drift error = %v", err)
	}
}

func TestParseFaultTraceRejectsSequenceGaps(t *testing.T) {
	_, err := ParseFaultTrace(strings.NewReader(`{"schema_version":"fault-trace.v1","run_id":"run-gap","sequence":1,"event":"process_started"}
{"schema_version":"fault-trace.v1","run_id":"run-gap","sequence":3,"event":"process_exited"}`))
	if err == nil || !strings.Contains(err.Error(), "sequence") {
		t.Fatalf("sequence gap error = %v, want sequence validation", err)
	}
}

func TestFaultCheckerRejectsMissedBoundaryAndMissingDurableJoin(t *testing.T) {
	trace := strings.NewReader(`{"schema_version":"fault-trace.v1","run_id":"run-missed","sequence":1,"event":"boundary_timeout","seed":23,"boundary":"attempt_claimed"}
{"schema_version":"fault-trace.v1","run_id":"run-missed","sequence":2,"event":"command","seed":23,"boundary":"attempt_claimed","command":"kill"}
{"schema_version":"fault-trace.v1","run_id":"run-missed","sequence":3,"event":"fault_observed","seed":23,"boundary":"attempt_claimed","action":"process_killed"}
{"schema_version":"fault-trace.v1","run_id":"run-missed","sequence":4,"event":"process_exited","seed":23,"boundary":"attempt_claimed","return_code":137}
{"schema_version":"fault-trace.v1","run_id":"run-missed","sequence":5,"event":"cleanup_completed","seed":23,"boundary":"attempt_claimed","bounded":true}
`)
	evidence, err := ParseFaultTrace(trace)
	if err != nil {
		t.Fatal(err)
	}
	if len(evidence) != 1 || !evidence[0].BoundaryMissed {
		t.Fatalf("boundary miss was not sticky: %+v", evidence)
	}
	verdict := CheckWithFaults(Trace{}, evidence)
	joined := strings.Join(verdict.Violations, "\n")
	if verdict.Valid || !strings.Contains(joined, "boundary miss") || !strings.Contains(joined, "durable workflow identity") {
		t.Fatalf("missed boundary without durable join was accepted: %+v", verdict)
	}
}

func TestFaultCheckerRejectsUnknownCampaignBoundary(t *testing.T) {
	evidence := validFaultEvidence()
	evidence.Boundary = "not-a-campaign-boundary"
	verdict := CheckWithFaults(validTrace(), []FaultEvidence{evidence})
	if verdict.Valid || !strings.Contains(strings.Join(verdict.Violations, "\n"), "unknown fault boundary") {
		t.Fatalf("unknown boundary was accepted: %+v", verdict)
	}
}

func TestFaultCheckerRejectsSubmissionBoundaryAfterActivityWork(t *testing.T) {
	evidence := linkedFaultEvidence()
	evidence.Boundary = "submission_committed"
	trace := Trace{
		History:     []HistoryRecord{{WorkflowID: "wf-1", Revision: 1, NewState: "RUNNABLE"}},
		Submissions: []SubmissionRecord{{Namespace: "default", Key: "k", Hash: "sub-v1:x", WorkflowID: "wf-1"}},
		Attempts:    []AttemptRecord{{WorkflowID: "wf-1", NodeID: "root", AttemptNumber: 1, State: "CLAIMED"}},
	}
	verdict := CheckWithFaults(trace, []FaultEvidence{evidence})
	joined := strings.Join(verdict.Violations, "\n")
	if verdict.Valid || !strings.Contains(joined, "submission boundary has durable activity work") {
		t.Fatalf("overshot submission boundary was accepted: %+v", verdict)
	}
}

func TestFaultCheckerRequiresOutboxBoundaryIdentityAndState(t *testing.T) {
	evidence := linkedFaultEvidence()
	evidence.Boundary = "after_broker_ack"
	evidence.Fields = map[string]any{"durable_event_type": "attempt.dispatch"}
	trace := Trace{
		History:     []HistoryRecord{{WorkflowID: "wf-1", Revision: 1, NewState: "RUNNABLE"}},
		Submissions: []SubmissionRecord{{Namespace: "default", Key: "k", Hash: "sub-v1:x", WorkflowID: "wf-1"}},
		Outbox: []OutboxRecord{{WorkflowID: "wf-1", EventID: "event-1", AggregateRevision: 2,
			EventType: "attempt.dispatch", Topic: "durable-agent.tasks.v1", PublishState: "CLAIMED", PayloadValid: true}},
	}
	verdict := CheckWithFaults(trace, []FaultEvidence{evidence})
	joined := strings.Join(verdict.Violations, "\n")
	if verdict.Valid || !strings.Contains(joined, "matching durable outbox event") {
		t.Fatalf("unpublished outbox was accepted as broker acknowledgement: %+v", verdict)
	}
	trace.Outbox[0].PublishState = "PUBLISHED"
	if verdict := CheckWithFaults(trace, []FaultEvidence{evidence}); !verdict.Valid {
		t.Fatalf("published outbox was rejected: %v", verdict.Violations)
	}
}
