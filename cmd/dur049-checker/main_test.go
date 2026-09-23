package main

import (
	"strings"
	"testing"

	"durable-agent-execution-engine/internal/invariants"
)

func TestFenceCheckRejectsPostBoundarySupersededEpoch(t *testing.T) {
	epoch := int64(7)
	trace := invariants.Trace{
		History: []invariants.HistoryRecord{
			{WorkflowID: "wf", Revision: 1, NewState: "RUNNABLE"},
			{WorkflowID: "wf", Revision: 4, SchedulerEpoch: func() *int64 { v := int64(6); return &v }()},
		},
	}
	verdict, fence := check(trace, 3, epoch, 0)
	if verdict.Valid {
		t.Fatal("fence check accepted a post-boundary history row with a superseded epoch")
	}
	if fence.SupersededCount != 1 {
		t.Fatalf("fence evidence = %+v, want one superseded history row", fence)
	}
	if !containsViolation(verdict.Violations, "post-pre-fault history records carry a superseded scheduler epoch") {
		t.Fatalf("violations = %v, want specific superseded-epoch violation", verdict.Violations)
	}
}

func TestFenceCheckRejectsAcceptedStaleAttemptResult(t *testing.T) {
	trace := invariants.Trace{
		History:  []invariants.HistoryRecord{{WorkflowID: "wf", Revision: 1, NewState: "RUNNABLE"}},
		Attempts: []invariants.AttemptRecord{{WorkflowID: "wf", NodeID: "node", AttemptNumber: 2, State: "REPLACED", IsCurrent: false, ResultRecorded: true}},
	}
	verdict, fence := check(trace, 1, 7, 2)
	if verdict.Valid || !fence.StaleResultRecorded {
		t.Fatalf("fence verdict=%+v evidence=%+v, want accepted stale result rejected", verdict, fence)
	}
	if !containsViolation(verdict.Violations, "stale attempt has a recorded result") {
		t.Fatalf("violations = %v, want specific stale-result violation", verdict.Violations)
	}
}

func TestFenceCheckAcceptsRejectedStaleAttempt(t *testing.T) {
	trace := invariants.Trace{
		History:  []invariants.HistoryRecord{{WorkflowID: "wf", Revision: 1, NewState: "RUNNABLE"}},
		Attempts: []invariants.AttemptRecord{{WorkflowID: "wf", NodeID: "node", AttemptNumber: 1, State: "REPLACED", IsCurrent: false, ResultRecorded: false}},
	}
	verdict, fence := check(trace, 3, 7, 1)
	if !verdict.Valid {
		t.Fatalf("fence check rejected a fenced stale attempt: %v", verdict.Violations)
	}
	if !fence.StaleAttemptFound || fence.StaleAttemptCurrent || fence.StaleResultRecorded {
		t.Fatalf("fence evidence = %+v, want rejected non-current attempt", fence)
	}
}

func containsViolation(violations []string, expected string) bool {
	for _, violation := range violations {
		if strings.Contains(violation, expected) {
			return true
		}
	}
	return false
}
