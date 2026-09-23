package main

import (
	"testing"

	"durable-agent-execution-engine/internal/invariants"
)

func TestFenceCheckRejectsSupersededHistoryAndLateResult(t *testing.T) {
	epoch := int64(7)
	trace := invariants.Trace{
		History:  []invariants.HistoryRecord{{WorkflowID: "wf", Revision: 4, SchedulerEpoch: func() *int64 { v := int64(6); return &v }()}},
		Attempts: []invariants.AttemptRecord{{WorkflowID: "wf", NodeID: "node", AttemptNumber: 2, IsCurrent: false, ResultRecorded: true}},
	}
	verdict, fence := check(trace, 3, epoch, 2)
	if verdict.Valid {
		t.Fatal("fence check accepted a superseded history row and recorded late result")
	}
	if fence.SupersededCount != 1 || !fence.StaleResultRecorded {
		t.Fatalf("fence evidence = %+v, want superseded history and recorded result", fence)
	}
}

func TestFenceCheckAcceptsRejectedStaleAttempt(t *testing.T) {
	trace := invariants.Trace{
		History:  []invariants.HistoryRecord{{WorkflowID: "wf", Revision: 3}},
		Attempts: []invariants.AttemptRecord{{WorkflowID: "wf", NodeID: "node", AttemptNumber: 1, IsCurrent: false, ResultRecorded: false}},
	}
	verdict, fence := check(trace, 3, 7, 1)
	if !verdict.Valid {
		t.Fatalf("fence check rejected a fenced stale attempt: %v", verdict.Violations)
	}
	if !fence.StaleAttemptFound || fence.StaleAttemptCurrent || fence.StaleResultRecorded {
		t.Fatalf("fence evidence = %+v, want rejected non-current attempt", fence)
	}
}
