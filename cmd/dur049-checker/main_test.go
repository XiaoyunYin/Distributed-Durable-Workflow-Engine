package main

import (
	"strings"
	"testing"

	"durable-agent-execution-engine/internal/invariants"
)

func TestFenceCheckUsesRecoveredTransitionBoundary(t *testing.T) {
	oldEpoch, recoveryEpoch, laterEpoch := int64(3), int64(8), int64(9)
	trace := invariants.Trace{History: []invariants.HistoryRecord{
		{WorkflowID: "wf", Revision: 2, ActorKind: "scheduler", SchedulerEpoch: &oldEpoch},
		{WorkflowID: "wf", Revision: 4, ActorKind: "scheduler", SchedulerEpoch: &oldEpoch},
		{WorkflowID: "wf", Revision: 5, ActorKind: "scheduler", SchedulerEpoch: &recoveryEpoch},
		{WorkflowID: "wf", Revision: 6, ActorKind: "scheduler", SchedulerEpoch: &laterEpoch},
	}}

	_, fence := check(trace, 1, 3, recoveryEpoch, 0)
	if fence == nil {
		t.Fatal("expected fence report")
	}
	if fence.SupersededCount != 1 {
		t.Fatalf("post-recovery superseded count = %d, want 1", fence.SupersededCount)
	}
}

func TestFenceCheckRejectsMissingOrNonIncreasingRecoveryBoundary(t *testing.T) {
	for _, tc := range []struct {
		name             string
		preFaultRevision int64
		recoveryRevision int64
	}{
		{name: "missing recovery revision", preFaultRevision: 1, recoveryRevision: 0},
		{name: "recovery before pre-fault", preFaultRevision: 3, recoveryRevision: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			verdict, _ := check(invariants.Trace{}, tc.preFaultRevision, tc.recoveryRevision, 8, 0)
			for _, violation := range verdict.Violations {
				if strings.Contains(violation, "later recovery revision") {
					return
				}
			}
			t.Fatalf("missing recovery-boundary violation: %v", verdict.Violations)
		})
	}
}

func TestFenceCheckRejectsPostBoundarySupersededEpoch(t *testing.T) {
	oldEpoch, recoveryEpoch := int64(7), int64(8)
	trace := invariants.Trace{History: []invariants.HistoryRecord{
		{WorkflowID: "wf", Revision: 5, ActorKind: "scheduler", SchedulerEpoch: &oldEpoch},
	}}
	verdict, fence := check(trace, 4, 4, recoveryEpoch, 0)
	const expected = "1 post-recovery history records carry an epoch older than the recovery owner"
	if fence == nil || fence.SupersededCount != 1 || !containsViolation(verdict.Violations, expected) {
		t.Fatalf("fence check accepted a post-boundary history row; fence=%+v violations=%v", fence, verdict.Violations)
	}
}

func TestFenceCheckRejectsAcceptedStaleAttemptResult(t *testing.T) {
	trace := invariants.Trace{Attempts: []invariants.AttemptRecord{{
		WorkflowID: "wf", NodeID: "activity", AttemptNumber: 1,
		State: "TIMED_OUT", IsCurrent: false, ResultRecorded: true,
	}}}
	verdict, fence := check(trace, 1, 2, 8, 1)
	const expected = "stale attempt has a recorded result after the stale-result rejection probe"
	if fence == nil || !fence.StaleAttemptFound || !fence.StaleResultRecorded || !containsViolation(verdict.Violations, expected) {
		t.Fatalf("fence check accepted a recorded stale attempt result; fence=%+v violations=%v", fence, verdict.Violations)
	}
}

func containsViolation(violations []string, expected string) bool {
	for _, violation := range violations {
		if violation == expected {
			return true
		}
	}
	return false
}
