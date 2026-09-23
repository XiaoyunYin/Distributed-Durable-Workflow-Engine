package main

import (
	"testing"

	"durable-agent-execution-engine/internal/state"
)

func TestSameOwnerEpochControlRequiresObservedFenceAndCleanup(t *testing.T) {
	valid := sameOwnerEpochResult{
		OwnerID: "owner-a", StaleEpoch: 4, CurrentEpoch: 5, SameOwner: true,
		BeforeRevision: 1, AfterRevision: 1, ProbeWorkflowRetained: true,
		ProbeWorkflowFinalState: string(state.StateCanceled), ProbeOutboxSuppressed: true,
		StaleTransitionRejected: true, CurrentLeaseOwnerID: "owner-a",
		CurrentLeaseEpoch: 5, CurrentLeaseActive: true,
	}
	if !sameOwnerEpochControlPassed(valid) {
		t.Fatal("complete same-owner stale-epoch control should pass")
	}

	cases := []struct {
		name   string
		mutate func(*sameOwnerEpochResult)
	}{
		{name: "stale transition accepted", mutate: func(r *sameOwnerEpochResult) { r.StaleTransitionRejected = false }},
		{name: "no live current lease", mutate: func(r *sameOwnerEpochResult) { r.CurrentLeaseActive = false }},
		{name: "wrong active owner", mutate: func(r *sameOwnerEpochResult) { r.CurrentLeaseOwnerID = "owner-b" }},
		{name: "cleanup evidence missing", mutate: func(r *sameOwnerEpochResult) { r.ProbeWorkflowRetained = false }},
		{name: "revision changed", mutate: func(r *sameOwnerEpochResult) { r.AfterRevision++ }},
		{name: "outbox not suppressed", mutate: func(r *sameOwnerEpochResult) { r.ProbeOutboxRowsObserved = 1 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			candidate := valid
			tc.mutate(&candidate)
			if sameOwnerEpochControlPassed(candidate) {
				t.Fatal("invalid evidence was accepted")
			}
		})
	}
}
