package main

import (
	"strings"
	"testing"
)

func TestMakeIntervalUsesObservedValues(t *testing.T) {
	got := makeInterval([]float64{9, 1, 5, 3, 7})
	if got.Count != 5 || got.Min != 1 || got.Median != 5 || got.Max != 9 {
		t.Fatalf("interval = %+v", got)
	}
}

func TestSummarizeSeparatesLeaseArms(t *testing.T) {
	runs := []runReport{
		{Arm: "ttl_100ms", TTLMS: 100, Status: "PASS", TakeoverDelayMS: interval{Median: 130}, RenewalCount: 12, Takeovers: 12, UsefulRecoveries: 12, LockWaits: 1},
		{Arm: "ttl_100ms", TTLMS: 100, Status: "PASS", TakeoverDelayMS: interval{Median: 120}, RenewalCount: 13, Takeovers: 12, UsefulRecoveries: 12, LockWaits: 1},
		{Arm: "ttl_250ms", TTLMS: 250, Status: "PASS", TakeoverDelayMS: interval{Median: 280}, RenewalCount: 5, Takeovers: 12, UsefulRecoveries: 12, LockWaits: 1},
		{Arm: "ttl_750ms", TTLMS: 750, Status: "PASS", TakeoverDelayMS: interval{Median: 790}, RenewalCount: 2, Takeovers: 12, UsefulRecoveries: 12, LockWaits: 1},
	}
	rows := summarize(runs)
	if len(rows) != 3 {
		t.Fatalf("summary rows = %d, want 3", len(rows))
	}
	if rows[0].MedianTakeoverMS != 125 || rows[0].RenewalsPerRun != 12.5 {
		t.Fatalf("short arm summary = %+v", rows[0])
	}
	if rows[0].RenewalsPerRun <= rows[1].RenewalsPerRun || rows[1].RenewalsPerRun <= rows[2].RenewalsPerRun {
		t.Fatalf("renewal traffic did not preserve observed ordering: %+v", rows)
	}
}

func TestConclusionsNamePauseLimitAndObservedSafety(t *testing.T) {
	result := artifact{Summary: []summaryRow{
		{Arm: "ttl_100ms", TTLMS: 100, MedianTakeoverMS: 130, RenewalsPerRun: 12},
		{Arm: "ttl_250ms", TTLMS: 250, MedianTakeoverMS: 280, RenewalsPerRun: 5},
		{Arm: "ttl_750ms", TTLMS: 750, MedianTakeoverMS: 790, RenewalsPerRun: 2},
	}, Validation: validationReport{FalseTakeovers: 0, UsefulRecoveries: 36, Takeovers: 36, FencedOldOwners: 108}}
	got := deriveConclusions(result)
	if !strings.Contains(got.PauseInterpretation, "without claiming equivalence to a hard kill") {
		t.Fatalf("pause interpretation = %q", got.PauseInterpretation)
	}
	if !strings.Contains(got.Safety, "0 false takeovers") || !strings.Contains(got.Recovery, "130.0 ms") {
		t.Fatalf("conclusions omitted observed values: %+v", got)
	}
}
