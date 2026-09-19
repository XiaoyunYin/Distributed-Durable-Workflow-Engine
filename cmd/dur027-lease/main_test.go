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

func TestSummarizePreservesFaultTypesAndObservedIntervals(t *testing.T) {
	runs := []runReport{
		{ConfigID: "ttl_100ms-owner_crash", FaultType: "owner_crash", TTLMS: 100, RenewalIntervalMS: 33, Status: "PASS", CompletedEpisodes: 10, Takeovers: 10, UsefulProgress: 10, RenewalsBeforeFault: 10, RenewalsAfterTakeover: 20, TakeoverDelayMS: interval{Count: 10, Median: 125}, UsefulProgressDelayMS: interval{Count: 10, Median: 220}},
		{ConfigID: "ttl_100ms-owner_pause", FaultType: "owner_pause", TTLMS: 100, RenewalIntervalMS: 33, Status: "PASS", CompletedEpisodes: 10, Takeovers: 10, UsefulProgress: 10, RenewalsBeforeFault: 10, RenewalsAfterTakeover: 20, TakeoverDelayMS: interval{Count: 10, Median: 126}, UsefulProgressDelayMS: interval{Count: 10, Median: 221}},
	}
	rows := summarize(runs)
	if len(rows) != 2 || rows[0].FaultType != "owner_crash" || rows[1].FaultType != "owner_pause" {
		t.Fatalf("summary rows = %+v", rows)
	}
	if rows[0].RenewalIntervalMS != 33 || rows[0].UsefulProgressDelayMS.Median != 220 {
		t.Fatalf("summary lost observed fields: %+v", rows[0])
	}
}

func TestConclusionsNameBothFaultTypesAndUsefulProgress(t *testing.T) {
	result := artifact{
		Summary: []summaryRow{
			{FaultType: "owner_crash", TTLMS: 100, TakeoverDelayMS: interval{Median: 125, Min: 120, Max: 130}, UsefulProgressDelayMS: interval{Median: 220, Min: 210, Max: 230}},
			{FaultType: "owner_pause", TTLMS: 100, TakeoverDelayMS: interval{Median: 126, Min: 121, Max: 131}, UsefulProgressDelayMS: interval{Median: 221, Min: 211, Max: 231}},
		},
		Validation: validationReport{Status: "PASS", FalseTakeovers: 0, UsefulProgress: 20, Takeovers: 20, FencedOldOwners: 60, CompletedEpisodes: 20},
	}
	got := deriveConclusions(result)
	for _, want := range []string{"owner_crash", "owner_pause", "useful progress", "0 false takeovers"} {
		if !strings.Contains(strings.ToLower(got.TakeoverAndProgress+got.FaultComparison+got.Safety), want) {
			t.Fatalf("conclusions omitted %q: %+v", want, got)
		}
	}
}
