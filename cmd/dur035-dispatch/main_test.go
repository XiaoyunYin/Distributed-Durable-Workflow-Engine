package main

import (
	"strings"
	"testing"
)

func TestDUR035ArmProtocol(t *testing.T) {
	if len(arms) != 4 {
		t.Fatalf("got %d arms, want four", len(arms))
	}
	want := []string{"poll_250ms", "poll_1s", "notify_direct", "notify_kafka"}
	for index, name := range want {
		if arms[index].Name != name {
			t.Fatalf("arm %d = %q, want %q", index, arms[index].Name, name)
		}
		if arms[index].FallbackPoll <= 0 || arms[index].PollInterval <= 0 {
			t.Fatalf("arm %q has an invalid interval", name)
		}
	}
	if arms[2].Mode != "direct_notify" || arms[3].Mode != "kafka" {
		t.Fatalf("notification and Kafka modes are not frozen: %#v", arms)
	}
}

func TestDUR035ConclusionUsesObservedIntervals(t *testing.T) {
	runs := make([]runReport, 0, 12)
	for _, config := range []string{"poll_250ms", "poll_1s", "notify_direct", "notify_kafka"} {
		values := map[string][]float64{
			"poll_250ms":    {150, 152, 155},
			"poll_1s":       {590, 600, 610},
			"notify_direct": {4, 5, 6},
			"notify_kafka":  {7, 8, 9},
		}[config]
		for repeat, value := range values {
			runs = append(runs, runReport{Configuration: config, Repeat: repeat + 1,
				Timings: timingReport{ReadyToOutboxClaimMS: medianReport{Count: 24, Median: value}, TerminalLatencyMS: medianReport{Count: 24, Median: value}}})
		}
	}
	conclusions := deriveConclusions(runs)
	if !conclusions.WakeMechanism.Resolved || !conclusions.TransportEffect.Resolved || !conclusions.CostEffectsResolved {
		t.Fatalf("observed separated effects were not promoted: %#v", conclusions)
	}
	if !strings.Contains(conclusions.Interpretation, "Notification-direct is faster than Kafka") ||
		!strings.Contains(conclusions.Interpretation, "not a latency optimization") {
		t.Fatalf("resolved direction was not stated: %s", conclusions.Interpretation)
	}

	for index := range runs {
		runs[index].Timings.ReadyToOutboxClaimMS.Median = 7
	}
	conclusions = deriveConclusions(runs)
	if conclusions.WakeMechanism.Resolved {
		t.Fatalf("wake conclusion ignored overlapping observed intervals: %#v", conclusions)
	}
}
