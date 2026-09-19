package main

import "testing"

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

func TestDUR035SummaryDoesNotPromoteCostEffects(t *testing.T) {
	runs := make([]runReport, 0, 3)
	for repeat, value := range []float64{10, 12, 11} {
		runs = append(runs, runReport{Configuration: "poll_250ms", Repeat: repeat + 1, Throughput: value,
			Timings: timingReport{ReadyToOutboxClaimMS: medianReport{Count: 12, Median: value}}})
	}
	summary := makeSummary(runs)
	if len(summary) != len(arms) || summary[0].CostEffectsResolved {
		t.Fatalf("summary promoted an unsupported cost effect: %#v", summary)
	}
}
