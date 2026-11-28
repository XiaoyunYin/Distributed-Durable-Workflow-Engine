package state

import (
	"errors"
	"testing"

	"durable-agent-execution-engine/migrations"
)

func TestJ4ProviderRetryVectorAndFixedDelayControl(t *testing.T) {
	for i, want := range []int64{2302, 4484, 8486, 16039} {
		if got := providerRetryDelay(i+1, 0, false); got != want*1_000_000 {
			t.Fatalf("attempt %d delay %d want %d ms", i+1, got, want)
		}
	}
	if providerRetryDelay(3, 12, false) != 12_000_000_000 ||
		providerRetryDelay(3, 12, true) != 12_000_000_000 {
		t.Fatal("Retry-After floor was lost")
	}
	if providerRetryDelay(1, 1, true) <= (2302+750)*1_000_000 ||
		providerRetryDelay(2, 0, true) >= 4484*1_000_000 {
		t.Fatal("mutant would fit the frozen ordinary window")
	}
}

func TestJ4ProviderOutcomeTimeValidation(t *testing.T) {
	row := providerRow{ProviderPermit: ProviderPermit{IssuedNS: 10, DeadlineNS: 20}}
	for _, tc := range []struct {
		kind string
		at   int64
		want int64
	}{
		{"timeout", 19, 0}, {"timeout", 22, 20}, {"timeout", 24, 0},
		{"success", 20, 0}, {"rate_limited", 20, 0}, {"rate_limited", 19, 19},
		{"ambiguous", 20, 0}, {"success", 9, 0},
	} {
		got, err := validateProviderOutcome(row, ProviderOutcome{Kind: tc.kind, ObservedNS: tc.at}, 23)
		if tc.want == 0 && !errors.Is(err, ErrProviderInput) {
			t.Fatalf("accepted %s at %d", tc.kind, tc.at)
		}
		if tc.want != 0 && (err != nil || got.ObservedNS != tc.want) {
			t.Fatalf("outcome %s: %+v, %v", tc.kind, got, err)
		}
	}
}

func TestJ4MigrationOutsideDefaultInventory(t *testing.T) {
	entries, err := migrations.SQL.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Name() == "j4_b2" || entry.Name() == "000025_j4_provider_resilience.up.sql" {
			t.Fatalf("B2 schema entered the default migration inventory: %s", entry.Name())
		}
	}
}
