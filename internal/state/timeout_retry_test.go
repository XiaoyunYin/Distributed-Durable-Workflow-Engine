package state

import (
	"testing"
	"time"
)

func TestTimeoutRetryPolicyEnvironment(t *testing.T) {
	for _, tc := range []struct {
		value   string
		enabled bool
		invalid bool
	}{
		{value: ""},
		{value: "0"},
		{value: "1", enabled: true},
		{value: "true", invalid: true},
	} {
		t.Run("value="+tc.value, func(t *testing.T) {
			t.Setenv(timeoutRetryPolicyEnv, tc.value)
			enabled, err := timeoutRetryPolicyFromEnvironment()
			if enabled != tc.enabled || (err != nil) != tc.invalid {
				t.Fatalf("enabled=%v, error=%v", enabled, err)
			}
		})
	}
}

func TestTimeoutReplacementDeadlineBoundary(t *testing.T) {
	deadline := time.Unix(100, 0)
	p := RetryPolicy{MaxRetries: 3, TotalDeadlineAt: &deadline}
	for _, tc := range []struct {
		name     string
		offset   time.Duration
		expiries int
		reason   string
	}{
		{"before", -time.Microsecond, 2, ""},
		{"at", 0, 2, timeoutDeadlineReason},
		{"after", time.Microsecond, 2, timeoutDeadlineReason},
		{"budget alone", -time.Microsecond, 3, timeoutBudgetReason},
		{"both", 0, 3, timeoutDeadlineReason},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := timeoutReplacementFailure(p, tc.expiries, deadline.Add(tc.offset)); got != tc.reason {
				t.Fatalf("reason = %q, want %q", got, tc.reason)
			}
		})
	}
}
