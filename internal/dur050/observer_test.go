package dur050

import (
	"testing"
	"time"
)

func TestFinePollGapValidityBoundary(t *testing.T) {
	for _, testCase := range []struct {
		gap  time.Duration
		want bool
	}{
		{gap: 99 * time.Millisecond, want: true},
		{gap: 100 * time.Millisecond, want: true},
		{gap: 101 * time.Millisecond, want: false},
		{gap: -time.Millisecond, want: false},
	} {
		if got := PollGapValid(testCase.gap, PilotMaxPollGap); got != testCase.want {
			t.Errorf("PollGapValid(%s)=%t, want %t", testCase.gap, got, testCase.want)
		}
	}
}

func TestObserverTerminalStateClassification(t *testing.T) {
	for _, state := range []string{"SUCCEEDED", "FAILED", "REJECTED", "CANCELED", "ABANDONED"} {
		if !IsTerminalWorkflowState(state) {
			t.Errorf("%s should be terminal", state)
		}
	}
	for _, state := range []string{"RUNNABLE", "WAITING_ACTIVITY", "RECONCILIATION_REQUIRED"} {
		if IsTerminalWorkflowState(state) {
			t.Errorf("%s should not be terminal", state)
		}
	}
}
