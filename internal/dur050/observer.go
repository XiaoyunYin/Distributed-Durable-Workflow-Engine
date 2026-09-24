// Package dur050 contains campaign-only measurement helpers.
package dur050

import "time"

const (
	PilotPollInterval = 50 * time.Millisecond
	PilotMaxPollGap   = 100 * time.Millisecond
	BatchPollInterval = time.Second
)

func IsTerminalWorkflowState(state string) bool {
	switch state {
	case "SUCCEEDED", "FAILED", "REJECTED", "CANCELED", "ABANDONED":
		return true
	default:
		return false
	}
}

// PollGapValid distinguishes a measured fine-resolution sample from one whose
// terminal observation was delayed by an observer scheduling/query stall.
func PollGapValid(gap, maximum time.Duration) bool {
	return gap >= 0 && maximum > 0 && gap <= maximum
}
