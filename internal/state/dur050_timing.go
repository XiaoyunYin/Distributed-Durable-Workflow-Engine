package state

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"
)

const dur050TransactionTimingsEnv = "DUR050_RECORD_TRANSACTION_TIMINGS"

type dur050TransactionTiming struct {
	Schema                string    `json:"schema"`
	Operation             string    `json:"operation"`
	Namespace             string    `json:"namespace"`
	WorkflowID            string    `json:"workflow_id"`
	Outcome               string    `json:"outcome"`
	State                 string    `json:"state,omitempty"`
	MeasurementClock      string    `json:"measurement_clock"`
	MeasurementScope      string    `json:"measurement_scope"`
	TransactionDurationUS int64     `json:"transaction_duration_us"`
	RecordedAtUTC         time.Time `json:"recorded_at_utc"`
}

func dur050TransactionTimingsFromEnvironment() (bool, error) {
	switch value := os.Getenv(dur050TransactionTimingsEnv); value {
	case "", "0":
		return false, nil
	case "1":
		return true, nil
	default:
		return false, fmt.Errorf("%s must be 0 or 1", dur050TransactionTimingsEnv)
	}
}

func formatDur050TransactionTiming(operation, namespace, workflowID, outcome, state string,
	duration time.Duration, recordedAt time.Time) (string, bool) {
	if !strings.HasPrefix(namespace, "dur050-") || workflowID == "" ||
		(operation != "submission" && operation != "terminal_transition") || duration < 0 {
		return "", false
	}
	row := dur050TransactionTiming{Schema: "dur050-transaction-timing.v1", Operation: operation,
		Namespace: namespace, WorkflowID: workflowID, Outcome: outcome, State: state,
		MeasurementClock:      "runtime_process_monotonic",
		MeasurementScope:      "pool_begin_to_commit_return_including_pool_wait_network",
		TransactionDurationUS: duration.Microseconds(), RecordedAtUTC: recordedAt.UTC()}
	encoded, err := json.Marshal(row)
	if err != nil {
		return "", false
	}
	return string(encoded), true
}

func (s *Store) recordDur050TransactionTiming(operation, namespace, workflowID, outcome, state string,
	started time.Time) {
	if s == nil || !s.dur050TransactionTimings {
		return
	}
	line, ok := formatDur050TransactionTiming(operation, namespace, workflowID, outcome, state,
		time.Since(started), time.Now())
	if !ok {
		return
	}
	if _, err := fmt.Fprintln(os.Stdout, "DUR050_TXN "+line); err != nil {
		slog.Error("write DUR-050 transaction timing record", "error", err)
	}
}
