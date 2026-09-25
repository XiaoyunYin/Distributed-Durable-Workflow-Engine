package state

import (
	"encoding/json"
	"testing"
	"time"
)

func TestDUR050TransactionTimingIsOptInAndStrict(t *testing.T) {
	for _, value := range []string{"", "0"} {
		t.Setenv(dur050TransactionTimingsEnv, value)
		enabled, err := dur050TransactionTimingsFromEnvironment()
		if err != nil || enabled {
			t.Fatalf("value %q: enabled=%v err=%v; want disabled", value, enabled, err)
		}
	}
	t.Setenv(dur050TransactionTimingsEnv, "1")
	enabled, err := dur050TransactionTimingsFromEnvironment()
	if err != nil || !enabled {
		t.Fatalf("value 1: enabled=%v err=%v; want enabled", enabled, err)
	}
	t.Setenv(dur050TransactionTimingsEnv, "true")
	if _, err := dur050TransactionTimingsFromEnvironment(); err == nil {
		t.Fatal("non-binary timing flag was accepted")
	}
}

func TestDUR050TransactionTimingLineIsNamespacedAndMachineReadable(t *testing.T) {
	startedAt := time.Date(2026, time.September, 25, 1, 2, 3, 0, time.UTC)
	if _, ok := formatDur050TransactionTiming("submission", "ordinary-demo", "workflow-1", "created", "", time.Millisecond, startedAt); ok {
		t.Fatal("non-campaign namespace emitted a timing row")
	}
	line, ok := formatDur050TransactionTiming("submission", "dur050-calibration", "workflow-1", "created", "", 12345*time.Microsecond, startedAt)
	if !ok {
		t.Fatal("valid DUR-050 transaction row was refused")
	}
	var row dur050TransactionTiming
	if err := json.Unmarshal([]byte(line), &row); err != nil {
		t.Fatalf("decode timing row: %v", err)
	}
	if row.Schema != "dur050-transaction-timing.v1" || row.Operation != "submission" ||
		row.Namespace != "dur050-calibration" || row.WorkflowID != "workflow-1" ||
		row.Outcome != "created" || row.MeasurementClock != "runtime_process_monotonic" ||
		row.MeasurementScope != "pool_begin_to_commit_return_including_pool_wait_network" ||
		row.TransactionDurationUS != 12345 || !row.RecordedAtUTC.Equal(startedAt) {
		t.Fatalf("unexpected timing row: %+v", row)
	}
	if _, ok := formatDur050TransactionTiming("query", "dur050-calibration", "workflow-1", "created", "", time.Millisecond, startedAt); ok {
		t.Fatal("unsupported transaction operation was emitted")
	}
}
