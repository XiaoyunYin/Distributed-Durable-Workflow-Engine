package telemetry

import (
	"strings"
	"testing"
	"time"
)

func TestMetricsRenderUsesBoundedRegistry(t *testing.T) {
	m := New("scheduler-a")
	m.MarkDurableReady()
	m.RecordLeaseAcquired()
	m.RecordClaimAccepted()
	m.RecordResultAccepted()
	m.RecordDBQuery(2 * time.Millisecond)
	m.RecordLockWait(time.Millisecond)
	m.SetBacklogAge(3 * time.Second)
	m.WorkerStarted()
	m.WorkerFinished()
	rendered := m.Render()
	for _, name := range []string{
		"durable_runtime_durable_ready_timestamp_seconds",
		"durable_scheduler_lease_acquisitions_total",
		"durable_worker_claims_accepted_total",
		"durable_worker_results_accepted_total",
		"durable_db_query_seconds_total",
		"durable_db_lock_wait_seconds_total",
		"durable_reconciliation_oldest_age_seconds",
	} {
		if !strings.Contains(rendered, name) {
			t.Fatalf("metrics output lacks %q:\n%s", name, rendered)
		}
	}
	if strings.Contains(rendered, "workflow") || strings.Contains(rendered, "incident") {
		t.Fatalf("metrics output contains an unbounded label or value: %s", rendered)
	}
	if snapshot := m.Snapshot(); snapshot.AcceptedClaims != 1 || snapshot.WorkerCompleted != 1 {
		t.Fatalf("unexpected snapshot: %+v", snapshot)
	}
}

func TestMetricsConcurrentUpdates(t *testing.T) {
	m := New("worker")
	done := make(chan struct{}, 8)
	for i := 0; i < 8; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				m.RecordClaimAccepted()
				m.RecordDBQuery(time.Microsecond)
			}
			done <- struct{}{}
		}()
	}
	for i := 0; i < 8; i++ {
		<-done
	}
	if got := m.Snapshot().AcceptedClaims; got != 800 {
		t.Fatalf("accepted claims = %d, want 800", got)
	}
}
