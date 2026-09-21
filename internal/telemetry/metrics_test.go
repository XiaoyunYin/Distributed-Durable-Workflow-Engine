package telemetry

import (
	"context"
	"strings"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
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

func TestDurableReadyTimestampIsStable(t *testing.T) {
	m := New("scheduler-a")
	m.MarkDurableReady()
	first := m.Snapshot().DurableReadyUnix
	if first == 0 {
		t.Fatal("durable-ready timestamp was not recorded")
	}
	time.Sleep(1100 * time.Millisecond)
	m.MarkDurableReady()
	if got := m.Snapshot().DurableReadyUnix; got != first {
		t.Fatalf("durable-ready timestamp changed from %d to %d", first, got)
	}
}

func TestTraceparentRoundTripUsesW3CContext(t *testing.T) {
	parent := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	ctx := ContextWithTraceparent(context.Background(), parent)
	values := map[string]string{}
	InjectMap(ctx, values)
	if got := values["traceparent"]; got != parent {
		t.Fatalf("traceparent = %q, want %q", got, parent)
	}
}

func TestSchedulerSpanCarriesDurableWorkflowLink(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(exporter)))
	tracing := &Tracing{tracer: provider.Tracer("durable-test")}
	parent := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	parentContext := ContextWithTraceparent(context.Background(), parent)
	parentSpanContext := trace.SpanContextFromContext(parentContext)
	_, span := tracing.Start(context.Background(), "scheduler.workflow",
		trace.WithLinks(trace.Link{SpanContext: parentSpanContext}))
	span.End()
	spans := exporter.GetSpans()
	if len(spans) != 1 || len(spans[0].Links) != 1 {
		t.Fatalf("scheduler span links = %+v, want one durable workflow link", spans)
	}
	if got := spans[0].Links[0].SpanContext.TraceID().String(); got != parentSpanContext.TraceID().String() {
		t.Fatalf("linked trace ID = %s, want %s", got, parentSpanContext.TraceID())
	}
}
