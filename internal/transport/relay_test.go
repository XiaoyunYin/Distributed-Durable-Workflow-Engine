package transport

import (
	"context"
	"sync"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/state"
)

func TestRelayRunReportsPassErrors(t *testing.T) {
	callbackCalls := 0
	relay := NewRelay(nil, nil, RelayConfig{OnError: func(error) { callbackCalls++ }})
	relay.runOnceAndReportError(context.Background())
	if relay.ErrorCount() != 1 || callbackCalls != 1 {
		t.Fatalf("relay errors = %d callback calls = %d, want one each", relay.ErrorCount(), callbackCalls)
	}
}

func TestRelayUsesSharedEventRegistry(t *testing.T) {
	for _, eventType := range []string{
		"workflow.created", "attempt.dispatch", "attempt.redispatch", "activity.result",
		"workflow.result_consumed", "graph.advanced", "timer.scheduled", "workflow.canceled",
		"reconciliation.required", "reconciliation.resolved",
	} {
		if !supportedEventType(eventType) {
			t.Fatalf("relay rejected registry event type %q", eventType)
		}
	}
	if supportedEventType("unregistered.event") {
		t.Fatal("relay accepted an unregistered event type")
	}
	if topic, ok := state.EventTopicFor("activity.result"); !ok || topic != DefaultTopic {
		t.Fatalf("activity result topic = %q, ok=%v", topic, ok)
	}
}

func TestK1RelayDrainJoinsListener(t *testing.T) {
	ctx, store := openM3TransportDatabase(t)
	defer store.Close()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	passDone := make(chan struct{})
	var passOnce sync.Once
	invalidPartition := int16(-1)
	drain := make(chan struct{})
	relay := NewRelay(store, &MemoryBroker{}, RelayConfig{Drain: drain, PartitionID: &invalidPartition, PollInterval: time.Hour,
		OnError: func(error) { passOnce.Do(func() { close(passDone) }) }, OnNotification: func() { once.Do(func() { close(entered) }); <-release }})
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- relay.Run(runCtx) }()
	deadline := time.Now().Add(2 * time.Second)
	for {
		_, _ = store.Pool().Exec(ctx, `SELECT pg_notify('durable_agent_outbox','k1')`)
		select {
		case <-entered:
			goto listening
		default:
		}
		if time.Now().After(deadline) {
			close(release)
			cancel()
			<-done
			t.Fatal("listener did not observe notification")
		}
		time.Sleep(10 * time.Millisecond)
	}
listening:
	// The deliberate invalid-partition pass reports its completion without
	// processing an unbounded amount of shared-database outbox work. Keep the
	// parent context alive so both drain selection and listener join are tested.
	select {
	case <-passDone:
	case <-time.After(time.Second):
		close(release)
		cancel()
		<-done
		t.Fatal("relay initial pass did not finish")
	}
	close(drain)
	select {
	case <-done:
		close(release)
		t.Fatal("relay returned before LISTEN joined")
	case <-time.After(time.Second):
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		cancel()
		<-done
		t.Fatal("relay ignored drain or failed to join LISTEN")
	}
}
