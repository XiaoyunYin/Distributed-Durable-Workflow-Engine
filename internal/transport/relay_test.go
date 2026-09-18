package transport

import (
	"context"
	"testing"

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
