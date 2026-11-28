package transport

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/state"
	"durable-agent-execution-engine/internal/telemetry"
)

func TestRelayBrokerHistogramBracketsWitnessedCall(t *testing.T) {
	m := telemetry.NewWithStageTimings("relay", true)
	var inside time.Duration
	brokerErr := errors.New("broker returned an error")
	broker := &MemoryBroker{OnPublish: func(state.OutboxEvent) error {
		started := time.Now()
		time.Sleep(5 * time.Millisecond)
		inside = time.Since(started)
		return brokerErr
	}}
	relay := NewRelay(nil, broker, RelayConfig{Metrics: m})
	started := time.Now()
	err := relay.publish(context.Background(), state.OutboxEvent{})
	outside := time.Since(started)
	if !errors.Is(err, brokerErr) {
		t.Fatalf("publish error = %v", err)
	}
	rendered := m.Render()
	prefix := `durable_j9a_relay_stage_seconds_sum{role="relay",stage="broker_publish"} `
	_, sumLine, found := strings.Cut(rendered, prefix)
	seconds, _, _ := strings.Cut(sumLine, "\n")
	measured, err := strconv.ParseFloat(seconds, 64)
	if !found || err != nil || measured < inside.Seconds() || measured > outside.Seconds() {
		t.Fatalf("broker sample %q outside witnessed bounds [%s, %s]", seconds, inside, outside)
	}
	if !strings.Contains(rendered, `durable_j9a_relay_stage_seconds_count{role="relay",stage="broker_publish"} 1`+"\n") {
		t.Fatal("broker error was not counted as a timed call")
	}
}
