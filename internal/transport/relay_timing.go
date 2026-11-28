package transport

import (
	"context"
	"time"

	"durable-agent-execution-engine/internal/state"
	"durable-agent-execution-engine/internal/telemetry"
)

func (r *Relay) publish(ctx context.Context, event state.OutboxEvent) error {
	started := r.Config.Metrics.StartStage()
	err := r.Broker.Publish(ctx, event)
	r.Config.Metrics.RecordStageSince(telemetry.StageRelayBrokerPublish, started)
	return err
}

func (r *Relay) finalizePublication(ctx context.Context, event state.OutboxEvent, acknowledged bool,
	publishErr string, retryAfter time.Duration) error {
	started := r.Config.Metrics.StartStage()
	err := r.Store.FinalizeOutboxPublication(ctx, event.EventID, r.Config.OwnerID, event.RelayAttempts,
		acknowledged, publishErr, retryAfter)
	r.Config.Metrics.RecordStageSince(telemetry.StageRelayFinalize, started)
	return err
}
