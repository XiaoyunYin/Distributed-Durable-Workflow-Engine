// Package effects is the cooperating sandbox effect boundary used by M4.
// Its ledger is independent of workflow transition decisions: retries are
// deduplicated by logical effect key and argument meaning, while resource
// fencing is ordered by a sink-local token.
package effects

import (
	"context"
	"encoding/json"

	"durable-agent-execution-engine/internal/state"
	"durable-agent-execution-engine/internal/telemetry"
	"go.opentelemetry.io/otel/attribute"
)

type Service struct {
	store   *state.Store
	tracing *telemetry.Tracing
}

func New(store *state.Store) *Service {
	return &Service{store: store}
}

func (s *Service) WithTracing(tracing *telemetry.Tracing) *Service {
	s.tracing = tracing
	return s
}

func (s *Service) Apply(ctx context.Context, input state.EffectApplyInput) (state.EffectReceipt, error) {
	if s.tracing != nil {
		spanCtx, span := s.tracing.Start(ctx, "effect.apply")
		ctx = spanCtx
		span.SetAttributes(attribute.String("durable.workflow_id", input.WorkflowID),
			attribute.String("durable.effect_key", input.LogicalEffectKey),
			attribute.String("durable.resource", input.ResourceID))
		defer span.End()
	}
	return s.store.ApplyEffect(ctx, input)
}

func (s *Service) Lookup(ctx context.Context, workflowID, logicalEffectKey string) (state.EffectRecord, error) {
	return s.store.LookupEffect(ctx, workflowID, logicalEffectKey)
}

func (s *Service) MarkUnknown(ctx context.Context, workflowID, logicalEffectKey, argumentHash, grantScopeHash string, attemptNumber int64) (state.EffectRecord, error) {
	return s.store.RecordUnknownEffect(ctx, workflowID, logicalEffectKey, argumentHash, grantScopeHash, attemptNumber)
}

func (s *Service) Resolve(ctx context.Context, actorID, workflowID, logicalEffectKey, argumentHash string, receipt json.RawMessage, abandon bool) error {
	return s.store.ResolveUnknownEffect(ctx, actorID, workflowID, logicalEffectKey, argumentHash, receipt, abandon)
}
