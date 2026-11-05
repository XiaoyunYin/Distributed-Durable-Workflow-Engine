package api

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"durable-agent-execution-engine/internal/state"
)

type DemandRepository interface {
	DurableDemand(context.Context, string) (state.DemandSample, error)
}

func (s *Server) WithDemand(repository DemandRepository, namespace string) *Server {
	s.demandRepository, s.demandNamespace = repository, namespace
	return s
}

func (s *Server) durableDemand(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	started := time.Now()
	if s.demandRepository == nil || s.demandNamespace == "" {
		writeError(w, http.StatusServiceUnavailable, "DEMAND_UNAVAILABLE", "demand is not configured", nil)
		return
	}
	sample, err := s.demandRepository.DurableDemand(r.Context(), s.demandNamespace)
	// Use monotonic elapsed time, not host-vs-DB wall-clock comparison: scheduler
	// skew must not turn a fresh DB timestamp into a false stale sample.
	if err != nil || sample.SampledAt.IsZero() || time.Since(started) > 5*time.Second {
		writeError(w, http.StatusServiceUnavailable, "DEMAND_UNAVAILABLE", "fresh demand sample unavailable", nil)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(sample)
}
