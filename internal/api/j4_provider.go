package api

import (
	"context"
	"errors"
	"net/http"

	"durable-agent-execution-engine/internal/state"
)

type j4ProviderControl interface {
	Reserve(context.Context, state.ProviderClaim, string) (state.ProviderSchedule, error)
	Outcome(context.Context, state.ProviderClaim, state.ProviderOutcome) (state.ProviderSchedule, error)
	CheckResult(context.Context, state.ResultInput) error
}

func (s *Server) WithJ4Provider(control *state.ProviderControl) *Server {
	if control != nil {
		s.j4 = control
	}
	return s
}

type j4ClaimRequest struct {
	Attempt int64  `json:"attempt_number"`
	Token   string `json:"claim_token"`
	BootID  string `json:"boot_id"`
}

func (c j4ClaimRequest) claim(r *http.Request) (state.ProviderClaim, bool) {
	workflow, node, iteration, ok := parseAttemptPath(r)
	return state.ProviderClaim{WorkflowID: workflow, NodeID: node, Iteration: iteration,
		Attempt: c.Attempt, ClaimToken: c.Token, BootID: c.BootID}, ok
}

func (s *Server) registerJ4Routes(mux *http.ServeMux) {
	const base = "POST /v1/workflows/{workflowID}/nodes/{nodeID}/iterations/{iteration}/provider-attempts/"
	mux.HandleFunc(base+"reserve", s.reserveJ4Provider)
	mux.HandleFunc(base+"outcome", s.outcomeJ4Provider)
}

func (s *Server) reserveJ4Provider(w http.ResponseWriter, r *http.Request) {
	var request struct {
		j4ClaimRequest
		RequestID string `json:"request_id"`
	}
	if err := decodeJSON(w, r, &request); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", err.Error(), nil)
		return
	}
	claim, ok := request.claim(r)
	if !ok {
		writeJ4Error(w, state.ErrProviderInput)
		return
	}
	reply, err := s.j4.Reserve(r.Context(), claim, request.RequestID)
	writeJ4Schedule(w, reply, err)
}

func (s *Server) outcomeJ4Provider(w http.ResponseWriter, r *http.Request) {
	var request struct {
		j4ClaimRequest
		CallID            string `json:"call_id"`
		Kind              string `json:"kind"`
		ObservedNS        int64  `json:"observed_ns"`
		RetryAfterSeconds int    `json:"retry_after_seconds"`
	}
	if err := decodeJSON(w, r, &request); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", err.Error(), nil)
		return
	}
	claim, ok := request.claim(r)
	if !ok {
		writeJ4Error(w, state.ErrProviderInput)
		return
	}
	reply, err := s.j4.Outcome(r.Context(), claim, state.ProviderOutcome{
		CallID: request.CallID, Kind: request.Kind, ObservedNS: request.ObservedNS,
		RetryAfterSeconds: request.RetryAfterSeconds,
	})
	writeJ4Schedule(w, reply, err)
}

func writeJ4Schedule(w http.ResponseWriter, reply state.ProviderSchedule, err error) {
	if err != nil {
		writeJ4Error(w, err)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, reply)
}

func writeJ4Error(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, state.ErrProviderInput):
		writeError(w, http.StatusBadRequest, "INVALID_PROVIDER_REQUEST", err.Error(), nil)
	case errors.Is(err, state.ErrProviderConflict):
		writeError(w, http.StatusConflict, "PROVIDER_CONFLICT", err.Error(), nil)
	case errors.Is(err, state.ErrProviderClock):
		writeError(w, http.StatusConflict, "PROVIDER_CLOCK_MISMATCH", err.Error(), nil)
	default:
		writeRepositoryError(w, err)
	}
}
