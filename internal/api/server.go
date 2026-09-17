package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"durable-agent-execution-engine/internal/partition"
	"durable-agent-execution-engine/internal/state"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	defaultHistoryLimit = 100
	maxRequestBytes     = 2 << 20
	submissionHashV1    = "sub-v1:"
)

type Repository interface {
	CreateWorkflow(context.Context, state.CreateWorkflowInput) (state.CreateWorkflowResult, error)
	GetWorkflow(context.Context, string) (state.Workflow, error)
	ListHistory(context.Context, string, int64, int) ([]state.TransitionRecord, error)
}

type Server struct {
	repository Repository
}

func NewServer(repository Repository) *Server {
	return &Server{repository: repository}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/workflows", s.createWorkflow)
	mux.HandleFunc("GET /v1/workflows/{workflowID}", s.getWorkflow)
	mux.HandleFunc("GET /v1/workflows/{workflowID}/history", s.getHistory)
	mux.HandleFunc("/v1/", func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "route was not found", nil)
	})
	return mux
}

type submitWorkflowRequest struct {
	WorkflowID        string          `json:"workflow_id,omitempty"`
	Namespace         string          `json:"namespace,omitempty"`
	SubmissionKey     string          `json:"submission_key"`
	Payload           json.RawMessage `json:"payload"`
	DefinitionID      string          `json:"definition_id"`
	DefinitionVersion int             `json:"definition_version"`
	InitialNodeID     string          `json:"initial_node_id"`
	InitialInput      json.RawMessage `json:"initial_input,omitempty"`
	ActorID           string          `json:"actor_id,omitempty"`
}

type workflowResponse struct {
	Workflow workflowView `json:"workflow"`
	Created  bool         `json:"created"`
}

type workflowView struct {
	WorkflowID        string              `json:"workflow_id"`
	Namespace         string              `json:"namespace"`
	SubmissionKey     string              `json:"submission_key"`
	PayloadHash       string              `json:"payload_hash"`
	DefinitionID      string              `json:"definition_id"`
	DefinitionVersion int                 `json:"definition_version"`
	PartitionID       int16               `json:"partition_id"`
	State             state.WorkflowState `json:"state"`
	Revision          int64               `json:"revision"`
	CreatedAt         time.Time           `json:"created_at"`
	UpdatedAt         time.Time           `json:"updated_at"`
}

type historyResponse struct {
	WorkflowID        string        `json:"workflow_id"`
	Entries           []historyView `json:"entries"`
	NextAfterRevision *int64        `json:"next_after_revision,omitempty"`
}

type historyView struct {
	TransitionID   string               `json:"transition_id"`
	WorkflowID     string               `json:"workflow_id"`
	Revision       int64                `json:"revision"`
	ActorKind      string               `json:"actor_kind"`
	ActorID        string               `json:"actor_id"`
	SchedulerEpoch *int64               `json:"scheduler_epoch,omitempty"`
	NodeID         *string              `json:"node_id,omitempty"`
	Iteration      *int                 `json:"iteration,omitempty"`
	AttemptNumber  *int64               `json:"attempt_number,omitempty"`
	OldState       *state.WorkflowState `json:"old_state,omitempty"`
	NewState       state.WorkflowState  `json:"new_state"`
	Reason         string               `json:"reason"`
	CreatedAt      time.Time            `json:"created_at"`
}

type errorResponse struct {
	Error errorView `json:"error"`
}

type errorView struct {
	Code            string `json:"code"`
	Message         string `json:"message"`
	CurrentRevision *int64 `json:"current_revision,omitempty"`
}

func (s *Server) createWorkflow(w http.ResponseWriter, r *http.Request) {
	var request submitWorkflowRequest
	if err := decodeJSON(w, r, &request); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", err.Error(), nil)
		return
	}
	if request.Namespace == "" {
		request.Namespace = "default"
	}
	if request.WorkflowID == "" {
		request.WorkflowID = state.NewID()
	}
	if request.ActorID == "" {
		request.ActorID = "client"
	}
	if request.SubmissionKey == "" || request.DefinitionID == "" || request.DefinitionVersion <= 0 || request.InitialNodeID == "" {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "submission_key, definition_id, positive definition_version, and initial_node_id are required", nil)
		return
	}
	initialInput := request.InitialInput
	if len(initialInput) == 0 {
		initialInput = json.RawMessage(`{}`)
	}
	payloadHash, err := canonicalSubmissionHash(request, initialInput)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", err.Error(), nil)
		return
	}
	partitionID, err := partition.ID(request.WorkflowID)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "workflow_id is invalid", nil)
		return
	}
	result, err := s.repository.CreateWorkflow(r.Context(), state.CreateWorkflowInput{
		WorkflowID:            request.WorkflowID,
		Namespace:             request.Namespace,
		SubmissionKey:         request.SubmissionKey,
		SubmissionPayloadHash: payloadHash,
		DefinitionID:          request.DefinitionID,
		DefinitionVersion:     request.DefinitionVersion,
		PartitionID:           int16(partitionID),
		InitialNodeID:         request.InitialNodeID,
		InitialInput:          initialInput,
		ActorID:               request.ActorID,
	})
	if err != nil {
		writeRepositoryError(w, err)
		return
	}
	status := http.StatusOK
	if result.Created {
		status = http.StatusCreated
	}
	writeJSON(w, status, workflowResponse{Workflow: makeWorkflowView(result.Workflow), Created: result.Created})
}

func (s *Server) getWorkflow(w http.ResponseWriter, r *http.Request) {
	workflowID := r.PathValue("workflowID")
	workflow, err := s.repository.GetWorkflow(r.Context(), workflowID)
	if err != nil {
		writeRepositoryError(w, err)
		return
	}
	if raw := r.URL.Query().Get("expected_revision"); raw != "" {
		expected, parseErr := strconv.ParseInt(raw, 10, 64)
		if parseErr != nil || expected < 0 {
			writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "expected_revision must be a non-negative integer", nil)
			return
		}
		if expected != workflow.Revision {
			current := workflow.Revision
			writeError(w, http.StatusConflict, "STALE_REVISION", "workflow revision is stale", &current)
			return
		}
	}
	writeJSON(w, http.StatusOK, workflowResponse{Workflow: makeWorkflowView(workflow), Created: false})
}

func (s *Server) getHistory(w http.ResponseWriter, r *http.Request) {
	workflowID := r.PathValue("workflowID")
	afterRevisionValue, err := parseNonNegativeQuery(r, "after_revision", 0)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", err.Error(), nil)
		return
	}
	afterRevision := int64(afterRevisionValue)
	limit, err := parseNonNegativeQuery(r, "limit", defaultHistoryLimit)
	if err != nil || limit == 0 || limit > 1000 {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "limit must be between 1 and 1000", nil)
		return
	}
	workflow, err := s.repository.GetWorkflow(r.Context(), workflowID)
	if err != nil {
		writeRepositoryError(w, err)
		return
	}
	if afterRevision > workflow.Revision {
		current := workflow.Revision
		writeError(w, http.StatusConflict, "STALE_REVISION", "history cursor is ahead of the workflow revision", &current)
		return
	}
	entries, err := s.repository.ListHistory(r.Context(), workflowID, afterRevision, limit)
	if err != nil {
		writeRepositoryError(w, err)
		return
	}
	response := historyResponse{WorkflowID: workflowID, Entries: make([]historyView, 0, len(entries))}
	for _, entry := range entries {
		response.Entries = append(response.Entries, makeHistoryView(entry))
	}
	if len(entries) == limit {
		next := entries[len(entries)-1].Revision
		response.NextAfterRevision = &next
	}
	writeJSON(w, http.StatusOK, response)
}

func decodeJSON(w http.ResponseWriter, r *http.Request, target any) error {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxRequestBytes))
	if err != nil {
		return fmt.Errorf("invalid request body: %w", err)
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return errors.New("request body is required")
	}
	if err := validateJSONDocument(body); err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		if errors.Is(err, io.EOF) {
			return errors.New("request body is required")
		}
		return fmt.Errorf("invalid JSON: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return errors.New("request body must contain one JSON value")
		}
		return fmt.Errorf("invalid trailing JSON: %w", err)
	}
	return nil
}

func validateJSONDocument(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := validateJSONValue(decoder); err != nil {
		return fmt.Errorf("JSON must be valid without duplicate object keys: %w", err)
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return errors.New("JSON must contain one value")
		}
		return fmt.Errorf("JSON must contain one value: %w", err)
	}
	return nil
}

func canonicalSubmissionHash(request submitWorkflowRequest, initialInput json.RawMessage) (string, error) {
	canonicalInitialInput, err := canonicalJSON(initialInput, "initial_input")
	if err != nil {
		return "", err
	}
	canonicalPayload, err := canonicalJSON(request.Payload, "payload")
	if err != nil {
		return "", err
	}
	fingerprint := struct {
		DefinitionID      string          `json:"definition_id"`
		DefinitionVersion int             `json:"definition_version"`
		InitialNodeID     string          `json:"initial_node_id"`
		InitialInput      json.RawMessage `json:"initial_input"`
		Payload           json.RawMessage `json:"payload"`
	}{
		DefinitionID: request.DefinitionID, DefinitionVersion: request.DefinitionVersion,
		InitialNodeID: request.InitialNodeID, InitialInput: canonicalInitialInput,
		Payload: canonicalPayload,
	}
	canonical, err := json.Marshal(fingerprint)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return submissionHashV1 + hex.EncodeToString(sum[:]), nil
}

// canonicalPayloadHash is retained for focused tests and callers that need to
// describe JSON canonicalization independently of submission identity.
func canonicalPayloadHash(raw json.RawMessage) (string, error) {
	canonical, err := canonicalJSON(raw, "payload")
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}

func canonicalJSON(raw json.RawMessage, field string) ([]byte, error) {
	if len(raw) == 0 {
		if field == "payload" {
			return nil, errors.New("payload is required")
		}
		return nil, fmt.Errorf("%s must be valid JSON", field)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := validateJSONValue(decoder); err != nil {
		return nil, fmt.Errorf("%s must be valid JSON without duplicate object keys: %w", field, err)
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("%s must contain one JSON value", field)
		}
		return nil, fmt.Errorf("%s must contain one JSON value: %w", field, err)
	}
	decoder = json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return canonical, nil
}

func validateJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, isDelimiter := token.(json.Delim)
	if !isDelimiter {
		return nil
	}
	switch delimiter {
	case '{':
		seen := map[string]struct{}{}
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return errors.New("object key is not a string")
			}
			if _, exists := seen[key]; exists {
				return fmt.Errorf("duplicate object key %q", key)
			}
			seen[key] = struct{}{}
			if err := validateJSONValue(decoder); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil {
			return err
		}
		if end != json.Delim('}') {
			return errors.New("object did not terminate")
		}
	case '[':
		for decoder.More() {
			if err := validateJSONValue(decoder); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil {
			return err
		}
		if end != json.Delim(']') {
			return errors.New("array did not terminate")
		}
	default:
		return fmt.Errorf("unexpected JSON delimiter %q", delimiter)
	}
	return nil
}

func parseNonNegativeQuery(r *http.Request, name string, fallback int) (int, error) {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 0 {
		return 0, fmt.Errorf("%s must be a non-negative integer", name)
	}
	return value, nil
}

func makeWorkflowView(workflow state.Workflow) workflowView {
	return workflowView{
		WorkflowID: workflow.WorkflowID, Namespace: workflow.Namespace,
		SubmissionKey: workflow.SubmissionKey, PayloadHash: workflow.PayloadHash,
		DefinitionID: workflow.DefinitionID, DefinitionVersion: workflow.DefinitionVersion,
		PartitionID: workflow.PartitionID, State: workflow.State, Revision: workflow.Revision,
		CreatedAt: workflow.CreatedAt, UpdatedAt: workflow.UpdatedAt,
	}
}

func makeHistoryView(record state.TransitionRecord) historyView {
	return historyView{
		TransitionID: record.TransitionID, WorkflowID: record.WorkflowID,
		Revision: record.Revision, ActorKind: record.ActorKind, ActorID: record.ActorID,
		SchedulerEpoch: record.SchedulerEpoch, NodeID: record.NodeID, Iteration: record.Iteration,
		AttemptNumber: record.AttemptNumber, OldState: record.OldState, NewState: record.NewState,
		Reason: record.Reason, CreatedAt: record.CreatedAt,
	}
}

func writeRepositoryError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, state.ErrSubmissionConflict):
		writeError(w, http.StatusConflict, "PAYLOAD_CONFLICT", "submission key already has a different payload", nil)
	case errors.Is(err, state.ErrDefinitionNotFound):
		writeError(w, http.StatusNotFound, "DEFINITION_NOT_FOUND", "workflow definition was not found", nil)
	case errors.Is(err, state.ErrUnknownNode):
		writeError(w, http.StatusUnprocessableEntity, "UNKNOWN_INITIAL_NODE", "initial node is not declared by the workflow definition", nil)
	case errors.Is(err, state.ErrWorkflowIDConflict):
		writeError(w, http.StatusConflict, "WORKFLOW_ID_CONFLICT", "workflow_id is already used by another submission", nil)
	case errors.Is(err, state.ErrWorkflowNotFound):
		writeError(w, http.StatusNotFound, "NOT_FOUND", "workflow was not found", nil)
	case errors.Is(err, state.ErrRevisionConflict):
		writeError(w, http.StatusConflict, "STALE_REVISION", "the requested workflow state is stale", nil)
	case errors.Is(err, state.ErrStaleClaim):
		writeError(w, http.StatusConflict, "STALE_CLAIM", "the claim is stale", nil)
	case errors.Is(err, state.ErrAttemptNotCurrent):
		writeError(w, http.StatusConflict, "STALE_ATTEMPT", "the attempt is no longer current", nil)
	case isDatabaseUnavailable(err):
		writeError(w, http.StatusServiceUnavailable, "DATABASE_UNAVAILABLE", "database is unavailable", nil)
	default:
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "internal server error", nil)
	}
}

func isDatabaseUnavailable(err error) bool {
	var connectErr *pgconn.ConnectError
	if errors.As(err, &connectErr) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if pgconn.SafeToRetry(err) {
		return true
	}
	var pgError *pgconn.PgError
	if errors.As(err, &pgError) {
		code := pgError.SQLState()
		if strings.HasPrefix(code, "08") || strings.HasPrefix(code, "57P0") || code == "53300" {
			return true
		}
	}
	var networkError net.Error
	if errors.As(err, &networkError) {
		return true
	}
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return true
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "connection is closed") ||
		strings.Contains(message, "connection closed unexpectedly") ||
		strings.Contains(message, "broken pipe") ||
		strings.Contains(message, "connection reset by peer")
}

func writeError(w http.ResponseWriter, status int, code, message string, currentRevision *int64) {
	writeJSON(w, status, errorResponse{Error: errorView{Code: code, Message: message, CurrentRevision: currentRevision}})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

var _ Repository = (*state.Store)(nil)
