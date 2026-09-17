package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/state"
)

type fakeRepository struct {
	createResult state.CreateWorkflowResult
	createErr    error
	workflow     state.Workflow
	workflowErr  error
	history      []state.TransitionRecord
	historyErr   error
	lastCreate   state.CreateWorkflowInput
}

func (f *fakeRepository) CreateWorkflow(_ context.Context, input state.CreateWorkflowInput) (state.CreateWorkflowResult, error) {
	f.lastCreate = input
	return f.createResult, f.createErr
}

func (f *fakeRepository) GetWorkflow(_ context.Context, _ string) (state.Workflow, error) {
	return f.workflow, f.workflowErr
}

func (f *fakeRepository) ListHistory(_ context.Context, _ string, _ int64, _ int) ([]state.TransitionRecord, error) {
	return f.history, f.historyErr
}

func TestSubmissionErrorMappingAndCanonicalPayloadHash(t *testing.T) {
	fake := &fakeRepository{createErr: state.ErrSubmissionConflict}
	handler := NewServer(fake).Handler()
	request := httptest.NewRequest(http.MethodPost, "/v1/workflows", strings.NewReader(`{
		"submission_key":"submission-1",
		"payload":{"b":2,"a":1},
		"definition_id":"definition-1",
		"definition_version":1,
		"initial_node_id":"root"
	}`))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusConflict)
	}
	var response errorResponse
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if response.Error.Code != "PAYLOAD_CONFLICT" {
		t.Fatalf("error code = %q, want PAYLOAD_CONFLICT", response.Error.Code)
	}
	if fake.lastCreate.SubmissionPayloadHash == "" {
		t.Fatal("payload hash was not computed")
	}

	fake.createErr = nil
	fake.createResult = state.CreateWorkflowResult{Created: true, Workflow: state.Workflow{
		WorkflowID: "workflow-1", Namespace: "default", SubmissionKey: "submission-1",
		PayloadHash: fake.lastCreate.SubmissionPayloadHash, DefinitionID: "definition-1", DefinitionVersion: 1,
		State: state.StateRunnable, Revision: 1, CreatedAt: time.Unix(1, 0).UTC(), UpdatedAt: time.Unix(1, 0).UTC(),
	}}
	request = httptest.NewRequest(http.MethodPost, "/v1/workflows", strings.NewReader(`{
		"submission_key":"submission-1",
		"payload":{"a":1,"b":2},
		"definition_id":"definition-1",
		"definition_version":1,
		"initial_node_id":"root"
	}`))
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusCreated)
	}
	if fake.lastCreate.SubmissionPayloadHash != fake.createResult.Workflow.PayloadHash {
		t.Fatalf("canonical payload hash = %q, want %q", fake.lastCreate.SubmissionPayloadHash, fake.createResult.Workflow.PayloadHash)
	}
}

func TestQueryNotFoundAndStaleRevisionMapping(t *testing.T) {
	fake := &fakeRepository{workflowErr: state.ErrWorkflowNotFound}
	handler := NewServer(fake).Handler()
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/workflows/missing", nil))
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("not-found status = %d, want %d", recorder.Code, http.StatusNotFound)
	}
	var response errorResponse
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if response.Error.Code != "NOT_FOUND" {
		t.Fatalf("not-found code = %q, want NOT_FOUND", response.Error.Code)
	}

	fake.workflowErr = nil
	fake.workflow = state.Workflow{WorkflowID: "workflow-1", State: state.StateRunnable, Revision: 4}
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/workflows/workflow-1?expected_revision=3", nil))
	if recorder.Code != http.StatusConflict {
		t.Fatalf("stale status = %d, want %d", recorder.Code, http.StatusConflict)
	}
	response = errorResponse{}
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if response.Error.Code != "STALE_REVISION" || response.Error.CurrentRevision == nil || *response.Error.CurrentRevision != 4 {
		t.Fatalf("stale response = %+v", response.Error)
	}
}

func TestHistoryResponsePaging(t *testing.T) {
	fake := &fakeRepository{
		workflow: state.Workflow{WorkflowID: "workflow-1", Revision: 2},
		history:  []state.TransitionRecord{{WorkflowID: "workflow-1", Revision: 1, NewState: state.StateRunnable, Reason: "WORKFLOW_CREATED"}},
	}
	recorder := httptest.NewRecorder()
	NewServer(fake).Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/workflows/workflow-1/history?limit=1", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("history status = %d, want %d", recorder.Code, http.StatusOK)
	}
	var response historyResponse
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if len(response.Entries) != 1 || response.NextAfterRevision == nil || *response.NextAfterRevision != 1 {
		t.Fatalf("history response = %+v", response)
	}
}

func TestRepositoryErrorMappingDoesNotExposeInternalDetails(t *testing.T) {
	fake := &fakeRepository{createErr: errors.New("database password leaked")}
	request := httptest.NewRequest(http.MethodPost, "/v1/workflows", strings.NewReader(`{
		"submission_key":"submission-1","payload":{},"definition_id":"definition-1",
		"definition_version":1,"initial_node_id":"root"
	}`))
	recorder := httptest.NewRecorder()
	NewServer(fake).Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusInternalServerError || strings.Contains(recorder.Body.String(), "database password") {
		t.Fatalf("internal error response = %d %s", recorder.Code, recorder.Body.String())
	}
}
