package api

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/state"
	"github.com/jackc/pgx/v5/pgconn"
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
	if !strings.HasPrefix(fake.lastCreate.SubmissionPayloadHash, submissionHashV1) {
		t.Fatalf("submission hash = %q, want %q prefix", fake.lastCreate.SubmissionPayloadHash, submissionHashV1)
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

func TestSubmissionFingerprintAndJSONRules(t *testing.T) {
	base := submitWorkflowRequest{
		DefinitionID: "definition-1", DefinitionVersion: 1, InitialNodeID: "root",
		InitialInput: json.RawMessage(`{"amount":10}`), Payload: json.RawMessage(`{"a":1}`),
	}
	hash, err := canonicalSubmissionHash(base, base.InitialInput)
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*submitWorkflowRequest){
		"definition": func(request *submitWorkflowRequest) { request.DefinitionID = "definition-2" },
		"version":    func(request *submitWorkflowRequest) { request.DefinitionVersion = 2 },
		"node":       func(request *submitWorkflowRequest) { request.InitialNodeID = "other" },
		"input":      func(request *submitWorkflowRequest) { request.InitialInput = json.RawMessage(`{"amount":9999}`) },
		"payload":    func(request *submitWorkflowRequest) { request.Payload = json.RawMessage(`{"a":2}`) },
	} {
		t.Run(name, func(t *testing.T) {
			request := base
			mutate(&request)
			changed, err := canonicalSubmissionHash(request, request.InitialInput)
			if err != nil {
				t.Fatal(err)
			}
			if changed == hash {
				t.Fatalf("changed execution request kept hash %q", hash)
			}
		})
	}
	if _, err := canonicalPayloadHash(json.RawMessage(`{"a":1,"a":2}`)); err == nil {
		t.Fatal("duplicate JSON keys were accepted")
	}
	one, err := canonicalPayloadHash(json.RawMessage(`1`))
	if err != nil {
		t.Fatal(err)
	}
	onePointZero, err := canonicalPayloadHash(json.RawMessage(`1.0`))
	if err != nil {
		t.Fatal(err)
	}
	if one == onePointZero {
		t.Fatal("JSON number spellings were collapsed")
	}
}

func TestRepositoryClientErrorMappingsAndUnknownRoutes(t *testing.T) {
	requestBody := `{"submission_key":"submission-1","payload":{},"definition_id":"definition-1","definition_version":1,"initial_node_id":"root"}`
	cases := []struct {
		name   string
		err    error
		status int
		code   string
	}{
		{name: "definition", err: state.ErrDefinitionNotFound, status: http.StatusNotFound, code: "DEFINITION_NOT_FOUND"},
		{name: "node", err: state.ErrUnknownNode, status: http.StatusUnprocessableEntity, code: "UNKNOWN_INITIAL_NODE"},
		{name: "workflow ID", err: state.ErrWorkflowIDConflict, status: http.StatusConflict, code: "WORKFLOW_ID_CONFLICT"},
		{name: "database", err: context.DeadlineExceeded, status: http.StatusServiceUnavailable, code: "DATABASE_UNAVAILABLE"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			fake := &fakeRepository{createErr: testCase.err}
			recorder := httptest.NewRecorder()
			NewServer(fake).Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/workflows", strings.NewReader(requestBody)))
			if recorder.Code != testCase.status || !strings.Contains(recorder.Body.String(), testCase.code) {
				t.Fatalf("response = %d %s", recorder.Code, recorder.Body.String())
			}
		})
	}

	recorder := httptest.NewRecorder()
	NewServer(&fakeRepository{}).Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/unknown", nil))
	if recorder.Code != http.StatusNotFound || !strings.Contains(recorder.Body.String(), `"code":"NOT_FOUND"`) {
		t.Fatalf("unknown route response = %d %s", recorder.Code, recorder.Body.String())
	}

	duplicateTopLevel := httptest.NewRecorder()
	NewServer(&fakeRepository{}).Handler().ServeHTTP(duplicateTopLevel, httptest.NewRequest(http.MethodPost, "/v1/workflows", strings.NewReader(`{"submission_key":"one","submission_key":"two","payload":{},"definition_id":"definition-1","definition_version":1,"initial_node_id":"root"}`)))
	if duplicateTopLevel.Code != http.StatusBadRequest || !strings.Contains(duplicateTopLevel.Body.String(), "duplicate object key") {
		t.Fatalf("top-level duplicate keys = %d %s", duplicateTopLevel.Code, duplicateTopLevel.Body.String())
	}

	if !isDatabaseUnavailable(&pgconn.PgError{Code: "57P01"}) {
		t.Fatal("PostgreSQL admin shutdown was not classified as unavailable")
	}
	if !isDatabaseUnavailable(&net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset by peer")}) {
		t.Fatal("network connection loss was not classified as unavailable")
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
