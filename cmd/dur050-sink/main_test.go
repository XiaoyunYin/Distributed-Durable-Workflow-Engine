package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestLoopbackSinkAcknowledgesOnlyWellFormedWorkflowRequests(t *testing.T) {
	handler := newHandler()
	health := httptest.NewRecorder()
	handler.ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if health.Code != http.StatusNoContent {
		t.Fatalf("health status=%d, want 204", health.Code)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/workflows",
		strings.NewReader(`{"workflow_id":"dur050-sink-check-000001"}`)))
	if response.Code != http.StatusCreated {
		t.Fatalf("sink acknowledgement status=%d body=%s", response.Code, response.Body.String())
	}
	var acknowledgement struct {
		Created  bool `json:"created"`
		Workflow struct {
			WorkflowID string `json:"workflow_id"`
		} `json:"workflow"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &acknowledgement); err != nil {
		t.Fatal(err)
	}
	if !acknowledgement.Created || acknowledgement.Workflow.WorkflowID != "dur050-sink-check-000001" {
		t.Fatalf("sink acknowledgement=%+v", acknowledgement)
	}
	invalid := httptest.NewRecorder()
	handler.ServeHTTP(invalid, httptest.NewRequest(http.MethodPost, "/v1/workflows", strings.NewReader(`{}`)))
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("invalid request status=%d, want 400", invalid.Code)
	}
}

func TestSinkRefusesNonLoopbackAddress(t *testing.T) {
	if err := serve("0.0.0.0:0"); err == nil || !strings.Contains(err.Error(), "loopback") {
		t.Fatalf("non-loopback bind error=%v, want loopback refusal", err)
	}
}
