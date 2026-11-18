package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestK6WorkerControlExcludesApprovalOperations(t *testing.T) {
	repository := &identityRepository{}
	handler := NewServer(repository).WithRoutes(RoutesWorkerControl).Handler()
	for _, route := range []string{
		"/v1/workflows/w/nodes/root/iterations/0/approval",
		"/v1/approvals/intent/decision",
		"/v1/approvals/intent/apply",
		"/v1/approval-decision",
	} {
		request := httptest.NewRequest(http.MethodPost, route, strings.NewReader(`{"actor_id":"worker","decision":"APPROVED"}`))
		request.Header.Set("Authorization", "Bearer worker-credential")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusNotFound || repository.approval.WorkflowID != "" ||
			repository.decision.IntentID != "" || repository.applied.IntentID != "" {
			t.Fatalf("worker-control exposed approval route %s", route)
		}
	}
}
