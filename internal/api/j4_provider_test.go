package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"durable-agent-execution-engine/internal/state"
)

type fakeJ4Control struct {
	claim   state.ProviderClaim
	request string
	outcome state.ProviderOutcome
}

func (f *fakeJ4Control) Reserve(_ context.Context, claim state.ProviderClaim, request string) (state.ProviderSchedule, error) {
	f.claim, f.request = claim, request
	return state.ProviderSchedule{BootID: "boot"}, nil
}

func (f *fakeJ4Control) Outcome(_ context.Context, claim state.ProviderClaim, outcome state.ProviderOutcome) (state.ProviderSchedule, error) {
	f.claim, f.outcome = claim, outcome
	return state.ProviderSchedule{}, state.ErrStaleClaim
}

func (f *fakeJ4Control) CheckResult(context.Context, state.ResultInput) error {
	return state.ErrProviderConflict
}

func TestJ4ProviderRoutesPrivateAndUnsetLegacy(t *testing.T) {
	const base = "/v1/workflows/w/nodes/j4.recorded-llm/iterations/0/provider-attempts/"
	for _, tc := range []struct {
		routes  RouteSet
		control j4ProviderControl
		want    int
	}{
		{RoutesPublic, &fakeJ4Control{}, 404},
		{RoutesCombined, &fakeJ4Control{}, 404},
		{RoutesWorkerControl, nil, 404},
		{RoutesWorkerControl, &fakeJ4Control{}, 200},
	} {
		server := NewServer(nil).WithRoutes(tc.routes)
		server.j4 = tc.control
		w := httptest.NewRecorder()
		server.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodPost, base+"reserve",
			strings.NewReader(`{"attempt_number":2,"claim_token":"token","boot_id":"boot","request_id":"request"}`)))
		if w.Code != tc.want {
			t.Fatalf("%s control=%t reserve status %d: %s", tc.routes, tc.control != nil, w.Code, w.Body)
		}
		if tc.want == 200 {
			f := tc.control.(*fakeJ4Control)
			if f.claim.WorkflowID != "w" || f.claim.NodeID != state.RecordedLLMActivity ||
				f.claim.ClaimToken != "token" || f.claim.BootID != "boot" || f.claim.Attempt != 2 || f.request != "request" {
				t.Fatalf("incorrect claim mapping: %+v", f)
			}
			if w.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("permit response can be cached")
			}
		}
		w = httptest.NewRecorder()
		server.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodPost, base+"outcome",
			strings.NewReader(`{"attempt_number":2,"claim_token":"token","boot_id":"boot","call_id":"call","kind":"timeout","observed_ns":123}`)))
		if tc.want == 404 && w.Code != 404 {
			t.Fatalf("%s exposed the outcome route", tc.routes)
		}
		if tc.want == 200 && (w.Code != 409 || tc.control.(*fakeJ4Control).outcome.CallID != "call") {
			t.Fatalf("outcome did not preserve claim authority: %d %s", w.Code, w.Body)
		}
	}
}
