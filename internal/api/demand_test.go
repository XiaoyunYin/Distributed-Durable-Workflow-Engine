package api

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/state"
)

type demandFixture struct {
	calls     int
	namespace string
	err       error
	zeroTime  bool
	delay     time.Duration
}

func (f *demandFixture) DurableDemand(_ context.Context, ns string) (state.DemandSample, error) {
	f.calls++
	f.namespace = ns
	at := time.Now()
	if f.zeroTime {
		at = time.Time{}
	}
	time.Sleep(f.delay)
	return state.DemandSample{Namespace: ns, SampledAt: at, Demand: 3, ActiveClaims: 1, ClaimableAttempts: 2}, f.err
}
func TestK2DemandHTTP(t *testing.T) {
	f := &demandFixture{}
	handler := NewServer(nil).WithRoutes(RoutesDemand).WithDemand(f, "campaign").Handler()
	request := func(method, path string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest(method, path, nil))
		return w
	}
	good := request("GET", "/v1/demand?namespace=spoofed")
	if good.Code != 200 || !strings.Contains(good.Body.String(), `"demand":3`) || f.namespace != "campaign" || good.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("demand HTTP contract violated: %d %s", good.Code, good.Body)
	}
	f.err = errors.New("database down")
	failed := request("GET", "/v1/demand")
	if failed.Code != 503 || strings.Contains(failed.Body.String(), `"demand":`) {
		t.Fatalf("demand failure became zero: %d %s", failed.Code, failed.Body)
	}
	f.err = nil
	f.zeroTime = true
	if request("GET", "/v1/demand").Code != 503 {
		t.Fatal("invalid demand sample accepted")
	}
	f.zeroTime = false
	f.delay = 5050 * time.Millisecond
	if request("GET", "/v1/demand").Code != 503 {
		t.Fatal("stale demand sample accepted")
	}
	f.delay = 0
	for _, entry := range []struct{ method, path string }{{"POST", "/v1/demand"}, {"POST", "/v1/workflows"}, {"GET", "/v1/workflows/x"}, {"POST", "/v1/worker-deliveries"}, {"POST", "/v1/approvals/x/decision"}} {
		if code := request(entry.method, entry.path).Code; code != 404 {
			t.Fatalf("demand route isolation bypassed: %s %s => %d", entry.method, entry.path, code)
		}
	}
	for _, routes := range []RouteSet{RoutesCombined, RoutesPublic, RoutesWorkerControl, RoutesOperator, RoutesNone} {
		w := httptest.NewRecorder()
		NewServer(nil).WithRoutes(routes).WithDemand(f, "campaign").Handler().ServeHTTP(w, httptest.NewRequest("GET", "/v1/demand", nil))
		if w.Code != 404 {
			t.Fatalf("demand route isolation bypassed: routes=%s code=%d", routes, w.Code)
		}
	}
	for _, server := range []*Server{NewServer(nil).WithRoutes(RoutesDemand), NewServer(nil).WithRoutes(RoutesDemand).WithDemand(f, "")} {
		w := httptest.NewRecorder()
		server.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/v1/demand", nil))
		if w.Code != 503 {
			t.Fatal("unconfigured demand accepted")
		}
	}
}
func TestK2DemandAuthentication(t *testing.T) {
	reviewer, fixture := identityFixture(t)
	reviewer.config.ServiceAccount = "keda"
	fixture.username = "system:serviceaccount:engine:keda"
	f := &demandFixture{}
	handler := NewServer(nil).WithRoutes(RoutesDemand).WithDemand(f, "campaign").WithAuthentication(reviewer).Handler()
	for _, entry := range []struct {
		subject string
		want    int
	}{{"", 401}, {"system:serviceaccount:engine:worker", 401}, {fixture.username, 200}} {
		r := httptest.NewRequest("GET", "/v1/demand", nil)
		if entry.subject != "" {
			r.Header.Set("Authorization", "Bearer "+projectedToken("https://cluster.example", entry.subject, "durable-worker", time.Now().Add(time.Hour).Unix()))
		}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != entry.want {
			t.Fatalf("demand auth guard bypassed: subject=%s status=%d body=%s", entry.subject, w.Code, w.Body)
		}
	}
	if f.calls != 1 {
		t.Fatalf("demand auth guard bypassed: DB called %d times", f.calls)
	}
}
