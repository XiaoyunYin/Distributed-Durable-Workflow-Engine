package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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
func TestK2DemandBearerAuthentication(t *testing.T) {
	secretPath := filepath.Join(t.TempDir(), "campaign-token")
	oldSecret := "old-campaign-secret-0123456789abcdef"
	newSecret := "new-campaign-secret-0123456789abcdef"
	if err := os.WriteFile(secretPath, []byte(oldSecret+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("RUNTIME_DEMAND_BEARER_TOKEN_FILE", secretPath)
	auth, err := NewDemandBearerAuthenticatorFromEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	f := &demandFixture{}
	handler := NewServer(nil).WithRoutes(RoutesDemand).WithDemand(f, "campaign").WithDemandAuthentication(auth).Handler()
	request := func(token string, includeHeader bool) *httptest.ResponseRecorder {
		r := httptest.NewRequest("GET", "/v1/demand", nil)
		if includeHeader {
			r.Header.Set("Authorization", "Bearer "+token)
		}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w
	}
	for _, entry := range []struct {
		token string
		set   bool
	}{{"", false}, {"", true}, {"wrong-campaign-secret-0123456789abcdef", true}} {
		if response := request(entry.token, entry.set); response.Code != http.StatusUnauthorized {
			t.Fatalf("demand bearer guard bypassed: token=%q status=%d", entry.token, response.Code)
		}
	}
	if f.calls != 0 {
		t.Fatalf("rejected demand credentials reached DB: %d calls", f.calls)
	}
	if response := request(oldSecret, true); response.Code != http.StatusOK || f.calls != 1 {
		t.Fatalf("valid demand bearer rejected: status=%d calls=%d body=%s", response.Code, f.calls, response.Body)
	}
	if err := os.WriteFile(secretPath, []byte(newSecret+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if response := request(oldSecret, true); response.Code != http.StatusUnauthorized || f.calls != 1 {
		t.Fatalf("rotated-out bearer accepted: status=%d calls=%d", response.Code, f.calls)
	}
	if response := request(newSecret, true); response.Code != http.StatusOK || f.calls != 2 {
		t.Fatalf("rotated-in bearer rejected: status=%d calls=%d body=%s", response.Code, f.calls, response.Body)
	}
	if err := os.Remove(secretPath); err != nil {
		t.Fatal(err)
	}
	if response := request(newSecret, true); response.Code != http.StatusUnauthorized || f.calls != 2 {
		t.Fatalf("unavailable rotated secret did not fail closed: status=%d calls=%d", response.Code, f.calls)
	}
}

func TestK2DemandBearerConfiguration(t *testing.T) {
	for _, contents := range [][]byte{nil, []byte("short")} {
		path := filepath.Join(t.TempDir(), "campaign-token")
		if err := os.WriteFile(path, contents, 0600); err != nil {
			t.Fatal(err)
		}
		t.Setenv("RUNTIME_DEMAND_BEARER_TOKEN_FILE", path)
		if _, err := NewDemandBearerAuthenticatorFromEnvironment(); err == nil {
			t.Fatal("demand bearer configuration guard bypassed")
		}
	}
	t.Setenv("RUNTIME_DEMAND_BEARER_TOKEN_FILE", filepath.Join(t.TempDir(), "missing"))
	if _, err := NewDemandBearerAuthenticatorFromEnvironment(); err == nil {
		t.Fatal("unreadable demand bearer configuration accepted")
	}
}
