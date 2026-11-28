package main

import (
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"durable-agent-execution-engine/internal/api"
	"durable-agent-execution-engine/internal/state"
)

func TestJ12SwitchExactValues(t *testing.T) {
	for _, tc := range []struct {
		name, value            string
		unset, enabled, reject bool
	}{
		{name: "absent", unset: true},
		{name: "zero", value: "0"},
		{name: "one", value: "1", enabled: true},
		{name: "empty", reject: true},
		{name: "space", value: " 1", reject: true},
		{name: "other", value: "true", reject: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("RUNTIME_PUBLIC_API_AUTH", tc.value)
			if tc.unset {
				if err := os.Unsetenv("RUNTIME_PUBLIC_API_AUTH"); err != nil {
					t.Fatal(err)
				}
			}
			got, err := publicAPIAuthFromEnvironment()
			if got != tc.enabled || (err != nil) != tc.reject {
				t.Fatalf("got %t and %v, want enabled=%t reject=%t", got, err, tc.enabled, tc.reject)
			}
		})
	}
}

func TestJ12StartupRejectsUnsafeListenerConfiguration(t *testing.T) {
	oldArgs := os.Args
	os.Args = []string{"runtime"}
	t.Cleanup(func() { os.Args = oldArgs })
	t.Setenv("RUNTIME_PUBLIC_API_AUTH", "1")
	t.Setenv("RUNTIME_TOKEN_AUTH", "disabled")
	t.Setenv("RUNTIME_NAMESPACE", "tenant-a")
	t.Setenv("DATABASE_URL", "fixture-placeholder")
	t.Setenv("RUNTIME_PUBLIC_JWT_ISSUER", "https://j12-fixture.invalid")
	t.Setenv("RUNTIME_PUBLIC_JWT_AUDIENCE", "durable-j12-api")
	t.Setenv("RUNTIME_PUBLIC_JWT_JWKS_FILE", filepath.Join(t.TempDir(), "absent"))
	for _, tc := range []struct{ roles, message string }{
		{"combined", "J12 requires its own split listener profile"},
		{"api,operator", "split HTTP roles require separate listeners/processes"},
		{"api", "public JWT JWKS unavailable"},
		{"operator", "require Kubernetes TokenReview"},
		{"worker-control", "require Kubernetes TokenReview"},
		{"demand", "requires its dedicated bearer credential"},
	} {
		t.Run(tc.roles, func(t *testing.T) {
			t.Setenv("RUNTIME_ROLES", tc.roles)
			err := serveLifecycle(context.Background())
			if err == nil || !strings.Contains(err.Error(), tc.message) {
				t.Fatalf("got %v, want %q", err, tc.message)
			}
		})
	}
	t.Setenv("RUNTIME_PUBLIC_API_AUTH", "")
	if err := serveLifecycle(context.Background()); err == nil || !strings.Contains(err.Error(), "must be unset, 0, or 1") {
		t.Fatalf("startup accepted invalid switch: %v", err)
	}
}

func TestJ12LegacyEquivalence(t *testing.T) {
	// The legacy public role never exposed decision or cancel-request. The
	// legacy operator role also served submission and reads without a tenant JWT.
	t.Setenv("RUNTIME_TOKEN_AUTH", "disabled")
	t.Setenv("RUNTIME_PUBLIC_JWT_JWKS_FILE", filepath.Join(t.TempDir(), "absent"))
	for _, mode := range []string{"unset", "0"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("RUNTIME_PUBLIC_API_AUTH", "0")
			if mode == "unset" {
				if err := os.Unsetenv("RUNTIME_PUBLIC_API_AUTH"); err != nil {
					t.Fatal(err)
				}
			}
			enabled, err := publicAPIAuthFromEnvironment()
			if enabled || err != nil {
				t.Fatalf("legacy profile failed switch parsing: %v", err)
			}
			for _, tc := range []struct {
				method, path string
				listeners    []api.RouteSet
				status       int
			}{
				{"POST", "/v1/workflows", []api.RouteSet{api.RoutesCombined, api.RoutesPublic, api.RoutesOperator}, 400},
				{"GET", "/v1/workflows/w/history?limit=0", []api.RouteSet{api.RoutesCombined, api.RoutesPublic, api.RoutesOperator}, 400},
				{"POST", "/v1/workflows/w/cancel-request", []api.RouteSet{api.RoutesCombined, api.RoutesOperator}, 400},
				{"POST", "/v1/approvals/i/decision", []api.RouteSet{api.RoutesCombined, api.RoutesOperator}, 400},
				{"POST", "/v1/workflows/w/nodes/n/iterations/0/approval", []api.RouteSet{api.RoutesCombined, api.RoutesOperator}, 400},
				{"POST", "/v1/approvals/i/apply", []api.RouteSet{api.RoutesCombined, api.RoutesOperator}, 400},
				{"POST", "/v1/cancellations/c/apply", []api.RouteSet{api.RoutesCombined, api.RoutesOperator}, 400},
				{"POST", "/v1/worker-deliveries", []api.RouteSet{api.RoutesCombined, api.RoutesWorkerControl}, 400},
				{"POST", "/v1/workflows/w/nodes/n/iterations/0/claim", []api.RouteSet{api.RoutesCombined, api.RoutesWorkerControl}, 400},
				{"POST", "/v1/workflows/w/nodes/n/iterations/0/heartbeat", []api.RouteSet{api.RoutesCombined, api.RoutesWorkerControl}, 400},
				{"POST", "/v1/workflows/w/nodes/n/iterations/0/result", []api.RouteSet{api.RoutesCombined, api.RoutesWorkerControl}, 400},
				{"GET", "/v1/demand", []api.RouteSet{api.RoutesDemand}, 503},
			} {
				for _, listener := range []api.RouteSet{api.RoutesCombined, api.RoutesPublic, api.RoutesOperator, api.RoutesWorkerControl, api.RoutesDemand, api.RoutesNone} {
					want := 404
					for _, legacy := range tc.listeners {
						if listener == legacy {
							want = tc.status
						}
					}
					server := api.NewServer(&state.Store{}).WithRoutes(listener).WithPublicAPIAuth(enabled, nil)
					response := httptest.NewRecorder()
					server.Handler().ServeHTTP(response, httptest.NewRequest(tc.method, tc.path, nil))
					if response.Code != want {
						t.Fatalf("legacy %q %s %s returned %d, want %d", listener, tc.method, tc.path, response.Code, want)
					}
				}
			}
		})
	}
}
