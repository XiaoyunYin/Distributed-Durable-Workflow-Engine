package main

import (
	"durable-agent-execution-engine/internal/api"
	"durable-agent-execution-engine/internal/state"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestK2RoleRoutes(t *testing.T) {
	t.Setenv("RUNTIME_TOKEN_AUTH", "")
	for value, want := range map[string]api.RouteSet{"demand": api.RoutesDemand, "combined": api.RoutesCombined, "api": api.RoutesPublic, "operator": api.RoutesOperator, "worker-control": api.RoutesWorkerControl, "scheduler,relay,ingestor": api.RoutesNone, "scheduler,worker-control": api.RoutesWorkerControl} {
		roles, err := lifecycleRoles(value)
		if err != nil {
			t.Fatal(err)
		}
		got, err := lifecycleRoutes(value, roles)
		if err != nil || got != want {
			t.Fatalf("role route isolation bypassed: %s => %s %v", value, got, err)
		}
	}
	t.Setenv("RUNTIME_TOKEN_AUTH", "enabled")
	if _, err := lifecycleRoutes("combined", nil); err == nil {
		t.Fatal("role route isolation bypassed: authenticated combined mode")
	}
	t.Setenv("RUNTIME_TOKEN_AUTH", "")
	for _, value := range []string{"api,worker-control", "operator,worker-control", "api,operator", "demand,api", "demand,worker-control", "demand,operator", "demand,scheduler", "demand,relay"} {
		roles, err := lifecycleRoles(value)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := lifecycleRoutes(value, roles); err == nil {
			t.Fatalf("role route isolation bypassed: %s", value)
		}
	}
}

func TestK2ReadinessRouteIsolation(t *testing.T) {
	t.Setenv("RUNTIME_ENGINE_MODE", "readiness")
	for _, combined := range []bool{false, true} {
		handler := lifecycleHandlerWithAPI("test", &state.Store{}, nil, nil, nil, http.NotFoundHandler(), combined)
		response := httptest.NewRecorder()
		// GET makes route presence observable without executing the diagnostic write.
		handler.ServeHTTP(response, httptest.NewRequest("GET", "/internal/readiness/run", nil))
		want := 404
		if combined {
			want = 405
		}
		if response.Code != want {
			t.Fatalf("readiness route isolation bypassed: combined=%v code=%d", combined, response.Code)
		}
	}
}
