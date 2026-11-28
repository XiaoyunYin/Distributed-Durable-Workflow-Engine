package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/api"
	"durable-agent-execution-engine/internal/engine"
	"durable-agent-execution-engine/internal/state"
	"durable-agent-execution-engine/internal/transport"
)

func TestJ4ServeRejectsInvalidModesBeforeBootstrapping(t *testing.T) {
	for _, tc := range []struct{ activity, resilience, want string }{
		{"true", "0", state.RecordedLLMMode},
		{"1", "true", state.ProviderResilienceMode},
		{"0", "1", state.ProviderResilienceMode + " requires"},
	} {
		t.Run(tc.activity+"-"+tc.resilience, func(t *testing.T) {
			t.Setenv(state.RecordedLLMMode, tc.activity)
			t.Setenv(state.ProviderResilienceMode, tc.resilience)
			err := serveLifecycle(context.Background())
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("startup error = %v, want %s", err, tc.want)
			}
		})
	}
}

func TestJ4C2GateHoldsOnlyTheNamedReceipt(t *testing.T) {
	dir := t.TempDir()
	gate := &j4ReceiptGate{dir: dir, workflow: "wf", bootID: "boot", clock: func() (int64, error) { return 42, nil }}
	j4 := &j4Deployment{hold: gate}
	var options engine.ServeOptions
	j4.scheduler(&options)
	if options.ScanInterval != time.Second {
		t.Fatal("wrong J4 scan interval")
	}
	input := state.ConsumeResultInput{WorkflowID: "foreign", NodeID: state.RecordedLLMActivity,
		AttemptNumber: 1, ExpectedRevision: 7}
	if err := options.BeforeResultConsumption(input, state.Attempt{}); err != nil {
		t.Fatal("gate held another workflow")
	}
	input.WorkflowID = "wf"
	for range 2 {
		if err := options.BeforeResultConsumption(input, state.Attempt{}); !errors.Is(err, errJ4ReceiptHeld) {
			t.Fatalf("missing hold: %v", err)
		}
	}
	release := filepath.Join(dir, "release")
	if err := os.WriteFile(release, []byte("different\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := options.BeforeResultConsumption(input, state.Attempt{}); err == nil {
		t.Fatal("foreign release opened the hold")
	}
	if err := os.WriteFile(release, []byte("wf\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := options.BeforeResultConsumption(input, state.Attempt{}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "scheduler.jsonl"))
	if err != nil || strings.Count(string(data), "\n") != 2 ||
		!strings.Contains(string(data), `"event":"held"`) ||
		!strings.Contains(string(data), `"event":"released"`) {
		t.Fatalf("unexpected gate evidence %s (%v)", data, err)
	}
}

func TestJ4ClockHealthIsStrictlyOptIn(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) })
	for _, tc := range []struct {
		deployment *j4Deployment
		path       string
		want       int
	}{
		{nil, "/healthz", 204},
		{&j4Deployment{bootID: "shared-boot", clock: func() (int64, error) { return 42, nil }}, "/healthz", 200},
		{&j4Deployment{}, "/v1/", 204},
	} {
		w := httptest.NewRecorder()
		tc.deployment.health(next).ServeHTTP(w, httptest.NewRequest(http.MethodGet, tc.path, nil))
		if w.Code != tc.want || tc.want == 200 && !strings.Contains(w.Body.String(), `"monotonic_ns":42`) {
			t.Fatalf("unexpected clock probe response %d: %s", w.Code, w.Body)
		}
	}
}

func TestJ4StartupRequiresPrivateWorkerListener(t *testing.T) {
	t.Setenv(state.RecordedLLMMode, "1")
	t.Setenv(state.ProviderResilienceMode, "1")
	if validateJ4Listener(api.RoutesPublic, "", false) == nil ||
		validateJ4Listener(api.RoutesCombined, "127.0.0.1:8083", false) == nil {
		t.Fatal("enabled J4 B2 accepted an invalid listener")
	}
	if err := validateJ4Listener(api.RoutesPublic, "127.0.0.1:8083", true); err != nil {
		t.Fatal(err)
	}
	if err := os.Unsetenv(state.ProviderResilienceMode); err != nil {
		t.Fatal(err)
	}
	if validateJ4Listener(api.RoutesPublic, "127.0.0.1:8083", true) == nil {
		t.Fatal("unset B2 accepted the fixed-delay control")
	}
	t.Setenv(state.RecordedLLMMode, "0")
	if err := validateJ4Listener(api.RoutesCombined, "", false); err != nil {
		t.Fatal(err)
	}
}

func TestJ4HealthReportsConstructedRelayMode(t *testing.T) {
	j4 := &j4Deployment{expectsRelay: true, clock: func() (int64, error) { return 42, nil }}
	probe := func() *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		j4.health(http.NotFoundHandler()).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/healthz", nil))
		return w
	}
	if w := probe(); w.Code != http.StatusServiceUnavailable {
		t.Fatalf("health before relay construction = %d", w.Code)
	}
	for _, tc := range []struct {
		mode, value string
		repaired    bool
	}{{"legacy", "0", false}, {"repaired", "1", true}} {
		t.Run(tc.mode, func(t *testing.T) {
			j4.relay.Store(transport.NewRelay(nil, nil, transport.RelayConfig{FullBatchLoop: tc.repaired}))
			w := probe()
			var report struct {
				Switch string `json:"relay_switch"`
				Value  string `json:"relay_value"`
				Mode   string `json:"effective_relay_mode"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &report); err != nil {
				t.Fatal(err)
			}
			if w.Code != http.StatusOK || report.Switch != relayFullBatchLoopEnv ||
				report.Value != tc.value || report.Mode != tc.mode {
				t.Fatalf("health = %d %s", w.Code, w.Body)
			}
		})
	}
}
