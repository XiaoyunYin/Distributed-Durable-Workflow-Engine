package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	"durable-agent-execution-engine/internal/api"
	"durable-agent-execution-engine/internal/engine"
	"durable-agent-execution-engine/internal/state"
	"durable-agent-execution-engine/internal/transport"
)

func validateJ4Listener(routes api.RouteSet, workerAddr string, fixedDelay bool) error {
	if fixedDelay && !state.ProviderResilienceEnabled() {
		return errors.New("J4 B5 control requires provider resilience")
	}
	if workerAddr == "" {
		if state.ProviderResilienceEnabled() {
			return errors.New("J4 provider resilience requires a private worker listener")
		}
		return nil
	}
	if !state.RecordedLLMEnabled() || state.K6Enabled() || routes != api.RoutesPublic {
		return errors.New("J4 worker listener requires the J4 activity and a separate public API")
	}
	return nil
}

type j4Deployment struct {
	bootID               string
	clock                func() (int64, error)
	resilient            bool
	hold                 *j4ReceiptGate
	expectsRepairedRelay bool
	relay                atomic.Pointer[transport.Relay]
}

func newJ4Deployment(workerAddr, workflow string, scheduler bool) (*j4Deployment, error) {
	if workflow != "" && (workerAddr == "" || !scheduler || state.ProviderResilienceEnabled() ||
		!regexp.MustCompile(`^[A-Za-z0-9._:-]{1,160}$`).MatchString(workflow)) {
		return nil, errors.New("J4 receipt gate requires the private core scheduler and a workflow identity")
	}
	if workerAddr == "" {
		return nil, nil
	}
	boot, clock, err := state.NewJ4HostClock()
	if err != nil {
		return nil, err
	}
	result := &j4Deployment{bootID: boot, clock: clock, resilient: state.ProviderResilienceEnabled()}
	if workflow != "" {
		result.hold = &j4ReceiptGate{dir: "/evidence", workflow: workflow, bootID: boot, clock: clock}
	}
	return result, nil
}

func (j *j4Deployment) health(next http.Handler) http.Handler {
	if j == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/healthz" {
			next.ServeHTTP(w, r)
			return
		}
		relay := j.relay.Load()
		if j.expectsRepairedRelay && relay == nil {
			http.Error(w, "J4 relay is starting", http.StatusServiceUnavailable)
			return
		}
		ns, err := j.clock()
		if err != nil {
			http.Error(w, "J4 clock unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		report := map[string]any{
			"boot_id": j.bootID, "monotonic_ns": ns, "status": "ok",
			"recorded_activity": true, "provider_resilience": j.resilient,
		}
		if relay != nil && relay.FullBatchLoopEnabled() {
			report["relay_switch"] = transport.RelayFullBatchLoopEnv
			report["relay_value"] = "1"
			report["effective_relay_mode"] = "repaired"
		}
		_ = json.NewEncoder(w).Encode(report)
	})
}

func (j *j4Deployment) scheduler(options *engine.ServeOptions) {
	if j == nil {
		return
	}
	options.ScanInterval = time.Second
	if j.hold != nil {
		options.BeforeResultConsumption = j.hold.beforeConsumption
	}
}

var errJ4ReceiptHeld = errors.New("J4 C2 receipt consumption held")

type j4ReceiptGate struct {
	dir      string
	workflow string
	bootID   string
	clock    func() (int64, error)
	mu       sync.Mutex
	held     bool
	released bool
}

func (g *j4ReceiptGate) beforeConsumption(input state.ConsumeResultInput, _ state.Attempt) error {
	if input.WorkflowID != g.workflow || input.NodeID != state.RecordedLLMActivity || input.Iteration != 0 {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.released {
		return nil
	}
	if !g.held {
		if err := g.append("held", input); err != nil {
			return err
		}
		g.held = true
	}
	data, err := os.ReadFile(filepath.Join(g.dir, "release"))
	if errors.Is(err, os.ErrNotExist) {
		return errJ4ReceiptHeld
	}
	if err != nil {
		return err
	}
	if string(data) != g.workflow+"\n" {
		return errors.New("J4 C2 receipt release identity mismatch")
	}
	if err := g.append("released", input); err != nil {
		return err
	}
	g.released = true
	return nil
}

func (g *j4ReceiptGate) append(event string, input state.ConsumeResultInput) error {
	ns, err := g.clock()
	if err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(g.dir, "scheduler.jsonl"), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("open J4 scheduler trace: %w", err)
	}
	defer f.Close()
	if err := json.NewEncoder(f).Encode(map[string]any{
		"event": event, "boot_id": g.bootID, "monotonic_ns": ns,
		"workflow_id": input.WorkflowID, "attempt": input.AttemptNumber,
		"expected_revision": input.ExpectedRevision,
	}); err != nil {
		return err
	}
	return f.Sync()
}
