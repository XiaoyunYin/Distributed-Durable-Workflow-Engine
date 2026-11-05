package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"durable-agent-execution-engine/internal/api"
	"durable-agent-execution-engine/internal/engine"
	"durable-agent-execution-engine/internal/partition"
	"durable-agent-execution-engine/internal/state"
	"durable-agent-execution-engine/internal/telemetry"
)

const version = "0.1.0-dev"

func runtimeCommand() {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "version":
			fmt.Println(version)
			return
		case "healthcheck":
			if err := checkHealth(); err != nil {
				slog.Error("health check failed", "error", err)
				os.Exit(1)
			}
			return
		}
	}

}

func newHandler(role string) http.Handler {
	return newHandlerWithMetrics(role, nil, telemetry.New(role), nil)
}

func newHandlerWithStore(role string, store *state.Store) http.Handler {
	return newHandlerWithMetrics(role, store, telemetry.New(role), nil)
}

func newHandlerWithMetrics(role string, store *state.Store, metrics *telemetry.Metrics, tracing *telemetry.Tracing) http.Handler {
	return lifecycleHandler(role, store, metrics, tracing, nil)
}

func lifecycleHandler(role string, store *state.Store, metrics *telemetry.Metrics, tracing *telemetry.Tracing, draining *atomic.Bool) http.Handler {
	return lifecycleHandlerWithAPI(role, store, metrics, tracing, draining, api.NewServer(store).WithTracing(tracing).Handler(), true)
}

func lifecycleHandlerWithAPI(role string, store *state.Store, metrics *telemetry.Metrics, tracing *telemetry.Tracing, draining *atomic.Bool, apiHandler http.Handler, combined bool) http.Handler {
	mux := http.NewServeMux()
	health := func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"role":    role,
			"status":  "ok",
			"version": version,
		})
	}
	mux.HandleFunc("GET /healthz", health)
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		var ping func(context.Context) error
		if store != nil {
			ping = store.Pool().Ping
		}
		if !runtimeReady(r.Context(), draining, ping) {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		health(w, r)
	})
	mux.Handle("GET /metrics", metricsHandler(role, metrics))
	if store != nil {
		mux.Handle("/v1/", apiHandler)
		if combined && envOrDefault("RUNTIME_ENGINE_MODE", "disabled") == "readiness" {
			mux.Handle("POST /internal/readiness/run", readinessHandler(store, metrics, role))
		}
	}
	return mux
}

type readinessResponse struct {
	WorkflowID    string              `json:"workflow_id"`
	DefinitionID  string              `json:"definition_id"`
	Mode          string              `json:"mode"`
	State         state.WorkflowState `json:"state"`
	Revision      int64               `json:"revision"`
	Resumed       bool                `json:"resumed,omitempty"`
	CrashBoundary string              `json:"crash_boundary,omitempty"`
	FirstRunError string              `json:"first_run_error,omitempty"`
	Metrics       telemetry.Snapshot  `json:"metrics"`
}

func readinessHandler(store *state.Store, metrics *telemetry.Metrics, role string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if store == nil {
			writeReadinessError(w, http.StatusServiceUnavailable, "readiness engine requires a database")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		mode := r.URL.Query().Get("mode")
		if mode == "" {
			mode = "normal"
		}
		if mode != "normal" && mode != "fault" {
			writeReadinessError(w, http.StatusBadRequest, "readiness mode must be normal or fault")
			return
		}
		workflowID := "dur036-runtime-" + state.NewID()
		definitionID := "dur036-runtime-def-" + state.NewID()
		graph := json.RawMessage(`{"entry":"root","nodes":[{"id":"root","kind":"activity","next":"done"},{"id":"done","kind":"success"}]}`)
		if err := store.CreateDefinition(ctx, state.DefinitionInput{
			DefinitionID: definitionID, Version: 1, DefinitionHash: definitionID,
			Graph: graph, ActivityVersions: json.RawMessage(`{"root":"1"}`),
			EffectClasses: json.RawMessage(`{"root":"PURE_ACTIVITY"}`),
		}); err != nil {
			writeReadinessError(w, http.StatusInternalServerError, err.Error())
			return
		}
		partitionID, err := partition.ID(workflowID)
		if err != nil {
			writeReadinessError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if _, err := store.CreateWorkflow(ctx, state.CreateWorkflowInput{
			WorkflowID: workflowID, Namespace: "dur036-runtime", SubmissionKey: workflowID,
			SubmissionPayloadHash: readinessPayloadHash(workflowID), DefinitionID: definitionID,
			DefinitionVersion: 1, PartitionID: int16(partitionID), InitialNodeID: "root",
			InitialInput: json.RawMessage(`{"readiness":true}`), ActorID: "dur036-readiness",
		}); err != nil {
			writeReadinessError(w, http.StatusInternalServerError, err.Error())
			return
		}
		driver := engine.ActivityDriverFunc(func(context.Context, engine.Activity) (engine.ActivityResult, error) {
			return engine.ActivityResult{Payload: json.RawMessage(`{"ok":true}`)}, nil
		})
		ownerID := state.NewID()
		newRunner := func() *engine.Engine {
			runner := engine.New(store, driver)
			// Partition lease ownership is persisted as a UUID. Keep the runtime
			// role in telemetry, but use a valid durable owner identity here.
			runner.OwnerID = ownerID
			runner.WorkerID = "dur036-runtime-worker"
			runner.ActorID = "dur036-runtime-engine"
			runner.MaxSteps = 20
			return runner
		}
		runner := newRunner()
		firstRunError := ""
		resumed := false
		crashBoundary := ""
		if mode == "fault" {
			crashBoundary = "result_recorded"
			runner.AfterBoundary = func(boundary string) error {
				if boundary == crashBoundary {
					return errors.New("dur036 deployed injected crash at result_recorded")
				}
				return nil
			}
			if _, firstErr := runner.Run(ctx, workflowID); firstErr == nil {
				writeReadinessError(w, http.StatusInternalServerError, "fault readiness run did not stop at the requested boundary")
				return
			} else {
				firstRunError = firstErr.Error()
			}
			// A fresh engine in the same deployed process models the scheduler
			// restart while retaining the committed result in PostgreSQL.
			runner = newRunner()
			resumed = true
		}
		result, err := runner.Run(ctx, workflowID)
		if err != nil {
			writeReadinessError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if result.Blocked || result.Workflow.State != state.StateSucceeded {
			writeReadinessError(w, http.StatusInternalServerError, fmt.Sprintf("readiness workflow did not succeed: %+v", result))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(readinessResponse{WorkflowID: workflowID,
			DefinitionID: definitionID, Mode: mode, State: result.Workflow.State,
			Revision: result.Workflow.Revision, Resumed: resumed,
			CrashBoundary: crashBoundary, FirstRunError: firstRunError,
			Metrics: metrics.Snapshot()})
	})
}

func writeReadinessError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}

func readinessPayloadHash(workflowID string) string {
	return fmt.Sprintf("sub-v1:%x", sha256.Sum256([]byte(workflowID)))
}

func metricsHandler(role string, metrics *telemetry.Metrics) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = fmt.Fprintln(w, "# HELP durable_runtime_up Whether the runtime process is up.")
		_, _ = fmt.Fprintln(w, "# TYPE durable_runtime_up gauge")
		_, _ = fmt.Fprintf(w, "durable_runtime_up{role=%s} 1\n", strconv.Quote(role))
		if metrics != nil {
			_, _ = w.Write([]byte(metrics.Render()))
		}
		_ = r
	})
}

func pprofHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
}

func checkHealth() error {
	url := envOrDefault("RUNTIME_HEALTH_URL", "http://127.0.0.1:8080/healthz")
	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Get(url)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status %s", response.Status)
	}
	return nil
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func splitBrokers(value string) []string {
	var brokers []string
	for _, broker := range strings.Split(value, ",") {
		if broker = strings.TrimSpace(broker); broker != "" {
			brokers = append(brokers, broker)
		}
	}
	return brokers
}
