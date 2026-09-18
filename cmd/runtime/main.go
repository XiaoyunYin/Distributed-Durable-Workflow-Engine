package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"durable-agent-execution-engine/internal/api"
	"durable-agent-execution-engine/internal/engine"
	"durable-agent-execution-engine/internal/faults"
	"durable-agent-execution-engine/internal/partition"
	"durable-agent-execution-engine/internal/state"
	"durable-agent-execution-engine/internal/telemetry"
	"durable-agent-execution-engine/internal/transport"
)

const version = "0.1.0-dev"

func main() {
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

	serve := flag.NewFlagSet("serve", flag.ExitOnError)
	addr := serve.String("addr", envOrDefault("RUNTIME_ADDR", "127.0.0.1:8080"), "HTTP listen address")
	_ = serve.Parse(os.Args[1:])

	role := envOrDefault("RUNTIME_ROLE", "runtime")
	metrics := telemetry.New(role)
	handler := newHandlerWithMetrics(role, nil, metrics)
	var store *state.Store
	if databaseURL := os.Getenv("DATABASE_URL"); databaseURL != "" {
		databaseContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		var err error
		store, err = state.NewFromURL(databaseContext, databaseURL)
		cancel()
		if err != nil {
			slog.Error("runtime database initialization failed", "error", err)
			os.Exit(1)
		}
		store.SetTelemetry(metrics)
		defer store.Close()
		handler = newHandlerWithMetrics(role, store, metrics)
	}
	server := &http.Server{
		Addr:              *addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var broker transport.Broker
	if store != nil {
		brokers := splitBrokers(os.Getenv("KAFKA_BOOTSTRAP_SERVERS"))
		if len(brokers) > 0 {
			var err error
			broker, err = transport.NewKafkaBroker(brokers, transport.DefaultTopic)
			if err != nil {
				slog.Error("runtime Kafka relay initialization failed", "error", err)
				os.Exit(1)
			}
			relay := transport.NewRelay(store, broker, transport.RelayConfig{OwnerID: state.NewID(),
				OnError: func(err error) {
					metrics.RecordRelayFailure()
					slog.Warn("runtime Kafka relay pass failed", "error", err)
				},
				OnSuccess: func(transport.RelayReport) {
					// Readiness is a one-time dependency transition, not a
					// heartbeat. The first successful relay pass is still valid
					// evidence when startup readiness was not yet observable.
					metrics.MarkDurableReady()
				}})
			go func() {
				if err := relay.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
					slog.Error("runtime Kafka relay stopped", "error", err)
				}
			}()
			defer broker.Close()
		}
	}
	if store != nil && broker != nil {
		// NewFromURL has already pinged PostgreSQL and Kafka relay construction
		// has succeeded. This is the one-time durable dependency transition;
		// the timestamp is never overwritten by a recovery heartbeat.
		metrics.MarkDurableReady()
	}
	if controller, err := faults.FromEnvironment(); err != nil {
		slog.Error("fault controller initialization failed", "error", err)
		os.Exit(1)
	} else if controller != nil {
		defer controller.Close()
		if err := controller.Hit("runtime_ready", map[string]any{"role": role}); err != nil {
			slog.Error("runtime fault boundary failed", "error", err)
			os.Exit(1)
		}
		_ = controller.Emit("released", "runtime_ready", map[string]any{"role": role})
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			slog.Error("server shutdown failed", "error", err)
		}
	}()

	slog.Info("runtime foundation server started", "addr", *addr, "role", role, "version", version)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("runtime server failed", "error", err)
		os.Exit(1)
	}
}

func newHandler(role string) http.Handler {
	return newHandlerWithMetrics(role, nil, telemetry.New(role))
}

func newHandlerWithStore(role string, store *state.Store) http.Handler {
	return newHandlerWithMetrics(role, store, telemetry.New(role))
}

func newHandlerWithMetrics(role string, store *state.Store, metrics *telemetry.Metrics) http.Handler {
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
	mux.HandleFunc("GET /readyz", health)
	mux.Handle("GET /metrics", metricsHandler(role, metrics))
	if store != nil {
		mux.Handle("/v1/", api.NewServer(store).Handler())
		if envOrDefault("RUNTIME_ENGINE_MODE", "disabled") == "readiness" {
			mux.Handle("POST /internal/readiness/run", readinessHandler(store, metrics, role))
		}
	}
	return mux
}

type readinessResponse struct {
	WorkflowID   string              `json:"workflow_id"`
	DefinitionID string              `json:"definition_id"`
	State        state.WorkflowState `json:"state"`
	Revision     int64               `json:"revision"`
	Metrics      telemetry.Snapshot  `json:"metrics"`
}

func readinessHandler(store *state.Store, metrics *telemetry.Metrics, role string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if store == nil {
			writeReadinessError(w, http.StatusServiceUnavailable, "readiness engine requires a database")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
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
		runner := engine.New(store, engine.ActivityDriverFunc(func(context.Context, engine.Activity) (engine.ActivityResult, error) {
			return engine.ActivityResult{Payload: json.RawMessage(`{"ok":true}`)}, nil
		}))
		// Partition lease ownership is persisted as a UUID. Keep the runtime
		// role in telemetry, but use a valid durable owner identity here.
		runner.OwnerID = state.NewID()
		runner.WorkerID = "dur036-runtime-worker"
		runner.ActorID = "dur036-runtime-engine"
		runner.MaxSteps = 20
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
			DefinitionID: definitionID, State: result.Workflow.State,
			Revision: result.Workflow.Revision, Metrics: metrics.Snapshot()})
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
