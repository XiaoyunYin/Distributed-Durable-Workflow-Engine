package main

import (
	"context"
	"durable-agent-execution-engine/internal/api"
	"durable-agent-execution-engine/internal/engine"
	"durable-agent-execution-engine/internal/faults"
	"durable-agent-execution-engine/internal/state"
	"durable-agent-execution-engine/internal/telemetry"
	"durable-agent-execution-engine/internal/transport"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"time"
)

// Campaign-only matched baseline. Keep the pre-K1 HTTP-return and defer order;
// role selection and shared routes are the same as in the new lifecycle.
func legacyServe(ctx context.Context) {
	serve := flag.NewFlagSet("serve", flag.ExitOnError)
	addr := serve.String("addr", envOrDefault("RUNTIME_ADDR", "127.0.0.1:8080"), "HTTP listen address")
	rolesValue := serve.String("roles", envOrDefault("RUNTIME_ROLES", "combined"), "runtime role set")
	_ = serve.Parse(os.Args[1:])
	roles, roleErr := lifecycleRoles(*rolesValue)
	if roleErr != nil {
		slog.Error("invalid roles", "error", roleErr)
		os.Exit(1)
	}

	// Demand did not exist before K2; it has one identical read-only process
	// in both campaign arms. Legacy lifecycle flags affect the application roles.
	if roles["demand"] {
		if err := serveLifecycle(ctx); err != nil {
			slog.Error("demand stopped", "error", err)
			os.Exit(1)
		}
		return
	}
	routes, routeErr := lifecycleRoutes(*rolesValue, roles)
	reviewer, authErr := api.AuthenticationFromEnvironment()
	if routeErr != nil || authErr != nil {
		slog.Error("invalid HTTP identity/route configuration", "route_error", routeErr, "auth_error", authErr)
		os.Exit(1)
	}
	role := envOrDefault("RUNTIME_ROLE", "runtime")
	metrics := telemetry.New(role)
	tracing, tracingErr := telemetry.NewTracing(context.Background(), "durable-runtime-"+role, os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
	if tracingErr != nil {
		slog.Error("runtime tracing initialization failed", "error", tracingErr)
		os.Exit(1)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := tracing.Shutdown(shutdownCtx); err != nil {
			slog.Warn("runtime tracing shutdown failed", "error", err)
		}
	}()
	handler := newHandlerWithMetrics(role, nil, metrics, tracing)
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
		handler = newHandlerWithMetrics(role, store, metrics, tracing)
	}
	handler = lifecycleHandlerWithAPI(role, store, metrics, tracing, nil, api.NewServer(store).WithRoutes(routes).WithAuthentication(reviewer).WithTracing(tracing).Handler(), routes == api.RoutesCombined)
	server := &http.Server{
		Addr:              *addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	if envOrDefault("RUNTIME_PPROF_ENABLED", "0") == "1" {
		pprofAddr := envOrDefault("RUNTIME_PPROF_ADDR", "127.0.0.1:6060")
		pprofServer := &http.Server{Addr: pprofAddr, Handler: pprofHandler(), ReadHeaderTimeout: 2 * time.Second}
		go func() {
			if err := pprofServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				slog.Error("pprof server failed", "error", err)
			}
		}()
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = pprofServer.Shutdown(shutdownCtx)
		}()
	}

	// The caller supplies the signal context. The original shutdown body,
	// including deferred writer/profile/store/trace cleanup, stays intact.
	wake := make(chan struct{}, 1)
	if store != nil && roles["scheduler"] {
		go func() {
			options := schedulerOptions(tracing)
			options.LegacyShutdown = true
			if err := engine.ServeWithOptions(ctx, store, envOrDefault("RUNTIME_NAMESPACE", "local-runtime"), wake, options); err != nil && !errors.Is(err, context.Canceled) {
				slog.Error("scheduler stopped", "error", err)
			}
		}()
	}
	if store != nil && roles["ingestor"] {
		go func() { _ = runIngestor(ctx, nil, store, splitBrokers(os.Getenv("KAFKA_BOOTSTRAP_SERVERS")), wake) }()
	}
	var broker transport.Broker
	if store != nil && roles["relay"] {
		brokers := splitBrokers(os.Getenv("KAFKA_BOOTSTRAP_SERVERS"))
		if len(brokers) > 0 {
			var err error
			broker, err = transport.NewKafkaBroker(brokers, transport.DefaultTopic)
			if err != nil {
				slog.Error("runtime Kafka relay initialization failed", "error", err)
				os.Exit(1)
			}
			relay := transport.NewRelay(store, broker, transport.RelayConfig{OwnerID: state.NewID(),
				Tracing: tracing,
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
