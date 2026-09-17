package main

import (
	"context"
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
	"durable-agent-execution-engine/internal/state"
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
	handler := newHandler(role)
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
		defer store.Close()
		handler = newHandlerWithStore(role, store)
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
			relay := transport.NewRelay(store, broker, transport.RelayConfig{OwnerID: state.NewID()})
			go func() {
				if err := relay.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
					slog.Error("runtime Kafka relay stopped", "error", err)
				}
			}()
			defer broker.Close()
		}
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
	return newHandlerWithStore(role, nil)
}

func newHandlerWithStore(role string, store *state.Store) http.Handler {
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
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = fmt.Fprintln(w, "# HELP durable_runtime_up Whether the runtime process is up.")
		_, _ = fmt.Fprintln(w, "# TYPE durable_runtime_up gauge")
		_, _ = fmt.Fprintf(w, "durable_runtime_up{role=%s} 1\n", strconv.Quote(role))
	})
	if store != nil {
		mux.Handle("/v1/", api.NewServer(store).Handler())
	}
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
