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
	"syscall"
	"time"
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
	addr := serve.String("addr", envOrDefault("RUNTIME_ADDR", ":8080"), "HTTP listen address")
	_ = serve.Parse(os.Args[1:])

	role := envOrDefault("RUNTIME_ROLE", "runtime")
	server := &http.Server{
		Addr:              *addr,
		Handler:           newHandler(role),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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
