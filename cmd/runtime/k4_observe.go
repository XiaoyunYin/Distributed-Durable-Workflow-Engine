package main

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"durable-agent-execution-engine/internal/k4observe"
	"durable-agent-execution-engine/internal/state"
)

func k4Handler(next http.Handler) http.Handler {
	if os.Getenv("DUR053_K4_CAMPAIGN") != "1" || os.Getenv("RUNTIME_ROLE") != "api" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/v1/workflows" {
			identity := r.Header.Get("X-Dur053-K4-Request")
			var workflows []string
			if identity == "" {
				identity = state.NewID()
			} else {
				workflows = []string{identity}
			}
			done := k4observe.Begin(r.Context(), "api", identity, state.K4Session, workflows)
			defer done()
		}
		next.ServeHTTP(w, r)
	})
}

func observeK4Signals() func() {
	if os.Getenv("DUR053_K4_CAMPAIGN") != "1" {
		return func() {}
	}
	ch := make(chan os.Signal, 4)
	done := make(chan struct{})
	signal.Notify(ch, syscall.SIGTERM, os.Interrupt)
	go func() {
		for {
			select {
			case sig := <-ch:
				data, _ := json.Marshal(map[string]any{"event": "k4_signal", "signal": int(sig.(syscall.Signal)),
					"session": state.K4Session, "pod_uid": os.Getenv("POD_UID"), "container_id": os.Getenv("CONTAINER_ID"), "timestamp": time.Now().UTC()})
				slog.Info("k4 process signal", "data", string(data))
			case <-done:
				return
			}
		}
	}()
	return func() { signal.Stop(ch); close(done) }
}
