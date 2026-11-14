package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
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

func k4SignalContext(parent context.Context, notify func(chan<- os.Signal, ...os.Signal), stopNotify func(chan<- os.Signal)) (context.Context, context.CancelFunc) {
	if os.Getenv("DUR053_K4_CAMPAIGN") != "1" {
		return signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	}
	ctx, cancel := context.WithCancel(parent)
	ch := make(chan os.Signal, 4)
	done := make(chan struct{})
	stopping := make(chan struct{})
	var once sync.Once
	notify(ch, syscall.SIGTERM, os.Interrupt)
	record := func(sig os.Signal) {
		received := time.Now().UTC()
		data, _ := json.Marshal(map[string]any{"event": "k4_signal", "signal": int(sig.(syscall.Signal)),
			"session": state.K4Session, "pod_uid": os.Getenv("POD_UID"), "container_id": os.Getenv("CONTAINER_ID"), "timestamp": received})
		slog.Info("k4 process signal", "data", string(data))
	}
	go func() {
		defer close(done)
		defer cancel()
		select {
		case sig := <-ch:
			record(sig)
		case <-stopping:
			// Stop has unsubscribed, so drain any already-received signal before
			// cancellation or an error exit can overtake its record.
			select {
			case sig := <-ch:
				record(sig)
			default:
			}
		case <-ctx.Done():
		}
	}()
	return ctx, func() {
		once.Do(func() { stopNotify(ch); close(stopping) })
		<-done
	}
}
