package main

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/state"
)

func TestK2DemandProcess(t *testing.T) {
	_, _, url := k1Database(t)
	saved := os.Args
	defer func() { os.Args = saved }()
	t.Setenv("RUNTIME_TOKEN_AUTH", "disabled")
	t.Setenv("KAFKA_BOOTSTRAP_SERVERS", "")
	t.Setenv("RUNTIME_ENDPOINT_REMOVAL_DELAY_MS", "0")
	t.Setenv("RUNTIME_NAMESPACE", "k2-empty-"+state.NewID())
	t.Setenv("DATABASE_URL", url)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	listener.Close()
	os.Args = []string{"runtime", "--roles", "demand", "--addr", addr}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- serveLifecycle(ctx) }()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(12 * time.Second):
			t.Error("demand process failed to stop")
		}
	}()
	client := &http.Client{Timeout: time.Second}
	deadline := time.Now().Add(5 * time.Second)
	for {
		response, err := client.Get("http://" + addr + "/v1/demand")
		if err == nil {
			var sample state.DemandSample
			err = json.NewDecoder(response.Body).Decode(&sample)
			response.Body.Close()
			if err != nil || response.StatusCode != 200 || sample.Namespace != os.Getenv("RUNTIME_NAMESPACE") || sample.Demand != 0 || sample.SampledAt.IsZero() {
				t.Fatalf("demand process wiring bypassed: %+v %v status=%d", sample, err, response.StatusCode)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("demand process did not listen: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestK2DemandStartupConfiguration(t *testing.T) {
	saved := os.Args
	defer func() { os.Args = saved }()
	os.Args = []string{"runtime", "--roles", "demand", "--addr", "127.0.0.1:0"}
	t.Setenv("RUNTIME_TOKEN_AUTH", "disabled")
	for _, entry := range []struct{ db, namespace string }{{"", ""}, {"", "campaign"}, {"postgresql://invalid:invalid@127.0.0.1:1/absent?sslmode=disable", ""}} {
		t.Setenv("DATABASE_URL", entry.db)
		t.Setenv("RUNTIME_NAMESPACE", entry.namespace)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := serveLifecycle(ctx); err == nil || err.Error() != "demand requires DATABASE_URL and RUNTIME_NAMESPACE" {
			t.Fatalf("demand startup guard bypassed: %v", err)
		}
	}
}
