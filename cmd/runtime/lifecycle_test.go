package main

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"
)

func TestK1ReadinessFreshAndDrain(t *testing.T) {
	var draining atomic.Bool
	if runtimeReady(context.Background(), &draining, nil) {
		t.Fatal("readiness accepted missing database")
	}
	calls := 0
	if !runtimeReady(context.Background(), &draining, func(ctx context.Context) error {
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > time.Second {
			t.Fatal("readiness probe lacked bounded deadline")
		}
		return nil
	}) {
		t.Fatal("bounded readiness check failed")
	}
	ping := func(context.Context) error { calls++; return nil }
	if !runtimeReady(context.Background(), &draining, ping) || !runtimeReady(context.Background(), &draining, ping) || calls != 2 {
		t.Fatal("readiness did not perform fresh DB checks")
	}
	if runtimeReady(context.Background(), &draining, func(context.Context) error { return errors.New("database down") }) {
		t.Fatal("readiness accepted database outage")
	}
	draining.Store(true)
	if runtimeReady(context.Background(), &draining, ping) {
		t.Fatal("draining runtime remained ready")
	}
	if calls != 2 {
		t.Fatal("drain performed unnecessary DB check")
	}
	draining.Store(false)
	if runtimeReady(context.Background(), &draining, func(context.Context) error { draining.Store(true); return nil }) {
		t.Fatal("readiness missed drain during DB check")
	}
	handler := lifecycleHandler("test", nil, nil, nil, &draining)
	for path, status := range map[string]int{"/readyz": 503, "/healthz": 200} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != status {
			t.Fatalf("%s status = %d, want %d", path, response.Code, status)
		}
	}
}

func TestK1RoleWaitsForPass(t *testing.T) {
	var group errgroup.Group
	pass := make(chan struct{})
	released := make(chan struct{})
	role := startRole(&group, func() {}, func(ctx context.Context, drain <-chan struct{}) error {
		<-drain
		<-pass
		close(released)
		return nil
	})
	done := make(chan error, 1)
	go func() { done <- role.stop(time.Second, time.Second) }()
	<-role.drain
	select {
	case <-done:
		t.Fatal("shutdown returned before pass released")
	case <-time.After(25 * time.Millisecond):
	}
	close(pass)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	select {
	case <-released:
	default:
		t.Fatal("pass was not released")
	}
	if err := group.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestK1RoleOutageBound(t *testing.T) {
	var group errgroup.Group
	role := startRole(&group, func() {}, func(ctx context.Context, drain <-chan struct{}) error { <-ctx.Done(); return ctx.Err() })
	started := time.Now()
	if err := role.stop(20*time.Millisecond, time.Second); err != nil {
		t.Fatal(err)
	}
	if time.Since(started) > time.Second {
		t.Fatal("outage shutdown exceeded bound")
	}
	if err := group.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestK1RoleHardBound(t *testing.T) {
	var group errgroup.Group
	stuck := make(chan struct{})
	role := startRole(&group, func() {}, func(context.Context, <-chan struct{}) error { <-stuck; return nil })
	if err := role.stop(time.Millisecond, time.Millisecond); err == nil {
		t.Fatal("uncooperative role bypassed hard deadline")
	}
	close(stuck)
	_ = group.Wait()
}

func TestK1RoleFlags(t *testing.T) {
	t.Setenv("RUNTIME_SCHEDULER", "enabled")
	roles, err := lifecycleRoles("combined")
	if err != nil || !roles["api"] || !roles["relay"] || !roles["scheduler"] || !roles["ingestor"] {
		t.Fatalf("combined roles = %v, %v", roles, err)
	}
	roles, err = lifecycleRoles("scheduler")
	if err != nil || !roles["scheduler"] || roles["relay"] || roles["ingestor"] {
		t.Fatalf("isolated role = %v, %v", roles, err)
	}
	if _, err = lifecycleRoles("typo"); err == nil {
		t.Fatal("invalid role accepted")
	}
}

func TestK1HTTPWaitsForActiveRequest(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(entered); <-release; w.WriteHeader(200) })}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serveDone := make(chan struct{})
	go func() { _ = server.Serve(listener); close(serveDone) }()
	requestDone := make(chan struct{})
	go func() {
		response, err := http.Get("http://" + listener.Addr().String())
		if err == nil {
			response.Body.Close()
		}
		close(requestDone)
	}()
	<-entered
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- shutdownHTTP(server, context.Background()) }()
	<-serveDone
	select {
	case <-shutdownDone:
		close(release)
		<-requestDone
		t.Fatal("HTTP shutdown returned before active request")
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	<-requestDone
	if err := <-shutdownDone; err != nil {
		t.Fatal(err)
	}
}

func TestK1IngestorDrainBeforeReceive(t *testing.T) {
	drain := make(chan struct{})
	close(drain)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runIngestor(ctx, drain, nil, []string{"127.0.0.1:1"}, make(chan struct{}, 1)) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(100 * time.Millisecond):
		cancel()
		<-done
		t.Fatal("draining ingestor fetched another delivery")
	}
}

func TestK1StartupHookDoesNotBlockDrain(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	t.Setenv("DATABASE_URL", "")
	t.Setenv("KAFKA_BOOTSTRAP_SERVERS", "")
	t.Setenv("RUNTIME_PPROF_ENABLED", "0")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("DURABLE_FAULT_ENDPOINT", listener.Addr().String())
	t.Setenv("DURABLE_FAULT_TOKEN", "k1-test-token")
	t.Setenv("DURABLE_FAULT_SEED", "1")
	previous := os.Args
	os.Args = []string{"runtime", "--addr", "127.0.0.1:0", "--roles", "api"}
	defer func() { os.Args = previous }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- serveLifecycle(ctx) }()
	if err := listener.(*net.TCPListener).SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	connection, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if err := connection.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	var event map[string]any
	if err := json.NewDecoder(connection).Decode(&event); err != nil {
		t.Fatal(err)
	}
	if event["boundary"] != "runtime_ready" {
		t.Fatal("runtime did not reach held startup hook")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(4 * time.Second):
		connection.Close()
		<-done
		t.Fatal("startup hook blocked runtime drain")
	}
}
