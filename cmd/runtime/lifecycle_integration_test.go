package main

import (
	"bytes"
	"context"
	"durable-agent-execution-engine/internal/api"
	"durable-agent-execution-engine/internal/engine"
	"durable-agent-execution-engine/internal/transport"
	"encoding/json"
	"github.com/jackc/pgx/v5"
	"golang.org/x/sync/errgroup"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/partition"
	"durable-agent-execution-engine/internal/state"
)

func k1Database(t *testing.T) (context.Context, *state.Store, string) {
	t.Helper()
	if os.Getenv("DURABLE_RUN_INTEGRATION") != "1" && os.Getenv("DURABLE_REQUIRE_DATABASE") != "1" {
		t.Skip("K1 PostgreSQL tests require integration/service mode")
	}
	databaseURL := os.Getenv("DURABLE_DATABASE_URL")
	if databaseURL == "" {
		data, err := os.ReadFile(filepath.Join("..", "..", ".env"))
		if err != nil {
			t.Fatal(err)
		}
		values := map[string]string{}
		for _, line := range strings.Split(string(data), "\n") {
			parts := strings.SplitN(strings.TrimSpace(line), "=", 2)
			if len(parts) == 2 {
				values[parts[0]] = strings.Trim(parts[1], "\r\"")
			}
		}
		port := values["POSTGRES_PORT"]
		if port == "" {
			port = "5432"
		}
		databaseURL = (&url.URL{Scheme: "postgresql", User: url.UserPassword(values["POSTGRES_USER"], values["POSTGRES_PASSWORD"]), Host: "127.0.0.1:" + port, Path: "/" + values["POSTGRES_DB"], RawQuery: "sslmode=disable"}).String()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	store, err := state.NewFromURL(ctx, databaseURL)
	if err != nil {
		t.Fatal("K1 PostgreSQL unavailable")
	}
	t.Cleanup(store.Close)
	return ctx, store, databaseURL
}

func TestK1RuntimeChild(t *testing.T) {
	if os.Getenv("DUR053_CHILD") != "1" {
		return
	}
	os.Args = []string{"runtime", "--addr", "127.0.0.1:0", "--roles", "scheduler"}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// Windows has no POSIX SIGTERM. The same NotifyContext cancellation path
	// is exercised through stdin there; Linux service CI sends real SIGTERM.
	go func() { var b [1]byte; _, _ = os.Stdin.Read(b[:]); cancel() }()
	if os.Getenv("RUNTIME_SHUTDOWN_MODE") == "legacy" {
		legacyServe(ctx)
		return
	}
	if err := serveLifecycle(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestK1RuntimeSignalCompetingAcquisition(t *testing.T) {
	ctx, store, databaseURL := k1Database(t)
	reference := k1ReferenceRuntime(t)
	for _, mode := range []string{"drain", "legacy", "pre-k1-reference"} {
		legacy := mode != "drain"
		t.Run(mode, func(t *testing.T) {
			id := "k1-process-" + state.NewID()
			part, _ := partition.ID(id)
			if err := store.CreateDefinition(ctx, state.DefinitionInput{DefinitionID: id, Version: 1, DefinitionHash: id,
				Graph: []byte(`{"entry":"root","nodes":[{"id":"root","kind":"success"}]}`), ActivityVersions: []byte(`{}`), EffectClasses: []byte(`{"root":"PURE_ACTIVITY"}`)}); err != nil {
				t.Fatal(err)
			}
			if _, err := store.CreateWorkflow(ctx, state.CreateWorkflowInput{WorkflowID: id, Namespace: id, SubmissionKey: id, SubmissionPayloadHash: "sub-v1:" + id, DefinitionID: id, DefinitionVersion: 1, PartitionID: int16(part), InitialNodeID: "root", InitialInput: []byte(`{}`), ActorID: "k1-test"}); err != nil {
				t.Fatal(err)
			}
			var childLease state.LeaseRef
			defer func() {
				_, _ = store.Pool().Exec(ctx, `DELETE FROM engine.workflow_executions WHERE workflow_id=$1`, id)
				_, _ = store.Pool().Exec(ctx, `DELETE FROM engine.workflow_definitions WHERE definition_id=$1`, id)
				if childLease.OwnerID != "" {
					_ = store.ReleaseLease(ctx, childLease)
				}
			}()
			command := exec.Command(os.Args[0], "-test.run=^TestK1RuntimeChild$")
			if mode == "pre-k1-reference" {
				command = exec.Command(reference, "-addr", "127.0.0.1:0")
			}
			command.Env = append(os.Environ(), "DUR053_CHILD=1", "DATABASE_URL="+databaseURL, "RUNTIME_NAMESPACE="+id, "RUNTIME_SHUTDOWN_MODE="+mode, "RUNTIME_SCHEDULER_HOLD_AFTER_ACQUIRE_MS=60000", "KAFKA_BOOTSTRAP_SERVERS=", "RUNTIME_PPROF_ENABLED=0", "RUNTIME_SCHEDULER=enabled")
			var output bytes.Buffer
			command.Stdout = &output
			command.Stderr = &output
			input, err := command.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err = command.Start(); err != nil {
				t.Fatal(err)
			}
			defer command.Process.Kill()
			wait := make(chan error, 1)
			go func() { wait <- command.Wait() }()
			deadline := time.Now().Add(20 * time.Second)
			var epoch int64
			for {
				select {
				case err := <-wait:
					t.Fatalf("runtime exited before acquisition: %v %s", err, output.String())
				default:
				}
				var owner *string
				if err := store.Pool().QueryRow(ctx, `SELECT owner_id::text,epoch FROM engine.partition_leases WHERE partition_id=$1`, int16(part)).Scan(&owner, &epoch); err != nil {
					t.Fatal(err)
				}
				if owner != nil {
					childLease = state.LeaseRef{PartitionID: int16(part), OwnerID: *owner, Epoch: epoch}
					break
				}
				if time.Now().After(deadline) {
					_ = command.Process.Kill()
					<-wait
					t.Fatalf("child did not acquire lease: %s", output.String())
				}
				time.Sleep(10 * time.Millisecond)
			}
			competitorID := state.NewID()
			acquireCtx, stopAcquire := context.WithCancel(ctx)
			defer stopAcquire()
			acquired := make(chan state.Lease, 1)
			competitorDone := make(chan struct{})
			defer func() { stopAcquire(); <-competitorDone }()
			go func() {
				defer close(competitorDone)
				for acquireCtx.Err() == nil {
					lease, ok, err := store.AcquireLease(acquireCtx, int16(part), competitorID, time.Minute)
					if err == nil && ok {
						acquired <- lease
						return
					}
					time.Sleep(10 * time.Millisecond)
				}
			}()
			started := time.Now()
			if runtime.GOOS == "windows" {
				_, err = input.Write([]byte("x"))
			} else {
				err = command.Process.Signal(syscall.SIGTERM)
			}
			if err != nil {
				t.Fatal(err)
			}
			select {
			case err = <-wait:
				if err != nil {
					t.Fatalf("runtime child: %v\n%s", err, output.String())
				}
			case <-time.After(12 * time.Second):
				t.Fatal("runtime shutdown exceeded deadline")
			}
			if legacy {
				select {
				case <-acquired:
					t.Fatal("legacy shutdown released lease before TTL")
				case <-time.After(200 * time.Millisecond):
				}
				t.Log("pre-K1 legacy parity: lease left to TTL after process exit")
			} else {
				select {
				case next := <-acquired:
					if next.Epoch <= epoch {
						t.Fatal("competing acquisition did not advance epoch")
					}
					_ = store.ReleaseLease(ctx, state.LeaseRef{PartitionID: next.PartitionID, OwnerID: next.OwnerID, Epoch: next.Epoch})
					position := -1
					for _, phase := range []string{"readiness_503", "scheduler", "relay_and_listener", "ingestor", "http_drain", "store_close"} {
						next := strings.Index(output.String(), "phase="+phase)
						if next <= position {
							t.Fatalf("shutdown phase order violated at %s: %s", phase, output.String())
						}
						position = next
					}
					t.Logf("shutdown order: readiness_503 -> endpoint delay -> scheduler -> relay_and_listener -> ingestor -> http_drain -> store_close")
					t.Logf("committed competing acquisition after signal: %s (15s TTL), epoch %d -> %d", time.Since(started), epoch, next.Epoch)
				case <-time.After(time.Second):
					t.Fatal("competing scheduler did not acquire promptly after shutdown")
				}
				if time.Since(started) > 12*time.Second {
					t.Fatal("signal takeover exceeded prompt bound")
				}
			}
			stopAcquire()
		})
	}
}

func TestK1PythonDrainResultsAndReplacement(t *testing.T) {
	ctx, store, _ := k1Database(t)
	server := httptest.NewServer(api.NewServer(store).Handler())
	defer server.Close()
	root, _ := filepath.Abs(filepath.Join("..", ".."))
	for _, mode := range []string{"drain", "abort-current", "abort"} {
		id := "k1-python-" + state.NewID()
		part, _ := partition.ID(id)
		if err := store.CreateDefinition(ctx, state.DefinitionInput{DefinitionID: id, Version: 1, DefinitionHash: id,
			Graph: []byte(`{"entry":"pure.echo","nodes":[{"id":"pure.echo","kind":"activity","next":"done"},{"id":"done","kind":"success"}]}`), ActivityVersions: []byte(`{"pure.echo":"v1"}`), EffectClasses: []byte(`{"pure.echo":"PURE_ACTIVITY"}`)}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.CreateWorkflow(ctx, state.CreateWorkflowInput{WorkflowID: id, Namespace: id, SubmissionKey: id, SubmissionPayloadHash: "sub-v1:" + id, DefinitionID: id, DefinitionVersion: 1, PartitionID: int16(part), InitialNodeID: "pure.echo", InitialInput: []byte(`{}`), ActorID: "k1-test"}); err != nil {
			t.Fatal(err)
		}
		defer func() {
			_, _ = store.Pool().Exec(ctx, `DELETE FROM engine.workflow_executions WHERE workflow_id=$1`, id)
			_, _ = store.Pool().Exec(ctx, `DELETE FROM engine.workflow_definitions WHERE definition_id=$1`, id)
		}()
		runner := engine.New(store, nil)
		runner.ExternalActivities = true
		if _, err := runner.Run(ctx, id); err != nil {
			t.Fatal(err)
		}
		script := `
import sys
from workers.control import ControlError
from workers.kafka_worker import RetryingControl
from workers.lifecycle import WorkerLifecycle
from workers.runner import ActivityRunner, ActivityTask
from workers.registry import ActivityRegistry
lifecycle = WorkerLifecycle()
control = RetryingControl(sys.argv[1], lifecycle.abort, lifecycle)
registry = ActivityRegistry()
def activity(value):
    lifecycle.begin_drain()
    if sys.argv[3].startswith('abort'):
        lifecycle.abort.set()
    return {'finished': True}
registry.register('test', 'v1', activity)
try:
    result = ActivityRunner(control, registry).run_task(ActivityTask(sys.argv[2], 'pure.echo', 0, 'k1-python', sys.argv[2]+'-request', 'test', 'v1', {}))
    assert sys.argv[3] == 'drain' and result['Disposition'] == 'ACCEPTED'
except ControlError as error:
    assert sys.argv[3].startswith('abort') and error.code == 'SHUTTING_DOWN'
`
		command := exec.CommandContext(ctx, "uv", "run", "--no-sync", "python", "-", server.URL, id, mode)
		command.Dir = root
		command.Stdin = strings.NewReader(script)
		command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "python"), "PYTHONDONTWRITEBYTECODE=1")
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("Python %s: %v %s", mode, err, output)
		}
		attempt, err := store.GetAttempt(ctx, id, "pure.echo", 0, 1)
		if err != nil {
			t.Fatal(err)
		}
		if mode == "drain" {
			if attempt.State != state.AttemptSucceeded {
				t.Fatal("draining Python result not durably accepted")
			}
			t.Log("Python result accepted during drain before deadline")
			continue
		}
		if attempt.State != state.AttemptClaimed {
			t.Fatal("aborted Python reported a result")
		}
		if _, err := store.Pool().Exec(ctx, `UPDATE engine.activity_attempts SET heartbeat_deadline=clock_timestamp()-interval '1 second' WHERE workflow_id=$1`, id); err != nil {
			t.Fatal(err)
		}
		if mode == "abort-current" {
			k1LateResultProbe(t, server.URL, id, attempt.ClaimToken, http.StatusOK)
			t.Log("abort and expired heartbeat alone did not make current claim stale")
			continue
		}
		// Abort itself has no durable meaning. Even an elapsed heartbeat deadline
		// does not authorize a STALE verdict before the replacement transaction.
		// Timeout replacement commits before the explicit late-result probe.
		if _, err := runner.Run(ctx, id); err != nil {
			t.Fatal(err)
		}
		replacement, err := store.GetAttempt(ctx, id, "pure.echo", 0, 2)
		if err != nil || replacement.State != state.AttemptDispatchable {
			t.Fatalf("replacement not committed: %+v %v", replacement, err)
		}
		k1LateResultProbe(t, server.URL, id, attempt.ClaimToken, http.StatusConflict)
		t.Log("aborted Python emitted no result; explicit late result STALE only after committed attempt 2")
	}
}

func TestK1DatabaseOutageReadinessAndShutdown(t *testing.T) {
	ctx, _, databaseURL := k1Database(t)
	config, err := pgx.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	target := &url.URL{Scheme: "postgresql", User: url.UserPassword(config.User, config.Password), Host: net.JoinHostPort(config.Host, strconv.Itoa(int(config.Port))), Path: "/" + config.Database, RawQuery: "sslmode=disable"}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var connectionsMu sync.Mutex
	var connections []net.Conn
	backendAddr := target.Host
	var proxy errgroup.Group
	proxy.Go(func() error {
		for {
			client, err := listener.Accept()
			if err != nil {
				return nil
			}
			backend, err := net.DialTimeout("tcp", backendAddr, time.Second)
			if err != nil {
				client.Close()
				continue
			}
			connectionsMu.Lock()
			connections = append(connections, client, backend)
			connectionsMu.Unlock()
			proxy.Go(func() error { defer client.Close(); defer backend.Close(); _, _ = io.Copy(backend, client); return nil })
			proxy.Go(func() error { defer client.Close(); defer backend.Close(); _, _ = io.Copy(client, backend); return nil })
		}
	})
	target.Host = listener.Addr().String()
	store, err := state.NewFromURL(ctx, target.String())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var draining atomic.Bool
	if !runtimeReady(ctx, &draining, store.Pool().Ping) {
		t.Fatal("proxy DB initially unready")
	}
	// Confirm a genuine connection outage, without touching the shared server.
	listener.Close()
	connectionsMu.Lock()
	for _, conn := range connections {
		conn.Close()
	}
	connectionsMu.Unlock()
	_ = proxy.Wait()
	if runtimeReady(ctx, &draining, store.Pool().Ping) {
		t.Fatal("real PostgreSQL connection outage remained ready")
	}
	handler := lifecycleHandler("outage", store, nil, nil, &draining)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest("GET", "/healthz", nil))
	if response.Code != 200 {
		t.Fatal("database outage failed liveness")
	}
	var group errgroup.Group
	entered := make(chan struct{}, 2)
	scheduler := startRole(&group, func() {}, func(ctx context.Context, drain <-chan struct{}) error {
		entered <- struct{}{}
		return engine.ServeWithOptions(ctx, store, "k1-outage", nil, engine.ServeOptions{Drain: drain})
	})
	relay := startRole(&group, func() {}, func(ctx context.Context, drain <-chan struct{}) error {
		entered <- struct{}{}
		return transport.NewRelay(store, &transport.MemoryBroker{}, transport.RelayConfig{Drain: drain}).Run(ctx)
	})
	<-entered
	<-entered
	time.Sleep(25 * time.Millisecond)
	started := time.Now()
	for _, role := range []*lifecycleRole{scheduler, relay} {
		if err := role.stop(25*time.Millisecond, 3*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	if err := group.Wait(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for store.Pool().Stat().AcquiredConns() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if store.Pool().Stat().AcquiredConns() != 0 {
		t.Fatal("relay LISTEN left an acquired connection after shutdown")
	}
	if time.Since(started) > 7*time.Second {
		t.Fatal("database outage shutdown exceeded bounds")
	}
	t.Logf("PostgreSQL proxy outage: readiness 503, liveness 200, scheduler/relay+LISTEN joined in %s", time.Since(started))
}

// Compile frozen pre-K1 lifecycle sources against the same dependencies. This
// reference does not consult current lifecycle decisions or validators.
func k1ReferenceRuntime(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	destination := t.TempDir()
	for _, directory := range []string{"internal", "cmd/runtime"} {
		err := filepath.WalkDir(filepath.Join(root, directory), func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			relative, _ := filepath.Rel(root, path)
			// The pre-K1 runtime had only main.go and scheduler.go.
			if directory == "cmd/runtime" {
				return nil
			}
			target := filepath.Join(destination, relative)
			if err := os.MkdirAll(filepath.Dir(target), 0700); err != nil {
				return err
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			return os.WriteFile(target, data, 0600)
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"go.mod", "go.sum"} {
		data, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(destination, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	for target, name := range map[string]string{"cmd/runtime/main.go": "runtime-main", "cmd/runtime/scheduler.go": "scheduler", "internal/engine/engine.go": "engine", "internal/engine/service.go": "service"} {
		data, err := os.ReadFile(filepath.Join(root, "tests", "fixtures", "k1-pre-"+name+".go.txt"))
		if err != nil {
			t.Fatal(err)
		}
		if runtime.GOOS == "windows" && name == "runtime-main" {
			// A test transport for NotifyContext cancellation; POSIX uses real SIGTERM.
			data = bytes.Replace(data, []byte("defer stop()"), []byte("defer stop(); ctx, cancelSignal := context.WithCancel(ctx); defer cancelSignal(); go func(){ var b [1]byte; _, _ = os.Stdin.Read(b[:]); cancelSignal() }()"), 1)
		}
		path := filepath.Join(destination, target)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	binary := filepath.Join(destination, "pre-k1-runtime")
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}
	buildCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	command := exec.CommandContext(buildCtx, "go", "build", "-o", binary, "./cmd/runtime")
	command.Dir = destination
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("pre-K1 reference build: %v %s", err, output)
	}
	return binary
}

func k1LateResultProbe(t *testing.T, baseURL, workflowID, token string, status int) {
	t.Helper()
	body, err := json.Marshal(map[string]any{"attempt_number": 1, "claim_token": token, "attempt_state": "SUCCEEDED", "payload": map[string]any{}})
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Post(baseURL+"/v1/workflows/"+workflowID+"/nodes/pure.echo/iterations/0/result", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	output, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != status {
		t.Fatalf("late-result HTTP status = %d, want %d: %s", response.StatusCode, status, output)
	}
	if status == http.StatusConflict && !bytes.Contains(output, []byte("STALE_")) {
		t.Fatalf("late result did not return typed STALE_*: %s", output)
	}
	if status == http.StatusOK && !bytes.Contains(output, []byte("ACCEPTED")) {
		t.Fatalf("current claim was not accepted before replacement: %s", output)
	}
}
