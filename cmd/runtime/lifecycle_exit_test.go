package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/engine"
	"durable-agent-execution-engine/internal/state"
	"durable-agent-execution-engine/internal/transport"
)

func TestK1LifecycleExitChild(t *testing.T) {
	if os.Getenv("DURABLE_K1_LIFECYCLE_CHILD") != "1" {
		return
	}
	if os.Getenv("DURABLE_K1_FAIL_KAFKA") == "1" {
		newKafkaBroker = func([]string, string) (*transport.KafkaBroker, error) {
			return nil, errors.New("injected Kafka construction failure")
		}
	}
	if os.Getenv("DURABLE_K1_FAIL_ROLE") == "1" {
		serveScheduler = func(context.Context, *state.Store, string, <-chan struct{}, engine.ServeOptions) error {
			return errors.New("injected scheduler role failure")
		}
	}
	if os.Getenv("DURABLE_K1_STUCK_ROLE") == "1" {
		serveScheduler = func(context.Context, *state.Store, string, <-chan struct{}, engine.ServeOptions) error {
			<-make(chan struct{})
			return nil
		}
	}
	if os.Getenv("DURABLE_K1_STUCK_STORE_CLOSE") == "1" {
		closeRuntimeStore = func(*state.Store) { <-make(chan struct{}) }
	}
	roles := os.Getenv("DURABLE_K1_ROLES")
	if roles == "" {
		roles = "api"
	}
	os.Args = []string{"runtime", "--addr", "127.0.0.1:0", "--roles", roles}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	started := time.Now()
	go func() {
		var input [1]byte
		_, _ = os.Stdin.Read(input[:])
		stop()
	}()
	if err := serveLifecycle(ctx); err != nil {
		if (os.Getenv("DURABLE_K1_STUCK_ROLE") == "1" || os.Getenv("DURABLE_K1_STUCK_STORE_CLOSE") == "1") && time.Since(started) > 22*time.Second {
			slog.Error("bounded lifecycle exceeded its test deadline", "elapsed", time.Since(started))
			os.Exit(2)
		}
		slog.Error("runtime stopped", "error", err)
		os.Exit(1)
	}
	os.Exit(0)
}

func TestK1LifecycleFatalExit(t *testing.T) {
	databaseURL := ""
	if os.Getenv("DURABLE_RUN_INTEGRATION") == "1" {
		_, _, databaseURL = k1Database(t)
	}
	tests := []struct {
		name       string
		vars       map[string]string
		logLine    string
		needsDB    bool
		cleanDrain bool
	}{
		{name: "invalid fault seed", vars: map[string]string{"DURABLE_FAULT_ENDPOINT": "127.0.0.1:1", "DURABLE_FAULT_TOKEN": "test", "DURABLE_FAULT_SEED": "invalid"}, logLine: "fault controller initialization failed"},
		{name: "unreachable fault endpoint", vars: map[string]string{"DURABLE_FAULT_ENDPOINT": "127.0.0.1:1", "DURABLE_FAULT_TOKEN": "test", "DURABLE_FAULT_SEED": "1"}, logLine: "fault controller initialization failed"},
		{name: "runtime ready boundary", logLine: "runtime fault boundary failed"},
		{name: "Kafka construction", vars: map[string]string{"DURABLE_K1_FAIL_KAFKA": "1", "DATABASE_URL": databaseURL, "KAFKA_BOOTSTRAP_SERVERS": "127.0.0.1:9092", "DURABLE_K1_ROLES": "relay"}, logLine: "runtime Kafka relay initialization failed", needsDB: true},
		{name: "scheduler role", vars: map[string]string{"DURABLE_K1_FAIL_ROLE": "1", "DATABASE_URL": databaseURL, "DURABLE_K1_ROLES": "scheduler"}, logLine: "injected scheduler role failure", needsDB: true},
		{name: "clean SIGTERM drain", vars: map[string]string{"DURABLE_K1_ROLES": "api"}, cleanDrain: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.needsDB && databaseURL == "" {
				t.Skip("requires DURABLE_RUN_INTEGRATION=1 and a fresh migrated database")
			}
			if test.name == "runtime ready boundary" {
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				test.vars = map[string]string{"DURABLE_FAULT_ENDPOINT": listener.Addr().String(), "DURABLE_FAULT_TOKEN": "test", "DURABLE_FAULT_SEED": "1", "DURABLE_K1_ROLES": "api"}
				go func() {
					connection, acceptErr := listener.Accept()
					if acceptErr == nil {
						_ = connection.Close()
					}
					_ = listener.Close()
				}()
			}
			command := exec.Command(os.Args[0], "-test.run=^TestK1LifecycleExitChild$")
			command.Env = lifecycleChildEnvironment(test.vars)
			var output synchronizedBuffer
			command.Stdout, command.Stderr = &output, &output
			stdin, err := command.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			if test.cleanDrain {
				deadline := time.Now().Add(10 * time.Second)
				for !strings.Contains(output.String(), "runtime started") && time.Now().Before(deadline) {
					time.Sleep(10 * time.Millisecond)
				}
				if !strings.Contains(output.String(), "runtime started") {
					_ = command.Process.Kill()
					_ = command.Wait()
					t.Fatalf("runtime did not start: %s", output.String())
				}
				_, _ = stdin.Write([]byte("x"))
			}
			wait := make(chan error, 1)
			go func() { wait <- command.Wait() }()
			select {
			case err := <-wait:
				if test.cleanDrain {
					if err != nil {
						t.Fatalf("clean SIGTERM drain exited nonzero: %v\n%s", err, output.String())
					}
					return
				}
				if err == nil {
					t.Fatalf("fatal startup exited 0; expected log %q; output: %s", test.logLine, output.String())
				}
				if !strings.Contains(output.String(), test.logLine) {
					t.Fatalf("fatal startup omitted log %q: %s", test.logLine, output.String())
				}
			case <-time.After(20 * time.Second):
				_ = command.Process.Kill()
				<-wait
				t.Fatalf("runtime process did not finish: %s", output.String())
			}
		})
	}
}

func TestK1LifecycleBoundedExit(t *testing.T) {
	if os.Getenv("DURABLE_RUN_INTEGRATION") != "1" {
		t.Skip("requires a fresh migrated integration database")
	}
	_, _, databaseURL := k1Database(t)
	for _, test := range []struct {
		name string
		vars map[string]string
	}{
		{name: "stuck scheduler", vars: map[string]string{"DURABLE_K1_STUCK_ROLE": "1", "DURABLE_K1_ROLES": "scheduler"}},
		{name: "stuck store close", vars: map[string]string{"DURABLE_K1_STUCK_STORE_CLOSE": "1", "DURABLE_K1_ROLES": "api"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			test.vars["DATABASE_URL"] = databaseURL
			command := exec.Command(os.Args[0], "-test.run=^TestK1LifecycleExitChild$")
			command.Env = lifecycleChildEnvironment(test.vars)
			var output synchronizedBuffer
			command.Stdout, command.Stderr = &output, &output
			stdin, err := command.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			startDeadline := time.Now().Add(10 * time.Second)
			for !strings.Contains(output.String(), "runtime started") && time.Now().Before(startDeadline) {
				time.Sleep(10 * time.Millisecond)
			}
			if !strings.Contains(output.String(), "runtime started") {
				_ = command.Process.Kill()
				_ = command.Wait()
				t.Fatalf("runtime did not start: %s", output.String())
			}
			_, _ = stdin.Write([]byte("x"))
			wait := make(chan error, 1)
			go func() { wait <- command.Wait() }()
			select {
			case err := <-wait:
				if err == nil {
					t.Fatalf("stalled shutdown exited 0: %s", output.String())
				}
				if !strings.Contains(output.String(), "runtime stopped") {
					t.Fatalf("stalled shutdown omitted fatal exit log: %s", output.String())
				}
			case <-time.After(25 * time.Second):
				_ = command.Process.Kill()
				<-wait
				t.Fatalf("stalled shutdown exceeded the 25 second test bound: %s", output.String())
			}
		})
	}
}

type synchronizedBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *synchronizedBuffer) Write(value []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(value)
}

func (b *synchronizedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}

func lifecycleChildEnvironment(overrides map[string]string) []string {
	values := map[string]string{}
	for _, entry := range os.Environ() {
		key, value, ok := strings.Cut(entry, "=")
		if ok {
			values[key] = value
		}
	}
	for _, key := range []string{"DATABASE_URL", "DURABLE_FAULT_ENDPOINT", "DURABLE_FAULT_TOKEN", "DURABLE_FAULT_SEED", "KAFKA_BOOTSTRAP_SERVERS", "DURABLE_K1_FAIL_KAFKA", "DURABLE_K1_FAIL_ROLE", "DURABLE_K1_STUCK_ROLE", "DURABLE_K1_STUCK_STORE_CLOSE", "DURABLE_K1_ROLES"} {
		delete(values, key)
	}
	values["DURABLE_K1_LIFECYCLE_CHILD"] = "1"
	values["RUNTIME_PPROF_ENABLED"] = "0"
	values["OTEL_EXPORTER_OTLP_ENDPOINT"] = ""
	for key, value := range overrides {
		values[key] = value
	}
	result := make([]string, 0, len(values))
	for key, value := range values {
		result = append(result, key+"="+value)
	}
	return result
}
