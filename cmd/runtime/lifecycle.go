package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"durable-agent-execution-engine/internal/engine"
	"durable-agent-execution-engine/internal/faults"
	"durable-agent-execution-engine/internal/state"
	"durable-agent-execution-engine/internal/telemetry"
	"durable-agent-execution-engine/internal/transport"
	"golang.org/x/sync/errgroup"
)

func main() {
	if len(os.Args) > 1 && (os.Args[1] == "version" || os.Args[1] == "healthcheck") {
		runtimeCommand()
		return
	}
	mode := envOrDefault("RUNTIME_SHUTDOWN_MODE", "drain")
	if mode != "drain" && mode != "legacy" {
		slog.Error("invalid RUNTIME_SHUTDOWN_MODE", "mode", mode)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if mode == "legacy" {
		legacyServe(ctx)
		return
	}
	if err := serveLifecycle(ctx); err != nil {
		slog.Error("runtime stopped", "error", err)
		os.Exit(1)
	}
}

// A role owns its goroutines until done closes. Drain stops at a durable pass
// boundary; cancel is the separate forced deadline for unavailable dependencies.
type lifecycleRole struct {
	drain  chan struct{}
	cancel context.CancelFunc
	done   chan struct{}
}

func startRole(group *errgroup.Group, fail context.CancelFunc, run func(context.Context, <-chan struct{}) error) *lifecycleRole {
	ctx, cancel := context.WithCancel(context.Background())
	role := &lifecycleRole{drain: make(chan struct{}), cancel: cancel, done: make(chan struct{})}
	group.Go(func() error {
		defer close(role.done)
		defer cancel()
		err := run(ctx, role.drain)
		if err != nil && !errors.Is(err, context.Canceled) {
			fail()
			return err
		}
		return nil
	})
	return role
}

func (r *lifecycleRole) stop(grace, forced time.Duration) error {
	if r == nil {
		return nil
	}
	close(r.drain)
	defer r.cancel()
	timer := time.NewTimer(grace)
	defer timer.Stop()
	select {
	case <-r.done:
		return nil
	case <-timer.C:
	}
	r.cancel()
	timer.Reset(forced)
	select {
	case <-r.done:
		return nil
	case <-timer.C:
		return errors.New("role exceeded forced shutdown deadline")
	}
}

func runtimeReady(ctx context.Context, draining *atomic.Bool, ping func(context.Context) error) bool {
	if draining != nil && draining.Load() {
		return false
	}
	if ping == nil {
		return false
	}
	if ping != nil {
		checkCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		if ping(checkCtx) != nil {
			return false
		}
	}
	return draining == nil || !draining.Load()
}

func lifecycleRoles(value string) (map[string]bool, error) {
	roles := map[string]bool{}
	if value == "combined" {
		roles["api"], roles["relay"] = true, true
		roles["scheduler"] = envOrDefault("RUNTIME_SCHEDULER", "disabled") == "enabled"
		roles["ingestor"] = roles["scheduler"]
		return roles, nil
	}
	for _, role := range strings.Split(value, ",") {
		switch role {
		case "api", "scheduler", "relay", "ingestor":
			roles[role] = true
		default:
			return nil, fmt.Errorf("unknown runtime role %q", role)
		}
	}
	return roles, nil
}

func serveLifecycle(signalCtx context.Context) error {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	addr := flags.String("addr", envOrDefault("RUNTIME_ADDR", "127.0.0.1:8080"), "HTTP listen address")
	rolesFlag := flags.String("roles", envOrDefault("RUNTIME_ROLES", "combined"), "combined or comma-separated api,scheduler,relay,ingestor")
	if err := flags.Parse(os.Args[1:]); err != nil {
		return err
	}
	roles, err := lifecycleRoles(*rolesFlag)
	if err != nil {
		return err
	}
	roleName := envOrDefault("RUNTIME_ROLE", "runtime")
	metrics := telemetry.New(roleName)
	tracing, err := telemetry.NewTracing(context.Background(), "durable-runtime-"+roleName, os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
	if err != nil {
		return err
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = tracing.Shutdown(ctx)
	}()
	var store *state.Store
	if url := os.Getenv("DATABASE_URL"); url != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		store, err = state.NewFromURL(ctx, url)
		cancel()
		if err != nil {
			return err
		}
		store.SetTelemetry(metrics)
	}
	var draining atomic.Bool
	handlerStore := store
	// Routing restrictions/authentication are K2; roles here control process work.
	handler := lifecycleHandler(roleName, handlerStore, metrics, tracing, &draining)
	server := &http.Server{Addr: *addr, Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second}
	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		if store != nil {
			store.Close()
		}
		return err
	}
	ctx, fail := context.WithCancel(signalCtx)
	defer fail()
	var group errgroup.Group
	httpDone := make(chan struct{})
	group.Go(func() error {
		defer close(httpDone)
		err := server.Serve(listener)
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		fail()
		return err
	})
	var profile *http.Server
	if envOrDefault("RUNTIME_PPROF_ENABLED", "0") == "1" {
		profile = &http.Server{Addr: envOrDefault("RUNTIME_PPROF_ADDR", "127.0.0.1:6060"), Handler: pprofHandler(), ReadHeaderTimeout: 2 * time.Second}
		group.Go(func() error {
			err := profile.ListenAndServe()
			if errors.Is(err, http.ErrServerClosed) {
				return nil
			}
			fail()
			return err
		})
	}
	brokers := splitBrokers(os.Getenv("KAFKA_BOOTSTRAP_SERVERS"))
	wake := make(chan struct{}, 1)
	var scheduler, relay, ingestor *lifecycleRole
	if store != nil && roles["scheduler"] {
		scheduler = startRole(&group, fail, func(ctx context.Context, drain <-chan struct{}) error {
			options := schedulerOptions(tracing)
			options.Drain = drain
			return engine.ServeWithOptions(ctx, store, envOrDefault("RUNTIME_NAMESPACE", "local-runtime"), wake, options)
		})
	}
	if store != nil && roles["relay"] && len(brokers) > 0 {
		broker, brokerErr := transport.NewKafkaBroker(brokers, transport.DefaultTopic)
		if brokerErr != nil {
			fail()
		} else {
			relay = startRole(&group, fail, func(ctx context.Context, drain <-chan struct{}) error {
				defer broker.Close()
				return transport.NewRelay(store, broker, transport.RelayConfig{OwnerID: state.NewID(), Tracing: tracing, Drain: drain,
					OnError:   func(err error) { metrics.RecordRelayFailure(); slog.Warn("relay pass failed", "error", err) },
					OnSuccess: func(transport.RelayReport) { metrics.MarkDurableReady() }}).Run(ctx)
			})
		}
	}
	if store != nil && roles["ingestor"] && len(brokers) > 0 {
		ingestor = startRole(&group, fail, func(ctx context.Context, drain <-chan struct{}) error {
			return runIngestor(ctx, drain, store, brokers, wake)
		})
	}
	if store != nil && len(brokers) > 0 {
		metrics.MarkDurableReady()
	}
	connectCtx, connectCancel := context.WithTimeout(ctx, 5*time.Second)
	controller, faultErr := faults.FromContext(connectCtx)
	connectCancel()
	if faultErr != nil {
		fail()
	} else if controller != nil {
		controllerDone := make(chan struct{})
		group.Go(func() error {
			select {
			case <-ctx.Done():
				_ = controller.Close()
			case <-controllerDone:
			}
			return nil
		})
		if hitErr := controller.Hit("runtime_ready", map[string]any{"role": roleName}); hitErr != nil {
			fail()
		}
		_ = controller.Emit("released", "runtime_ready", map[string]any{"role": roleName})
		_ = controller.Close()
		close(controllerDone)
	}
	slog.Info("runtime started", "addr", listener.Addr(), "roles", *rolesFlag)
	<-ctx.Done()
	draining.Store(true)
	slog.Info("runtime shutdown phase", "phase", "readiness_503")
	// A fixed endpoint-removal delay is inside the total shutdown budget.
	timer := time.NewTimer(2 * time.Second)
	<-timer.C
	for _, phase := range []struct {
		name          string
		role          *lifecycleRole
		grace, forced time.Duration
	}{
		{"scheduler", scheduler, 6 * time.Second, 3 * time.Second},
		{"relay_and_listener", relay, 3 * time.Second, 3 * time.Second},
		{"ingestor", ingestor, 3 * time.Second, 3 * time.Second},
	} {
		slog.Info("runtime shutdown phase", "phase", phase.name)
		if err := phase.role.stop(phase.grace, phase.forced); err != nil {
			return fmt.Errorf("%s: %w", phase.name, err)
		}
	}
	slog.Info("runtime shutdown phase", "phase", "http_drain")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	shutdownErr := shutdownHTTP(server, shutdownCtx)
	cancel()
	if shutdownErr != nil {
		_ = server.Close()
	}
	<-httpDone // Serve returning does not mean Shutdown has drained requests.
	if profile != nil {
		_ = profile.Close()
	}
	if err := group.Wait(); err != nil {
		slog.Warn("runtime role failed", "error", err)
	}
	slog.Info("runtime shutdown phase", "phase", "store_close")
	if store != nil {
		closed := make(chan struct{})
		group.Go(func() error { store.Close(); close(closed); return nil })
		select {
		case <-closed:
		case <-time.After(2 * time.Second):
			return errors.New("store close exceeded deadline")
		}
		_ = group.Wait()
	}
	return shutdownErr
}

// Shutdown, unlike Serve, waits for active handlers to return.
func shutdownHTTP(server *http.Server, ctx context.Context) error {
	return server.Shutdown(ctx)
}
