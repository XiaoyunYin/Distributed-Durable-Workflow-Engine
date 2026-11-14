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
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"durable-agent-execution-engine/internal/api"
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
	if len(os.Args) > 1 && (os.Args[1] == "activity-registry" || os.Args[1] == "compatibility-check") {
		if err := registryCommand(os.Args[1:], os.Stdout); err != nil {
			slog.Error("activity compatibility failed", "error", err)
			os.Exit(1)
		}
		return
	}
	mode := envOrDefault("RUNTIME_SHUTDOWN_MODE", "drain")
	if mode != "drain" && mode != "legacy" {
		slog.Error("invalid RUNTIME_SHUTDOWN_MODE", "mode", mode)
		os.Exit(1)
	}
	stopObservation := observeK4Signals()
	defer stopObservation()
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
	return startRoleWithFailure(group, func(error) { fail() }, run)
}

func startRoleWithFailure(group *errgroup.Group, fail func(error), run func(context.Context, <-chan struct{}) error) *lifecycleRole {
	ctx, cancel := context.WithCancel(context.Background())
	role := &lifecycleRole{drain: make(chan struct{}), cancel: cancel, done: make(chan struct{})}
	group.Go(func() error {
		defer close(role.done)
		defer cancel()
		err := run(ctx, role.drain)
		if err != nil && !errors.Is(err, context.Canceled) {
			fail(err)
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
	checkCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if ping(checkCtx) != nil {
		return false
	}
	return draining == nil || !draining.Load()
}

var newKafkaBroker = transport.NewKafkaBroker
var serveScheduler = engine.ServeWithOptions
var closeRuntimeStore = func(store *state.Store) { store.Close() }

// The final role/controller join may add at most two seconds to the phase budget.
const runtimeGroupWaitBudget = 2 * time.Second

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
		case "api", "operator", "worker-control", "scheduler", "relay", "ingestor", "demand":
			roles[role] = true
		default:
			return nil, fmt.Errorf("unknown runtime role %q", role)
		}
	}
	return roles, nil
}

func serveLifecycle(signalCtx context.Context) error {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	defaultAddr := "127.0.0.1:8080"
	if os.Getenv("RUNTIME_ROLES") == "demand" {
		defaultAddr = "127.0.0.1:8091"
	}
	addr := flags.String("addr", envOrDefault("RUNTIME_ADDR", defaultAddr), "HTTP listen address")
	rolesFlag := flags.String("roles", envOrDefault("RUNTIME_ROLES", "combined"), "combined or comma-separated api,operator,worker-control,scheduler,relay,ingestor,demand")
	if err := flags.Parse(os.Args[1:]); err != nil {
		return err
	}
	roles, err := lifecycleRoles(*rolesFlag)
	if err != nil {
		return err
	}
	routes, err := lifecycleRoutes(*rolesFlag, roles)
	if err != nil {
		return err
	}
	if roles["demand"] && os.Getenv("RUNTIME_ADDR") == "" {
		explicit := false
		flags.Visit(func(f *flag.Flag) {
			if f.Name == "addr" {
				explicit = true
			}
		})
		if !explicit {
			*addr = "127.0.0.1:8091"
		}
	}
	if roles["demand"] && (os.Getenv("DATABASE_URL") == "" || os.Getenv("RUNTIME_NAMESPACE") == "") {
		return errors.New("demand requires DATABASE_URL and RUNTIME_NAMESPACE")
	}
	var reviewer *api.TokenReviewer
	var demandAuth *api.DemandBearerAuthenticator
	if routes == api.RoutesDemand {
		authSetting := os.Getenv("RUNTIME_TOKEN_AUTH")
		if authSetting == "" || authSetting == "disabled" {
			// Local and Compose demand behavior remains auth-disabled by default.
		} else if authSetting == "enabled" {
			demandAuth, err = api.NewDemandBearerAuthenticatorFromEnvironment()
			if err != nil {
				return err
			}
		} else {
			return errors.New("RUNTIME_TOKEN_AUTH must be enabled or disabled")
		}
	} else {
		reviewer, err = api.AuthenticationFromEnvironment()
		if err != nil {
			return err
		}
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
		if roles["demand"] {
			store, err = state.NewDemandFromURL(ctx, url)
		} else {
			store, err = state.NewFromURL(ctx, url)
		}
		cancel()
		if err != nil {
			return err
		}
		store.SetTelemetry(metrics)
	}
	var draining atomic.Bool
	handlerStore := store
	apiServer := api.NewServer(store).WithRoutes(routes).WithAuthentication(reviewer).WithDemandAuthentication(demandAuth).WithTracing(tracing).WithDemand(store, os.Getenv("RUNTIME_NAMESPACE"))
	apiHandler := apiServer.Handler()
	handler := lifecycleHandlerWithAPI(roleName, handlerStore, metrics, tracing, &draining, apiHandler, routes == api.RoutesCombined)
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
	var fatalMu sync.Mutex
	var fatalErr error
	recordFatal := func(err error) {
		if err == nil {
			return
		}
		fatalMu.Lock()
		if fatalErr == nil {
			fatalErr = err
		}
		fatalMu.Unlock()
		fail()
	}
	var group errgroup.Group
	httpDone := make(chan struct{})
	group.Go(func() error {
		defer close(httpDone)
		err := server.Serve(listener)
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		recordFatal(err)
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
			recordFatal(err)
			return err
		})
	}
	brokers := splitBrokers(os.Getenv("KAFKA_BOOTSTRAP_SERVERS"))
	wake := make(chan struct{}, 1)
	var scheduler, relay, ingestor *lifecycleRole
	if store != nil && roles["scheduler"] {
		scheduler = startRoleWithFailure(&group, recordFatal, func(ctx context.Context, drain <-chan struct{}) error {
			options := schedulerOptions(tracing)
			options.Drain = drain
			return serveScheduler(ctx, store, envOrDefault("RUNTIME_NAMESPACE", "local-runtime"), wake, options)
		})
	}
	if store != nil && roles["relay"] && len(brokers) > 0 {
		broker, brokerErr := newKafkaBroker(brokers, transport.DefaultTopic)
		if brokerErr != nil {
			slog.Error("runtime Kafka relay initialization failed", "error", brokerErr)
			recordFatal(brokerErr)
		} else {
			relay = startRoleWithFailure(&group, recordFatal, func(ctx context.Context, drain <-chan struct{}) error {
				defer broker.Close()
				return transport.NewRelay(store, broker, transport.RelayConfig{OwnerID: state.NewID(), Tracing: tracing, Drain: drain,
					OnError:   func(err error) { metrics.RecordRelayFailure(); slog.Warn("relay pass failed", "error", err) },
					OnSuccess: func(transport.RelayReport) { metrics.MarkDurableReady() }}).Run(ctx)
			})
		}
	}
	if store != nil && roles["ingestor"] && len(brokers) > 0 {
		ingestor = startRoleWithFailure(&group, recordFatal, func(ctx context.Context, drain <-chan struct{}) error {
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
		slog.Error("fault controller initialization failed", "error", faultErr)
		recordFatal(faultErr)
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
			slog.Error("runtime fault boundary failed", "error", hitErr)
			recordFatal(hitErr)
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
			recordFatal(fmt.Errorf("%s: %w", phase.name, err))
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
	if err := waitLifecycleGroup(&group, runtimeGroupWaitBudget); err != nil {
		recordFatal(err)
		return lifecycleFatalError(&fatalMu, &fatalErr)
	}
	slog.Info("runtime shutdown phase", "phase", "store_close")
	if store != nil {
		closed := make(chan struct{})
		go func() { closeRuntimeStore(store); close(closed) }()
		select {
		case <-closed:
		case <-time.After(2 * time.Second):
			recordFatal(errors.New("store close exceeded deadline"))
			return lifecycleFatalError(&fatalMu, &fatalErr)
		}
	}
	if err := lifecycleFatalError(&fatalMu, &fatalErr); err != nil {
		return err
	}
	return shutdownErr
}

func waitLifecycleGroup(group *errgroup.Group, budget time.Duration) error {
	done := make(chan error, 1)
	go func() { done <- group.Wait() }()
	timer := time.NewTimer(budget)
	defer timer.Stop()
	select {
	case err := <-done:
		return err
	case <-timer.C:
		return errors.New("runtime goroutines exceeded shutdown deadline")
	}
}

func lifecycleFatalError(mu *sync.Mutex, fatalErr *error) error {
	mu.Lock()
	defer mu.Unlock()
	return *fatalErr
}

// Shutdown, unlike Serve, waits for active handlers to return.
func shutdownHTTP(server *http.Server, ctx context.Context) error {
	return server.Shutdown(ctx)
}

// lifecycleRoutes separates operator authority from the worker listener. Background
// roles expose only health/metrics, including when combined with each other.
func lifecycleRoutes(value string, roles map[string]bool) (api.RouteSet, error) {
	if roles["demand"] && len(roles) != 1 {
		return "", errors.New("demand requires its own process")
	}
	if value == "combined" {
		if os.Getenv("RUNTIME_TOKEN_AUTH") == "enabled" {
			return "", errors.New("token authentication requires a split HTTP role")
		}
		return api.RoutesCombined, nil
	}
	routes := api.RoutesNone
	for role, set := range map[string]api.RouteSet{"api": api.RoutesPublic, "operator": api.RoutesOperator, "worker-control": api.RoutesWorkerControl, "demand": api.RoutesDemand} {
		if roles[role] {
			if routes != api.RoutesNone {
				return "", errors.New("split HTTP roles require separate listeners/processes")
			}
			routes = set
		}
	}
	return routes, nil
}
