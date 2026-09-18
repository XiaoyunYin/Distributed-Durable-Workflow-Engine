// Command dur026-benchmark runs one deterministic DUR-026 throughput case.
// It uses the same Store and Engine implementation as the correctness
// campaign; the PowerShell runner supplies the process CPU measurement and
// archives this JSON with the rest of the study evidence.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"durable-agent-execution-engine/internal/engine"
	"durable-agent-execution-engine/internal/partition"
	"durable-agent-execution-engine/internal/state"
	"durable-agent-execution-engine/internal/telemetry"
)

type config struct {
	Workload       string
	SchedulerCount int
	RatePerSecond  float64
	WorkflowCount  int
	Seed           int64
	RunID          string
	ActivityWork   int
	OutputPath     string
}

type workflowJob struct {
	ID          string
	ScheduledAt time.Time
	CreatedAt   time.Time
}

type workflowOutcome struct {
	WorkflowID  string    `json:"workflow_id"`
	ScheduledAt time.Time `json:"scheduled_at"`
	CreatedAt   time.Time `json:"created_at"`
	FinishedAt  time.Time `json:"finished_at"`
	State       string    `json:"state"`
	Attempts    int       `json:"scheduler_attempts"`
	Error       string    `json:"error,omitempty"`
}

type result struct {
	SchemaVersion string            `json:"schema_version"`
	Status        string            `json:"status"`
	GeneratedAt   time.Time         `json:"generated_at_utc"`
	Config        configReport      `json:"config"`
	Timing        timingReport      `json:"timing"`
	Cohort        cohortReport      `json:"cohort"`
	Telemetry     telemetryReport   `json:"telemetry"`
	Outcomes      []workflowOutcome `json:"outcomes"`
	Failure       string            `json:"failure,omitempty"`
}

type configReport struct {
	Workload       string  `json:"workload"`
	SchedulerCount int     `json:"scheduler_count"`
	RatePerSecond  float64 `json:"arrival_rate_per_second"`
	WorkflowCount  int     `json:"workflow_count"`
	Seed           int64   `json:"seed"`
	ActivityWork   int     `json:"activity_work_units"`
	WorkerSlots    int     `json:"worker_slots"`
	Graph          string  `json:"graph_profile"`
}

type timingReport struct {
	MeasuredStart  time.Time `json:"measured_start_utc"`
	MeasuredEnd    time.Time `json:"measured_end_utc"`
	ElapsedSeconds float64   `json:"elapsed_seconds"`
	Throughput     float64   `json:"terminal_workflows_per_second"`
	MinLatency     float64   `json:"min_completion_latency_seconds"`
	MedianLatency  float64   `json:"median_completion_latency_seconds"`
	MaxLatency     float64   `json:"max_completion_latency_seconds"`
}

type cohortReport struct {
	Scheduled  int `json:"scheduled"`
	Submitted  int `json:"submitted"`
	Accepted   int `json:"accepted"`
	Terminal   int `json:"terminal"`
	Pending    int `json:"pending"`
	Ambiguous  int `json:"ambiguous_submissions"`
	FailedRuns int `json:"failed_runs"`
}

type telemetryReport struct {
	LeaseAcquires   uint64  `json:"lease_acquires"`
	LeaseRenews     uint64  `json:"lease_renews"`
	FencedWrites    uint64  `json:"fenced_writes"`
	AcceptedClaims  uint64  `json:"accepted_claims"`
	AcceptedResults uint64  `json:"accepted_results"`
	DBTransactions  uint64  `json:"db_transactions"`
	DBQueries       uint64  `json:"db_queries"`
	QuerySeconds    float64 `json:"query_seconds"`
	LockWaits       uint64  `json:"lock_waits"`
	LockWaitSeconds float64 `json:"lock_wait_seconds"`
	WorkerCompleted uint64  `json:"worker_completed"`
}

func main() {
	var cfg config
	flag.StringVar(&cfg.Workload, "workload", "t1", "workload profile: t1 or t2")
	flag.IntVar(&cfg.SchedulerCount, "scheduler-count", 1, "number of scheduler workers")
	flag.Float64Var(&cfg.RatePerSecond, "rate", 2, "open-loop workflow arrival rate")
	flag.IntVar(&cfg.WorkflowCount, "workflow-count", 12, "number of workflows in the measured cohort")
	flag.Int64Var(&cfg.Seed, "seed", 1, "deterministic seed")
	flag.StringVar(&cfg.RunID, "run-id", "", "unique run identity")
	flag.IntVar(&cfg.ActivityWork, "activity-work", 5000, "deterministic CPU work units per activity")
	flag.StringVar(&cfg.OutputPath, "output", "", "optional JSON output path")
	flag.Parse()

	if cfg.RunID == "" {
		cfg.RunID = state.NewID()
	}
	if cfg.Workload != "t1" && cfg.Workload != "t2" {
		fatal("workload must be t1 or t2")
	}
	if cfg.SchedulerCount < 1 || cfg.SchedulerCount > 2 {
		fatal("scheduler-count must be 1 or 2")
	}
	if cfg.RatePerSecond <= 0 || cfg.WorkflowCount <= 0 || cfg.ActivityWork <= 0 {
		fatal("rate, workflow-count, and activity-work must be positive")
	}

	result, err := run(cfg)
	if err != nil {
		result.Status = "FAIL"
		result.Failure = err.Error()
	}
	data, marshalErr := json.MarshalIndent(result, "", "  ")
	if marshalErr != nil {
		fatal(marshalErr.Error())
	}
	if cfg.OutputPath != "" {
		if err := os.WriteFile(cfg.OutputPath, append(data, '\n'), 0o644); err != nil {
			fatal(err.Error())
		}
	} else {
		fmt.Println(string(data))
	}
	if err != nil {
		os.Exit(1)
	}
}

func run(cfg config) (result, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	databaseURL, err := databaseURL()
	if err != nil {
		return result{}, err
	}
	store, err := state.NewFromURL(ctx, databaseURL)
	if err != nil {
		return result{}, err
	}
	defer store.Close()

	metrics := telemetry.New("dur026-benchmark")
	store.SetTelemetry(metrics)
	namespace := "dur026-bench-" + cfg.RunID
	definitionID := namespace + "-def"
	graph, effects, versions, err := benchmarkGraph(cfg.Workload)
	if err != nil {
		return result{}, err
	}
	defer cleanup(ctx, store, namespace, definitionID)
	entryNode := "root"
	if cfg.Workload == "t1" {
		entryNode = "activity-0"
	}
	if err := store.CreateDefinition(ctx, state.DefinitionInput{DefinitionID: definitionID,
		Version: 1, DefinitionHash: namespace, Graph: graph,
		ActivityVersions: versions, EffectClasses: effects}); err != nil {
		return result{}, fmt.Errorf("create benchmark definition: %w", err)
	}
	baseline := metrics.Snapshot()
	measuredStart := time.Now().UTC()
	jobs := make(chan workflowJob)
	var schedulerWG sync.WaitGroup
	var outcomesMu sync.Mutex
	outcomes := make([]workflowOutcome, 0, cfg.WorkflowCount)
	var workSink atomic.Uint64
	driver := engine.ActivityDriverFunc(func(ctx context.Context, activity engine.Activity) (engine.ActivityResult, error) {
		busyWork(ctx, cfg.Seed, activity, cfg.ActivityWork, &workSink)
		return engine.ActivityResult{Payload: []byte(`{"ok":true}`)}, nil
	})

	for schedulerIndex := 0; schedulerIndex < cfg.SchedulerCount; schedulerIndex++ {
		schedulerWG.Add(1)
		go func(index int) {
			defer schedulerWG.Done()
			runner := engine.New(store, driver)
			// Lease owner_id is a PostgreSQL UUID. Keep the durable owner random
			// and use the actor/worker fields for the bounded benchmark labels.
			runner.OwnerID = state.NewID()
			runner.WorkerID = fmt.Sprintf("dur026-%s-worker-%d", cfg.RunID, index%4)
			runner.ActorID = fmt.Sprintf("dur026-%s-actor-%d", cfg.RunID, index)
			runner.MaxSteps = 500
			runner.LeaseTTL = time.Minute
			runner.AttemptLease = time.Minute
			for job := range jobs {
				outcome := executeJob(ctx, runner, store, job)
				outcomesMu.Lock()
				outcomes = append(outcomes, outcome)
				outcomesMu.Unlock()
			}
		}(schedulerIndex)
	}

	for index := 0; index < cfg.WorkflowCount; index++ {
		scheduledAt := measuredStart.Add(time.Duration(float64(index) / cfg.RatePerSecond * float64(time.Second)))
		if waitErr := waitUntil(ctx, scheduledAt); waitErr != nil {
			close(jobs)
			schedulerWG.Wait()
			return result{}, waitErr
		}
		workflowID := fmt.Sprintf("%s-%04d", namespace, index)
		partitionID, partitionErr := partition.ID(workflowID)
		if partitionErr != nil {
			close(jobs)
			schedulerWG.Wait()
			return result{}, partitionErr
		}
		createdAt := time.Now().UTC()
		created, createErr := store.CreateWorkflow(ctx, state.CreateWorkflowInput{
			WorkflowID: workflowID, Namespace: namespace, SubmissionKey: "key-" + workflowID,
			SubmissionPayloadHash: "sub-v1:" + workflowID, DefinitionID: definitionID,
			DefinitionVersion: 1, PartitionID: int16(partitionID), InitialNodeID: entryNode,
			InitialInput: json.RawMessage(fmt.Sprintf(`{"seed":%d,"workflow":%q}`, cfg.Seed, workflowID)), ActorID: "dur026-client",
		})
		if createErr != nil {
			close(jobs)
			schedulerWG.Wait()
			return result{}, fmt.Errorf("create workflow %s: %w", workflowID, createErr)
		}
		if !created.Created {
			close(jobs)
			schedulerWG.Wait()
			return result{}, fmt.Errorf("workflow %s was unexpectedly reused", workflowID)
		}
		jobs <- workflowJob{ID: workflowID, ScheduledAt: scheduledAt, CreatedAt: createdAt}
	}
	close(jobs)
	schedulerWG.Wait()
	measuredEnd := time.Now().UTC()

	outcomesMu.Lock()
	stableOutcomes := append([]workflowOutcome(nil), outcomes...)
	outcomesMu.Unlock()
	if len(stableOutcomes) != cfg.WorkflowCount {
		return result{}, fmt.Errorf("scheduler completed %d of %d workflows", len(stableOutcomes), cfg.WorkflowCount)
	}
	cohort, timing, reconcileErr := summarize(ctx, store, stableOutcomes, measuredStart, measuredEnd)
	if reconcileErr != nil {
		return result{}, reconcileErr
	}
	if cohort.Pending != 0 || cohort.FailedRuns != 0 || cohort.Terminal != cohort.Accepted {
		return result{}, fmt.Errorf("cohort did not reconcile: %+v", cohort)
	}
	return result{
		SchemaVersion: "dur026-run.v1", Status: "PASS", GeneratedAt: time.Now().UTC(),
		Config: configReport{Workload: cfg.Workload, SchedulerCount: cfg.SchedulerCount,
			RatePerSecond: cfg.RatePerSecond, WorkflowCount: cfg.WorkflowCount, Seed: cfg.Seed,
			ActivityWork: cfg.ActivityWork, WorkerSlots: 4, Graph: cfg.Workload},
		Timing: timing, Cohort: cohort, Telemetry: deltaTelemetry(metrics.Snapshot(), baseline),
		Outcomes: stableOutcomes,
	}, nil
}

func executeJob(ctx context.Context, runner *engine.Engine, store *state.Store, job workflowJob) workflowOutcome {
	outcome := workflowOutcome{WorkflowID: job.ID, ScheduledAt: job.ScheduledAt, CreatedAt: job.CreatedAt}
	for attempt := 1; attempt <= 1000; attempt++ {
		outcome.Attempts = attempt
		run, err := runner.Run(ctx, job.ID)
		if err != nil && !errors.Is(err, state.ErrLeaseNotOwned) {
			outcome.Error = err.Error()
			break
		}
		if err == nil && isTerminal(run.Workflow.State) {
			outcome.State = string(run.Workflow.State)
			outcome.FinishedAt = time.Now().UTC()
			return outcome
		}
		select {
		case <-ctx.Done():
			outcome.Error = ctx.Err().Error()
			return outcome
		case <-time.After(2 * time.Millisecond):
		}
	}
	if outcome.FinishedAt.IsZero() {
		if workflow, err := store.GetWorkflow(ctx, job.ID); err == nil {
			outcome.State = string(workflow.State)
		}
	}
	return outcome
}

func summarize(ctx context.Context, store *state.Store, outcomes []workflowOutcome, measuredStart, measuredEnd time.Time) (cohortReport, timingReport, error) {
	cohort := cohortReport{Scheduled: len(outcomes), Submitted: len(outcomes), Accepted: len(outcomes)}
	latencies := make([]float64, 0, len(outcomes))
	for index := range outcomes {
		workflow, err := store.GetWorkflow(ctx, outcomes[index].WorkflowID)
		if err != nil {
			return cohort, timingReport{}, fmt.Errorf("reconcile workflow %s: %w", outcomes[index].WorkflowID, err)
		}
		outcomes[index].State = string(workflow.State)
		if isTerminal(workflow.State) {
			cohort.Terminal++
		} else {
			cohort.Pending++
		}
		if outcomes[index].Error != "" {
			cohort.FailedRuns++
		}
		if !outcomes[index].FinishedAt.IsZero() {
			latencies = append(latencies, outcomes[index].FinishedAt.Sub(outcomes[index].ScheduledAt).Seconds())
		}
	}
	if len(latencies) == 0 {
		states := make([]string, 0, len(outcomes))
		for _, item := range outcomes {
			states = append(states, item.WorkflowID+"="+item.State+"/"+item.Error)
		}
		return cohort, timingReport{}, fmt.Errorf("no terminal completion latencies recorded: %s", strings.Join(states, ","))
	}
	for i := 1; i < len(latencies); i++ {
		for j := i; j > 0 && latencies[j] < latencies[j-1]; j-- {
			latencies[j], latencies[j-1] = latencies[j-1], latencies[j]
		}
	}
	elapsed := measuredEnd.Sub(measuredStart).Seconds()
	if elapsed <= 0 {
		return cohort, timingReport{}, errors.New("measured interval was not positive")
	}
	return cohort, timingReport{MeasuredStart: measuredStart, MeasuredEnd: measuredEnd,
		ElapsedSeconds: elapsed, Throughput: float64(cohort.Terminal) / elapsed,
		MinLatency: latencies[0], MedianLatency: latencies[len(latencies)/2], MaxLatency: latencies[len(latencies)-1]}, nil
}

func benchmarkGraph(workload string) (json.RawMessage, json.RawMessage, json.RawMessage, error) {
	if workload == "t1" {
		nodes := make([]map[string]any, 0, 10)
		effects := map[string]string{}
		versions := map[string]string{}
		for index := 0; index < 8; index++ {
			id := fmt.Sprintf("activity-%d", index)
			next := any("done")
			if index < 7 {
				next = fmt.Sprintf("activity-%d", index+1)
			}
			nodes = append(nodes, map[string]any{"id": id, "kind": "activity", "next": next})
			effects[id] = string(state.EffectPure)
			versions[id] = "1"
		}
		nodes = append(nodes, map[string]any{"id": "done", "kind": "success"})
		graph, err := json.Marshal(map[string]any{"entry": "activity-0", "nodes": nodes})
		if err != nil {
			return nil, nil, nil, err
		}
		effectJSON, _ := json.Marshal(effects)
		versionJSON, _ := json.Marshal(versions)
		return graph, effectJSON, versionJSON, nil
	}
	branches := make([]string, 8)
	nodes := []map[string]any{{"id": "root", "kind": "fanout", "branches": branches, "join": "join"}}
	effects := map[string]string{}
	versions := map[string]string{}
	for index := range branches {
		id := fmt.Sprintf("branch-%d", index)
		branches[index] = id
		nodes = append(nodes, map[string]any{"id": id, "kind": "activity", "next": "join"})
		effects[id] = string(state.EffectPure)
		versions[id] = "1"
	}
	nodes[0]["branches"] = branches
	nodes = append(nodes, map[string]any{"id": "join", "kind": "join", "next": "done"})
	nodes = append(nodes, map[string]any{"id": "done", "kind": "success"})
	graph, err := json.Marshal(map[string]any{"entry": "root", "nodes": nodes})
	if err != nil {
		return nil, nil, nil, err
	}
	effectJSON, _ := json.Marshal(effects)
	versionJSON, _ := json.Marshal(versions)
	return graph, effectJSON, versionJSON, nil
}

func busyWork(ctx context.Context, seed int64, activity engine.Activity, units int, sink *atomic.Uint64) {
	var value = uint64(seed) ^ uint64(len(activity.NodeID))
	for index := 0; index < units; index++ {
		value ^= value << 13
		value ^= value >> 7
		value ^= value << 17
		if index%4096 == 0 {
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}
	sink.Add(value)
}

func deltaTelemetry(after, before telemetry.Snapshot) telemetryReport {
	return telemetryReport{LeaseAcquires: after.LeaseAcquires - before.LeaseAcquires,
		LeaseRenews: after.LeaseRenews - before.LeaseRenews, FencedWrites: after.FencedWrites - before.FencedWrites,
		AcceptedClaims: after.AcceptedClaims - before.AcceptedClaims, AcceptedResults: after.AcceptedResults - before.AcceptedResults,
		DBTransactions: after.DBTransactions - before.DBTransactions, DBQueries: after.DBQueries - before.DBQueries,
		QuerySeconds: after.QuerySeconds - before.QuerySeconds, LockWaits: after.LockWaitCount - before.LockWaitCount,
		LockWaitSeconds: after.LockWaitSeconds - before.LockWaitSeconds, WorkerCompleted: after.WorkerCompleted - before.WorkerCompleted}
}

func cleanup(ctx context.Context, store *state.Store, namespace, definitionID string) {
	_, _ = store.Pool().Exec(ctx, `UPDATE engine.partition_leases SET owner_id = NULL, lease_expires_at = NULL, updated_at = clock_timestamp() WHERE owner_id::text LIKE 'dur026-%'`)
	_, _ = store.Pool().Exec(ctx, `DELETE FROM engine.workflow_executions WHERE namespace = $1`, namespace)
	_, _ = store.Pool().Exec(ctx, `DELETE FROM engine.workflow_definitions WHERE definition_id = $1`, definitionID)
}

func isTerminal(value state.WorkflowState) bool {
	return value == state.StateSucceeded || value == state.StateFailed || value == state.StateRejected ||
		value == state.StateCanceled || value == state.StateAbandoned
}

func waitUntil(ctx context.Context, target time.Time) error {
	delay := time.Until(target)
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func databaseURL() (string, error) {
	for _, name := range []string{"DURABLE_DATABASE_URL", "DATABASE_URL"} {
		if value := os.Getenv(name); value != "" {
			return value, nil
		}
	}
	workingDirectory, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for directory := workingDirectory; ; directory = filepath.Dir(directory) {
		data, readErr := os.ReadFile(filepath.Join(directory, ".env"))
		if readErr == nil {
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
			if _, err := strconv.Atoi(port); err != nil {
				return "", fmt.Errorf("invalid POSTGRES_PORT: %w", err)
			}
			return (&url.URL{Scheme: "postgresql", User: url.UserPassword(values["POSTGRES_USER"], values["POSTGRES_PASSWORD"]),
				Host: "127.0.0.1:" + port, Path: "/" + values["POSTGRES_DB"], RawQuery: "sslmode=disable"}).String(), nil
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			break
		}
	}
	return "", errors.New("set DURABLE_DATABASE_URL or provide .env")
}

func fatal(message string) {
	fmt.Fprintln(os.Stderr, message)
	os.Exit(2)
}
