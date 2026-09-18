//go:build dur034_ablation

// Command dur034-ablation measures the four DUR-034 test profiles. The
// weakened profiles are explicit context-scoped test seams and are never
// enabled by the runtime binaries.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
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
	"github.com/jackc/pgx/v5"
)

type runReport struct {
	Profile          string  `json:"profile"`
	Repeat           int     `json:"repeat"`
	WorkflowCount    int     `json:"workflow_count"`
	Terminal         int     `json:"terminal"`
	Pending          int     `json:"pending"`
	ElapsedSeconds   float64 `json:"elapsed_seconds"`
	MedianLatency    float64 `json:"median_latency_seconds"`
	HistoryRows      int     `json:"history_rows"`
	OutboxRows       int     `json:"outbox_rows"`
	OutboxPending    int     `json:"outbox_pending_rows"`
	ManualRecoveryMS float64 `json:"manual_recovery_delay_ms,omitempty"`
	DBQueries        uint64  `json:"db_queries"`
	DBTransactions   uint64  `json:"db_transactions"`
	QuerySeconds     float64 `json:"query_seconds"`
	LockWaits        uint64  `json:"lock_waits"`
	SchedulerCPU     float64 `json:"scheduler_cpu_seconds"`
	WorkerCPU        float64 `json:"worker_cpu_seconds"`
	Throughput       float64 `json:"terminal_workflows_per_second"`
	SLOViolations    int     `json:"completion_slo_violations"`
	Status           string  `json:"status"`
	Failure          string  `json:"failure,omitempty"`
}

type leaseControlReport struct {
	Profile                        string `json:"profile"`
	CheckObserved                  bool   `json:"check_observed"`
	TakeoverCommitted              bool   `json:"takeover_committed"`
	OldOwnerCommitted              bool   `json:"old_owner_committed"`
	OldOwnerMutationAfterTakeover  bool   `json:"old_owner_mutation_after_takeover"`
	ExpectedNegativeControlFailure bool   `json:"expected_negative_control_failure"`
	ObservedCommitOrder            string `json:"observed_commit_order,omitempty"`
	OldOwnerEpoch                  int64  `json:"old_owner_epoch,omitempty"`
	TakeoverEpoch                  int64  `json:"takeover_epoch,omitempty"`
	OldOwnerCommittedAt            string `json:"old_owner_committed_at,omitempty"`
	TakeoverCommittedAt            string `json:"takeover_committed_at,omitempty"`
	OldOwnerError                  string `json:"old_owner_error,omitempty"`
	TakeoverError                  string `json:"takeover_error,omitempty"`
}

type controlReport struct {
	HistoryEvidenceLoss bool               `json:"history_evidence_loss"`
	UnsafeLease         leaseControlReport `json:"unsafe_lease"`
	SafeLease           leaseControlReport `json:"safe_lease"`
	NoOutboxRecovery    bool               `json:"no_outbox_recovery_delay_observed"`
}

type artifact struct {
	SchemaVersion       string         `json:"schema_version"`
	Status              string         `json:"status"`
	GitCommit           string         `json:"git_commit"`
	GeneratedAt         time.Time      `json:"generated_at_utc"`
	Protocol            map[string]any `json:"protocol"`
	Runs                []runReport    `json:"runs"`
	Controls            controlReport  `json:"negative_controls"`
	Validation          map[string]any `json:"validation"`
	Limitations         []string       `json:"limitations"`
	Failure             string         `json:"failure,omitempty"`
	Summary             map[string]any `json:"summary,omitempty"`
	CostEffectsResolved bool           `json:"cost_effects_resolved"`
	CostInterpretation  string         `json:"cost_interpretation"`
	ResolvedCostEffects []string       `json:"resolved_cost_effects"`
}

type config struct {
	ActivityWork int
	OutputPath   string
	WorkerBinary string
	WorkerSlots  int
	SLOSeconds   int
}

const (
	warmupWorkflows   = 4
	measuredWorkflows = 24
)

type workerRequest struct {
	Seed      int64  `json:"seed"`
	NodeID    string `json:"node_id"`
	WorkUnits int    `json:"work_units"`
}

type workerResponse struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

type workerProcess struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	encode *json.Encoder
	decode *json.Decoder
	mu     sync.Mutex
}

type workerPool struct {
	workers []*workerProcess
	next    atomic.Uint64
}

func newWorkerPool(binary string, slots int) (*workerPool, error) {
	pool := &workerPool{workers: make([]*workerProcess, 0, slots)}
	for index := 0; index < slots; index++ {
		command := exec.Command(binary)
		stdin, err := command.StdinPipe()
		if err != nil {
			_, _ = pool.Close()
			return nil, fmt.Errorf("worker %d stdin: %w", index, err)
		}
		stdout, err := command.StdoutPipe()
		if err != nil {
			_ = stdin.Close()
			_, _ = pool.Close()
			return nil, fmt.Errorf("worker %d stdout: %w", index, err)
		}
		command.Stderr = os.Stderr
		if err := command.Start(); err != nil {
			_ = stdin.Close()
			_, _ = pool.Close()
			return nil, fmt.Errorf("start worker %d: %w", index, err)
		}
		pool.workers = append(pool.workers, &workerProcess{cmd: command, stdin: stdin,
			encode: json.NewEncoder(stdin), decode: json.NewDecoder(stdout)})
	}
	return pool, nil
}

func (pool *workerPool) Run(ctx context.Context, seed int64, activity engine.Activity, units int) error {
	if len(pool.workers) == 0 {
		return errors.New("worker pool has no workers")
	}
	worker := pool.workers[(pool.next.Add(1)-1)%uint64(len(pool.workers))]
	worker.mu.Lock()
	defer worker.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if err := worker.encode.Encode(workerRequest{Seed: seed, NodeID: activity.NodeID, WorkUnits: units}); err != nil {
		return fmt.Errorf("send activity to worker: %w", err)
	}
	var response workerResponse
	if err := worker.decode.Decode(&response); err != nil {
		return fmt.Errorf("read worker response: %w", err)
	}
	if !response.OK {
		if response.Error == "" {
			response.Error = "worker rejected activity"
		}
		return errors.New(response.Error)
	}
	return nil
}

func (pool *workerPool) Close() (float64, error) {
	var closeErr error
	for _, worker := range pool.workers {
		if err := worker.stdin.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
	}
	var cpu time.Duration
	for _, worker := range pool.workers {
		if err := worker.cmd.Wait(); err != nil && closeErr == nil {
			closeErr = err
		}
		if worker.cmd.ProcessState != nil {
			cpu += worker.cmd.ProcessState.UserTime() + worker.cmd.ProcessState.SystemTime()
		}
	}
	return cpu.Seconds(), closeErr
}

func main() {
	var cfg config
	flag.IntVar(&cfg.ActivityWork, "activity-work", 5000, "deterministic CPU work units per activity")
	flag.StringVar(&cfg.OutputPath, "output", "experiments/m7/dur034/results.json", "artifact output path")
	flag.StringVar(&cfg.WorkerBinary, "worker-binary", "", "fixed-capacity worker executable")
	flag.IntVar(&cfg.WorkerSlots, "worker-slots", 4, "fixed number of worker processes")
	flag.IntVar(&cfg.SLOSeconds, "completion-slo-seconds", 120, "completion SLO in seconds")
	flag.Parse()
	if cfg.ActivityWork <= 0 || cfg.WorkerSlots <= 0 || cfg.SLOSeconds <= 0 {
		fatal("activity-work, worker-slots, and completion-slo-seconds must be positive")
	}
	if cfg.WorkerBinary == "" {
		fatal("worker-binary is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	result, err := run(ctx, cfg)
	if err != nil {
		result.Status = "FAIL"
		result.Failure = err.Error()
	}
	data, marshalErr := json.MarshalIndent(result, "", "  ")
	if marshalErr != nil {
		fatal(marshalErr.Error())
	}
	if parent := filepath.Dir(cfg.OutputPath); parent != "." {
		if err := os.MkdirAll(parent, 0o755); err != nil {
			fatal(err.Error())
		}
	}
	if err := os.WriteFile(cfg.OutputPath, append(data, '\n'), 0o644); err != nil {
		fatal(err.Error())
	}
	if err != nil {
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg config) (artifact, error) {
	database, err := databaseURL()
	if err != nil {
		return artifact{}, err
	}
	store, err := state.NewFromURL(ctx, database)
	if err != nil {
		return artifact{}, err
	}
	defer store.Close()
	commit, err := currentCommit()
	if err != nil {
		return artifact{}, err
	}
	metrics := telemetry.New("dur034-ablation")
	store.SetTelemetry(metrics)
	profiles := []state.TestSafeguardProfile{
		{Name: "full"},
		{Name: "history_disabled", DisableHistory: true},
		{Name: "unsafe_lease_check", UnsafeLeaseValidation: true},
		{Name: "no_outbox", DisableOutbox: true},
	}
	result := artifact{
		SchemaVersion: "dur034-ablation.v1",
		Status:        "PASS",
		GitCommit:     commit,
		GeneratedAt:   time.Now().UTC(),
		Protocol: map[string]any{
			"profiles":               []string{"full", "history_disabled", "unsafe_lease_check", "no_outbox"},
			"workload":               "T1: eight sequential pure activities",
			"warmup_workflows":       warmupWorkflows,
			"measured_workflows":     measuredWorkflows,
			"repeats_per_profile":    3,
			"scheduler_count":        1,
			"worker_slots":           cfg.WorkerSlots,
			"completion_slo_seconds": cfg.SLOSeconds,
			"activity_work_units":    cfg.ActivityWork,
			"near_saturation_rate":   "inherited from DUR-026 calibration: 2 workflows/second",
			"negative_controls":      "history evidence loss, unsafe check-to-commit takeover, and no-outbox delayed recovery",
			"cost_claim_policy":      "No performance delta is promoted from this fixed three-repeat campaign; cross-campaign stability evidence is required.",
		},
	}
	for _, profile := range profiles {
		for repeat := 1; repeat <= 3; repeat++ {
			report, runErr := runProfile(ctx, store, metrics, profile, repeat, cfg)
			result.Runs = append(result.Runs, report)
			if runErr != nil {
				return result, runErr
			}
		}
	}
	fullControl, err := runLeaseControl(ctx, store, false)
	if err != nil {
		return result, err
	}
	unsafeControl, err := runLeaseControl(ctx, store, true)
	if err != nil {
		return result, err
	}
	result.Controls = controlReport{
		HistoryEvidenceLoss: result.Runs[3].HistoryRows == 0,
		SafeLease:           fullControl,
		UnsafeLease:         unsafeControl,
		NoOutboxRecovery:    anyManualRecovery(result.Runs),
	}
	result.Summary = summarizeRuns(result.Runs)
	result.ResolvedCostEffects = []string{}
	result.CostEffectsResolved = false
	result.CostInterpretation = "No safeguard-cost delta is resolved or promoted by this fixed three-repeat campaign. Mechanism controls are measured separately; performance differences remain descriptive until a separately qualified stability campaign supports a comparison."
	result.Validation = map[string]any{
		"expected_runs":                   12,
		"measured_runs":                   len(result.Runs),
		"all_runs_reconciled":             allRunsReconciled(result.Runs),
		"history_disabled_has_no_history": result.Controls.HistoryEvidenceLoss,
		"unsafe_negative_control_fails":   result.Controls.UnsafeLease.ExpectedNegativeControlFailure,
		"safe_lease_preserves_order":      !result.Controls.SafeLease.OldOwnerMutationAfterTakeover,
		"no_outbox_delay_measured":        result.Controls.NoOutboxRecovery,
		"all_slo_values_within_limit":     allSLOValuesWithinLimit(result.Runs),
		"cost_effects_resolved":           result.CostEffectsResolved,
	}
	if len(result.Runs) != 12 || !allRunsReconciled(result.Runs) || !result.Controls.HistoryEvidenceLoss ||
		!result.Controls.UnsafeLease.ExpectedNegativeControlFailure || result.Controls.SafeLease.OldOwnerMutationAfterTakeover ||
		!result.Controls.NoOutboxRecovery || !allSLOValuesWithinLimit(result.Runs) {
		return result, errors.New("DUR-034 validation did not prove every profile and negative control")
	}
	result.Limitations = []string{
		"Profiles are test-only context seams; none is a deployable runtime mode.",
		"The study uses the committed Store/Engine path on the DUR-036 single-node WSL2 host with four fixed worker subprocesses, not Kafka/API/relay dispatch.",
		"The no-outbox arm measures a bounded test-profile reconciliation scan before Engine dispatch; it does not claim production reconciliation throughput.",
		"The unsafe lease arm is a deliberate negative control and its result is not a supported safety or performance alternative.",
		"Scheduler CPU is the ablation command process CPU excluding the four worker subprocesses; the study is descriptive and does not claim a production scheduler CPU model.",
	}
	return result, nil
}

func runProfile(ctx context.Context, store *state.Store, metrics *telemetry.Metrics, profile state.TestSafeguardProfile, repeat int, cfg config) (runReport, error) {
	namespace := fmt.Sprintf("dur034-ablation-%s-%d-%s", profile.Name, repeat, state.NewID())
	definitionID := namespace + "-def"
	profileCtx := state.WithTestSafeguardProfile(ctx, profile)
	defer cleanup(ctx, store, namespace, definitionID)
	graph, effects, versions := benchmarkGraph()
	if err := store.CreateDefinition(profileCtx, state.DefinitionInput{DefinitionID: definitionID, Version: 1,
		DefinitionHash: namespace, Graph: graph, ActivityVersions: versions, EffectClasses: effects}); err != nil {
		return failedRun(profile, repeat, err), err
	}
	warmupIDs := make([]string, 0, warmupWorkflows)
	workflowIDs := make([]string, 0, measuredWorkflows)
	for index := 0; index < warmupWorkflows+measuredWorkflows; index++ {
		workflowID := fmt.Sprintf("%s-%04d", namespace, index)
		partitionID, err := partition.ID(workflowID)
		if err != nil {
			return failedRun(profile, repeat, err), err
		}
		created, err := store.CreateWorkflow(profileCtx, state.CreateWorkflowInput{
			WorkflowID: workflowID, Namespace: namespace, SubmissionKey: "key-" + workflowID,
			SubmissionPayloadHash: "sub-v1:" + workflowID, DefinitionID: definitionID, DefinitionVersion: 1,
			PartitionID: int16(partitionID), InitialNodeID: "activity-0",
			InitialInput: json.RawMessage(fmt.Sprintf(`{"workflow":%q}`, workflowID)), ActorID: "dur034-client",
		})
		if err != nil {
			return failedRun(profile, repeat, err), err
		}
		if !created.Created {
			return failedRun(profile, repeat, errors.New("workflow was unexpectedly reused")), errors.New("workflow was unexpectedly reused")
		}
		if index < warmupWorkflows {
			warmupIDs = append(warmupIDs, workflowID)
		} else {
			workflowIDs = append(workflowIDs, workflowID)
		}
	}
	workers, err := newWorkerPool(cfg.WorkerBinary, cfg.WorkerSlots)
	if err != nil {
		return failedRun(profile, repeat, err), err
	}
	workersClosed := false
	defer func() {
		if !workersClosed {
			_, _ = workers.Close()
		}
	}()
	driver := engine.ActivityDriverFunc(func(ctx context.Context, activity engine.Activity) (engine.ActivityResult, error) {
		if err := workers.Run(ctx, int64(activity.AttemptNumber), activity, cfg.ActivityWork); err != nil {
			return engine.ActivityResult{}, err
		}
		return engine.ActivityResult{Payload: json.RawMessage(`{"ok":true}`)}, nil
	})
	finishWorkflow := func(workflowID string) (float64, error) {
		workflowStart := time.Now()
		runner := engine.New(store, driver)
		runner.OwnerID = state.NewID()
		runner.WorkerID = "dur034-worker"
		runner.ActorID = "dur034-scheduler"
		runner.MaxSteps = 500
		runner.LeaseTTL = time.Minute
		runner.AttemptLease = time.Minute
		for attempt := 0; attempt < 1000; attempt++ {
			runResult, err := runner.Run(profileCtx, workflowID)
			if err != nil {
				return 0, err
			}
			if isTerminal(runResult.Workflow.State) {
				return time.Since(workflowStart).Seconds(), nil
			}
			if !runResult.Blocked {
				continue
			}
			time.Sleep(2 * time.Millisecond)
		}
		return 0, fmt.Errorf("workflow %s did not reach terminal state", workflowID)
	}
	dispatchIDs := append(append([]string(nil), warmupIDs...), workflowIDs...)
	latencies := make([]float64, 0, len(workflowIDs))
	terminal := 0
	manualRecovery := 0.0
	if profile.DisableOutbox {
		// With no outbox there is no commit-time dispatch hint. The only
		// remaining path is a bounded reconciliation scan over durable state.
		recoveryStart := time.Now()
		time.Sleep(25 * time.Millisecond)
		var discoverErr error
		dispatchIDs, discoverErr = discoverRunnableWorkflows(profileCtx, store, namespace)
		if discoverErr != nil {
			return failedRun(profile, repeat, discoverErr), discoverErr
		}
		if len(dispatchIDs) != len(warmupIDs)+len(workflowIDs) {
			return failedRun(profile, repeat, fmt.Errorf("reconciliation discovered %d of %d workflows", len(dispatchIDs), len(warmupIDs)+len(workflowIDs))), fmt.Errorf("reconciliation discovered %d of %d workflows", len(dispatchIDs), len(warmupIDs)+len(workflowIDs))
		}
		manualRecovery = time.Since(recoveryStart).Seconds() * 1000
	}
	for _, workflowID := range dispatchIDs[:len(warmupIDs)] {
		if _, err := finishWorkflow(workflowID); err != nil {
			return failedRun(profile, repeat, err), err
		}
	}
	if _, err := workers.Close(); err != nil {
		return failedRun(profile, repeat, fmt.Errorf("close warmup worker pool: %w", err)), err
	}
	workersClosed = true
	workers, err = newWorkerPool(cfg.WorkerBinary, cfg.WorkerSlots)
	if err != nil {
		return failedRun(profile, repeat, err), err
	}
	workersClosed = false
	baseline := metrics.Snapshot()
	cpuStart, err := processCPUSeconds()
	if err != nil {
		return failedRun(profile, repeat, err), err
	}
	measurementStart := time.Now()
	for _, workflowID := range dispatchIDs[len(warmupIDs):] {
		latency, err := finishWorkflow(workflowID)
		if err != nil {
			return failedRun(profile, repeat, err), err
		}
		terminal++
		latencies = append(latencies, latency)
	}
	measurementElapsed := time.Since(measurementStart)
	cpuEnd, err := processCPUSeconds()
	if err != nil {
		return failedRun(profile, repeat, err), err
	}
	workerCPU, workerCloseErr := workers.Close()
	workersClosed = true
	if workerCloseErr != nil {
		return failedRun(profile, repeat, workerCloseErr), workerCloseErr
	}
	historyRows, outboxRows, pendingOutbox, err := countEvidence(ctx, store, namespace)
	if err != nil {
		return failedRun(profile, repeat, err), err
	}
	latency := median(latencies)
	delta := metrics.Snapshot()
	sloViolations := 0
	for _, value := range latencies {
		if value > float64(cfg.SLOSeconds) {
			sloViolations++
		}
	}
	return runReport{Profile: profile.Name, Repeat: repeat, WorkflowCount: len(workflowIDs), Terminal: terminal,
		Pending: len(workflowIDs) - terminal, ElapsedSeconds: measurementElapsed.Seconds(), MedianLatency: latency,
		HistoryRows: historyRows, OutboxRows: outboxRows, OutboxPending: pendingOutbox,
		ManualRecoveryMS: manualRecovery, DBQueries: delta.DBQueries - baseline.DBQueries,
		DBTransactions: delta.DBTransactions - baseline.DBTransactions, QuerySeconds: delta.QuerySeconds - baseline.QuerySeconds,
		LockWaits: delta.LockWaitCount - baseline.LockWaitCount, SchedulerCPU: cpuEnd - cpuStart,
		WorkerCPU: workerCPU, Throughput: float64(terminal) / measurementElapsed.Seconds(), SLOViolations: sloViolations, Status: "PASS"}, nil
}

func runLeaseControl(ctx context.Context, store *state.Store, unsafe bool) (leaseControlReport, error) {
	profileName := "full"
	if unsafe {
		profileName = "unsafe_lease_check"
	}
	namespace := fmt.Sprintf("dur034-lease-control-%s-%s", profileName, state.NewID())
	definitionID := namespace + "-def"
	defer cleanup(ctx, store, namespace, definitionID)
	graph, effects, versions := benchmarkGraph()
	if err := store.CreateDefinition(ctx, state.DefinitionInput{DefinitionID: definitionID, Version: 1,
		DefinitionHash: namespace, Graph: graph, ActivityVersions: versions, EffectClasses: effects}); err != nil {
		return leaseControlReport{Profile: profileName}, err
	}
	workflowID := namespace + "-workflow"
	partitionID, err := partition.ID(workflowID)
	if err != nil {
		return leaseControlReport{Profile: profileName}, err
	}
	if _, err := store.CreateWorkflow(ctx, state.CreateWorkflowInput{WorkflowID: workflowID, Namespace: namespace,
		SubmissionKey: "key-" + workflowID, SubmissionPayloadHash: "sub-v1:" + workflowID, DefinitionID: definitionID,
		DefinitionVersion: 1, PartitionID: int16(partitionID), InitialNodeID: "activity-0", ActorID: "dur034-client"}); err != nil {
		return leaseControlReport{Profile: profileName}, err
	}
	ownerA, ownerB := state.NewID(), state.NewID()
	lease, acquired, err := store.AcquireLease(ctx, int16(partitionID), ownerA, 70*time.Millisecond)
	if err != nil || !acquired {
		return leaseControlReport{Profile: profileName}, fmt.Errorf("acquire control lease: acquired=%v err=%v", acquired, err)
	}
	checked := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseBarrier := func() { releaseOnce.Do(func() { close(release) }) }
	var once atomic.Bool
	profile := state.TestSafeguardProfile{Name: profileName, UnsafeLeaseValidation: unsafe,
		CheckToCommit: func(waitCtx context.Context) error {
			if once.CompareAndSwap(false, true) {
				close(checked)
			}
			select {
			case <-release:
				return nil
			case <-waitCtx.Done():
				return waitCtx.Err()
			}
		}}
	transitionInput := state.OwnerTransitionInput{Lease: state.LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch},
		WorkflowID: workflowID, ExpectedRevision: 1, NewState: state.StateWaitingActivity, ActorID: "dur034-old-owner",
		Reason: "DUR034_CHECK_TO_COMMIT", NodeID: "activity-0", Iteration: 0, NodeState: ptrState(state.StateWaitingActivity)}
	done := make(chan error, 1)
	go func() {
		done <- store.ApplyOwnerTransition(state.WithTestSafeguardProfile(ctx, profile), transitionInput)
	}()
	select {
	case <-checked:
	case <-time.After(5 * time.Second):
		releaseBarrier()
		return leaseControlReport{Profile: profileName}, errors.New("lease check-to-commit barrier was not reached")
	}
	time.Sleep(90 * time.Millisecond)
	takeover, tookOver, takeoverErr := store.AcquireLease(ctx, int16(partitionID), ownerB, time.Minute)
	releaseBarrier()
	var takeoverAt time.Time
	var observedEpoch int64
	var observedOwner string
	if takeoverErr == nil && tookOver {
		if err := store.Pool().QueryRow(ctx, `
			SELECT owner_id::text, epoch, updated_at
			FROM engine.partition_leases
			WHERE partition_id = $1`, partitionID).Scan(&observedOwner, &observedEpoch, &takeoverAt); err != nil {
			return leaseControlReport{Profile: profileName}, fmt.Errorf("read takeover commit: %w", err)
		}
	}
	oldErr := <-done
	oldCommitAt, oldEpoch, historyErr := readOwnerTransition(ctx, store, workflowID)
	if historyErr != nil && !errors.Is(historyErr, state.ErrLeaseNotOwned) {
		return leaseControlReport{Profile: profileName}, historyErr
	}
	if takeoverErr == nil && tookOver {
		_ = store.ReleaseLease(ctx, state.LeaseRef{PartitionID: takeover.PartitionID, OwnerID: takeover.OwnerID, Epoch: takeover.Epoch})
	}
	oldCommitted := oldCommitAt != nil
	oldAfterTakeover := oldCommitted && takeoverAt.After(time.Time{}) && oldCommitAt.After(takeoverAt)
	order := "old_rejected_after_takeover"
	if oldCommitted && takeoverAt.After(time.Time{}) {
		if oldCommitAt.After(takeoverAt) {
			order = "old_mutation_after_takeover"
		} else {
			order = "old_mutation_before_takeover"
		}
	}
	return leaseControlReport{Profile: profileName, CheckObserved: true, TakeoverCommitted: tookOver && takeoverErr == nil && observedOwner == ownerB && observedEpoch == takeover.Epoch,
		OldOwnerCommitted: oldCommitted, OldOwnerMutationAfterTakeover: oldAfterTakeover,
		ExpectedNegativeControlFailure: unsafe && oldAfterTakeover,
		ObservedCommitOrder:            order, OldOwnerEpoch: oldEpoch, TakeoverEpoch: observedEpoch,
		OldOwnerCommittedAt: formatTime(oldCommitAt), TakeoverCommittedAt: formatTime(&takeoverAt),
		OldOwnerError: errorText(oldErr), TakeoverError: errorText(takeoverErr)}, nil
}

func readOwnerTransition(ctx context.Context, store *state.Store, workflowID string) (*time.Time, int64, error) {
	var committedAt time.Time
	var epoch int64
	err := store.Pool().QueryRow(ctx, `
		SELECT created_at, COALESCE(scheduler_epoch, -1)
		FROM engine.transition_history
		WHERE workflow_id = $1 AND actor_id = 'dur034-old-owner' AND reason = 'DUR034_CHECK_TO_COMMIT'
		ORDER BY created_at DESC
		LIMIT 1`, workflowID).Scan(&committedAt, &epoch)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, fmt.Errorf("read old-owner transition: %w", err)
	}
	return &committedAt, epoch, nil
}

func formatTime(value *time.Time) string {
	if value == nil || value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func discoverRunnableWorkflows(ctx context.Context, store *state.Store, namespace string) ([]string, error) {
	rows, err := store.Pool().Query(ctx, `
		SELECT workflow_id::text
		FROM engine.workflow_executions
		WHERE namespace = $1 AND state NOT IN ('SUCCEEDED', 'FAILED', 'REJECTED', 'CANCELED', 'ABANDONED')
		ORDER BY created_at, workflow_id`, namespace)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var workflowIDs []string
	for rows.Next() {
		var workflowID string
		if err := rows.Scan(&workflowID); err != nil {
			return nil, err
		}
		workflowIDs = append(workflowIDs, workflowID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return workflowIDs, nil
}

func countEvidence(ctx context.Context, store *state.Store, namespace string) (int, int, int, error) {
	var history, outbox, pending int
	if err := store.Pool().QueryRow(ctx, `SELECT count(*) FROM engine.transition_history h JOIN engine.workflow_executions w ON w.workflow_id = h.workflow_id WHERE w.namespace = $1`, namespace).Scan(&history); err != nil {
		return 0, 0, 0, err
	}
	if err := store.Pool().QueryRow(ctx, `SELECT count(*) FROM engine.outbox o JOIN engine.workflow_executions w ON w.workflow_id = o.workflow_id WHERE w.namespace = $1`, namespace).Scan(&outbox); err != nil {
		return 0, 0, 0, err
	}
	if err := store.Pool().QueryRow(ctx, `SELECT count(*) FROM engine.outbox o JOIN engine.workflow_executions w ON w.workflow_id = o.workflow_id WHERE w.namespace = $1 AND o.publish_state IN ('PENDING','CLAIMED')`, namespace).Scan(&pending); err != nil {
		return 0, 0, 0, err
	}
	return history, outbox, pending, nil
}

func cleanup(ctx context.Context, store *state.Store, namespace, definitionID string) {
	_, _ = store.Pool().Exec(ctx, `UPDATE engine.partition_leases SET owner_id = NULL, lease_expires_at = NULL, updated_at = clock_timestamp() WHERE owner_id::text LIKE 'dur034-%'`)
	_, _ = store.Pool().Exec(ctx, `DELETE FROM engine.workflow_executions WHERE namespace = $1`, namespace)
	_, _ = store.Pool().Exec(ctx, `DELETE FROM engine.workflow_definitions WHERE definition_id = $1`, definitionID)
}

func benchmarkGraph() (json.RawMessage, json.RawMessage, json.RawMessage) {
	nodes := make([]map[string]any, 0, 9)
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
	graph, _ := json.Marshal(map[string]any{"entry": "activity-0", "nodes": nodes})
	effectJSON, _ := json.Marshal(effects)
	versionJSON, _ := json.Marshal(versions)
	return graph, effectJSON, versionJSON
}

func isTerminal(stateValue state.WorkflowState) bool {
	return stateValue == state.StateSucceeded || stateValue == state.StateFailed || stateValue == state.StateRejected || stateValue == state.StateCanceled || stateValue == state.StateAbandoned
}

func median(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && values[j] < values[j-1]; j-- {
			values[j], values[j-1] = values[j-1], values[j]
		}
	}
	return values[len(values)/2]
}

func allRunsReconciled(runs []runReport) bool {
	for _, run := range runs {
		if run.Status != "PASS" || run.Terminal != run.WorkflowCount || run.Pending != 0 {
			return false
		}
	}
	return true
}

func allSLOValuesWithinLimit(runs []runReport) bool {
	for _, run := range runs {
		if run.SLOViolations != 0 {
			return false
		}
	}
	return true
}

func summarizeRuns(runs []runReport) map[string]any {
	profiles := make(map[string][]runReport)
	for _, run := range runs {
		profiles[run.Profile] = append(profiles[run.Profile], run)
	}
	result := make(map[string]any, len(profiles)+1)
	for profile, values := range profiles {
		throughput := make([]float64, 0, len(values))
		latency := make([]float64, 0, len(values))
		schedulerCPU := make([]float64, 0, len(values))
		workerCPU := make([]float64, 0, len(values))
		for _, value := range values {
			throughput = append(throughput, value.Throughput)
			latency = append(latency, value.MedianLatency)
			schedulerCPU = append(schedulerCPU, value.SchedulerCPU/float64(value.WorkflowCount))
			workerCPU = append(workerCPU, value.WorkerCPU/float64(value.WorkflowCount))
		}
		throughputMedian := median(append([]float64(nil), throughput...))
		latencyMedian := median(append([]float64(nil), latency...))
		result[profile] = map[string]any{
			"runs":                         len(values),
			"throughput_per_second_median": throughputMedian,
			"throughput_min":               minFloat(throughput),
			"throughput_max":               maxFloat(throughput),
			"throughput_spread_percent":    spreadPercent(throughputMedian, throughput),
			"median_latency_seconds":       latencyMedian,
			"latency_min":                  minFloat(latency),
			"latency_max":                  maxFloat(latency),
			"latency_spread_percent":       spreadPercent(latencyMedian, latency),
			"scheduler_cpu_per_workflow":   median(append([]float64(nil), schedulerCPU...)),
			"worker_cpu_per_workflow":      median(append([]float64(nil), workerCPU...)),
			"history_rows_per_run":         values[0].HistoryRows,
			"outbox_rows_per_run":          values[0].OutboxRows,
			"outbox_pending_per_run":       values[0].OutboxPending,
		}
	}
	result["interpretation"] = "Descriptive medians and min/max spread are reported per profile. Differences whose intervals overlap the within-profile spread are not treated as resolved safeguard-cost effects."
	return result
}

func minFloat(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	minimum := values[0]
	for _, value := range values[1:] {
		if value < minimum {
			minimum = value
		}
	}
	return minimum
}

func maxFloat(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	maximum := values[0]
	for _, value := range values[1:] {
		if value > maximum {
			maximum = value
		}
	}
	return maximum
}

func spreadPercent(center float64, values []float64) float64 {
	if center == 0 || len(values) == 0 {
		return 0
	}
	return (maxFloat(values) - minFloat(values)) / center * 100
}

func anyManualRecovery(runs []runReport) bool {
	for _, run := range runs {
		if run.Profile == "no_outbox" && run.ManualRecoveryMS > 0 {
			return true
		}
	}
	return false
}

func failedRun(profile state.TestSafeguardProfile, repeat int, err error) runReport {
	return runReport{Profile: profile.Name, Repeat: repeat, Status: "FAIL", Failure: err.Error()}
}

func ptrState(value state.WorkflowState) *state.WorkflowState { return &value }

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
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

func currentCommit() (string, error) {
	output, err := exec.Command("git", "rev-parse", "HEAD").Output()
	if err != nil {
		return "", fmt.Errorf("resolve measurement commit: %w", err)
	}
	commit := strings.TrimSpace(string(output))
	if commit == "" {
		return "", errors.New("git returned an empty measurement commit")
	}
	return commit, nil
}

func fatal(message string) {
	fmt.Fprintln(os.Stderr, message)
	os.Exit(2)
}
