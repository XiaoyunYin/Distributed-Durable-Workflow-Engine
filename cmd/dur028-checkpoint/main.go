// Command dur028-checkpoint measures pure-work checkpoint tradeoffs.
//
// It deliberately uses the committed Store/Engine checkpoint extension rather
// than a second benchmark implementation. The activity is deterministic and
// has no external effect; a panic at the frozen mid-activity barrier models a
// worker crash before the next checkpoint/result commit.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"time"

	"durable-agent-execution-engine/internal/engine"
	"durable-agent-execution-engine/internal/partition"
	"durable-agent-execution-engine/internal/state"
	"durable-agent-execution-engine/internal/telemetry"

	"github.com/jackc/pgx/v5"
)

const (
	chunkCount        = 200
	crashChunk        = 100
	workUnitsPerChunk = 1
	attemptLease      = 100 * time.Millisecond
	recoveryWait      = 150 * time.Millisecond
	checkpointSchema  = 1
)

type checkpointSetting struct {
	ID       string `json:"id"`
	Interval int    `json:"interval_chunks"`
}

var settings = []checkpointSetting{
	{ID: "activity_boundary_only", Interval: 0},
	{ID: "every_5_chunks", Interval: 5},
	{ID: "every_chunk", Interval: 1},
}

type chunkPayload struct {
	Completed int    `json:"completed_chunk"`
	Digest    string `json:"digest"`
}

type chunkDriver struct {
	interval         int
	seed             int64
	workUnits        int
	crash            bool
	crashed          bool
	barrier          int
	calls            int
	computed         int
	firstRun         int
	recoveryRun      int
	checkpointWrites int
	checkpointBytes  int
	crashProgress    int
	finalDigest      string
}

func (d *chunkDriver) Run(context.Context, engine.Activity) (engine.ActivityResult, error) {
	return engine.ActivityResult{}, errors.New("checkpoint driver required")
}

func (d *chunkDriver) RunWithCheckpoint(ctx context.Context, _ engine.Activity, sink engine.CheckpointSink) (engine.ActivityResult, error) {
	d.calls++
	workUnits := d.workUnits
	if workUnits <= 0 {
		workUnits = workUnitsPerChunk
	}
	start := 1
	digest := ""
	if reader, ok := sink.(engine.CheckpointReader); ok {
		checkpoint, err := reader.Load(ctx)
		if err == nil {
			var progress chunkPayload
			if err := json.Unmarshal(checkpoint.Payload, &progress); err != nil {
				return engine.ActivityResult{}, fmt.Errorf("decode checkpoint: %w", err)
			}
			start = progress.Completed + 1
			digest = progress.Digest
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return engine.ActivityResult{}, fmt.Errorf("load checkpoint: %w", err)
		}
	}
	if d.calls == 1 {
		d.firstRun = 0
	} else {
		d.recoveryRun = 0
	}
	for chunk := start; chunk <= chunkCount; chunk++ {
		d.computed++
		if d.calls == 1 {
			d.firstRun++
		} else {
			d.recoveryRun++
		}
		digest = chunkDigest(digest, d.seed, chunk, workUnits)
		if d.crash && !d.crashed && chunk == d.barrier {
			d.crashed = true
			d.crashProgress = chunk - 1
			panic("dur028 crash at frozen mid-activity barrier")
		}
		if d.interval > 0 && chunk%d.interval == 0 {
			payload, err := json.Marshal(chunkPayload{Completed: chunk, Digest: digest})
			if err != nil {
				return engine.ActivityResult{}, err
			}
			sequence := int64(chunk/d.interval - 1)
			if err := sink.Save(ctx, sequence, checkpointSchema, payload); err != nil {
				return engine.ActivityResult{}, fmt.Errorf("save checkpoint %d: %w", chunk, err)
			}
			d.checkpointWrites++
			d.checkpointBytes += len(payload)
		}
	}
	d.finalDigest = digest
	payload, err := json.Marshal(map[string]any{"chunks": chunkCount, "sha256": digest})
	if err != nil {
		return engine.ActivityResult{}, err
	}
	return engine.ActivityResult{AttemptState: state.AttemptSucceeded, Payload: payload}, nil
}

func chunkDigest(previous string, seed int64, chunk, workUnits int) string {
	if workUnits <= 0 {
		workUnits = workUnitsPerChunk
	}
	digest := previous
	for unit := 0; unit < workUnits; unit++ {
		h := sha256.Sum256([]byte(fmt.Sprintf("%s|%d|%d|%d", digest, seed, chunk, unit)))
		digest = hex.EncodeToString(h[:])
	}
	return digest
}

func expectedDigest(seed int64) string {
	return expectedDigestFor(seed, workUnitsPerChunk)
}

func expectedDigestFor(seed int64, workUnits int) string {
	digest := ""
	for chunk := 1; chunk <= chunkCount; chunk++ {
		digest = chunkDigest(digest, seed, chunk, workUnits)
	}
	return digest
}

type metricDelta struct {
	DBQueries       uint64  `json:"db_queries"`
	DBTransactions  uint64  `json:"db_transactions"`
	QuerySeconds    float64 `json:"query_seconds"`
	LockWaits       uint64  `json:"lock_waits"`
	LockWaitSeconds float64 `json:"lock_wait_seconds"`
}

type runReport struct {
	CaseID            string      `json:"case_id"`
	Setting           string      `json:"checkpoint_setting"`
	Interval          int         `json:"checkpoint_interval_chunks"`
	FailureCondition  string      `json:"failure_condition"`
	Repeat            int         `json:"repeat"`
	Seed              int64       `json:"seed"`
	WorkUnitsPerChunk int         `json:"work_units_per_chunk"`
	WorkflowID        string      `json:"workflow_id"`
	Status            string      `json:"status"`
	CommittedProgress int         `json:"committed_progress_at_failure"`
	ComputedChunks    int         `json:"computed_chunks"`
	FirstRunChunks    int         `json:"first_run_computed_chunks"`
	RecoveryRunChunks int         `json:"recovery_run_computed_chunks"`
	RecomputedChunks  int         `json:"recomputed_chunks"`
	CheckpointWrites  int         `json:"checkpoint_writes"`
	CheckpointBytes   int         `json:"checkpoint_bytes"`
	FinalHash         string      `json:"final_output_sha256"`
	ExpectedHash      string      `json:"expected_output_sha256"`
	CheckpointMax     int64       `json:"checkpoint_max_sequence"`
	CheckpointRows    int         `json:"checkpoint_rows"`
	InitialSeconds    float64     `json:"initial_completion_seconds"`
	RecoverySeconds   float64     `json:"recovery_seconds"`
	TotalSeconds      float64     `json:"total_completion_seconds"`
	Telemetry         metricDelta `json:"persistence_telemetry"`
	TerminalState     string      `json:"terminal_state"`
	Reconciled        bool        `json:"reconciled"`
	Failure           string      `json:"failure,omitempty"`
}

type artifact struct {
	SchemaVersion string       `json:"schema_version"`
	Status        string       `json:"status"`
	GeneratedAt   time.Time    `json:"generated_at_utc"`
	GitCommit     string       `json:"git_commit,omitempty"`
	Seed          int64        `json:"seed"`
	Protocol      protocol     `json:"protocol"`
	Runs          []runReport  `json:"runs"`
	Summary       []summaryRow `json:"summary"`
	Conclusions   conclusions  `json:"conclusions"`
	Limitations   []string     `json:"limitations"`
	Validation    validation   `json:"validation"`
	Failure       string       `json:"failure,omitempty"`
}

type protocol struct {
	Chunks            int                 `json:"chunks"`
	CrashBarrierChunk int                 `json:"crash_barrier_chunk"`
	AttemptLeaseMS    int                 `json:"attempt_lease_ms"`
	RecoveryWaitMS    int                 `json:"recovery_wait_ms"`
	CheckpointSchema  int                 `json:"checkpoint_schema_version"`
	Settings          []checkpointSetting `json:"settings"`
	Workload          string              `json:"workload"`
	HashAlgorithm     string              `json:"final_hash_algorithm"`
	WorkUnitsPerChunk int                 `json:"work_units_per_chunk"`
}

type numericInterval struct {
	Count  int     `json:"count"`
	Min    float64 `json:"min"`
	Median float64 `json:"median"`
	Max    float64 `json:"max"`
}

type summaryRow struct {
	Setting           string          `json:"checkpoint_setting"`
	FailureCondition  string          `json:"failure_condition"`
	Repeats           int             `json:"repeats"`
	WorkUnitsPerChunk int             `json:"work_units_per_chunk"`
	TotalSeconds      numericInterval `json:"total_completion_seconds"`
	RecoverySeconds   numericInterval `json:"recovery_seconds"`
	RecomputedChunks  numericInterval `json:"recomputed_chunks"`
	CheckpointWrites  numericInterval `json:"checkpoint_writes"`
	CheckpointBytes   numericInterval `json:"checkpoint_bytes"`
	DBQueries         numericInterval `json:"db_queries"`
	QuerySeconds      numericInterval `json:"query_seconds"`
}

type conclusions struct {
	Scope                           string  `json:"scope"`
	WorkUnitsPerChunk               int     `json:"work_units_per_chunk"`
	FailureModel                    string  `json:"failure_model"`
	TimingEffectResolved            bool    `json:"timing_effect_resolved"`
	RecomputationEffectResolved     bool    `json:"recomputation_effect_resolved"`
	PersistenceEffectResolved       bool    `json:"persistence_effect_resolved"`
	BoundaryCrashMedianSeconds      float64 `json:"boundary_only_crash_median_seconds"`
	EveryChunkCrashMedianSeconds    float64 `json:"every_chunk_crash_median_seconds"`
	BoundaryCrashRecomputedMedian   float64 `json:"boundary_only_crash_recomputed_chunks"`
	EveryChunkCrashRecomputedMedian float64 `json:"every_chunk_crash_recomputed_chunks"`
	CrashTimingRatio                float64 `json:"every_chunk_to_boundary_crash_time_ratio"`
	SavedRecomputedChunks           float64 `json:"median_recomputed_chunks_saved"`
	AdditionalCheckpointWrites      float64 `json:"median_additional_checkpoint_writes"`
	AdditionalCheckpointBytes       float64 `json:"median_additional_checkpoint_bytes"`
	Statement                       string  `json:"statement"`
}

var limitations = []string{
	"This is a single-node Docker Desktop/WSL2 PostgreSQL Store/Engine measurement, not a multi-host or production-cost claim.",
	"The crash condition is an in-process panic after the chunk-100 computation and before its next checkpoint/result commit; it is not an operating-system process-kill or host-failure test.",
	"The workload is one pure deterministic activity with one SHA-256 work unit per chunk; the timing tradeoff is scoped to that chunk cost and is not a general checkpoint policy.",
	"Pure-work checkpoint evidence cannot establish atomicity or safety for unrelated external effects.",
}

type validation struct {
	ExpectedRuns       int    `json:"expected_runs"`
	CompletedRuns      int    `json:"completed_runs"`
	ExpectedFinalHash  bool   `json:"all_final_hashes_match"`
	ExpectedTerminal   bool   `json:"all_terminal_succeeded"`
	ExpectedCheckpoint bool   `json:"checkpoint_prefixes_valid"`
	CleanWorkflows     int    `json:"clean_workflow_rows"`
	CleanDefinitions   int    `json:"clean_definition_rows"`
	Status             string `json:"status"`
	SummaryRows        int    `json:"summary_rows"`
	ConclusionsPresent bool   `json:"conclusions_present"`
	LimitationsPresent bool   `json:"limitations_present"`
}

func main() {
	databaseURL := flag.String("database-url", "", "PostgreSQL URL")
	outputPath := flag.String("output", "experiments/m7/dur028/results.json", "artifact path")
	seed := flag.Int64("seed", 280028, "deterministic seed")
	repeats := flag.Int("repeats", 3, "repeats per setting and failure condition")
	pilot := flag.Bool("pilot", false, "run one pilot per setting and failure condition")
	workUnits := flag.Int("work-units-per-chunk", workUnitsPerChunk, "SHA-256 work units performed per chunk")
	gitCommit := flag.String("git-commit", "", "source commit recorded in the artifact")
	flag.Parse()
	if *databaseURL == "" {
		failArtifact(*outputPath, *gitCommit, *seed, "database URL is required")
		return
	}
	if *seed == 0 || *repeats <= 0 || *workUnits <= 0 {
		failArtifact(*outputPath, *gitCommit, *seed, "seed must be non-zero and repeats must be positive")
		return
	}
	ctx := context.Background()
	store, err := state.NewFromURL(ctx, *databaseURL)
	if err != nil {
		failArtifact(*outputPath, *gitCommit, *seed, err.Error())
		return
	}
	defer store.Close()
	store.SetTelemetry(telemetry.New("dur028-checkpoint"))
	count := *repeats
	if *pilot {
		count = 1
	}
	result := artifact{SchemaVersion: "dur028-checkpoint.v1", Status: "PASS", GeneratedAt: time.Now().UTC(), GitCommit: *gitCommit, Seed: *seed, Limitations: append([]string(nil), limitations...),
		Protocol: protocol{Chunks: chunkCount, CrashBarrierChunk: crashChunk, AttemptLeaseMS: int(attemptLease / time.Millisecond), RecoveryWaitMS: int(recoveryWait / time.Millisecond), CheckpointSchema: checkpointSchema, Settings: settings, Workload: "pure_chunked_activity", HashAlgorithm: "sha256", WorkUnitsPerChunk: *workUnits}}
	for _, setting := range settings {
		for _, failureCondition := range []string{"no_failure", "crash_mid_activity"} {
			for repeat := 1; repeat <= count; repeat++ {
				row, runErr := executeRun(ctx, store, *seed+int64(repeat)+int64(setting.Interval)*1000, setting, failureCondition == "crash_mid_activity", repeat, *workUnits)
				result.Runs = append(result.Runs, row)
				if runErr != nil {
					result.Status = "FAIL"
					if result.Failure == "" {
						result.Failure = runErr.Error()
					}
				}
			}
		}
	}
	result.Summary = summarizeRuns(result.Runs)
	result.Conclusions = deriveConclusions(result.Summary)
	result.Validation = validateArtifact(result.Runs, count, result.Summary, result.Conclusions, len(result.Limitations))
	var workflowRows, definitionRows int
	if err := store.Pool().QueryRow(ctx, `SELECT count(*) FROM engine.workflow_executions WHERE namespace = 'dur028-checkpoint'`).Scan(&workflowRows); err != nil {
		result.Status = "FAIL"
		result.Failure = "count checkpoint workflow rows: " + err.Error()
	} else if err := store.Pool().QueryRow(ctx, `SELECT count(*) FROM engine.workflow_definitions WHERE definition_id LIKE 'dur028-checkpoint-def-%'`).Scan(&definitionRows); err != nil {
		result.Status = "FAIL"
		result.Failure = "count checkpoint definition rows: " + err.Error()
	}
	result.Validation.CleanWorkflows, result.Validation.CleanDefinitions = workflowRows, definitionRows
	if workflowRows != 0 || definitionRows != 0 {
		result.Status = "FAIL"
		result.Failure = "checkpoint campaign left durable fixture rows behind"
	}
	if result.Validation.Status != "PASS" {
		result.Status = "FAIL"
		if result.Failure == "" {
			result.Failure = "one or more checkpoint rows failed validation"
		}
	}
	if err := writeJSON(*outputPath, result); err != nil {
		fmt.Println(err)
		return
	}
	if result.Status != "PASS" {
		fmt.Println(result.Failure)
	}
}

func executeRun(ctx context.Context, store *state.Store, seed int64, setting checkpointSetting, crash bool, repeat, workUnits int) (runReport, error) {
	row := runReport{Setting: setting.ID, Interval: setting.Interval, FailureCondition: map[bool]string{true: "crash_mid_activity", false: "no_failure"}[crash], Repeat: repeat, Seed: seed, WorkUnitsPerChunk: workUnits, Status: "FAIL"}
	row.CaseID = fmt.Sprintf("%s-%s-r%d", setting.ID, row.FailureCondition, repeat)
	definitionID := "dur028-checkpoint-def-" + state.NewID()
	ownerID := state.NewID()
	lease, _, err := acquireFreeLease(ctx, store, ownerID)
	if err != nil {
		row.Failure = err.Error()
		return row, err
	}
	defer func() {
		_ = store.ReleaseLease(context.Background(), state.LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch})
	}()
	workflowID, err := createFixture(ctx, store, lease, definitionID)
	if err != nil {
		row.Failure = err.Error()
		return row, err
	}
	row.WorkflowID = workflowID
	defer cleanupFixture(ctx, store, workflowID, definitionID)
	before := store.Telemetry().Snapshot()
	driver := &chunkDriver{interval: setting.Interval, seed: seed, workUnits: workUnits, crash: crash, barrier: crashChunk}
	first := engine.New(store, driver)
	first.OwnerID, first.WorkerID, first.ActorID = ownerID, "dur028-worker", "dur028-scheduler"
	first.AttemptLease, first.LeaseTTL, first.MaxSteps = attemptLease, time.Minute, 30
	started := time.Now()
	firstResult, firstErr := runCatchingPanic(func() (engine.RunResult, error) { return first.Run(ctx, workflowID) })
	initialEnd := time.Now()
	if crash {
		if firstErr == nil && !firstResult.Panicked {
			return failRow(&row, "crash barrier did not interrupt first run")
		}
		checkpoints, listErr := store.ListCheckpoints(ctx, workflowID)
		if listErr != nil {
			return failRow(&row, listErr.Error())
		}
		row.CheckpointRows = len(checkpoints)
		if len(checkpoints) > 0 {
			row.CheckpointMax = checkpoints[len(checkpoints)-1].Sequence
			row.CommittedProgress = int((row.CheckpointMax + 1) * int64(setting.Interval))
		}
		if setting.Interval == 0 && row.CommittedProgress != 0 {
			return failRow(&row, "boundary-only setting committed checkpoint progress")
		}
		time.Sleep(recoveryWait)
		second := engine.New(store, driver)
		second.OwnerID, second.WorkerID, second.ActorID = ownerID, "dur028-worker", "dur028-scheduler"
		second.AttemptLease, second.LeaseTTL, second.MaxSteps = time.Minute, time.Minute, 30
		recoveryStart := time.Now()
		secondResult, secondErr := runCatchingPanic(func() (engine.RunResult, error) { return second.Run(ctx, workflowID) })
		if secondErr != nil || secondResult.Panicked {
			return failRow(&row, fmt.Sprintf("recovery failed: %v", secondErr))
		}
		row.RecoverySeconds = time.Since(recoveryStart).Seconds()
	} else if firstErr != nil || firstResult.Panicked {
		return failRow(&row, fmt.Sprintf("unexpected no-failure error: %v", firstErr))
	}
	row.InitialSeconds = initialEnd.Sub(started).Seconds()
	row.TotalSeconds = time.Since(started).Seconds()
	row.FirstRunChunks, row.RecoveryRunChunks = driver.firstRun, driver.recoveryRun
	row.ComputedChunks, row.CheckpointWrites, row.CheckpointBytes = driver.computed, driver.checkpointWrites, driver.checkpointBytes
	row.RecomputedChunks = driver.recoveryRun
	row.FinalHash, row.ExpectedHash = driver.finalDigest, expectedDigestFor(seed, workUnits)
	if !crash {
		row.CommittedProgress = chunkCount
	}
	if checkpoints, listErr := store.ListCheckpoints(ctx, workflowID); listErr == nil {
		row.CheckpointRows = len(checkpoints)
		if len(checkpoints) > 0 {
			row.CheckpointMax = checkpoints[len(checkpoints)-1].Sequence
		}
	}
	after := store.Telemetry().Snapshot()
	row.Telemetry = metricDelta{DBQueries: after.DBQueries - before.DBQueries, DBTransactions: after.DBTransactions - before.DBTransactions, QuerySeconds: after.QuerySeconds - before.QuerySeconds, LockWaits: after.LockWaitCount - before.LockWaitCount, LockWaitSeconds: after.LockWaitSeconds - before.LockWaitSeconds}
	wf, getErr := store.GetWorkflow(ctx, workflowID)
	if getErr != nil {
		return failRow(&row, getErr.Error())
	}
	row.TerminalState = string(wf.State)
	expectedComputed := chunkCount
	if crash {
		expectedComputed = crashChunk + chunkCount - row.CommittedProgress
	}
	row.Reconciled = wf.State == state.StateSucceeded && row.FinalHash == row.ExpectedHash && row.ComputedChunks == expectedComputed && row.FirstRunChunks == map[bool]int{true: crashChunk, false: chunkCount}[crash]
	if !row.Reconciled {
		return failRow(&row, fmt.Sprintf("reconciliation failed: state=%s computed=%d first=%d recovery=%d hash=%s expected=%s", wf.State, row.ComputedChunks, row.FirstRunChunks, row.RecoveryRunChunks, row.FinalHash, row.ExpectedHash))
	}
	row.Status = "PASS"
	return row, nil
}

type runResult struct{ Panicked bool }

func runCatchingPanic(fn func() (engine.RunResult, error)) (runResult, error) {
	var result runResult
	var err error
	func() {
		defer func() {
			if recover() != nil {
				result.Panicked = true
			}
		}()
		_, err = fn()
	}()
	return result, err
}

func acquireFreeLease(ctx context.Context, store *state.Store, ownerID string) (state.Lease, bool, error) {
	for partitionID := int16(0); partitionID < 16; partitionID++ {
		lease, acquired, err := store.AcquireLease(ctx, partitionID, ownerID, time.Minute)
		if err != nil {
			return state.Lease{}, false, err
		}
		if acquired {
			return lease, true, nil
		}
	}
	return state.Lease{}, false, errors.New("no free measurement partition lease")
}

func createFixture(ctx context.Context, store *state.Store, lease state.Lease, definitionID string) (string, error) {
	graph := json.RawMessage(`{"entry":"root","nodes":[{"id":"root","kind":"activity","next":"done"},{"id":"done","kind":"success"}]}`)
	if err := store.CreateDefinition(ctx, state.DefinitionInput{DefinitionID: definitionID, Version: 1, DefinitionHash: "hash-" + definitionID, Graph: graph, ActivityVersions: []byte(`{"root":"1"}`), EffectClasses: []byte(`{"root":"PURE_ACTIVITY"}`)}); err != nil {
		return "", err
	}
	for attempt := 0; attempt < 1000; attempt++ {
		workflowID := "dur028-checkpoint-" + state.NewID()
		mapped, err := partition.ID(workflowID)
		if err != nil {
			return "", err
		}
		if int16(mapped) != lease.PartitionID {
			continue
		}
		created, err := store.CreateWorkflow(ctx, state.CreateWorkflowInput{WorkflowID: workflowID, Namespace: "dur028-checkpoint", SubmissionKey: "key-" + workflowID, SubmissionPayloadHash: "sub-v1:" + workflowID, DefinitionID: definitionID, DefinitionVersion: 1, PartitionID: lease.PartitionID, InitialNodeID: "root", InitialInput: []byte(`{"chunks":200}`), ActorID: "dur028"})
		if err != nil {
			return "", err
		}
		return created.Workflow.WorkflowID, nil
	}
	return "", fmt.Errorf("could not allocate workflow on partition %d", lease.PartitionID)
}

func cleanupFixture(ctx context.Context, store *state.Store, workflowID, definitionID string) {
	_, _ = store.Pool().Exec(ctx, `DELETE FROM engine.workflow_executions WHERE workflow_id = $1`, workflowID)
	_, _ = store.Pool().Exec(ctx, `DELETE FROM engine.workflow_definitions WHERE definition_id = $1`, definitionID)
}

func summarizeRuns(runs []runReport) []summaryRow {
	rows := make([]summaryRow, 0, len(settings)*2)
	for _, setting := range settings {
		for _, failureCondition := range []string{"no_failure", "crash_mid_activity"} {
			group := make([]runReport, 0)
			for _, row := range runs {
				if row.Setting == setting.ID && row.FailureCondition == failureCondition {
					group = append(group, row)
				}
			}
			if len(group) == 0 {
				continue
			}
			row := summaryRow{Setting: setting.ID, FailureCondition: failureCondition, Repeats: len(group), WorkUnitsPerChunk: group[0].WorkUnitsPerChunk}
			row.TotalSeconds = summarizeNumbers(group, func(item runReport) float64 { return item.TotalSeconds })
			row.RecoverySeconds = summarizeNumbers(group, func(item runReport) float64 { return item.RecoverySeconds })
			row.RecomputedChunks = summarizeNumbers(group, func(item runReport) float64 { return float64(item.RecomputedChunks) })
			row.CheckpointWrites = summarizeNumbers(group, func(item runReport) float64 { return float64(item.CheckpointWrites) })
			row.CheckpointBytes = summarizeNumbers(group, func(item runReport) float64 { return float64(item.CheckpointBytes) })
			row.DBQueries = summarizeNumbers(group, func(item runReport) float64 { return float64(item.Telemetry.DBQueries) })
			row.QuerySeconds = summarizeNumbers(group, func(item runReport) float64 { return item.Telemetry.QuerySeconds })
			rows = append(rows, row)
		}
	}
	return rows
}

func summarizeNumbers(runs []runReport, value func(runReport) float64) numericInterval {
	values := make([]float64, 0, len(runs))
	for _, row := range runs {
		values = append(values, value(row))
	}
	sort.Float64s(values)
	result := numericInterval{Count: len(values)}
	if len(values) == 0 {
		return result
	}
	result.Min, result.Max = values[0], values[len(values)-1]
	if len(values)%2 == 0 {
		result.Median = (values[len(values)/2-1] + values[len(values)/2]) / 2
	} else {
		result.Median = values[len(values)/2]
	}
	return result
}

func findSummary(rows []summaryRow, setting, failureCondition string) (summaryRow, bool) {
	for _, row := range rows {
		if row.Setting == setting && row.FailureCondition == failureCondition {
			return row, true
		}
	}
	return summaryRow{}, false
}

func intervalsSeparate(lower, higher numericInterval) bool {
	return higher.Min > lower.Max || lower.Min > higher.Max
}

func deriveConclusions(rows []summaryRow) conclusions {
	result := conclusions{Scope: "one pure 200-chunk activity on the declared single-node Store/Engine host", WorkUnitsPerChunk: workUnitsPerChunk,
		FailureModel: "in-process panic after chunk 100 computation and before its next checkpoint/result commit"}
	boundaryCrash, boundaryOK := findSummary(rows, "activity_boundary_only", "crash_mid_activity")
	everyChunkCrash, everyChunkOK := findSummary(rows, "every_chunk", "crash_mid_activity")
	if !boundaryOK || !everyChunkOK {
		result.Statement = "The checkpoint tradeoff is unresolved because the required comparison cells are incomplete."
		return result
	}
	result.BoundaryCrashMedianSeconds = boundaryCrash.TotalSeconds.Median
	result.EveryChunkCrashMedianSeconds = everyChunkCrash.TotalSeconds.Median
	result.BoundaryCrashRecomputedMedian = boundaryCrash.RecomputedChunks.Median
	result.EveryChunkCrashRecomputedMedian = everyChunkCrash.RecomputedChunks.Median
	result.SavedRecomputedChunks = boundaryCrash.RecomputedChunks.Median - everyChunkCrash.RecomputedChunks.Median
	result.AdditionalCheckpointWrites = everyChunkCrash.CheckpointWrites.Median - boundaryCrash.CheckpointWrites.Median
	result.AdditionalCheckpointBytes = everyChunkCrash.CheckpointBytes.Median - boundaryCrash.CheckpointBytes.Median
	result.TimingEffectResolved = intervalsSeparate(boundaryCrash.TotalSeconds, everyChunkCrash.TotalSeconds)
	result.RecomputationEffectResolved = intervalsSeparate(everyChunkCrash.RecomputedChunks, boundaryCrash.RecomputedChunks)
	result.PersistenceEffectResolved = everyChunkCrash.CheckpointWrites.Min > boundaryCrash.CheckpointWrites.Max && everyChunkCrash.CheckpointBytes.Min > boundaryCrash.CheckpointBytes.Max
	if boundaryCrash.TotalSeconds.Median > 0 {
		result.CrashTimingRatio = everyChunkCrash.TotalSeconds.Median / boundaryCrash.TotalSeconds.Median
	}
	if result.TimingEffectResolved && result.RecomputationEffectResolved && result.PersistenceEffectResolved {
		result.Statement = fmt.Sprintf("At %d SHA-256 work unit per chunk, every-chunk checkpointing reduces median crash replay from %.0f to %.0f chunks, saving %.0f chunks, but adds %.0f checkpoint writes (%.0f bytes) and raises median crash completion from %.3f s to %.3f s (%.1fx). Checkpointing does not pay for this measured pure workload; this conclusion is scoped to the recorded chunk cost and crash model.", result.WorkUnitsPerChunk, result.BoundaryCrashRecomputedMedian, result.EveryChunkCrashRecomputedMedian, result.SavedRecomputedChunks, result.AdditionalCheckpointWrites, result.AdditionalCheckpointBytes, result.BoundaryCrashMedianSeconds, result.EveryChunkCrashMedianSeconds, result.CrashTimingRatio)
	} else {
		result.Statement = "The measured rows show checkpoint writes and replay differences, but their timing intervals do not separate enough to promote a checkpoint-cost conclusion for this workload."
	}
	return result
}

func validateArtifact(runs []runReport, repeats int, summary []summaryRow, conclusion conclusions, limitationCount int) validation {
	v := validation{ExpectedRuns: len(settings) * 2 * repeats, CompletedRuns: len(runs), ExpectedFinalHash: true, ExpectedTerminal: true, ExpectedCheckpoint: true, SummaryRows: len(summary), ConclusionsPresent: conclusion.Statement != "", LimitationsPresent: limitationCount > 0, Status: "PASS"}
	if len(runs) != v.ExpectedRuns {
		v.Status = "FAIL"
	}
	if len(summary) != len(settings)*2 || !v.ConclusionsPresent || !v.LimitationsPresent {
		v.Status = "FAIL"
	}
	seen := map[string]bool{}
	for _, row := range runs {
		seen[row.CaseID] = true
		if row.Status != "PASS" || row.FinalHash != row.ExpectedHash || row.TerminalState != string(state.StateSucceeded) || row.WorkUnitsPerChunk <= 0 {
			v.ExpectedFinalHash, v.ExpectedTerminal = false, false
			v.Status = "FAIL"
		}
		if row.FailureCondition == "crash_mid_activity" && row.FirstRunChunks != crashChunk {
			v.ExpectedCheckpoint = false
			v.Status = "FAIL"
		}
	}
	if len(seen) != len(runs) {
		v.Status = "FAIL"
	}
	return v
}

func failRow(row *runReport, failure string) (runReport, error) {
	row.Failure = failure
	return *row, errors.New(failure)
}

func failArtifact(path, commit string, seed int64, failure string) {
	_ = writeJSON(path, artifact{SchemaVersion: "dur028-checkpoint.v1", Status: "FAIL", GeneratedAt: time.Now().UTC(), GitCommit: commit, Seed: seed, Failure: failure})
	fmt.Println(failure)
}

func writeJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o644)
}
