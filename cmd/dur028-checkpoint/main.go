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
	"time"

	"durable-agent-execution-engine/internal/engine"
	"durable-agent-execution-engine/internal/partition"
	"durable-agent-execution-engine/internal/state"

	"github.com/jackc/pgx/v5"
)

const (
	chunkCount       = 200
	crashChunk       = 100
	attemptLease     = 100 * time.Millisecond
	recoveryWait     = 150 * time.Millisecond
	checkpointSchema = 1
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
		digest = chunkDigest(digest, d.seed, chunk)
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

func chunkDigest(previous string, seed int64, chunk int) string {
	h := sha256.Sum256([]byte(fmt.Sprintf("%s|%d|%d", previous, seed, chunk)))
	return hex.EncodeToString(h[:])
}

func expectedDigest(seed int64) string {
	digest := ""
	for chunk := 1; chunk <= chunkCount; chunk++ {
		digest = chunkDigest(digest, seed, chunk)
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
	SchemaVersion string      `json:"schema_version"`
	Status        string      `json:"status"`
	GeneratedAt   time.Time   `json:"generated_at_utc"`
	GitCommit     string      `json:"git_commit,omitempty"`
	Seed          int64       `json:"seed"`
	Protocol      protocol    `json:"protocol"`
	Runs          []runReport `json:"runs"`
	Validation    validation  `json:"validation"`
	Failure       string      `json:"failure,omitempty"`
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
}

func main() {
	databaseURL := flag.String("database-url", "", "PostgreSQL URL")
	outputPath := flag.String("output", "experiments/m7/dur028/results.json", "artifact path")
	seed := flag.Int64("seed", 280028, "deterministic seed")
	repeats := flag.Int("repeats", 3, "repeats per setting and failure condition")
	pilot := flag.Bool("pilot", false, "run one pilot per setting and failure condition")
	gitCommit := flag.String("git-commit", "", "source commit recorded in the artifact")
	flag.Parse()
	if *databaseURL == "" {
		failArtifact(*outputPath, *gitCommit, *seed, "database URL is required")
		return
	}
	if *seed == 0 || *repeats <= 0 {
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
	count := *repeats
	if *pilot {
		count = 1
	}
	result := artifact{SchemaVersion: "dur028-checkpoint.v1", Status: "PASS", GeneratedAt: time.Now().UTC(), GitCommit: *gitCommit, Seed: *seed,
		Protocol: protocol{Chunks: chunkCount, CrashBarrierChunk: crashChunk, AttemptLeaseMS: int(attemptLease / time.Millisecond), RecoveryWaitMS: int(recoveryWait / time.Millisecond), CheckpointSchema: checkpointSchema, Settings: settings, Workload: "pure_chunked_activity", HashAlgorithm: "sha256"}}
	for _, setting := range settings {
		for _, failureCondition := range []string{"no_failure", "crash_mid_activity"} {
			for repeat := 1; repeat <= count; repeat++ {
				row, runErr := executeRun(ctx, store, *seed+int64(repeat)+int64(setting.Interval)*1000, setting, failureCondition == "crash_mid_activity", repeat)
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
	result.Validation = validateArtifact(result.Runs, count)
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

func executeRun(ctx context.Context, store *state.Store, seed int64, setting checkpointSetting, crash bool, repeat int) (runReport, error) {
	row := runReport{Setting: setting.ID, Interval: setting.Interval, FailureCondition: map[bool]string{true: "crash_mid_activity", false: "no_failure"}[crash], Repeat: repeat, Seed: seed, Status: "FAIL"}
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
	driver := &chunkDriver{interval: setting.Interval, seed: seed, crash: crash, barrier: crashChunk}
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
	row.FinalHash, row.ExpectedHash = driver.finalDigest, expectedDigest(seed)
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

func validateArtifact(runs []runReport, repeats int) validation {
	v := validation{ExpectedRuns: len(settings) * 2 * repeats, CompletedRuns: len(runs), ExpectedFinalHash: true, ExpectedTerminal: true, ExpectedCheckpoint: true, Status: "PASS"}
	if len(runs) != v.ExpectedRuns {
		v.Status = "FAIL"
	}
	seen := map[string]bool{}
	for _, row := range runs {
		seen[row.CaseID] = true
		if row.Status != "PASS" || row.FinalHash != row.ExpectedHash || row.TerminalState != string(state.StateSucceeded) {
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
