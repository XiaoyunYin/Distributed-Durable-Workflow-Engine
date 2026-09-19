// Command dur027-lease runs the bounded DUR-027 lease tradeoff study.
//
// The campaign deliberately models an owner pause in-process. It measures the
// PostgreSQL lease protocol and a real post-takeover workflow transition; it
// does not claim to be a process-kill, multi-host, or storage-durability test.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"durable-agent-execution-engine/internal/partition"
	"durable-agent-execution-engine/internal/state"
	"durable-agent-execution-engine/internal/telemetry"
)

const (
	artifactSchema = "dur027-lease.v1"
	caseCount      = 24
	normalCases    = 12
	pauseCases     = 12
)

type leaseArm struct {
	ID    string        `json:"id"`
	TTL   time.Duration `json:"ttl"`
	TTLMS int           `json:"ttl_ms"`
}

type caseReport struct {
	CaseID          string  `json:"case_id"`
	Mode            string  `json:"mode"`
	Status          string  `json:"status"`
	Partition       int16   `json:"partition"`
	Acquired        bool    `json:"acquired"`
	Renewals        int     `json:"renewals"`
	FalseTakeover   bool    `json:"false_takeover"`
	Takeover        bool    `json:"takeover"`
	UsefulRecovery  bool    `json:"useful_recovery"`
	FencedOldOwner  int     `json:"fenced_old_owner_writes"`
	LockContention  bool    `json:"lock_contention"`
	LockWaits       uint64  `json:"lock_waits"`
	LockWaitSeconds float64 `json:"lock_wait_seconds"`
	TakeoverDelayMS float64 `json:"takeover_delay_ms,omitempty"`
	WorkflowID      string  `json:"workflow_id"`
	Failure         string  `json:"failure,omitempty"`
}

type runReport struct {
	RunID                string       `json:"run_id"`
	Arm                  string       `json:"arm"`
	Repeat               int          `json:"repeat"`
	TTLMS                int          `json:"ttl_ms"`
	Status               string       `json:"status"`
	CaseCount            int          `json:"case_count"`
	NormalCases          int          `json:"normal_cases"`
	PauseCases           int          `json:"pause_cases"`
	CompletedCases       int          `json:"completed_cases"`
	Takeovers            int          `json:"takeovers"`
	UsefulRecoveries     int          `json:"useful_recoveries"`
	FalseTakeovers       int          `json:"false_takeovers"`
	FencedOldOwnerWrites int          `json:"fenced_old_owner_writes"`
	RenewalCount         int          `json:"renewal_count"`
	LockContentionCases  int          `json:"lock_contention_cases"`
	LockWaits            uint64       `json:"lock_waits"`
	LockWaitSeconds      float64      `json:"lock_wait_seconds"`
	TakeoverDelayMS      interval     `json:"takeover_delay_ms_interval"`
	Cases                []caseReport `json:"cases"`
	Failure              string       `json:"failure,omitempty"`
}

type interval struct {
	Count  int     `json:"count"`
	Min    float64 `json:"min"`
	Median float64 `json:"median"`
	Max    float64 `json:"max"`
}

type summaryRow struct {
	Arm                   string  `json:"arm"`
	TTLMS                 int     `json:"ttl_ms"`
	Runs                  int     `json:"runs"`
	MedianTakeoverMS      float64 `json:"median_takeover_ms"`
	RenewalsPerRun        float64 `json:"renewals_per_run"`
	LockWaitsPerRun       float64 `json:"lock_waits_per_run"`
	LockWaitSecondsPerRun float64 `json:"lock_wait_seconds_per_run"`
	FalseTakeovers        int     `json:"false_takeovers"`
	UsefulRecoveries      int     `json:"useful_recoveries"`
	Takeovers             int     `json:"takeovers"`
}

type validationReport struct {
	ExpectedRuns        int    `json:"expected_runs"`
	ObservedRuns        int    `json:"observed_runs"`
	ExpectedCases       int    `json:"expected_cases"`
	CompletedCases      int    `json:"completed_cases"`
	Takeovers           int    `json:"takeovers"`
	UsefulRecoveries    int    `json:"useful_recoveries"`
	FalseTakeovers      int    `json:"false_takeovers"`
	FencedOldOwners     int    `json:"fenced_old_owner_writes"`
	LockContentionCases int    `json:"lock_contention_cases"`
	CleanWorkflowRows   int    `json:"clean_workflow_rows"`
	CleanDefinitionRows int    `json:"clean_definition_rows"`
	Status              string `json:"status"`
	Failure             string `json:"failure,omitempty"`
}

type conclusions struct {
	Recovery            string `json:"recovery"`
	RenewalTraffic      string `json:"renewal_traffic"`
	Safety              string `json:"safety"`
	LockContention      string `json:"lock_contention"`
	PauseInterpretation string `json:"pause_interpretation"`
}

type artifact struct {
	SchemaVersion string           `json:"schema_version"`
	Status        string           `json:"status"`
	GeneratedAt   time.Time        `json:"generated_at_utc"`
	GitCommit     string           `json:"git_commit"`
	Protocol      map[string]any   `json:"protocol"`
	Runs          []runReport      `json:"runs"`
	Summary       []summaryRow     `json:"summary"`
	Validation    validationReport `json:"validation"`
	Conclusions   conclusions      `json:"conclusions"`
	Limitations   []string         `json:"limitations"`
	Failure       string           `json:"failure,omitempty"`
}

type config struct {
	DatabaseURL string
	OutputPath  string
	Repeats     int
	Seed        int64
}

func main() {
	var cfg config
	flag.StringVar(&cfg.DatabaseURL, "database-url", databaseURLFromEnvironment(), "PostgreSQL connection URL")
	flag.StringVar(&cfg.OutputPath, "output", "experiments/m7/dur027/results.json", "artifact output path")
	flag.IntVar(&cfg.Repeats, "repeats", 3, "measured repeats per lease arm")
	flag.Int64Var(&cfg.Seed, "seed", 270027, "deterministic campaign seed")
	flag.Parse()
	if cfg.Repeats <= 0 || cfg.Seed == 0 || strings.TrimSpace(cfg.DatabaseURL) == "" {
		writeFailure(cfg.OutputPath, "positive repeats, non-zero seed, and a database URL are required")
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	store, err := state.NewFromURL(ctx, cfg.DatabaseURL)
	if err != nil {
		writeFailure(cfg.OutputPath, fmt.Sprintf("open database: %v", err))
		os.Exit(1)
	}
	defer store.Close()

	metrics := telemetry.New("dur027-lease")
	store.SetTelemetry(metrics)
	result := runCampaign(ctx, store, cfg)
	if err := writeArtifact(cfg.OutputPath, result); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if result.Status != "PASS" {
		fmt.Fprintln(os.Stderr, result.Failure)
		os.Exit(1)
	}
}

func runCampaign(ctx context.Context, store *state.Store, cfg config) artifact {
	commit := currentCommit()
	result := artifact{
		SchemaVersion: artifactSchema,
		Status:        "FAIL",
		GeneratedAt:   time.Now().UTC(),
		GitCommit:     commit,
		Protocol: map[string]any{
			"seed":                  cfg.Seed,
			"repeats_per_arm":       cfg.Repeats,
			"arms":                  []map[string]any{{"id": "ttl_100ms", "ttl_ms": 100}, {"id": "ttl_250ms", "ttl_ms": 250}, {"id": "ttl_750ms", "ttl_ms": 750}},
			"cases_per_run":         caseCount,
			"normal_renewal_cases":  normalCases,
			"owner_pause_cases":     pauseCases,
			"normal_duration_ms":    400,
			"pause_grace_ms":        30,
			"lock_contention_cases": 1,
			"pause_model":           "in-process renewal pause; not a process-kill or storage-durability claim",
			"workflow_transition":   "new owner applies WAITING_ACTIVITY transition after takeover",
		},
		Limitations: []string{
			"single-node PostgreSQL on the declared Docker Desktop/WSL2 host",
			"owner pauses are bounded in-process fixtures rather than process termination",
			"results do not establish multi-host, hard-kill, production scheduler, or maximum-throughput behavior",
			"short-TTL arms are measurement configurations, not recommended deployment settings",
		},
	}
	for armIndex, arm := range leaseArms() {
		for repeat := 1; repeat <= cfg.Repeats; repeat++ {
			runID := fmt.Sprintf("dur027-lease-%d-%d-%d", cfg.Seed, armIndex, repeat)
			report, err := runOne(ctx, store, arm, repeat, runID)
			result.Runs = append(result.Runs, report)
			if err != nil && result.Failure == "" {
				result.Failure = err.Error()
			}
		}
	}
	result.Summary = summarize(result.Runs)
	result.Validation = validateArtifact(ctx, store, result, cfg.Repeats)
	result.Conclusions = deriveConclusions(result)
	if result.Validation.Status == "PASS" && result.Failure == "" {
		result.Status = "PASS"
	} else if result.Failure == "" {
		result.Failure = result.Validation.Failure
	}
	return result
}

func leaseArms() []leaseArm {
	return []leaseArm{{ID: "ttl_100ms", TTL: 100 * time.Millisecond, TTLMS: 100},
		{ID: "ttl_250ms", TTL: 250 * time.Millisecond, TTLMS: 250},
		{ID: "ttl_750ms", TTL: 750 * time.Millisecond, TTLMS: 750}}
}

func runOne(ctx context.Context, store *state.Store, arm leaseArm, repeat int, runID string) (report runReport, runErr error) {
	report = runReport{RunID: runID, Arm: arm.ID, Repeat: repeat, TTLMS: arm.TTLMS,
		Status: "FAIL", CaseCount: caseCount, NormalCases: normalCases, PauseCases: pauseCases}
	definitionID := runID + "-definition"
	namespace := runID + "-namespace"
	if err := store.CreateDefinition(ctx, state.DefinitionInput{DefinitionID: definitionID, Version: 1,
		DefinitionHash: "dur027-" + runID, Graph: []byte(`{"entry":"root","nodes":[{"id":"root","kind":"activity"}]}`),
		ActivityVersions: []byte(`{"root":"1"}`), EffectClasses: []byte(`{"root":"PURE_ACTIVITY"}`)}); err != nil {
		report.Failure = fmt.Sprintf("create definition: %v", err)
		return report, errors.New(report.Failure)
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if _, err := store.Pool().Exec(cleanupCtx, `DELETE FROM engine.workflow_executions WHERE namespace = $1`, namespace); err != nil && runErr == nil {
			runErr = fmt.Errorf("cleanup workflows: %w", err)
			report.Failure = runErr.Error()
		}
		if _, err := store.Pool().Exec(cleanupCtx, `DELETE FROM engine.workflow_definitions WHERE definition_id = $1`, definitionID); err != nil && runErr == nil {
			runErr = fmt.Errorf("cleanup definition: %w", err)
			report.Failure = runErr.Error()
		}
	}()

	for index := 0; index < caseCount; index++ {
		mode := "normal_renewal"
		if index >= normalCases {
			mode = "owner_pause"
		}
		caseID := fmt.Sprintf("%s-case-%02d", runID, index+1)
		caseResult, err := runCase(ctx, store, arm, namespace, definitionID, caseID, mode, index == 0)
		report.Cases = append(report.Cases, caseResult)
		if err != nil {
			report.Failure = err.Error()
			return report, err
		}
		report.CompletedCases++
		report.RenewalCount += caseResult.Renewals
		report.FencedOldOwnerWrites += caseResult.FencedOldOwner
		report.LockWaits += caseResult.LockWaits
		report.LockWaitSeconds += caseResult.LockWaitSeconds
		if caseResult.FalseTakeover {
			report.FalseTakeovers++
		}
		if caseResult.Takeover {
			report.Takeovers++
		}
		if caseResult.UsefulRecovery {
			report.UsefulRecoveries++
		}
		if caseResult.LockContention {
			report.LockContentionCases++
		}
	}
	var delays []float64
	for _, item := range report.Cases {
		if item.TakeoverDelayMS > 0 {
			delays = append(delays, item.TakeoverDelayMS)
		}
	}
	report.TakeoverDelayMS = makeInterval(delays)
	if report.CompletedCases != caseCount || report.Takeovers != pauseCases || report.UsefulRecoveries != pauseCases || report.FalseTakeovers != 0 || report.FencedOldOwnerWrites != pauseCases*3 || report.LockContentionCases != 1 {
		report.Failure = fmt.Sprintf("run reconciliation failed: completed=%d takeovers=%d useful=%d false=%d fenced=%d lock_cases=%d", report.CompletedCases, report.Takeovers, report.UsefulRecoveries, report.FalseTakeovers, report.FencedOldOwnerWrites, report.LockContentionCases)
		return report, errors.New(report.Failure)
	}
	report.Status = "PASS"
	return report, nil
}

func runCase(ctx context.Context, store *state.Store, arm leaseArm, namespace, definitionID, caseID, mode string, lockCase bool) (caseReport, error) {
	result := caseReport{CaseID: caseID, Mode: mode, Status: "FAIL"}
	ownerA := state.NewID()
	ownerB := state.NewID()
	var lease state.Lease
	var acquired bool
	var workflowID string
	var err error
	for partitionID := int16(0); partitionID < 16; partitionID++ {
		candidate := fmt.Sprintf("%s-workflow-p%d", caseID, partitionID)
		mapped, mapErr := partition.ID(candidate)
		if mapErr != nil || int16(mapped) != partitionID {
			continue
		}
		lease, acquired, err = store.AcquireLease(ctx, partitionID, ownerA, arm.TTL)
		if err != nil {
			return result, fmt.Errorf("%s acquire owner A: %w", caseID, err)
		}
		if acquired {
			result.Partition = lease.PartitionID
			workflowID = candidate
			break
		}
	}
	if !acquired {
		return result, fmt.Errorf("%s could not acquire a free partition", caseID)
	}
	if workflowID == "" {
		return result, fmt.Errorf("%s could not find a free partition with a matching workflow ID", caseID)
	}
	created, err := store.CreateWorkflow(ctx, state.CreateWorkflowInput{WorkflowID: workflowID, Namespace: namespace,
		SubmissionKey: caseID + "-submission", SubmissionPayloadHash: "sub-v1:" + caseID,
		DefinitionID: definitionID, DefinitionVersion: 1, PartitionID: lease.PartitionID,
		InitialNodeID: "root", InitialInput: []byte(`{"case":"dur027"}`), ActorID: "dur027-client"})
	if err != nil || !created.Created {
		_ = store.ReleaseLease(ctx, state.LeaseRef{PartitionID: lease.PartitionID, OwnerID: ownerA, Epoch: lease.Epoch})
		return result, fmt.Errorf("%s create workflow: created=%v err=%v", caseID, created.Created, err)
	}
	result.WorkflowID = workflowID
	refA := state.LeaseRef{PartitionID: lease.PartitionID, OwnerID: ownerA, Epoch: lease.Epoch}
	refB := state.LeaseRef{PartitionID: lease.PartitionID, OwnerID: ownerB, Epoch: lease.Epoch + 1}

	if lockCase {
		waits, seconds, err := measureLockContention(ctx, store, lease.PartitionID, ownerB)
		if err != nil {
			_ = store.ReleaseLease(ctx, refA)
			return result, fmt.Errorf("%s lock contention: %w", caseID, err)
		}
		result.LockContention = true
		result.LockWaits = waits
		result.LockWaitSeconds = seconds
	}

	if mode == "normal_renewal" {
		// Workflow creation and the lock-wait fixture happen before the
		// renewal window. Arm the lease immediately so the 100 ms control
		// measures renewal traffic rather than setup latency.
		lease, err = store.RenewLease(ctx, refA, arm.TTL)
		if err != nil {
			_ = store.ReleaseLease(ctx, refA)
			return result, fmt.Errorf("%s initial renewal: %w", caseID, err)
		}
		result.Renewals++
		deadline := time.Now().Add(400 * time.Millisecond)
		for time.Now().Before(deadline) {
			time.Sleep(arm.TTL / 4)
			lease, err = store.RenewLease(ctx, refA, arm.TTL)
			if err != nil {
				_ = store.ReleaseLease(ctx, refA)
				return result, fmt.Errorf("%s renewal: %w", caseID, err)
			}
			result.Renewals++
		}
		if _, competitorAcquired, err := store.AcquireLease(ctx, lease.PartitionID, ownerB, arm.TTL); err != nil {
			_ = store.ReleaseLease(ctx, refA)
			return result, fmt.Errorf("%s false-takeover probe: %w", caseID, err)
		} else if competitorAcquired {
			_ = store.ReleaseLease(ctx, state.LeaseRef{PartitionID: lease.PartitionID, OwnerID: ownerB, Epoch: lease.Epoch + 1})
			_ = store.ReleaseLease(ctx, refA)
			return result, fmt.Errorf("%s competitor acquired an unexpired lease", caseID)
		} else {
			result.FalseTakeover = true
		}
		if err := applyTransition(ctx, store, refA, workflowID, "normal renewal"); err != nil {
			_ = store.ReleaseLease(ctx, refA)
			return result, fmt.Errorf("%s useful owner transition: %w", caseID, err)
		}
		if err := store.ReleaseLease(ctx, refA); err != nil {
			return result, fmt.Errorf("%s release: %w", caseID, err)
		}
		result.Acquired = true
		result.Status = "PASS"
		return result, nil
	}

	startPause := time.Now()
	wait := time.Until(lease.LeaseExpiresAt) + 30*time.Millisecond
	if wait > 0 {
		time.Sleep(wait)
	}
	var takeoverLease state.Lease
	var tookOver bool
	for time.Since(startPause) < 3*arm.TTL {
		takeoverLease, tookOver, err = store.AcquireLease(ctx, lease.PartitionID, ownerB, arm.TTL)
		if err != nil {
			return result, fmt.Errorf("%s takeover: %w", caseID, err)
		}
		if tookOver {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !tookOver {
		return result, fmt.Errorf("%s owner pause did not produce takeover", caseID)
	}
	result.Takeover = true
	result.TakeoverDelayMS = float64(time.Since(startPause).Microseconds()) / 1000
	refB = state.LeaseRef{PartitionID: takeoverLease.PartitionID, OwnerID: ownerB, Epoch: takeoverLease.Epoch}
	for staleIndex, staleErr := range []error{
		func() error { _, err := store.RenewLease(ctx, refA, arm.TTL); return err }(),
		store.ReleaseLease(ctx, refA),
		applyTransition(ctx, store, refA, workflowID, "stale owner"),
	} {
		if !errors.Is(staleErr, state.ErrLeaseNotOwned) {
			_ = store.ReleaseLease(ctx, refB)
			return result, fmt.Errorf("%s stale owner operation %d = %v, want ErrLeaseNotOwned", caseID, staleIndex+1, staleErr)
		}
		result.FencedOldOwner++
	}
	if err := applyTransition(ctx, store, refB, workflowID, "takeover recovery"); err != nil {
		_ = store.ReleaseLease(ctx, refB)
		return result, fmt.Errorf("%s post-takeover transition: %w", caseID, err)
	}
	if err := store.ReleaseLease(ctx, refB); err != nil {
		return result, fmt.Errorf("%s new-owner release: %w", caseID, err)
	}
	result.Acquired = true
	result.UsefulRecovery = true
	result.Status = "PASS"
	return result, nil
}

func applyTransition(ctx context.Context, store *state.Store, lease state.LeaseRef, workflowID, reason string) error {
	nodeState := state.StateWaitingActivity
	return store.ApplyOwnerTransition(ctx, state.OwnerTransitionInput{Lease: lease, WorkflowID: workflowID,
		ExpectedRevision: 1, NewState: state.StateWaitingActivity, ActorID: "dur027-scheduler",
		Reason: "DUR027_" + strings.ToUpper(strings.ReplaceAll(reason, " ", "_")), NodeID: "root", Iteration: 0,
		NodeState: &nodeState})
}

func measureLockContention(ctx context.Context, store *state.Store, partitionID int16, ownerID string) (uint64, float64, error) {
	metrics := store.Telemetry()
	before := telemetry.Snapshot{}
	if metrics != nil {
		before = metrics.Snapshot()
	}
	tx, err := store.Pool().Begin(ctx)
	if err != nil {
		return 0, 0, err
	}
	var lockedPartition int16
	if err := tx.QueryRow(ctx, `SELECT partition_id FROM engine.partition_leases WHERE partition_id = $1 FOR UPDATE`, partitionID).Scan(&lockedPartition); err != nil {
		_ = tx.Rollback(ctx)
		return 0, 0, err
	}
	done := make(chan error, 1)
	go func() {
		_, _, acquireErr := store.AcquireLease(ctx, partitionID, ownerID, time.Second)
		done <- acquireErr
	}()
	time.Sleep(25 * time.Millisecond)
	if err := tx.Commit(ctx); err != nil {
		return 0, 0, err
	}
	if err := <-done; err != nil {
		return 0, 0, err
	}
	if metrics == nil {
		return 0, 0, nil
	}
	after := metrics.Snapshot()
	return after.LockWaitCount - before.LockWaitCount, after.LockWaitSeconds - before.LockWaitSeconds, nil
}

func summarize(runs []runReport) []summaryRow {
	byArm := map[string][]runReport{}
	for _, run := range runs {
		byArm[run.Arm] = append(byArm[run.Arm], run)
	}
	rows := make([]summaryRow, 0, len(byArm))
	for _, arm := range leaseArms() {
		group := byArm[arm.ID]
		if len(group) == 0 {
			continue
		}
		takeovers := make([]float64, 0, len(group))
		var renewals, waits, takeoversCount, useful, falseCount int
		var waitSeconds float64
		for _, run := range group {
			takeovers = append(takeovers, run.TakeoverDelayMS.Median)
			renewals += run.RenewalCount
			waits += int(run.LockWaits)
			waitSeconds += run.LockWaitSeconds
			takeoversCount += run.Takeovers
			useful += run.UsefulRecoveries
			falseCount += run.FalseTakeovers
		}
		rows = append(rows, summaryRow{Arm: arm.ID, TTLMS: arm.TTLMS, Runs: len(group),
			MedianTakeoverMS: median(takeovers), RenewalsPerRun: float64(renewals) / float64(len(group)),
			LockWaitsPerRun: float64(waits) / float64(len(group)), LockWaitSecondsPerRun: waitSeconds / float64(len(group)),
			FalseTakeovers: falseCount, UsefulRecoveries: useful, Takeovers: takeoversCount})
	}
	return rows
}

func deriveConclusions(result artifact) conclusions {
	if len(result.Summary) < 3 {
		return conclusions{Recovery: "unresolved: fewer than three lease arms completed", RenewalTraffic: "unresolved: incomplete campaign", Safety: "not established: incomplete campaign", LockContention: "unresolved: incomplete campaign", PauseInterpretation: "The owner-pause fixture is not a process-crash equivalence."}
	}
	short, middle, long := result.Summary[0], result.Summary[1], result.Summary[2]
	recovery := fmt.Sprintf("Observed owner-pause takeover medians were %.1f ms at %d ms, %.1f ms at %d ms, and %.1f ms at %d ms; these are expiry-detection measurements, not process-crash recovery times.", short.MedianTakeoverMS, short.TTLMS, middle.MedianTakeoverMS, middle.TTLMS, long.MedianTakeoverMS, long.TTLMS)
	renewal := fmt.Sprintf("Normal-renewal traffic averaged %.1f, %.1f, and %.1f renewals per run for %d, %d, and %d ms TTLs; shorter leases require more renewal traffic in this bounded cohort.", short.RenewalsPerRun, middle.RenewalsPerRun, long.RenewalsPerRun, short.TTLMS, middle.TTLMS, long.TTLMS)
	safety := fmt.Sprintf("Safety control passed: %d false takeovers, %d useful recoveries for %d takeovers, and %d stale-owner writes rejected.", result.Validation.FalseTakeovers, result.Validation.UsefulRecoveries, result.Validation.Takeovers, result.Validation.FencedOldOwners)
	locks := fmt.Sprintf("The campaign measured %d lock-contention cases and %.4f cumulative lock-wait seconds; this is local PostgreSQL contention evidence, not a production capacity estimate.", result.Validation.LockContentionCases, totalLockWaitSeconds(result.Runs))
	return conclusions{Recovery: recovery, RenewalTraffic: renewal, Safety: safety, LockContention: locks, PauseInterpretation: "Owner pauses stop renewal inside the campaign process. They intentionally exercise lease expiry and fencing without claiming equivalence to a hard kill or host failure."}
}

func validateArtifact(ctx context.Context, store *state.Store, result artifact, repeats int) validationReport {
	v := validationReport{ExpectedRuns: len(leaseArms()) * repeats, ObservedRuns: len(result.Runs), ExpectedCases: len(leaseArms()) * repeats * caseCount, Status: "FAIL"}
	for _, run := range result.Runs {
		if run.Status != "PASS" || run.CompletedCases != caseCount {
			v.Failure = fmt.Sprintf("run %s did not pass reconciliation", run.RunID)
			return v
		}
		v.CompletedCases += run.CompletedCases
		v.Takeovers += run.Takeovers
		v.UsefulRecoveries += run.UsefulRecoveries
		v.FalseTakeovers += run.FalseTakeovers
		v.FencedOldOwners += run.FencedOldOwnerWrites
		v.LockContentionCases += run.LockContentionCases
	}
	if v.ObservedRuns != v.ExpectedRuns || v.CompletedCases != v.ExpectedCases || v.Takeovers != repeats*len(leaseArms())*pauseCases || v.UsefulRecoveries != v.Takeovers || v.FalseTakeovers != 0 || v.FencedOldOwners != v.Takeovers*3 || v.LockContentionCases != v.ExpectedRuns {
		v.Failure = fmt.Sprintf("aggregate reconciliation failed: runs=%d/%d cases=%d/%d takeovers=%d useful=%d false=%d fenced=%d lock_cases=%d", v.ObservedRuns, v.ExpectedRuns, v.CompletedCases, v.ExpectedCases, v.Takeovers, v.UsefulRecoveries, v.FalseTakeovers, v.FencedOldOwners, v.LockContentionCases)
		return v
	}
	// The run namespaces are deleted in each run's defer. Assert that no
	// durable workflow survives; this catches cleanup failures instead of
	// allowing a successful-looking artifact to contaminate later studies.
	if err := store.Pool().QueryRow(ctx, `SELECT count(*) FROM engine.workflow_executions WHERE namespace LIKE 'dur027-lease-%'`).Scan(&v.CleanWorkflowRows); err != nil {
		v.Failure = fmt.Sprintf("check workflow cleanup: %v", err)
		return v
	}
	if err := store.Pool().QueryRow(ctx, `SELECT count(*) FROM engine.workflow_definitions WHERE definition_id LIKE 'dur027-lease-%'`).Scan(&v.CleanDefinitionRows); err != nil {
		v.Failure = fmt.Sprintf("check definition cleanup: %v", err)
		return v
	}
	if v.CleanWorkflowRows != 0 || v.CleanDefinitionRows != 0 {
		v.Failure = fmt.Sprintf("cleanup left workflow rows=%d definition rows=%d", v.CleanWorkflowRows, v.CleanDefinitionRows)
		return v
	}
	v.Status = "PASS"
	return v
}

func makeInterval(values []float64) interval {
	if len(values) == 0 {
		return interval{}
	}
	sort.Float64s(values)
	return interval{Count: len(values), Min: values[0], Median: median(values), Max: values[len(values)-1]}
}

func median(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	copyValues := append([]float64(nil), values...)
	sort.Float64s(copyValues)
	middle := len(copyValues) / 2
	if len(copyValues)%2 == 1 {
		return copyValues[middle]
	}
	return (copyValues[middle-1] + copyValues[middle]) / 2
}

func totalLockWaitSeconds(runs []runReport) float64 {
	var total float64
	for _, run := range runs {
		total += run.LockWaitSeconds
	}
	return total
}

func currentCommit() string {
	command := exec.Command("git", "rev-parse", "HEAD")
	output, err := command.Output()
	if err != nil {
		return "unavailable"
	}
	return strings.TrimSpace(string(output))
}

func databaseURLFromEnvironment() string {
	for _, name := range []string{"DURABLE_DATABASE_URL", "DATABASE_URL"} {
		if value := os.Getenv(name); value != "" {
			return value
		}
	}
	return ""
}

func writeArtifact(path string, result artifact) error {
	if parent := filepath.Dir(path); parent != "." {
		if err := os.MkdirAll(parent, 0o755); err != nil {
			return fmt.Errorf("create artifact directory: %w", err)
		}
	}
	encoded, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return fmt.Errorf("encode artifact: %w", err)
	}
	encoded = append(encoded, '\n')
	if err := os.WriteFile(path, encoded, 0o644); err != nil {
		return fmt.Errorf("write artifact: %w", err)
	}
	return nil
}

func writeFailure(path, failure string) {
	result := artifact{SchemaVersion: artifactSchema, Status: "FAIL", GeneratedAt: time.Now().UTC(), Failure: failure,
		Validation: validationReport{Status: "FAIL", Failure: failure}}
	if err := writeArtifact(path, result); err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
}
