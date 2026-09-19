// Command dur027-lease runs the DUR-027 lease tradeoff campaign.
//
// The final matrix crosses three frozen scheduler-lease TTLs with two real
// fault types: killing a durable target process tree and pausing its renewal
// until expiry before resuming it. Each episode follows takeover through the
// first useful affected-work progress in PostgreSQL.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"durable-agent-execution-engine/internal/state"
	"durable-agent-execution-engine/internal/telemetry"
)

const (
	artifactSchema        = "dur027-lease.v2"
	pilotSchema           = "dur027-lease-pilot.v1"
	settingsCount         = 3
	faultTypeCount        = 2
	episodesPerConfig     = 10
	expectedEpisodes      = settingsCount * faultTypeCount * episodesPerConfig
	staleWritesPerEpisode = 3
)

type leaseArm struct {
	ID    string        `json:"id"`
	TTL   time.Duration `json:"ttl"`
	TTLMS int           `json:"ttl_ms"`
}

type boundaryRecord struct {
	Boundary            string    `json:"boundary"`
	WorkflowID          string    `json:"workflow_id"`
	DefinitionID        string    `json:"definition_id"`
	Namespace           string    `json:"namespace"`
	Partition           int16     `json:"partition"`
	OwnerID             string    `json:"owner_id"`
	Epoch               int64     `json:"epoch"`
	AttemptNumber       int64     `json:"attempt_number"`
	ClaimToken          string    `json:"claim_token"`
	LeaseExpiresAt      time.Time `json:"lease_expires_at"`
	AttemptDeadlineAt   time.Time `json:"attempt_deadline_at"`
	RenewalIntervalMS   int       `json:"renewal_interval_ms"`
	Mode                string    `json:"mode"`
	RenewalsBeforeFault int       `json:"renewals_before_fault"`
}

type episodeReport struct {
	EpisodeID             string  `json:"episode_id"`
	FaultType             string  `json:"fault_type"`
	Status                string  `json:"status"`
	TTLMS                 int     `json:"ttl_ms"`
	RenewalIntervalMS     int     `json:"renewal_interval_ms"`
	WorkflowID            string  `json:"workflow_id"`
	Partition             int16   `json:"partition"`
	InjectedAt            string  `json:"injected_at_utc"`
	TakeoverAt            string  `json:"takeover_at_utc"`
	UsefulProgressAt      string  `json:"useful_progress_at_utc"`
	TakeoverDelayMS       float64 `json:"takeover_delay_ms"`
	UsefulProgressDelayMS float64 `json:"useful_progress_delay_ms"`
	DefinitionID          string  `json:"definition_id"`
	TargetDead            bool    `json:"target_dead"`
	TargetResumedStale    bool    `json:"target_resumed_stale"`
	LeaseHeldAfterInject  bool    `json:"lease_held_after_injection"`
	LeasePreserved        bool    `json:"lease_preserved_before_injection"`
	Takeover              bool    `json:"takeover"`
	UsefulProgress        bool    `json:"useful_progress"`
	FalseTakeover         bool    `json:"false_takeover"`
	FencedOldOwnerWrites  int     `json:"fenced_old_owner_writes"`
	RenewalsBeforeFault   int     `json:"renewals_before_fault"`
	RenewalsAfterTakeover int     `json:"renewals_after_takeover"`
	LockWaits             uint64  `json:"lock_waits"`
	LockWaitSeconds       float64 `json:"lock_wait_seconds"`
	Failure               string  `json:"failure,omitempty"`
}

type runReport struct {
	ConfigID              string          `json:"config_id"`
	FaultType             string          `json:"fault_type"`
	TTLMS                 int             `json:"ttl_ms"`
	RenewalIntervalMS     int             `json:"renewal_interval_ms"`
	Status                string          `json:"status"`
	EpisodeCount          int             `json:"episode_count"`
	CompletedEpisodes     int             `json:"completed_episodes"`
	Takeovers             int             `json:"takeovers"`
	UsefulProgress        int             `json:"useful_progress"`
	FalseTakeovers        int             `json:"false_takeovers"`
	FencedOldOwnerWrites  int             `json:"fenced_old_owner_writes"`
	RenewalsBeforeFault   int             `json:"renewals_before_fault"`
	RenewalsAfterTakeover int             `json:"renewals_after_takeover"`
	LockContentionCases   int             `json:"lock_contention_cases"`
	LockWaits             uint64          `json:"lock_waits"`
	LockWaitSeconds       float64         `json:"lock_wait_seconds"`
	TargetDead            int             `json:"target_dead"`
	TargetResumedStale    int             `json:"target_resumed_stale"`
	LeaseHeldAfterInject  int             `json:"lease_held_after_injection"`
	TakeoverDelayMS       interval        `json:"takeover_delay_ms_interval"`
	UsefulProgressDelayMS interval        `json:"useful_progress_delay_ms_interval"`
	Episodes              []episodeReport `json:"episodes"`
	Failure               string          `json:"failure,omitempty"`
}

type interval struct {
	Count  int     `json:"count"`
	Min    float64 `json:"min"`
	Median float64 `json:"median"`
	Max    float64 `json:"max"`
}

type pilotArm struct {
	ID                 string   `json:"id"`
	TTLMS              int      `json:"ttl_ms"`
	RenewalIntervalMS  int      `json:"renewal_interval_ms"`
	TransactionDelayMS interval `json:"transaction_delay_ms"`
	SchedulingDelayMS  interval `json:"scheduling_delay_ms"`
}

type pilotArtifact struct {
	SchemaVersion string     `json:"schema_version"`
	Status        string     `json:"status"`
	GeneratedAt   time.Time  `json:"generated_at_utc"`
	GitCommit     string     `json:"git_commit"`
	Seed          int64      `json:"seed"`
	SamplesPerArm int        `json:"samples_per_arm"`
	Arms          []pilotArm `json:"arms"`
	Failure       string     `json:"failure,omitempty"`
}

type summaryRow struct {
	ConfigID                   string   `json:"config_id"`
	FaultType                  string   `json:"fault_type"`
	TTLMS                      int      `json:"ttl_ms"`
	RenewalIntervalMS          int      `json:"renewal_interval_ms"`
	Episodes                   int      `json:"episodes"`
	TakeoverDelayMS            interval `json:"takeover_delay_ms"`
	UsefulProgressDelayMS      interval `json:"useful_progress_delay_ms"`
	RenewalsBeforeFaultPerEp   float64  `json:"renewals_before_fault_per_episode"`
	RenewalsAfterTakeoverPerEp float64  `json:"renewals_after_takeover_per_episode"`
	FalseTakeovers             int      `json:"false_takeovers"`
	UsefulProgress             int      `json:"useful_progress"`
	Takeovers                  int      `json:"takeovers"`
	LockWaitsPerEpisode        float64  `json:"lock_waits_per_episode"`
}

type validationReport struct {
	ExpectedConfigurations int    `json:"expected_configurations"`
	ObservedConfigurations int    `json:"observed_configurations"`
	ExpectedEpisodes       int    `json:"expected_episodes"`
	CompletedEpisodes      int    `json:"completed_episodes"`
	Takeovers              int    `json:"takeovers"`
	UsefulProgress         int    `json:"useful_progress"`
	FalseTakeovers         int    `json:"false_takeovers"`
	FencedOldOwners        int    `json:"fenced_old_owner_writes"`
	LockContentionCases    int    `json:"lock_contention_cases"`
	CrashTargetsDead       int    `json:"crash_targets_dead"`
	PausedTargetsResumed   int    `json:"paused_targets_resumed_stale"`
	LeaseHeldAfterInject   int    `json:"lease_held_after_injection"`
	CleanWorkflowRows      int    `json:"clean_workflow_rows"`
	CleanDefinitionRows    int    `json:"clean_definition_rows"`
	Status                 string `json:"status"`
	Failure                string `json:"failure,omitempty"`
}

type conclusions struct {
	TakeoverAndProgress string `json:"takeover_and_progress"`
	FaultComparison     string `json:"fault_comparison"`
	RenewalTraffic      string `json:"renewal_traffic"`
	Safety              string `json:"safety"`
	LockContention      string `json:"lock_contention"`
	Limit               string `json:"pause_and_crash_limit"`
}

type artifact struct {
	SchemaVersion string           `json:"schema_version"`
	Status        string           `json:"status"`
	GeneratedAt   time.Time        `json:"generated_at_utc"`
	GitCommit     string           `json:"git_commit"`
	Pilot         pilotArtifact    `json:"pilot"`
	Protocol      map[string]any   `json:"protocol"`
	Runs          []runReport      `json:"runs"`
	Summary       []summaryRow     `json:"summary"`
	Validation    validationReport `json:"validation"`
	Conclusions   conclusions      `json:"conclusions"`
	Limitations   []string         `json:"limitations"`
	Failure       string           `json:"failure,omitempty"`
}

type config struct {
	DatabaseURL   string
	OutputPath    string
	Pilot         bool
	PilotInput    string
	FixtureBinary string
	Seed          int64
	Episodes      int
}

type fixtureProcess struct {
	cmd     *exec.Cmd
	records <-chan boundaryRecord
	wait    <-chan error
	done    <-chan struct{}
}

func main() {
	var cfg config
	flag.StringVar(&cfg.DatabaseURL, "database-url", databaseURLFromEnvironment(), "PostgreSQL connection URL")
	flag.StringVar(&cfg.OutputPath, "output", "experiments/m7/dur027/results.json", "artifact output path")
	flag.BoolVar(&cfg.Pilot, "pilot", false, "run the pilot instead of the final campaign")
	flag.StringVar(&cfg.PilotInput, "pilot-input", "", "committed pilot artifact for the final campaign")
	flag.StringVar(&cfg.FixtureBinary, "fixture-binary", "", "dur027 crash fixture binary")
	flag.Int64Var(&cfg.Seed, "seed", 270027, "deterministic campaign seed")
	flag.IntVar(&cfg.Episodes, "episodes-per-config", episodesPerConfig, "fault episodes per configuration")
	flag.Parse()
	if strings.TrimSpace(cfg.DatabaseURL) == "" || cfg.Seed == 0 {
		writeFailure(cfg.OutputPath, "a database URL and non-zero seed are required")
		os.Exit(1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	store, err := state.NewFromURL(ctx, cfg.DatabaseURL)
	if err != nil {
		writeFailure(cfg.OutputPath, fmt.Sprintf("open database: %v", err))
		os.Exit(1)
	}
	defer store.Close()
	store.SetTelemetry(telemetry.New("dur027-lease"))
	var result any
	if cfg.Pilot {
		result = runPilot(ctx, store, cfg)
	} else {
		if cfg.Episodes != episodesPerConfig || strings.TrimSpace(cfg.FixtureBinary) == "" || strings.TrimSpace(cfg.PilotInput) == "" {
			writeFailure(cfg.OutputPath, "final campaign requires the fixture binary, pilot input, and exactly 10 episodes per configuration")
			os.Exit(1)
		}
		pilot, pilotErr := loadPilot(cfg.PilotInput)
		if pilotErr != nil {
			writeFailure(cfg.OutputPath, pilotErr.Error())
			os.Exit(1)
		}
		result = runCampaign(ctx, store, cfg, pilot)
	}
	if err := writeJSONArtifact(cfg.OutputPath, result); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	status := artifactStatus(result)
	if status != "PASS" {
		fmt.Fprintln(os.Stderr, artifactFailure(result))
		os.Exit(1)
	}
}

func leaseArms() []leaseArm {
	return []leaseArm{{ID: "ttl_100ms", TTL: 100 * time.Millisecond, TTLMS: 100},
		{ID: "ttl_250ms", TTL: 250 * time.Millisecond, TTLMS: 250},
		{ID: "ttl_750ms", TTL: 750 * time.Millisecond, TTLMS: 750}}
}

func faultTypes() []string { return []string{"owner_crash", "owner_pause"} }

func runPilot(ctx context.Context, store *state.Store, cfg config) pilotArtifact {
	pilot := pilotArtifact{SchemaVersion: pilotSchema, Status: "FAIL", GeneratedAt: time.Now().UTC(), GitCommit: currentCommit(), Seed: cfg.Seed, SamplesPerArm: 3}
	for _, arm := range leaseArms() {
		var transactionValues, schedulingValues []float64
		for sample := 0; sample < pilot.SamplesPerArm; sample++ {
			owner := state.NewID()
			lease, err := acquireAvailableLease(ctx, store, owner, arm.TTL)
			if err != nil {
				pilot.Failure = fmt.Sprintf("pilot %s sample %d acquire: %v", arm.ID, sample, err)
				return pilot
			}
			interval := time.Duration(arm.TTLMS/3) * time.Millisecond
			for renewal := 0; renewal < 3; renewal++ {
				scheduled := time.Now().Add(interval)
				time.Sleep(time.Until(scheduled))
				start := time.Now()
				if lease, err = store.RenewLease(ctx, state.LeaseRef{PartitionID: lease.PartitionID, OwnerID: owner, Epoch: lease.Epoch}, arm.TTL); err != nil {
					pilot.Failure = fmt.Sprintf("pilot %s sample %d renewal: %v", arm.ID, sample, err)
					return pilot
				}
				transactionValues = append(transactionValues, time.Since(start).Seconds()*1000)
				schedulingValues = append(schedulingValues, time.Since(scheduled).Seconds()*1000)
			}
			if err := store.ReleaseLease(ctx, state.LeaseRef{PartitionID: lease.PartitionID, OwnerID: owner, Epoch: lease.Epoch}); err != nil {
				pilot.Failure = fmt.Sprintf("pilot %s sample %d release: %v", arm.ID, sample, err)
				return pilot
			}
		}
		pilot.Arms = append(pilot.Arms, pilotArm{ID: arm.ID, TTLMS: arm.TTLMS, RenewalIntervalMS: arm.TTLMS / 3,
			TransactionDelayMS: makeInterval(transactionValues), SchedulingDelayMS: makeInterval(schedulingValues)})
	}
	pilot.Status = "PASS"
	return pilot
}

func runCampaign(ctx context.Context, store *state.Store, cfg config, pilot pilotArtifact) artifact {
	result := artifact{SchemaVersion: artifactSchema, Status: "FAIL", GeneratedAt: time.Now().UTC(), GitCommit: currentCommit(), Pilot: pilot,
		Protocol: map[string]any{"seed": cfg.Seed, "lease_settings": leaseArms(), "fault_types": faultTypes(),
			"configurations": settingsCount * faultTypeCount, "episodes_per_configuration": episodesPerConfig,
			"total_fault_episodes": expectedEpisodes, "renewal_interval": "TTL / 3", "pilot": "three samples per TTL; settings frozen before final run",
			"progress_boundary": "timeout of the claimed affected attempt commits a replacement attempt"},
		Limitations: []string{"single-node PostgreSQL on Docker Desktop/WSL2", "owner crash and pause are bounded local process fixtures, not host or storage failure claims", "the harness measures lease recovery mechanics, not production throughput or multi-host scale", "short TTLs are measurement settings, not deployment recommendations"}}
	for _, arm := range leaseArms() {
		for _, faultType := range faultTypes() {
			runID := fmt.Sprintf("dur027-%s-%s", arm.ID, faultType)
			report, err := runConfig(ctx, store, cfg, arm, faultType, runID)
			result.Runs = append(result.Runs, report)
			if err != nil && result.Failure == "" {
				result.Failure = err.Error()
			}
		}
	}
	result.Summary = summarize(result.Runs)
	result.Validation = validateArtifact(ctx, store, result)
	result.Conclusions = deriveConclusions(result)
	if result.Validation.Status == "PASS" && result.Failure == "" {
		result.Status = "PASS"
	} else if result.Failure == "" {
		result.Failure = result.Validation.Failure
	}
	return result
}

func runConfig(ctx context.Context, store *state.Store, cfg config, arm leaseArm, faultType, configID string) (runReport, error) {
	report := runReport{ConfigID: configID, FaultType: faultType, TTLMS: arm.TTLMS, RenewalIntervalMS: arm.TTLMS / 3, Status: "FAIL", EpisodeCount: episodesPerConfig}
	for episode := 1; episode <= episodesPerConfig; episode++ {
		episodeID := fmt.Sprintf("%s-episode-%02d-%d", configID, episode, cfg.Seed)
		item, err := runEpisode(ctx, store, cfg, arm, faultType, episodeID)
		report.Episodes = append(report.Episodes, item)
		if err != nil {
			report.Failure = err.Error()
			return report, err
		}
		report.CompletedEpisodes++
		report.Takeovers++
		report.UsefulProgress++
		report.FencedOldOwnerWrites += item.FencedOldOwnerWrites
		report.RenewalsBeforeFault += item.RenewalsBeforeFault
		report.RenewalsAfterTakeover += item.RenewalsAfterTakeover
		report.LockWaits += item.LockWaits
		report.LockWaitSeconds += item.LockWaitSeconds
		report.LockContentionCases++
		if item.FalseTakeover {
			report.FalseTakeovers++
		}
		if item.TargetDead {
			report.TargetDead++
		}
		if item.TargetResumedStale {
			report.TargetResumedStale++
		}
		if item.LeaseHeldAfterInject {
			report.LeaseHeldAfterInject++
		}
	}
	var takeoverValues, progressValues []float64
	for _, item := range report.Episodes {
		takeoverValues = append(takeoverValues, item.TakeoverDelayMS)
		progressValues = append(progressValues, item.UsefulProgressDelayMS)
	}
	report.TakeoverDelayMS = makeInterval(takeoverValues)
	report.UsefulProgressDelayMS = makeInterval(progressValues)
	if report.CompletedEpisodes != episodesPerConfig || report.Takeovers != episodesPerConfig || report.UsefulProgress != episodesPerConfig || report.FalseTakeovers != 0 || report.FencedOldOwnerWrites != episodesPerConfig*staleWritesPerEpisode {
		report.Failure = fmt.Sprintf("configuration reconciliation failed: completed=%d takeovers=%d progress=%d false=%d fenced=%d", report.CompletedEpisodes, report.Takeovers, report.UsefulProgress, report.FalseTakeovers, report.FencedOldOwnerWrites)
		return report, errors.New(report.Failure)
	}
	report.Status = "PASS"
	return report, nil
}

func runEpisode(ctx context.Context, store *state.Store, cfg config, arm leaseArm, faultType, episodeID string) (report episodeReport, runErr error) {
	report = episodeReport{EpisodeID: episodeID, FaultType: faultType, TTLMS: arm.TTLMS, RenewalIntervalMS: arm.TTLMS / 3, Status: "FAIL"}
	resumeFile := filepath.Join(os.TempDir(), "dur027-"+episodeID+".resume")
	_ = os.Remove(resumeFile)
	process, err := startFixture(cfg, arm, faultType, episodeID, resumeFile)
	if err != nil {
		return report, err
	}
	defer func() {
		select {
		case <-process.done:
		default:
			_ = killProcessTree(process.cmd)
			<-process.done
		}
		_ = os.Remove(resumeFile)
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if report.WorkflowID != "" {
			if _, cleanupErr := store.Pool().Exec(cleanupCtx, `DELETE FROM engine.workflow_executions WHERE namespace = $1`, "dur027-crash-"+episodeID); cleanupErr != nil && runErr == nil {
				runErr = cleanupErr
				report.Failure = cleanupErr.Error()
			}
		}
		if report.DefinitionID != "" {
			if _, cleanupErr := store.Pool().Exec(cleanupCtx, `DELETE FROM engine.workflow_definitions WHERE definition_id = $1`, report.DefinitionID); cleanupErr != nil && runErr == nil {
				runErr = cleanupErr
				report.Failure = cleanupErr.Error()
			}
		}
	}()
	ready, err := awaitBoundary(ctx, process.records, "owner_ready")
	if err != nil {
		return report, err
	}
	report.WorkflowID, report.DefinitionID, report.Partition = ready.WorkflowID, ready.DefinitionID, ready.Partition
	probeOwner := state.NewID()
	_, probeAcquired, err := store.AcquireLease(ctx, ready.Partition, probeOwner, arm.TTL)
	if err != nil {
		return report, err
	}
	if probeAcquired {
		_ = store.ReleaseLease(ctx, state.LeaseRef{PartitionID: ready.Partition, OwnerID: probeOwner, Epoch: ready.Epoch + 1})
		report.FalseTakeover = true
		return report, fmt.Errorf("%s competitor acquired before fault injection", episodeID)
	}
	report.LeasePreserved = true
	report.RenewalsBeforeFault = ready.RenewalsBeforeFault
	injectedAt := time.Now().UTC()
	report.InjectedAt = injectedAt.Format(time.RFC3339Nano)
	if faultType == "owner_crash" {
		if err := killProcessTree(process.cmd); err != nil {
			return report, fmt.Errorf("kill owner target: %w", err)
		}
		select {
		case waitErr := <-process.wait:
			if waitErr == nil {
				return report, errors.New("crash target exited cleanly")
			}
		case <-time.After(10 * time.Second):
			return report, errors.New("crash target did not die after process-tree kill")
		}
		report.TargetDead = true
	} else {
		if err := os.WriteFile(resumeFile, []byte("pause\n"), 0o644); err != nil {
			return report, err
		}
		if _, err := awaitBoundary(ctx, process.records, "owner_paused_expired"); err != nil {
			return report, err
		}
	}
	report.LeaseHeldAfterInject = leaseRowMatches(ctx, store, ready.Partition, ready.OwnerID, ready.Epoch)
	newOwner := state.NewID()
	startTakeover := time.Now()
	var takeover state.Lease
	var acquired bool
	for time.Since(startTakeover) < 10*time.Second {
		takeover, acquired, err = store.AcquireLease(ctx, ready.Partition, newOwner, arm.TTL)
		if err != nil {
			return report, err
		}
		if acquired {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !acquired {
		return report, fmt.Errorf("%s did not take over lease", episodeID)
	}
	takeoverAt := time.Now().UTC()
	report.Takeover = true
	report.TakeoverDelayMS = takeoverAt.Sub(injectedAt).Seconds() * 1000
	newRef := state.LeaseRef{PartitionID: takeover.PartitionID, OwnerID: newOwner, Epoch: takeover.Epoch}
	oldRef := state.LeaseRef{PartitionID: ready.Partition, OwnerID: ready.OwnerID, Epoch: ready.Epoch}
	for index, staleErr := range []error{
		func() error { _, renewErr := store.RenewLease(ctx, oldRef, arm.TTL); return renewErr }(),
		store.ReleaseLease(ctx, oldRef),
		fenceTransition(ctx, store, oldRef, ready.WorkflowID),
	} {
		if !errors.Is(staleErr, state.ErrLeaseNotOwned) {
			_ = store.ReleaseLease(ctx, newRef)
			return report, fmt.Errorf("%s stale owner operation %d = %v", episodeID, index+1, staleErr)
		}
		report.FencedOldOwnerWrites++
	}
	lockLease, err := acquireLockFixture(ctx, store, ready.Partition)
	if err != nil {
		_ = store.ReleaseLease(ctx, newRef)
		return report, err
	}
	waits, waitSeconds, err := measureLockContention(ctx, store, lockLease.PartitionID, state.NewID(), 5*time.Second)
	_ = store.ReleaseLease(ctx, state.LeaseRef{PartitionID: lockLease.PartitionID, OwnerID: lockLease.OwnerID, Epoch: lockLease.Epoch})
	if err != nil {
		_ = store.ReleaseLease(ctx, newRef)
		return report, err
	}
	report.LockWaits, report.LockWaitSeconds = waits, waitSeconds
	attempt, err := store.GetAttempt(ctx, ready.WorkflowID, "root", 0, ready.AttemptNumber)
	if err != nil {
		_ = store.ReleaseLease(ctx, newRef)
		return report, err
	}
	deadline := ready.AttemptDeadlineAt
	if attempt.HeartbeatDeadline != nil {
		deadline = *attempt.HeartbeatDeadline
	}
	for time.Now().Before(deadline) {
		time.Sleep(maxDuration(5*time.Millisecond, arm.TTL/3))
		if _, err := store.RenewLease(ctx, newRef, arm.TTL); err != nil {
			_ = store.ReleaseLease(ctx, newRef)
			return report, err
		}
		report.RenewalsAfterTakeover++
	}
	if _, err := store.RenewLease(ctx, newRef, arm.TTL); err != nil {
		_ = store.ReleaseLease(ctx, newRef)
		return report, err
	}
	report.RenewalsAfterTakeover++
	workflow, err := store.GetWorkflow(ctx, ready.WorkflowID)
	if err != nil {
		_ = store.ReleaseLease(ctx, newRef)
		return report, err
	}
	timeoutResult, err := store.TimeoutAttempt(ctx, state.TimeoutInput{Lease: newRef, WorkflowID: ready.WorkflowID, NodeID: "root", Iteration: 0, AttemptNumber: ready.AttemptNumber, ExpectedRevision: workflow.Revision, ActorID: "dur027-recovery", DispatchLease: 2 * arm.TTL})
	if err != nil || timeoutResult.ReplacementNumber == nil {
		_ = store.ReleaseLease(ctx, newRef)
		return report, fmt.Errorf("%s useful recovery timeout: result=%+v err=%v", episodeID, timeoutResult, err)
	}
	progressAt := time.Now().UTC()
	report.UsefulProgress = true
	report.UsefulProgressAt = progressAt.Format(time.RFC3339Nano)
	report.UsefulProgressDelayMS = progressAt.Sub(injectedAt).Seconds() * 1000
	if faultType == "owner_pause" {
		if err := os.WriteFile(resumeFile, []byte("resume\n"), 0o644); err != nil {
			_ = store.ReleaseLease(ctx, newRef)
			return report, err
		}
		if _, err := awaitBoundary(ctx, process.records, "owner_resumed_stale"); err != nil {
			_ = store.ReleaseLease(ctx, newRef)
			return report, err
		}
		report.TargetResumedStale = true
		select {
		case waitErr := <-process.wait:
			if waitErr != nil {
				return report, fmt.Errorf("paused target exit: %w", waitErr)
			}
		case <-time.After(10 * time.Second):
			return report, errors.New("paused target did not exit after resume")
		}
	}
	if err := store.ReleaseLease(ctx, newRef); err != nil {
		return report, err
	}
	if report.FencedOldOwnerWrites != staleWritesPerEpisode || !report.Takeover || !report.UsefulProgress || !report.LeaseHeldAfterInject || !report.LeasePreserved || (faultType == "owner_crash" && !report.TargetDead) || (faultType == "owner_pause" && !report.TargetResumedStale) {
		return report, fmt.Errorf("%s episode reconciliation failed", episodeID)
	}
	report.Status = "PASS"
	return report, nil
}

func startFixture(cfg config, arm leaseArm, faultType, episodeID, resumeFile string) (fixtureProcess, error) {
	cmd := exec.Command(cfg.FixtureBinary, "-database-url", cfg.DatabaseURL, "-run-id", episodeID, "-mode", faultType, "-ttl-ms", strconv.Itoa(arm.TTLMS), "-resume-file", resumeFile)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fixtureProcess{}, err
	}
	cmd.Stderr = os.Stderr
	prepareProcess(cmd)
	if err := cmd.Start(); err != nil {
		return fixtureProcess{}, err
	}
	records := make(chan boundaryRecord, 8)
	go func() {
		defer close(records)
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			if !strings.Contains(line, "{") {
				continue
			}
			var record boundaryRecord
			if json.Unmarshal([]byte(line[strings.Index(line, "{"):]), &record) == nil {
				records <- record
			}
		}
	}()
	wait := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		wait <- cmd.Wait()
		close(done)
	}()
	return fixtureProcess{cmd: cmd, records: records, wait: wait, done: done}, nil
}

func awaitBoundary(ctx context.Context, records <-chan boundaryRecord, wanted string) (boundaryRecord, error) {
	for {
		select {
		case record, ok := <-records:
			if !ok {
				return boundaryRecord{}, fmt.Errorf("target closed before boundary %s", wanted)
			}
			if record.Boundary == wanted {
				return record, nil
			}
		case <-ctx.Done():
			return boundaryRecord{}, fmt.Errorf("waiting for %s: %w", wanted, ctx.Err())
		}
	}
}

func acquireAvailableLease(ctx context.Context, store *state.Store, owner string, ttl time.Duration) (state.Lease, error) {
	for partitionID := int16(0); partitionID < 16; partitionID++ {
		lease, acquired, err := store.AcquireLease(ctx, partitionID, owner, ttl)
		if err != nil {
			return state.Lease{}, err
		}
		if acquired {
			return lease, nil
		}
	}
	return state.Lease{}, errors.New("no available partition lease")
}

func acquireLockFixture(ctx context.Context, store *state.Store, excludedPartition int16) (state.Lease, error) {
	for partitionID := int16(0); partitionID < 16; partitionID++ {
		if partitionID == excludedPartition {
			continue
		}
		owner := state.NewID()
		lease, acquired, err := store.AcquireLease(ctx, partitionID, owner, 5*time.Second)
		if err != nil {
			return state.Lease{}, err
		}
		if acquired {
			return lease, nil
		}
	}
	return state.Lease{}, errors.New("no available lock-contention partition")
}

func leaseRowMatches(ctx context.Context, store *state.Store, partitionID int16, owner string, epoch int64) bool {
	var storedOwner *string
	var storedEpoch int64
	if err := store.Pool().QueryRow(ctx, `SELECT owner_id::text, epoch FROM engine.partition_leases WHERE partition_id = $1`, partitionID).Scan(&storedOwner, &storedEpoch); err != nil {
		return false
	}
	return storedOwner != nil && *storedOwner == owner && storedEpoch == epoch
}

func fenceTransition(ctx context.Context, store *state.Store, lease state.LeaseRef, workflowID string) error {
	workflow, err := store.GetWorkflow(ctx, workflowID)
	if err != nil {
		return err
	}
	nodeState := state.StateWaitingActivity
	return store.ApplyOwnerTransition(ctx, state.OwnerTransitionInput{Lease: lease, WorkflowID: workflowID, ExpectedRevision: workflow.Revision,
		NewState: state.StateWaitingActivity, ActorID: "dur027-stale", Reason: "DUR027_STALE_OWNER", NodeID: "root", Iteration: 0, NodeState: &nodeState})
}

func measureLockContention(ctx context.Context, store *state.Store, partitionID int16, ownerID string, ttl time.Duration) (uint64, float64, error) {
	metrics := store.Telemetry()
	before := telemetry.Snapshot{}
	if metrics != nil {
		before = metrics.Snapshot()
	}
	tx, err := store.Pool().Begin(ctx)
	if err != nil {
		return 0, 0, err
	}
	var locked int16
	if err := tx.QueryRow(ctx, `SELECT partition_id FROM engine.partition_leases WHERE partition_id = $1 FOR UPDATE`, partitionID).Scan(&locked); err != nil {
		_ = tx.Rollback(ctx)
		return 0, 0, err
	}
	done := make(chan error, 1)
	go func() { _, _, acquireErr := store.AcquireLease(ctx, partitionID, ownerID, ttl); done <- acquireErr }()
	time.Sleep(10 * time.Millisecond)
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
	rows := make([]summaryRow, 0, len(runs))
	for _, run := range runs {
		rows = append(rows, summaryRow{ConfigID: run.ConfigID, FaultType: run.FaultType, TTLMS: run.TTLMS, RenewalIntervalMS: run.RenewalIntervalMS,
			Episodes: run.CompletedEpisodes, TakeoverDelayMS: run.TakeoverDelayMS, UsefulProgressDelayMS: run.UsefulProgressDelayMS,
			RenewalsBeforeFaultPerEp: float64(run.RenewalsBeforeFault) / float64(maxInt(1, run.CompletedEpisodes)), RenewalsAfterTakeoverPerEp: float64(run.RenewalsAfterTakeover) / float64(maxInt(1, run.CompletedEpisodes)),
			FalseTakeovers: run.FalseTakeovers, UsefulProgress: run.UsefulProgress, Takeovers: run.Takeovers, LockWaitsPerEpisode: float64(run.LockWaits) / float64(maxInt(1, run.CompletedEpisodes))})
	}
	return rows
}

func deriveConclusions(result artifact) conclusions {
	if result.Validation.Status != "PASS" {
		return conclusions{TakeoverAndProgress: "unresolved: campaign reconciliation failed", FaultComparison: "unresolved: incomplete fault matrix", RenewalTraffic: "unresolved: incomplete campaign", Safety: "not established", LockContention: "unresolved", Limit: "No fault conclusion is promoted until both process-crash and owner-pause episodes reconcile."}
	}
	parts := make([]string, 0, len(result.Summary))
	for _, row := range result.Summary {
		parts = append(parts, fmt.Sprintf("%s/%dms takeover %.1f ms [%.1f, %.1f], useful progress %.1f ms [%.1f, %.1f]", row.FaultType, row.TTLMS, row.TakeoverDelayMS.Median, row.TakeoverDelayMS.Min, row.TakeoverDelayMS.Max, row.UsefulProgressDelayMS.Median, row.UsefulProgressDelayMS.Min, row.UsefulProgressDelayMS.Max))
	}
	return conclusions{
		TakeoverAndProgress: "Observed per-episode takeover and first useful affected-work progress: " + strings.Join(parts, "; ") + ".",
		FaultComparison:     "Both owner-crash process-tree kills and owner-pause/resume episodes reached a new-owner replacement transition; comparisons remain bounded to this six-configuration local matrix.",
		RenewalTraffic:      fmt.Sprintf("Renewal intervals are recorded as TTL/3: %d stale-owner-safe episodes included %d renewals before injection and %d while the new owner waited for affected work.", result.Validation.CompletedEpisodes, totalBefore(result.Runs), totalAfter(result.Runs)),
		Safety:              fmt.Sprintf("Safety control passed: %d false takeovers, %d useful recoveries for %d takeovers, and %d stale-owner writes rejected.", result.Validation.FalseTakeovers, result.Validation.UsefulProgress, result.Validation.Takeovers, result.Validation.FencedOldOwners),
		LockContention:      fmt.Sprintf("Measured %d PostgreSQL lock-contention cases across the matrix with %.4f cumulative lock-wait seconds.", result.Validation.LockContentionCases, totalLockWaitSeconds(result.Runs)),
		Limit:               "The crash fixture is a killed local process tree and the pause fixture is an in-process renewal pause; neither establishes host failure, storage durability, multi-host behavior, or production throughput.",
	}
}

func validateArtifact(ctx context.Context, store *state.Store, result artifact) validationReport {
	v := validationReport{ExpectedConfigurations: settingsCount * faultTypeCount, ObservedConfigurations: len(result.Runs), ExpectedEpisodes: expectedEpisodes, Status: "FAIL"}
	if result.Pilot.Status != "PASS" || len(result.Pilot.Arms) != settingsCount {
		v.Failure = "pilot is missing or failed"
		return v
	}
	for _, run := range result.Runs {
		if run.Status != "PASS" || run.CompletedEpisodes != episodesPerConfig {
			v.Failure = fmt.Sprintf("configuration %s did not pass", run.ConfigID)
			return v
		}
		v.CompletedEpisodes += run.CompletedEpisodes
		v.Takeovers += run.Takeovers
		v.UsefulProgress += run.UsefulProgress
		v.FalseTakeovers += run.FalseTakeovers
		v.FencedOldOwners += run.FencedOldOwnerWrites
		v.LockContentionCases += run.LockContentionCases
		v.CrashTargetsDead += run.TargetDead
		v.PausedTargetsResumed += run.TargetResumedStale
		v.LeaseHeldAfterInject += run.LeaseHeldAfterInject
	}
	if v.ObservedConfigurations != v.ExpectedConfigurations || v.CompletedEpisodes != v.ExpectedEpisodes || v.Takeovers != expectedEpisodes || v.UsefulProgress != expectedEpisodes || v.FalseTakeovers != 0 || v.FencedOldOwners != expectedEpisodes*staleWritesPerEpisode || v.LockContentionCases != expectedEpisodes || v.CrashTargetsDead != expectedEpisodes/2 || v.PausedTargetsResumed != expectedEpisodes/2 || v.LeaseHeldAfterInject != expectedEpisodes {
		v.Failure = fmt.Sprintf("matrix reconciliation failed: configs=%d/%d episodes=%d/%d takeovers=%d progress=%d false=%d fenced=%d crash_dead=%d pause_resumed=%d held=%d locks=%d", v.ObservedConfigurations, v.ExpectedConfigurations, v.CompletedEpisodes, v.ExpectedEpisodes, v.Takeovers, v.UsefulProgress, v.FalseTakeovers, v.FencedOldOwners, v.CrashTargetsDead, v.PausedTargetsResumed, v.LeaseHeldAfterInject, v.LockContentionCases)
		return v
	}
	if err := store.Pool().QueryRow(ctx, `SELECT count(*) FROM engine.workflow_executions WHERE namespace LIKE 'dur027-crash-%'`).Scan(&v.CleanWorkflowRows); err != nil {
		v.Failure = err.Error()
		return v
	}
	if err := store.Pool().QueryRow(ctx, `SELECT count(*) FROM engine.workflow_definitions WHERE definition_id LIKE 'dur027-crash-%'`).Scan(&v.CleanDefinitionRows); err != nil {
		v.Failure = err.Error()
		return v
	}
	if v.CleanWorkflowRows != 0 || v.CleanDefinitionRows != 0 {
		v.Failure = fmt.Sprintf("cleanup left workflows=%d definitions=%d", v.CleanWorkflowRows, v.CleanDefinitionRows)
		return v
	}
	v.Status = "PASS"
	return v
}

func loadPilot(path string) (pilotArtifact, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return pilotArtifact{}, fmt.Errorf("read pilot: %w", err)
	}
	var pilot pilotArtifact
	if err := json.Unmarshal(data, &pilot); err != nil {
		return pilotArtifact{}, fmt.Errorf("decode pilot: %w", err)
	}
	if pilot.Status != "PASS" || len(pilot.Arms) != settingsCount {
		return pilotArtifact{}, errors.New("pilot must be PASS with three frozen arms")
	}
	for index, arm := range leaseArms() {
		if pilot.Arms[index].ID != arm.ID || pilot.Arms[index].TTLMS != arm.TTLMS || pilot.Arms[index].RenewalIntervalMS != arm.TTLMS/3 {
			return pilotArtifact{}, fmt.Errorf("pilot arm %d does not match frozen setting", index)
		}
	}
	return pilot, nil
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
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	middle := len(sorted) / 2
	if len(sorted)%2 == 1 {
		return sorted[middle]
	}
	return (sorted[middle-1] + sorted[middle]) / 2
}
func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}
func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
func totalBefore(runs []runReport) int {
	total := 0
	for _, run := range runs {
		total += run.RenewalsBeforeFault
	}
	return total
}
func totalAfter(runs []runReport) int {
	total := 0
	for _, run := range runs {
		total += run.RenewalsAfterTakeover
	}
	return total
}
func totalLockWaitSeconds(runs []runReport) float64 {
	total := 0.0
	for _, run := range runs {
		total += run.LockWaitSeconds
	}
	return total
}

func currentCommit() string {
	output, err := exec.Command("git", "rev-parse", "HEAD").Output()
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
func artifactStatus(value any) string {
	switch item := value.(type) {
	case artifact:
		return item.Status
	case pilotArtifact:
		return item.Status
	default:
		return "FAIL"
	}
}
func artifactFailure(value any) string {
	switch item := value.(type) {
	case artifact:
		return item.Failure
	case pilotArtifact:
		return item.Failure
	default:
		return "artifact failed"
	}
}

func writeJSONArtifact(path string, value any) error {
	if parent := filepath.Dir(path); parent != "." {
		if err := os.MkdirAll(parent, 0o755); err != nil {
			return err
		}
	}
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')
	return os.WriteFile(path, encoded, 0o644)
}

func writeFailure(path, failure string) {
	_ = writeJSONArtifact(path, map[string]any{"schema_version": artifactSchema, "status": "FAIL", "generated_at_utc": time.Now().UTC(), "failure": failure})
}
