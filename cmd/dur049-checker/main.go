// dur049-checker exports and independently checks durable workflow evidence
// for the AWS recovery campaign. Live mode reads PostgreSQL; snapshot mode
// checks the exported Trace after the database has been destroyed.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"durable-agent-execution-engine/internal/invariants"
	"durable-agent-execution-engine/internal/state"
)

type report struct {
	InvariantsCovered    []string                   `json:"invariants_covered"`
	InvariantsNotCovered []string                   `json:"invariants_not_covered"`
	InvariantOutcomes    map[string]string          `json:"invariant_outcomes"`
	NonTerminal          []string                   `json:"non_terminal"`
	Campaign             *invariants.CampaignReport `json:"campaign,omitempty"`
	Status               string                     `json:"status"`
	WorkflowIDs          []string                   `json:"workflow_ids,omitempty"`
	Trace                *invariants.Trace          `json:"trace,omitempty"`
	Valid                bool                       `json:"valid"`
	Violations           []string                   `json:"violations,omitempty"`
	Fence                *fenceReport               `json:"fence,omitempty"`
}

type fenceReport struct {
	PreFaultRevision    int64 `json:"pre_fault_revision"`
	RecoveryRevision    int64 `json:"recovery_revision"`
	RecoveryEpoch       int64 `json:"recovery_epoch"`
	SupersededCount     int   `json:"superseded_epoch_transitions_after_recovery_boundary"`
	StaleAttemptNumber  int64 `json:"stale_attempt_number"`
	StaleAttemptFound   bool  `json:"stale_attempt_found"`
	StaleAttemptCurrent bool  `json:"stale_attempt_current"`
	StaleResultRecorded bool  `json:"stale_result_recorded"`
}

func main() {
	workflowIDs := flag.String("workflow-ids", "", "comma-separated workflow IDs to export")
	snapshotPath := flag.String("snapshot", "", "offline Trace JSON snapshot")
	databaseURL := flag.String("database-url", os.Getenv("DATABASE_URL"), "PostgreSQL URL for live export")
	preFaultRevision := flag.Int64("pre-fault-revision", 0, "durable revision captured before fault injection; the lower bound for post-fault fence checks")
	recoveryRevision := flag.Int64("recovery-revision", 0, "TIMEOUT_REPLACEMENT revision that establishes the recovered owner boundary")
	recoveryEpoch := flag.Int64("recovery-epoch", 0, "epoch at the first new-owner acquisition")
	staleAttemptNumber := flag.Int64("stale-attempt-number", 0, "old attempt number that must remain unrecorded and non-current")
	campaignPath := flag.String("arrivals", "", "DUR-053 arrival manifest; requires witness-export")
	witnessPath := flag.String("witness-export", "", "external worker witness export")
	namespace := flag.String("namespace", "", "export every durable submission in this namespace")
	requireTerminal := flag.Bool("require-terminal", false, "fail on non-terminal accepted work")
	flag.Parse()
	if (*campaignPath == "") != (*witnessPath == "") {
		fatal(fmt.Errorf("arrivals and witness-export required together"))
	}
	emit := func(trace invariants.Trace, ids []string) bool {
		verdict, fence := check(trace, *preFaultRevision, *recoveryRevision, *recoveryEpoch, *staleAttemptNumber)
		if *requireTerminal && len(verdict.NonTerminal) > 0 {
			verdict.Violations = append(verdict.Violations, "accepted work is non-terminal")
			verdict.Valid = false
		}
		covered, missing, outcomes := invariants.PureCoverage()
		result := report{Status: status(verdict.Valid), WorkflowIDs: ids, Trace: &trace, Valid: verdict.Valid, Violations: verdict.Violations, Fence: fence, NonTerminal: verdict.NonTerminal, InvariantsCovered: covered, InvariantsNotCovered: missing, InvariantOutcomes: outcomes}
		// Legacy reports do not claim a PURE campaign's reconciliation coverage.
		for key := range outcomes {
			outcomes[key] = "not-covered (legacy trace check only)"
		}
		result.InvariantsCovered = []string{"supplied history/results/submission/attempt/transport structural checks"}
		result.InvariantsNotCovered = append(missing, "campaign-wide submission and external witness reconciliation NOT COVERED without -arrivals")
		if *campaignPath != "" {
			var arrivals invariants.Arrivals
			var witness invariants.WitnessExport
			read := func(path string, target any) {
				b, e := os.ReadFile(path)
				if e != nil {
					fatal(e)
				}
				if e = json.Unmarshal(b, target); e != nil {
					fatal(e)
				}
			}
			read(*campaignPath, &arrivals)
			read(*witnessPath, &witness)
			campaign := invariants.CheckCampaign(trace, arrivals, witness)
			result.Campaign = &campaign
			result.Valid = result.Valid && campaign.Valid
			result.Status = status(result.Valid)
			result.InvariantsCovered = campaign.InvariantsCovered
			result.InvariantsNotCovered = campaign.InvariantsNotCovered
			result.InvariantOutcomes = campaign.InvariantOutcomes
		}
		write(result)
		return result.Valid
	}

	if *snapshotPath != "" {
		data, err := os.ReadFile(*snapshotPath)
		if err != nil {
			fatal(err)
		}
		var trace invariants.Trace
		if err := json.Unmarshal(data, &trace); err != nil {
			fatal(err)
		}
		if !emit(trace, nil) {
			os.Exit(1)
		}
		return
	}

	ids := splitIDs(*workflowIDs)
	if (len(ids) == 0 && *namespace == "") || *databaseURL == "" {
		fatal(fmt.Errorf("workflow-ids and database-url are required for live export"))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	store, err := state.NewCheckerFromURL(ctx, *databaseURL)
	if err != nil {
		fatal(err)
	}
	defer store.Close()
	var trace invariants.Trace
	if *namespace != "" {
		trace, err = invariants.LoadNamespace(ctx, store, *namespace)
		ids = nil
		for _, workflow := range trace.Workflows {
			ids = append(ids, workflow.WorkflowID)
		}
	} else {
		trace, err = invariants.Load(ctx, store, ids)
	}
	if err != nil {
		fatal(err)
	}
	if !emit(trace, ids) {
		os.Exit(1)
	}
}

func check(trace invariants.Trace, preFaultRevision, recoveryRevision, recoveryEpoch, staleAttemptNumber int64) (invariants.Verdict, *fenceReport) {
	verdict := invariants.Check(trace)
	if preFaultRevision == 0 && recoveryRevision == 0 && recoveryEpoch == 0 && staleAttemptNumber == 0 {
		return verdict, nil
	}
	fence := &fenceReport{PreFaultRevision: preFaultRevision, RecoveryRevision: recoveryRevision, RecoveryEpoch: recoveryEpoch, StaleAttemptNumber: staleAttemptNumber}
	if preFaultRevision <= 0 || recoveryRevision <= preFaultRevision || recoveryEpoch <= 0 {
		verdict.Violations = append(verdict.Violations, "recovery fence evidence requires positive pre-fault revision, a later recovery revision, and a positive epoch")
	}
	for _, record := range trace.History {
		if record.Revision > recoveryRevision && record.SchedulerEpoch != nil && *record.SchedulerEpoch < recoveryEpoch {
			fence.SupersededCount++
		}
	}
	if fence.SupersededCount != 0 {
		verdict.Violations = append(verdict.Violations, fmt.Sprintf("%d post-recovery history records carry an epoch older than the recovery owner", fence.SupersededCount))
	}
	if staleAttemptNumber > 0 {
		for _, attempt := range trace.Attempts {
			if attempt.AttemptNumber == staleAttemptNumber {
				fence.StaleAttemptFound = true
				fence.StaleAttemptCurrent = attempt.IsCurrent
				fence.StaleResultRecorded = attempt.ResultRecorded
				break
			}
		}
		if !fence.StaleAttemptFound {
			verdict.Violations = append(verdict.Violations, "stale attempt evidence is missing from the durable trace")
		} else {
			if fence.StaleAttemptCurrent {
				verdict.Violations = append(verdict.Violations, "stale attempt remains current after recovery")
			}
			if fence.StaleResultRecorded {
				verdict.Violations = append(verdict.Violations, "stale attempt has a recorded result after the stale-result rejection probe")
			}
		}
	}
	verdict.Valid = len(verdict.Violations) == 0
	return verdict, fence
}

func splitIDs(value string) []string {
	var ids []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			ids = append(ids, item)
		}
	}
	return ids
}

func status(valid bool) string {
	if valid {
		return "PASS"
	}
	return "FAIL"
}

func write(value report) {
	if err := json.NewEncoder(os.Stdout).Encode(value); err != nil {
		fatal(err)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
