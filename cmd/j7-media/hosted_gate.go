package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"path/filepath"
	"slices"

	"durable-agent-execution-engine/internal/restoreledger"
)

const hostedCap = 350

type registeredSwitch struct {
	Task     string            `json:"task"`
	Default  string            `json:"unset_default"`
	Values   map[string]string `json:"values"`
	Profiles []string          `json:"permitted_enabled_profiles"`
	Unset    []string          `json:"required_unset_profiles"`
}

type hostedCandidate struct {
	Schema      string            `json:"schema"`
	RelaySwitch string            `json:"relay_switch"`
	RelayValue  string            `json:"relay_value"`
	RuntimePath string            `json:"runtime_mode_source"`
	Files       map[string]string `json:"sealed_files"`
}

var hostedCandidateFiles = []string{
	"deploy/j7/controller", "deploy/j7/source-overlay.json", "deploy/j7/target-overlay.json",
	"deploy/j7/workload.json", "deploy/j7/kind.json", "deploy/runtime-switches.json",
	"deploy/k8s/tools.lock.json", "experiments/portfolio/postgres-restore/protocol.md",
}

func selectRelay(registry map[string]registeredSwitch) (string, registeredSwitch, error) {
	var name string
	row := registeredSwitch{}
	for key, sw := range registry {
		if sw.Task != "J9a" {
			continue
		}
		if name != "" {
			return "", row, errors.New("J9A_ADMISSION: more than one repaired relay registration")
		}
		name, row = key, sw
	}
	if name == "" || !slices.Contains(row.Profiles, "J7_source") || !slices.Contains(row.Profiles, "J7_target") ||
		slices.Contains(row.Unset, "J7_source") || slices.Contains(row.Unset, "J7_target") {
		return "", row, errors.New("J9A_ADMISSION: reviewed repaired relay mode is missing for J7_source and J7_target")
	}
	return name, row, nil
}

func readRelayAdmission(root string) (string, registeredSwitch, error) {
	var row registeredSwitch
	var registry struct {
		Schema   int                         `json:"schema_version"`
		Switches map[string]registeredSwitch `json:"switches"`
	}
	raw, err := evidenceFile(root, "deploy/runtime-switches.json")
	if err != nil {
		return "", row, err
	}
	if err := json.Unmarshal(raw, &registry); err != nil {
		return "", row, err
	}
	if registry.Schema != 1 {
		return "", row, errors.New("J9A_ADMISSION: unsupported switch registry")
	}
	name, row, err := selectRelay(registry.Switches)
	if err != nil {
		return "", row, err
	}
	if name != repairedRelaySwitch || row.Default != "0" || row.Values["1"] == "" {
		return "", row, errors.New("J9A_ADMISSION: registry does not declare the reviewed repaired mode")
	}
	return name, row, nil
}

func checkCandidate(root string) (hostedCandidate, string, error) {
	var c hostedCandidate
	name, _, err := readRelayAdmission(root)
	if err != nil {
		return c, "", err
	}
	sha, err := readFaultJSON(filepath.Join(root, "deploy/j7/candidate.json"), &c)
	if err != nil {
		return c, "", errors.New("J7_CANDIDATE_ADMISSION: tracked reviewed candidate and fixture controller are required")
	}
	if c.Schema != "j7.hosted-candidate.v1" || c.RelaySwitch != name || c.RelayValue != "1" || c.RuntimePath != relayReaderPath {
		return c, "", errors.New("J9A_ADMISSION: candidate does not select the registered nondefault repaired mode")
	}
	expected, err := candidateDigests(root)
	if err != nil {
		return c, "", err
	}
	if len(c.Files) != len(expected) {
		return c, "", errors.New("J7_CANDIDATE_ADMISSION: unexpected fixture inventory")
	}
	for p, digest := range expected {
		if c.Files[p] != digest {
			return c, "", fmt.Errorf("J7_CANDIDATE_ADMISSION: candidate file is unbound: %s", p)
		}
	}
	return c, sha, nil
}

type stageMinutes struct {
	Bootstrap int `json:"bootstrap"`
	Drill     int `json:"drill"`
	Between   int `json:"between"`
	Controls  int `json:"controls"`
	Final     int `json:"final"`
}

func forecast(mode string, elapsed, completed int, bootstrapped bool, obs stageMinutes) (int, error) {
	total, controls := 1, 15
	if mode == "measurement" {
		total, controls = 3, 0
	} else if mode != "qualification" {
		return 0, errors.New("forecast requires qualification or measurement")
	}
	if elapsed < 0 || elapsed > hostedCap || completed < 0 || completed > total || (!bootstrapped && completed != 0) ||
		min(obs.Bootstrap, obs.Drill, obs.Between, obs.Controls, obs.Final) < 0 ||
		max(obs.Bootstrap, obs.Drill, obs.Between, obs.Controls, obs.Final) > hostedCap {
		return 0, errors.New("invalid forecast stage timings")
	}
	left := total - completed
	reserve := left*max(20, obs.Drill) + max(0, left-1)*max(5, obs.Between) + max(35, obs.Final)
	if !bootstrapped {
		reserve += max(35, obs.Bootstrap)
	}
	if controls != 0 {
		reserve += max(controls, obs.Controls)
	}
	if reserve > hostedCap-elapsed {
		return reserve, errors.New("J7_FORECAST: remaining registered and observed stages plus final reserve exceed the cap")
	}
	return reserve, nil
}

type qualificationSeal struct {
	Schema       string       `json:"schema"`
	Run          uint64       `json:"run_id"`
	Attempt      uint32       `json:"run_attempt"`
	Revision     string       `json:"revision"`
	Candidate    string       `json:"candidate_sha256"`
	Status       string       `json:"status"`
	DrillStatus  string       `json:"drill_status"`
	DrillIndex   string       `json:"drill_index_sha256"`
	Controls     []string     `json:"controls"`
	ReplayPassed bool         `json:"replay_passed"`
	CleanupOK    bool         `json:"cleanup_verified"`
	Times        stageMinutes `json:"stage_minutes"`
}

var qualificationControls = []string{
	"PRE_CUT_MARKER_MISSING", "POST_CUT_MARKER_PRESENT", "WAL_ARCHIVE_GAP", "CONSUMER_GENERATION_MISMATCH",
}

func verifyQualification(q qualificationSeal, run uint64, sha, revision string) error {
	if q.Schema != "j7.qualification.v1" || q.Run != run || q.Attempt != 1 || q.Revision != revision ||
		q.Candidate != sha || q.Status != "passed" || q.DrillStatus != "passed" ||
		!restoreledger.IsSHA256(q.DrillIndex) || !q.ReplayPassed || !q.CleanupOK || !slices.Equal(q.Controls, qualificationControls) {
		return errors.New("J7_QUALIFICATION: measured jobs require passing replay, controls and cleanup on the exact reviewed candidate")
	}
	return nil
}

func runHostedGate(args []string, out, diagnostics io.Writer) error {
	flags := flag.NewFlagSet(args[0], flag.ContinueOnError)
	flags.SetOutput(diagnostics)
	root := flags.String("root", "", "absolute checked out repository root")
	mode := flags.String("mode", "", "qualification or measurement")
	onlyForecast := flags.Bool("forecast-only", false, "evaluate registered and observed stage allowances")
	times := flags.String("stage-times", "", "qualification or observed timing JSON")
	elapsed := flags.Int("elapsed-minutes", 0, "ceil of elapsed runner time")
	completed := flags.Int("completed-drills", 0, "completed unique drills")
	bootstrapped := flags.Bool("bootstrapped", false, "runner bootstrap is complete")
	qualification := flags.String("qualification", "", "downloaded exact qualification artifact seal")
	run := flags.Uint64("qualification-run", 0, "reviewed qualification run ID")
	revision := flags.String("revision", "", "exact reviewed candidate git commit")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("hosted gate accepts flags only")
	}
	var obs stageMinutes
	if *times != "" {
		if _, err := readFaultJSON(*times, &obs); err != nil {
			return err
		}
	}
	sha := ""
	if !*onlyForecast {
		if err := checkedEvidenceDir(*root); err != nil {
			return err
		}
		var err error
		_, sha, err = checkCandidate(*root)
		if err != nil {
			return err
		}
		if *mode == "measurement" {
			var q qualificationSeal
			if _, err := readFaultJSON(*qualification, &q); err != nil {
				return err
			}
			if err := verifyQualification(q, *run, sha, *revision); err != nil {
				return err
			}
			obs = q.Times
		}
	}
	reserve, err := forecast(*mode, *elapsed, *completed, *bootstrapped, obs)
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(struct {
		Candidate string `json:"candidate_sha256,omitempty"`
		Forecast  int    `json:"forecast_minutes"`
		Remaining int    `json:"remaining_minutes"`
	}{sha, reserve, hostedCap - *elapsed})
}
