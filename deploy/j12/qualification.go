package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"syscall"
	"time"
)

const (
	authSwitch    = "RUNTIME_PUBLIC_API_AUTH"
	relaySwitch   = "RUNTIME_RELAY_FULL_BATCH_LOOP"
	issuer        = "https://j12-fixture.invalid"
	audience      = "durable-j12-api"
	clusterIssuer = "https://kubernetes.default.svc.cluster.local"
	maxEvidence   = 16 << 20
)

type switchRow struct {
	Task       string            `json:"task"`
	Default    string            `json:"unset_default"`
	Values     map[string]string `json:"values"`
	Profiles   []string          `json:"permitted_enabled_profiles"`
	Unset      []string          `json:"required_unset_profiles"`
	Migrations []string          `json:"migrations_added"`
}

type registry struct {
	Version     int                  `json:"schema_version"`
	Switches    map[string]switchRow `json:"switches"`
	Inventories map[string]struct {
		Base       string          `json:"base"`
		Additional []string        `json:"additional"`
		Source     json.RawMessage `json:"source"`
		Versions   string          `json:"versions"`
	} `json:"migration_inventories"`
}

func selectRelay(r registry, name, value string) error {
	if r.Version != 1 || name != relaySwitch || value != "1" {
		return errors.New("J9A_ADMISSION: provide the registered repaired relay switch and literal 1")
	}
	var selected string
	for name, candidate := range r.Switches {
		if candidate.Task != "J9a" {
			continue
		}
		if selected != "" {
			return errors.New("J9A_ADMISSION: more than one repaired relay registration")
		}
		selected = name
	}
	row := r.Switches[name]
	if selected != name || row.Default != "0" || row.Values[value] == "" ||
		!slices.Contains(row.Profiles, "J12") || slices.Contains(row.Unset, "J12") {
		return errors.New("J9A_ADMISSION: reviewed repaired relay mode is missing for J12")
	}
	return nil
}

const requesterMigration = "migrations/j12/000026_j12_workflow_requesters.up.sql"

func migrationInventory(r registry, base []string) ([]string, error) {
	row, profile := r.Switches[authSwitch], r.Inventories["J12"]
	baseProfile := r.Inventories["runtime_base"]
	if row.Task != "J12" || row.Default != "0" || row.Values["1"] == "" || !slices.Equal(row.Profiles, []string{"J12"}) ||
		profile.Base != "runtime_base" || !slices.Equal(profile.Additional, []string{requesterMigration}) ||
		!slices.Equal(profile.Additional, row.Migrations) || len(base) != 23 ||
		string(baseProfile.Source) != `"migrations/*.up.sql"` || baseProfile.Versions != "000001-000023 inclusive" {
		return nil, errors.New("J12 migration registry or base inventory mismatch")
	}
	for name, other := range r.Inventories {
		if name != "J12" && slices.Contains(other.Additional, requesterMigration) {
			return nil, errors.New("J12 migration escaped its registered profile")
		}
	}
	for i, path := range base {
		if !strings.HasPrefix(filepath.Base(path), fmt.Sprintf("%06d_", i+1)) {
			return nil, errors.New("J12 base migration version mismatch")
		}
	}
	return append(slices.Clone(base), requesterMigration), nil
}

type inputs struct {
	run, attempt, source, output, scratch, runtime, relayName, relayValue string
	started                                                               int64
}

type result struct {
	ID      string `json:"id"`
	Verdict string `json:"verdict"`
	Want    int    `json:"expected_status,omitempty"`
	Status  int    `json:"http_status,omitempty"`
	Code    string `json:"error_code,omitempty"`
	Target  string `json:"path,omitempty"`
	Before  string `json:"before_sql,omitempty"`
	After   string `json:"after_sql,omitempty"`
}

type report struct {
	Schema     string                     `json:"schema"`
	Run        string                     `json:"run_id"`
	Attempt    string                     `json:"run_attempt"`
	Source     string                     `json:"source_revision"`
	Started    int64                      `json:"job_started_unix"`
	Verdict    string                     `json:"verdict"`
	Stage      string                     `json:"stage"`
	Cleanup    bool                       `json:"cleanup"`
	Inventory  object                     `json:"inventory,omitempty"`
	Migrations []string                   `json:"intended_migrations,omitempty"`
	SQL        map[string]json.RawMessage `json:"sql_snapshots,omitempty"`
	Results    []result                   `json:"controls"`
}

type contractMismatch struct{}

func (contractMismatch) Error() string { return "registered control failed" }

func qualify(parent context.Context, in inputs) (err error) {
	if runtime.GOOS != "linux" || os.Getenv("GITHUB_ACTIONS") != "true" || os.Getenv("RUNNER_ENVIRONMENT") != "github-hosted" {
		return errors.New("J12 qualification requires a GitHub-hosted Linux runner")
	}
	if !regexp.MustCompile(`^[0-9]{1,18}$`).MatchString(in.run) || !regexp.MustCompile(`^[0-9]{1,5}$`).MatchString(in.attempt) ||
		!regexp.MustCompile(`^[0-9a-f]{40}$`).MatchString(in.source) || in.started <= 0 || in.started > time.Now().Unix() ||
		in.output == "" || in.scratch == "" || in.runtime == "" {
		return errors.New("J12 requires run-bound hosted inputs")
	}
	if err := os.Mkdir(in.output, 0700); err != nil {
		return fmt.Errorf("create evidence directory: %w", err)
	}
	witness := report{Schema: "j12.qualification.v1", Run: in.run, Attempt: in.attempt, Source: in.source,
		Started: in.started, Stage: "admission", Verdict: "invalid", SQL: map[string]json.RawMessage{}}
	for _, id := range controlIDs() {
		witness.Results = append(witness.Results, result{ID: id, Verdict: "unperformed"})
	}
	host := newHost(in)
	defer func() {
		cleanup, cancel := context.WithDeadline(context.Background(), time.Unix(in.started, 0).Add(29*time.Minute))
		defer cancel()
		witness.Cleanup = host.cleanup(cleanup)
		var mismatch contractMismatch
		switch {
		case !witness.Cleanup:
			witness.Verdict = "invalid"
			err = errors.New("J12 cleanup could not be verified")
		case errors.As(err, &mismatch):
			witness.Verdict = "fail"
		case err == nil && allPassed(witness.Results):
			witness.Verdict, witness.Stage = "pass", "complete"
		case err == nil:
			err = errors.New("J12 evidence has unperformed or invalid controls")
		}
		if writeErr := writeReport(in.output, witness); writeErr != nil {
			err = writeErr
		}
	}()
	var switches registry
	if err := readFileJSON("deploy/runtime-switches.json", &switches); err != nil {
		return err
	}
	if err := selectRelay(switches, in.relayName, in.relayValue); err != nil {
		return err
	}
	modeSource, err := os.ReadFile("cmd/runtime/relay_mode.go")
	if err != nil || !bytes.Contains(modeSource, []byte(`"`+in.relayName+`"`)) {
		return errors.New("J9A_ADMISSION: registered relay switch has no runtime reader at the candidate")
	}
	base, err := filepath.Glob("migrations/*.up.sql")
	if err != nil {
		return err
	}
	witness.Migrations, err = migrationInventory(switches, base)
	if err != nil {
		return err
	}
	ctx, stop := context.WithDeadline(parent, time.Unix(in.started, 0).Add(25*time.Minute))
	defer stop()
	witness.Stage = "offline-controls"
	if err := (campaign{ctx: ctx, host: host, report: &witness}).offlineControls(); err != nil {
		return err
	}
	witness.Stage = "bootstrap"
	fixture, err := newCredentials()
	if err != nil {
		return errors.New("generate J12 fixture credentials")
	}
	if err := host.bootstrap(ctx, witness.Migrations, fixture); err != nil {
		return err
	}
	witness.Inventory, err = host.inventory(ctx, hexDigest(modeSource))
	if err != nil {
		return err
	}
	witness.Stage = "controls"
	return runControls(ctx, host, &witness, fixture)
}

func allPassed(rows []result) bool {
	return len(rows) != 0 && !slices.ContainsFunc(rows, func(row result) bool { return row.Verdict != "pass" })
}

func writeReport(path string, witness report) error {
	raw, err := json.MarshalIndent(witness, "", "  ")
	if err != nil {
		return err
	}
	if len(raw)+1 > maxEvidence {
		witness.SQL = nil
		witness.Stage, witness.Verdict = "evidence-budget", "invalid"
		// Keep identity, outcomes and references even when SQL bytes exceed the cap.
		if err := writeReport(path, witness); err != nil {
			return err
		}
		return errors.New("J12 evidence exceeded the registered limit")
	}
	return os.WriteFile(filepath.Join(path, "qualification.json"), append(raw, '\n'), 0600)
}

func hexDigest(raw []byte) string { return fmt.Sprintf("%x", sha256.Sum256(raw)) }

func readFileJSON(path string, value any) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, value)
}

func execute(ctx context.Context, env []string, input []byte, program string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, program, args...)
	cmd.Env, cmd.Stdin = env, bytes.NewReader(input)
	var output bytes.Buffer
	cmd.Stdout = &output
	// Child errors can contain credentials. Retain only the caller's fixed stage label.
	cmd.Stderr = io.Discard
	if err := cmd.Run(); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}

func main() {
	var err error
	if len(os.Args) == 2 && os.Args[1] == "probe" {
		err = podProbe(os.Stdin, os.Stdout)
	} else if len(os.Args) == 2 && os.Args[1] == "hold" {
		time.Sleep(30 * time.Minute)
	} else {
		var in inputs
		flag.StringVar(&in.run, "run", "", "GitHub run ID")
		flag.StringVar(&in.attempt, "attempt", "", "GitHub run attempt")
		flag.StringVar(&in.source, "source", "", "checked-out revision")
		flag.StringVar(&in.output, "output", "", "new evidence directory")
		flag.StringVar(&in.scratch, "scratch", "", "new runner scratch directory")
		flag.StringVar(&in.runtime, "runtime", "", "compiled runtime executable")
		flag.StringVar(&in.relayName, "relay-switch", "", "registered J9a switch")
		flag.StringVar(&in.relayValue, "relay-value", "", "registered non-default value")
		flag.Int64Var(&in.started, "started", 0, "job start Unix seconds")
		flag.Parse()
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		err = qualify(ctx, in)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
