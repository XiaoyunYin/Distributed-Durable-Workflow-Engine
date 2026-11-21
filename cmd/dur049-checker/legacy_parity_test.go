package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"durable-agent-execution-engine/internal/invariants"
)

func TestLegacyCheckerProcess(t *testing.T) {
	if os.Getenv("DUR049_PARITY_CHILD") != "1" {
		return
	}
	i := slices.Index(os.Args, "--")
	os.Args = append([]string{"dur049-checker"}, os.Args[i+1:]...)
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	main()
	os.Exit(0)
}

type legacyGolden struct {
	Exit   int    `json:"exit"`
	Stdout string `json:"stdout_sha256"`
	Stderr string `json:"stderr_sha256"`
}

// PostgreSQL and observer clocks are sampled again on every invocation. This
// substitution changes only the values of the three volatile timestamps.
var legacyClockValues = regexp.MustCompile(`"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z"`)

func legacyParity(t *testing.T, name string, args []string, clock bool) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(exe, append([]string{"-test.run=^TestLegacyCheckerProcess$", "--"}, args...)...)
	command.Env = append(os.Environ(), "DUR049_PARITY_CHILD=1", "DATABASE_URL=", "DUR049_RECORD_LEASE_ACQUISITIONS=", "TZ=UTC")
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	err = command.Run()
	exit := 0
	if err != nil {
		var process *exec.ExitError
		if !errors.As(err, &process) {
			t.Fatal(err)
		}
		exit = process.ExitCode()
	}
	out := stdout.Bytes()
	if clock {
		if len(legacyClockValues.FindAll(out, -1)) != 3 {
			t.Fatalf("expected three UTC clock samples: %s", out)
		}
		out = legacyClockValues.ReplaceAll(out, []byte(`"<utc>"`))
	}
	got := legacyGolden{Exit: exit, Stdout: fmt.Sprintf("%x", sha256.Sum256(out)), Stderr: fmt.Sprintf("%x", sha256.Sum256(stderr.Bytes()))}
	var goldens map[string]legacyGolden
	if err := json.Unmarshal(getFile(t, "testdata/legacy-099be63.json"), &goldens); err != nil {
		t.Fatal(err)
	}
	if want, ok := goldens[name]; !ok || got != want {
		t.Fatalf("%s differs from baseline: got %+v, want %+v", name, got, want)
	}
}

func TestLegacyParity(t *testing.T) {
	dir := t.TempDir()
	file := func(name string, raw []byte) string {
		t.Helper()
		path := filepath.Join(dir, name)
		putFile(t, path, raw)
		return path
	}
	empty := file("empty.json", []byte(`{}`))
	invalid := file("malformed.json", []byte(`{`))
	archived := "../../experiments/portfolio/k8s-recovery/k2-prerequisites-v1/compose-prerequisites/short/"
	var archivedChecker struct {
		Trace json.RawMessage `json:"trace"`
	}
	if err := json.Unmarshal(getFile(t, archived+"checker.json"), &archivedChecker); err != nil || len(archivedChecker.Trace) == 0 {
		t.Fatal("missing campaign fixture trace", err)
	}
	trace := file("trace.json", archivedChecker.Trace)
	arrivals := archived + "arrivals.json"
	badArrivals := file("bad-arrivals.json", bytes.Replace(getFile(t, arrivals), []byte("dur053.arrivals.v1"), []byte("dur053.arrivals.v0"), 1))
	witness := file("witness.json", legacyCampaignWitness(t, archived+"witness-export.json"))
	timeline, leases := legacyLifecycleFiles(t, file)
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"snapshot-pass", []string{"-snapshot", empty}},
		{"snapshot-decode-fail", []string{"-snapshot", invalid}},
		{"snapshot-verdict-fail", []string{"-snapshot", empty, "-recovery-epoch", "1"}},
		{"campaign-pass", []string{"-snapshot", trace, "-arrivals", arrivals, "-witness-export", witness, "-require-terminal"}},
		{"campaign-verdict-fail", []string{"-snapshot", trace, "-arrivals", badArrivals, "-witness-export", witness}},
		{"campaign-dispatch-fail", []string{"-snapshot", empty, "-arrivals", arrivals}},
		{"lifecycle-pass", []string{"-lifecycle", timeline, "-lease-audit", leases}},
		{"lifecycle-decode-fail", []string{"-lifecycle", invalid, "-lease-audit", leases}},
		{"lifecycle-dispatch-fail", []string{"-lifecycle", timeline, "-lease-audit", leases, "-snapshot", empty}},
		{"clock-fail", []string{"-clock-probe", "-database-url", "://"}},
		{"live-usage-fail", nil},
	} {
		t.Run(tc.name, func(t *testing.T) { legacyParity(t, tc.name, tc.args, false) })
	}
}

func legacyCampaignWitness(t *testing.T, path string) []byte {
	t.Helper()
	var witness invariants.WitnessExport
	if err := json.Unmarshal(getFile(t, path), &witness); err != nil {
		t.Fatal(err)
	}
	for i := range witness.Ledgers {
		ledger := &witness.Ledgers[i]
		var normalized bytes.Buffer
		for index, line := range bytes.Split(bytes.TrimSuffix(ledger.Data, []byte{'\n'}), []byte{'\n'}) {
			var event invariants.WitnessEvent
			if err := json.Unmarshal(line, &event); err != nil {
				t.Fatal(err)
			}
			// Use integral second durations in the input fixture so the
			// legacy sum is stable across Go's map iteration orders.
			event.MonotonicNS = int64(index+1) * 1e9
			if err := json.NewEncoder(&normalized).Encode(event); err != nil {
				t.Fatal(err)
			}
		}
		ledger.Data = normalized.Bytes()
		ledger.Inventory.SHA256 = fmt.Sprintf("%x", sha256.Sum256(ledger.Data))
	}
	raw, err := json.Marshal(witness)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func legacyLifecycleFiles(t *testing.T, file func(string, []byte) string) (string, string) {
	t.Helper()
	end, death := 4.0, 5.0
	i := invariants.LifecycleIdentity{Namespace: "ns", Workflow: "w", Node: "n", Attempt: 1, TokenHash: strings.Repeat("a", 64)}
	var events bytes.Buffer
	for _, event := range []struct {
		kind string
		at   float64
	}{{"start", 2}, {"finish", 4}} {
		if err := json.NewEncoder(&events).Encode(map[string]any{"event": event.kind, "id": "v", "identity": i, "at": event.at}); err != nil {
			t.Fatal(err)
		}
	}
	data := events.String()
	timeline := invariants.LifecycleTimeline{Schema: "dur053.k4-timeline.v1", Episode: "e", Namespace: "ns",
		Sessions:    []invariants.LifecycleSession{{Session: "s", PodUID: "uid", ContainerID: "container", Role: "worker", Start: 0, LastAlive: 4, Death: &death, DeathEvidence: "independent-watch", Signals: []invariants.LifecycleSignal{{At: 3, Signal: 15, Evidence: "signal-delivery"}}, Readiness: []invariants.LifecycleMembership{}, Endpoints: []invariants.LifecycleMembership{}}},
		Claims:      []invariants.LifecycleClaim{{Identity: i, Session: "s", Order: 1, At: 1}},
		Invocations: []invariants.LifecycleInvocation{{ID: "v", Identity: i, Session: "s", Start: 2, Finish: &end, LastAlive: 3, Outcome: "success"}},
		History:     []invariants.LifecycleHistory{{Identity: i, Order: 4, At: 4, Kind: "receipt", Receipt: "r", Revision: 1}},
		Receipts:    []invariants.LifecycleReceipt{{ID: "r", Identity: i, Session: "s", Order: 4, At: 4, Category: "committed", Outcome: "success"}},
		Workflows:   []invariants.LifecycleWorkflow{{Workflow: "w", State: "SUCCEEDED", Terminal: &end}},
		HTTP:        []invariants.LifecycleHTTP{},
		Witness:     []invariants.LifecycleWitness{{Session: "s", PodUID: "uid", ContainerID: "container", Data: data, SHA256: fmt.Sprintf("%x", sha256.Sum256([]byte(data))), Acknowledged: []string{"v"}, Complete: true}},
	}
	leases := invariants.LifecycleLeaseAudit{Schema: "dur053.k4-lease-audit.v1", Episode: "e", Rows: []invariants.LifecycleLease{}}
	rawTimeline, err := json.Marshal(timeline)
	if err != nil {
		t.Fatal(err)
	}
	rawLeases, err := json.Marshal(leases)
	if err != nil {
		t.Fatal(err)
	}
	return file("timeline.json", rawTimeline), file("leases.json", rawLeases)
}

func TestLegacyParityPostgres(t *testing.T) {
	conn, dsn := k6TestDatabase(t)
	execK6SQL(t, conn, `
		UPDATE engine.schema_migrations SET applied_at='2026-01-01 00:00:00+00';
		UPDATE engine.partition_leases SET updated_at='2026-01-01 00:00:00+00';
		INSERT INTO engine.workflow_definitions(definition_id,version,definition_hash,graph,created_at)
		VALUES ('def',1,'hash','{"nodes":["pure.echo"]}','2026-01-01 00:00:00+00');
		INSERT INTO engine.workflow_executions(workflow_id,namespace,submission_key,submission_payload_hash,definition_id,definition_version,partition_id,state,revision,created_at,updated_at)
		VALUES ('legacy','legacy','key','hash','def',1,0,'SUCCEEDED',2,'2026-01-01 00:00:00+00','2026-01-01 00:00:00+00');
		INSERT INTO engine.transition_history(transition_id,workflow_id,revision,actor_kind,actor_id,scheduler_epoch,old_state,new_state,reason,created_at)
		VALUES ('deadbeef-0000-0000-0000-000000000001','legacy',1,'client','actor',NULL,NULL,'RUNNABLE','WORKFLOW_CREATED','2026-01-01 00:00:00+00'),
		       ('deadbeef-0000-0000-0000-000000000002','legacy',2,'scheduler','actor',1,'RUNNABLE','SUCCEEDED','WORKFLOW_ADVANCED','2026-01-01 00:00:00+00');
		INSERT INTO engine.outbox(event_id,workflow_id,aggregate_revision,event_type,payload,created_at,next_attempt_at)
		VALUES ('deadbeef-0000-0000-0000-000000000003','legacy',1,'workflow.created','{"workflow_id":"legacy"}','2026-01-01 00:00:00+00','2026-01-01 00:00:00+00');
	`)
	legacyParity(t, "live-pass", []string{"-database-url", dsn, "-workflow-ids", "legacy"}, false)
	legacyParity(t, "live-read-fail", []string{"-database-url", dsn, "-workflow-ids", "absent"}, false)
	legacyParity(t, "clock-pass", []string{"-database-url", dsn, "-clock-probe"}, true)
}
