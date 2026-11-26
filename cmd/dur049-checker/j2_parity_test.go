package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/invariants"
)

// DUR049_PREVIOUS_CHECKER points to the checker built from 03ae92f, the
// pre-revision J2 slice 1c base. Preserve that source for comparisons.
func TestJ2CheckerParity(t *testing.T) {
	previous := os.Getenv("DUR049_PREVIOUS_CHECKER")
	if previous == "" {
		t.Skip("set DUR049_PREVIOUS_CHECKER to the checker built from 03ae92f")
	}
	const archived = "../../experiments/portfolio/k8s-recovery/k2-prerequisites-v1/compose-prerequisites/short/"
	var original struct {
		Trace json.RawMessage `json:"trace"`
	}
	readParityJSON(t, getFile(t, archived+"checker.json"), &original)
	witness, busy, reexecuted := j2ParityWitness(t, getFile(t, archived+"witness-export.json"))
	root := t.TempDir()
	witnessPath := filepath.Join(root, "witness.json")
	putFile(t, witnessPath, witness)
	for _, tc := range []struct {
		name      string
		violation string
		change    func(*invariants.Trace)
	}{
		{name: "complete"},
		{"missing", "missing snapshot table: engine.checkpoints", func(trace *invariants.Trace) {
			delete(trace.Snapshot, "engine.checkpoints")
		}},
		{"duplicate", "outbox event ID is duplicated:", func(trace *invariants.Trace) {
			trace.Outbox = append(trace.Outbox, trace.Outbox[0])
		}},
		{"corrupt", "snapshot count/hash mismatch: engine.lease_acquisitions", func(trace *invariants.Trace) {
			trace.Snapshot["engine.lease_acquisitions"].Rows[0] = json.RawMessage(`{"corrupt":true}`)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var trace invariants.Trace
			readParityJSON(t, original.Trace, &trace)
			if tc.change != nil {
				tc.change(&trace)
			}
			path := filepath.Join(root, tc.name+".json")
			putFile(t, path, parityJSON(t, trace))
			args := []string{"-snapshot", path, "-arrivals", archived + "arrivals.json", "-witness-export", witnessPath, "-require-terminal"}
			old := parityProcess(t, exec.Command(previous, args...), false)
			new := parityProcess(t, currentParityProcess(t, args), true)
			wantExit := 0
			if tc.violation != "" {
				wantExit = 1
			}
			if old.exit != wantExit || new.exit != wantExit || len(old.stderr) != 0 {
				t.Fatalf("exit (%d, %d), want %d, baseline stderr %s", old.exit, new.exit, wantExit, old.stderr)
			}
			checkPhaseSequence(t, new.stderr, []string{"offline_decode", "hash", "check", "campaign_check", "write"})
			oldTrace, oldMasked := parityReport(t, old.stdout, busy, reexecuted, tc.violation)
			newTrace, newMasked := parityReport(t, new.stdout, busy, reexecuted, tc.violation)
			if !bytes.Equal(oldTrace, newTrace) || !bytes.Equal(oldTrace, parityJSON(t, trace)) {
				t.Fatal("trace bytes differ from the baseline or supplied snapshot")
			}
			if !bytes.Equal(oldMasked, newMasked) {
				t.Fatal("report bytes differ after masking the two duration sums")
			}
		})
	}
}

type parityOutput struct {
	stdout, stderr []byte
	exit           int
}

func currentParityProcess(t *testing.T, args []string) *exec.Cmd {
	t.Helper()
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return exec.Command(self, append([]string{"-test.run=^TestLegacyCheckerProcess$", "--"}, args...)...)
}

func parityProcess(t *testing.T, cmd *exec.Cmd, phases bool) parityOutput {
	t.Helper()
	cmd.Env = append(os.Environ(), "DUR049_PARITY_CHILD=1", "DATABASE_URL=", "DUR049_RECORD_LEASE_ACQUISITIONS=", "TZ=UTC", "J2_CHECKER_PHASES=0")
	if phases {
		cmd.Env = append(cmd.Env, "J2_CHECKER_PHASES=1")
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	exit := 0
	if err := cmd.Run(); err != nil {
		var failed *exec.ExitError
		if !errors.As(err, &failed) {
			t.Fatal(err)
		}
		exit = failed.ExitCode()
	}
	return parityOutput{stdout.Bytes(), stderr.Bytes(), exit}
}

var durationFields = regexp.MustCompile(`"(busy_seconds|reexecuted_seconds)":-?[0-9.eE+\-]+`)

func parityReport(t *testing.T, stdout []byte, busy, reexecuted float64, violation string) (trace, masked []byte) {
	t.Helper()
	var report struct {
		Trace    json.RawMessage `json:"trace"`
		Campaign json.RawMessage `json:"campaign"`
		Status   string          `json:"status"`
	}
	readParityJSON(t, stdout, &report)
	var campaign invariants.CampaignReport
	readParityJSON(t, report.Campaign, &campaign)
	if math.Abs(campaign.BusySeconds-busy) > 1e-12 || math.Abs(campaign.ReexecutedSeconds-reexecuted) > 1e-12 {
		t.Fatalf("durations (%g, %g), want (%g, %g) from witness events", campaign.BusySeconds, campaign.ReexecutedSeconds, busy, reexecuted)
	}
	if violation == "" {
		if report.Status != "PASS" || !campaign.Valid {
			t.Fatalf("complete fixture failed: %v", campaign.Violations)
		}
	} else if report.Status != "FAIL" || campaign.Valid || len(campaign.Violations) != 1 || !strings.HasPrefix(campaign.Violations[0], violation) {
		t.Fatalf("negative fixture produced %s %v, want %q", report.Status, campaign.Violations, violation)
	}
	var snapshot invariants.Trace
	readParityJSON(t, report.Trace, &snapshot)
	for name, table := range snapshot.Snapshot {
		wantHash := fmt.Sprintf("%x", sha256.Sum256(parityJSON(t, table.Rows)))
		corrupted := violation == "snapshot count/hash mismatch: "+name
		if table.Count != len(table.Rows) || (table.SHA256 != wantHash) != corrupted {
			t.Fatalf("table %s has unexpected count/hash (%d, %s)", name, table.Count, table.SHA256)
		}
	}
	if matches := durationFields.FindAll(report.Campaign, -1); len(matches) != 2 {
		t.Fatalf("found %d duration members, want two", len(matches))
	}
	return report.Trace, bytes.Replace(stdout, report.Campaign, durationFields.ReplaceAll(report.Campaign, []byte(`"${1}":0`)), 1)
}

func checkPhaseSequence(t *testing.T, stderr []byte, want []string) {
	t.Helper()
	lines := bytes.Split(bytes.TrimSuffix(stderr, []byte("\n")), []byte("\n"))
	previous := int64(0)
	if len(lines) != len(want) {
		t.Fatalf("unexpected phase log: %s", stderr)
	}
	for i, line := range lines {
		payload, ok := bytes.CutPrefix(line, []byte("J2_PHASE "))
		if !ok {
			t.Fatalf("unexpected phase marker %s", line)
		}
		var phase struct {
			Phase     string `json:"phase"`
			AtNS      int64  `json:"at_ns"`
			HeapAlloc uint64 `json:"heap_alloc"`
			HeapInuse uint64 `json:"heap_inuse"`
			Sys       uint64 `json:"sys"`
		}
		readParityJSON(t, payload, &phase)
		if phase.Phase != want[i] || phase.AtNS < previous || phase.HeapAlloc == 0 || phase.HeapInuse < phase.HeapAlloc || phase.Sys < phase.HeapInuse {
			t.Fatalf("invalid phase %s, want %s after %d", line, want[i], previous)
		}
		previous = phase.AtNS
	}
}

// Add a second witnessed execution to a matched claim so both floating-point
// metrics are exercised. Compute busy time independently in integer nanos.
func j2ParityWitness(t *testing.T, input []byte) ([]byte, float64, float64) {
	t.Helper()
	var witness invariants.WitnessExport
	readParityJSON(t, input, &witness)
	var busyNS int64
	const rerunNS = 375_000_001
	for i := range witness.Ledgers {
		ledger := &witness.Ledgers[i]
		var events []invariants.WitnessEvent
		for _, line := range bytes.Split(bytes.TrimSuffix(ledger.Data, []byte("\n")), []byte("\n")) {
			var event invariants.WitnessEvent
			readParityJSON(t, line, &event)
			events = append(events, event)
		}
		if i == 0 {
			start, end := events[1], events[2]
			if start.Event != "start" || end.Event != "end" || start.InvocationID != end.InvocationID {
				t.Fatal("expected a matched invocation at the start of the first ledger")
			}
			start.InvocationID, end.InvocationID = "j2-parity-rerun", "j2-parity-rerun"
			last := events[len(events)-1]
			start.Sequence, end.Sequence = len(events)+1, len(events)+2
			start.Timestamp = last.Timestamp.Add(time.Second)
			end.Timestamp = start.Timestamp.Add(rerunNS)
			start.MonotonicNS = last.MonotonicNS + int64(time.Second)
			end.MonotonicNS = start.MonotonicNS + rerunNS
			events = append(events, start, end)
			ledger.Inventory.Written, ledger.Inventory.Attempted = len(events), len(events)
		}
		starts := map[string]int64{}
		var data bytes.Buffer
		for _, event := range events {
			switch event.Event {
			case "start":
				starts[event.InvocationID] = event.MonotonicNS
			case "end":
				start, ok := starts[event.InvocationID]
				if !ok {
					t.Fatalf("unmatched fixture invocation %s", event.InvocationID)
				}
				busyNS += event.MonotonicNS - start
			}
			data.Write(parityJSON(t, event))
			data.WriteByte('\n')
		}
		ledger.Data = data.Bytes()
		ledger.Inventory.SHA256 = fmt.Sprintf("%x", sha256.Sum256(ledger.Data))
	}
	return parityJSON(t, witness), float64(busyNS) / 1e9, float64(rerunNS) / 1e9
}

func parityJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func readParityJSON(t *testing.T, raw []byte, target any) {
	t.Helper()
	if err := json.Unmarshal(raw, target); err != nil {
		t.Fatal(err)
	}
}
