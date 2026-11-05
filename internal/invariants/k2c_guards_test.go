package invariants

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func rewriteWitness(w *WitnessExport, mutate func([]WitnessEvent) []WitnessEvent) {
	var events []WitnessEvent
	for _, line := range strings.Split(strings.TrimSpace(string(w.Ledgers[0].Data)), "\n") {
		var e WitnessEvent
		_ = json.Unmarshal([]byte(line), &e)
		events = append(events, e)
	}
	events = mutate(events)
	var data []byte
	for i, e := range events {
		e.Sequence = i + 1
		b, _ := json.Marshal(e)
		data = append(data, append(b, '\n')...)
	}
	sum := sha256.Sum256(data)
	w.Ledgers[0].Data = data
	w.Ledgers[0].Inventory.Written = len(events)
	w.Ledgers[0].Inventory.Attempted = len(events)
	w.Ledgers[0].Inventory.SHA256 = hex.EncodeToString(sum[:])
}
func TestK2cWitnessGuards(t *testing.T) {
	cases := []struct {
		name, message string
		mutate        func(*Trace, *WitnessExport)
	}{
		{"inventory-complete", "count/hash/write failure", func(_ *Trace, w *WitnessExport) { w.Ledgers[0].Inventory.Complete = false }},
		{"inventory-errors", "count/hash/write failure", func(_ *Trace, w *WitnessExport) { w.Ledgers[0].Inventory.Errors = 1 }},
		{"inventory-attempted", "count/hash/write failure", func(_ *Trace, w *WitnessExport) { w.Ledgers[0].Inventory.Attempted++ }},
		{"inventory-hash", "count/hash/write failure", func(_ *Trace, w *WitnessExport) { w.Ledgers[0].Inventory.SHA256 = "wrong" }},
		{"expected-empty", "missing expected witness pod inventory", func(_ *Trace, w *WitnessExport) { w.ExpectedPods = nil }},
		{"expected-duplicate", "invalid expected witness pod inventory", func(_ *Trace, w *WitnessExport) { w.ExpectedPods = append(w.ExpectedPods, "uid") }},
		{"missing-pod", "missing witness pod export", func(_ *Trace, w *WitnessExport) { w.Ledgers = nil }},
		{"session", "unexpected or duplicate witness session", func(_ *Trace, w *WitnessExport) { w.Ledgers = append(w.Ledgers, w.Ledgers[0]) }},
		{"unexpected-pod", "unexpected or duplicate witness session", func(_ *Trace, w *WitnessExport) { w.ExpectedPods = []string{"another"} }},
		{"line-count", "line count", func(_ *Trace, w *WitnessExport) { w.Ledgers[0].Inventory.Written++; w.Ledgers[0].Inventory.Attempted++ }},
		{"json", "invalid witness JSON", func(_ *Trace, w *WitnessExport) { w.Ledgers[0].Data = []byte("invalid\n") }},
		{"sequence", "identity or sequence", func(_ *Trace, w *WitnessExport) {
			w.Ledgers[0].Data = []byte(strings.Replace(string(w.Ledgers[0].Data), `"sequence":2`, `"sequence":9`, 1))
		}},
		{"header", "missing witness session header", func(_ *Trace, w *WitnessExport) {
			rewriteWitness(w, func(e []WitnessEvent) []WitnessEvent { e[0].Event = "start"; return e })
		}},
		{"identity", "incomplete invocation identity", func(_ *Trace, w *WitnessExport) {
			rewriteWitness(w, func(e []WitnessEvent) []WitnessEvent { e[1].ClaimToken = ""; return e })
		}},
		{"duplicate-start", "duplicate invocation ID", func(_ *Trace, w *WitnessExport) {
			rewriteWitness(w, func(e []WitnessEvent) []WitnessEvent { return append(e, e[1]) })
		}},
		{"duplicate-end", "duplicate invocation end", func(_ *Trace, w *WitnessExport) {
			rewriteWitness(w, func(e []WitnessEvent) []WitnessEvent { return append(e, e[2]) })
		}},
		{"unknown-event", "unknown witness event", func(_ *Trace, w *WitnessExport) {
			rewriteWitness(w, func(e []WitnessEvent) []WitnessEvent { e[2].Event = "ignored"; return e })
		}},
		{"claim", "does not match engine claim", func(trace *Trace, _ *WitnessExport) { trace.Attempts[0].ClaimToken = "new" }},
		{"worker", "does not match engine claim", func(trace *Trace, _ *WitnessExport) { trace.Attempts[0].WorkerID = "other" }},
		{"unfinished", "unfinished invocation", func(_ *Trace, w *WitnessExport) {
			rewriteWitness(w, func(e []WitnessEvent) []WitnessEvent { return e[:2] })
		}},
		{"end-identity", "end identity/time mismatch", func(_ *Trace, w *WitnessExport) {
			rewriteWitness(w, func(e []WitnessEvent) []WitnessEvent { e[2].ClaimToken = "new"; return e })
		}},
		{"end-duration", "end identity/time mismatch", func(_ *Trace, w *WitnessExport) {
			rewriteWitness(w, func(e []WitnessEvent) []WitnessEvent { e[2].MonotonicNS = 1; return e })
		}},
		{"end-outcome", "end identity/time mismatch", func(_ *Trace, w *WitnessExport) {
			rewriteWitness(w, func(e []WitnessEvent) []WitnessEvent { e[2].Outcome = "FAILED_RETRYABLE"; return e })
		}},
		{"orphan-end", "end without start", func(_ *Trace, w *WitnessExport) {
			rewriteWitness(w, func(e []WitnessEvent) []WitnessEvent { return []WitnessEvent{e[0], e[2]} })
		}},
		{"unwitnessed-claim", "claimed attempt missing invocation witness", func(trace *Trace, _ *WitnessExport) {
			trace.Attempts = append(trace.Attempts, AttemptRecord{WorkflowID: "wf", NodeID: "other", Iteration: 0, AttemptNumber: 1, ClaimToken: "token"})
		}},
		{"unwitnessed-result", "result lacks externally executed iteration", func(trace *Trace, _ *WitnessExport) {
			trace.Attempts = append(trace.Attempts, AttemptRecord{WorkflowID: "wf", NodeID: "other", Iteration: 0, AttemptNumber: 1, ResultRecorded: true})
		}},
		{"receipt-history", "receipt/history reconciliation", func(trace *Trace, _ *WitnessExport) { trace.Snapshot["engine.transition_history"] = SnapshotTable{} }},
		{"orphan-receipt", "receipt history has no matching accepted result", func(trace *Trace, _ *WitnessExport) { trace.Attempts[0].ResultRecorded = false }},
		{"unsuccessful", "unexpected unsuccessful result receipt", func(trace *Trace, _ *WitnessExport) { trace.Attempts[0].State = "FAILED_FINAL" }},
		{"winner", "exactly one winning", func(trace *Trace, _ *WitnessExport) { trace.Attempts[0].ResultRecorded = false }},
		{"no-execution", "no external execution witness", func(_ *Trace, w *WitnessExport) {
			rewriteWitness(w, func(e []WitnessEvent) []WitnessEvent { return e[:1] })
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			trace, _, w := campaignFixture()
			c.mutate(&trace, &w)
			r := CampaignReport{}
			reconcileWitness(trace, w, &r)
			if !strings.Contains(strings.Join(r.Violations, "\n"), c.message) {
				t.Fatalf("witness guard bypassed: %s: expected %s, got %v", c.name, c.message, r.Violations)
			}
		})
	}
	trace, _, w := campaignFixture()
	rewriteWitness(&w, func(e []WitnessEvent) []WitnessEvent {
		start, end := e[1], e[2]
		start.InvocationID = "second"
		end.InvocationID = "second"
		start.Timestamp = start.Timestamp.Add(2 * time.Second)
		end.Timestamp = end.Timestamp.Add(2 * time.Second)
		return append(e, start, end)
	})
	r := CampaignReport{}
	reconcileWitness(trace, w, &r)
	if len(r.Violations) > 0 || r.DuplicateExecutions != 1 || r.ExecutedIterations != 1 || r.AcceptedResults != 1 || r.ReexecutedSeconds != 1 {
		t.Fatalf("duplicate execution metric guard bypassed: %+v", r)
	}
}
func TestK2cTiedWitnessStarts(t *testing.T) {
	trace, _, w := campaignFixture()
	rewriteWitness(&w, func(e []WitnessEvent) []WitnessEvent {
		start, end := e[1], e[2]
		start.InvocationID = "second"
		end.InvocationID = "second"
		return append(e, start, end)
	})
	r := CampaignReport{}
	reconcileWitness(trace, w, &r)
	if len(r.Violations) > 0 || r.DuplicateExecutions != 1 || r.ReexecutedSeconds != 1 {
		t.Fatalf("tied execution metric guard bypassed: %+v", r)
	}
}

func TestK2cSnapshotGuards(t *testing.T) {
	trace, _, _ := campaignFixture()
	if SnapshotIntegrity(trace) != nil {
		t.Fatal("valid snapshot rejected")
	}
	for _, name := range []string{"engine.transition_history", "engine.timers", "engine.attempt_result_evidence", "effects.effect_records"} {
		trace, _, _ = campaignFixture()
		delete(trace.Snapshot, name)
		if SnapshotIntegrity(trace) == nil {
			t.Fatal("snapshot required-table guard bypassed")
		}
	}
	trace, _, _ = campaignFixture()
	table := trace.Snapshot["engine.node_instances"]
	table.Count++
	trace.Snapshot["engine.node_instances"] = table
	if SnapshotIntegrity(trace) == nil {
		t.Fatal("snapshot count guard bypassed")
	}
	trace, _, _ = campaignFixture()
	table = trace.Snapshot["engine.node_instances"]
	table.SHA256 = "wrong"
	trace.Snapshot["engine.node_instances"] = table
	if SnapshotIntegrity(trace) == nil {
		t.Fatal("snapshot hash guard bypassed")
	}
}

func TestK2cCampaignGuards(t *testing.T) {
	cases := []struct {
		name, message string
		mutate        func(*Trace, *Arrivals)
	}{
		{"manifest-status", "arrival manifest incomplete", func(_ *Trace, a *Arrivals) { a.Status = "IN_PROGRESS" }},
		{"manifest-count", "arrival manifest incomplete", func(_ *Trace, a *Arrivals) { a.Offered++ }},
		{"schedule", "schedule/send/response evidence incomplete", func(_ *Trace, a *Arrivals) { a.Rows[0].ScheduledNS = 1 }},
		{"send-evidence", "schedule/send/response evidence incomplete", func(_ *Trace, a *Arrivals) { a.Rows[0].SendNS = nil }},
		{"raw-count", "accepted-count reconciliation failed", func(trace *Trace, _ *Arrivals) { trace.Snapshot["engine.workflow_executions"] = SnapshotTable{} }},
		{"raw-identity", "accepted identity missing from projection", func(trace *Trace, _ *Arrivals) { trace.Submissions[0].WorkflowID = "other" }},
		{"wrong-namespace", "snapshot includes wrong namespace", func(trace *Trace, _ *Arrivals) { trace.Submissions[0].Namespace = "other" }},
		{"row-identity", "arrival identity duplicate or incomplete", func(_ *Trace, a *Arrivals) { a.Rows[0].Sequence = 1 }},
		{"workflow", "key maps to unexpected workflow", func(_ *Trace, a *Arrivals) { a.Rows[0].WorkflowID = "other" }},
		{"missing", "accepted workflow missing from durable submissions", func(trace *Trace, _ *Arrivals) { trace.Submissions = nil }},
		{"extra", "extra durable accepted submission", func(trace *Trace, _ *Arrivals) {
			trace.Submissions = append(trace.Submissions, SubmissionRecord{Namespace: "default", Key: "extra", Hash: "x", WorkflowID: "extra"})
		}},
		{"uncertain", "uncertain submission unresolved", func(trace *Trace, a *Arrivals) { trace.Submissions = nil; a.Rows[0].Outcome = "UNCERTAIN" }},
		{"unexpected-acceptance", "rejected/unsent arrival found durably accepted", func(_ *Trace, a *Arrivals) { a.Rows[0].Outcome = "REJECTED" }},
		{"unsent", "every offered arrival accepted", func(_ *Trace, a *Arrivals) { a.Rows[0].Outcome = "CAPACITY_MISSED" }},
		{"nonterminal", "remain non-terminal", func(trace *Trace, _ *Arrivals) { trace.History = trace.History[:1] }},
		{"failed", "unexpected unsuccessful terminal", func(trace *Trace, _ *Arrivals) { trace.History[1].NewState = "FAILED" }},
		{"result", "unexpected accepted activity result", func(_ *Trace, a *Arrivals) { a.Rows[0].Expected = json.RawMessage(`6`) }},
		{"result-missing", "missing accepted activity result", func(trace *Trace, _ *Arrivals) { trace.Snapshot["engine.node_instances"] = SnapshotTable{} }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			trace, a, w := campaignFixture()
			c.mutate(&trace, &a)
			r := CheckCampaign(trace, a, w)
			if r.Valid || !strings.Contains(strings.Join(r.Violations, "\n"), c.message) {
				t.Fatalf("campaign completeness guard bypassed: %s (%v)", c.name, r.Violations)
			}
		})
	}
}
