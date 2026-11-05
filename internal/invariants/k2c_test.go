package invariants

import (
	"context"
	"crypto/sha256"
	"durable-agent-execution-engine/internal/partition"
	"durable-agent-execution-engine/internal/state"
	"encoding/hex"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"
)

func TestK2cHistory1001(t *testing.T) {
	if os.Getenv("DURABLE_RUN_INTEGRATION") != "1" && os.Getenv("DURABLE_REQUIRE_DATABASE") != "1" {
		t.Skip("requires fresh migrated PostgreSQL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := state.NewFromURL(ctx, invariantDatabaseURL(t))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	id := "k2c-" + state.NewID()
	if err := store.CreateDefinition(ctx, state.DefinitionInput{DefinitionID: id, Version: 1, DefinitionHash: id, Graph: []byte(`{"entry":"pure.add","nodes":[{"id":"pure.add","kind":"activity"}]}`), ActivityVersions: []byte(`{"pure.add":"v1"}`), EffectClasses: []byte(`{"pure.add":"PURE_ACTIVITY"}`)}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = store.Pool().Exec(ctx, `DELETE FROM engine.workflow_executions WHERE workflow_id=$1`, id)
		_, _ = store.Pool().Exec(ctx, `DELETE FROM engine.workflow_definitions WHERE definition_id=$1`, id)
	}()
	part, _ := partition.ID(id)
	if _, err := store.CreateWorkflow(ctx, state.CreateWorkflowInput{WorkflowID: id, PartitionID: int16(part), Namespace: id, SubmissionKey: id, SubmissionPayloadHash: id, DefinitionID: id, DefinitionVersion: 1, InitialNodeID: "pure.add", InitialInput: []byte(`[2,3]`), ActorID: "fixture"}); err != nil {
		t.Fatal(err)
	}
	_, err = store.Pool().Exec(ctx, `INSERT INTO engine.transition_history(transition_id,workflow_id,revision,actor_kind,actor_id,old_state,new_state,reason)
      SELECT gen_random_uuid(),$1,i,'fixture','fixture','RUNNABLE',CASE WHEN i=1001 THEN 'SUCCEEDED' ELSE 'RUNNABLE' END,'FIXTURE' FROM generate_series(2,1001) i;
      `, id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.Pool().Exec(ctx, `UPDATE engine.workflow_executions SET revision=1001,state='SUCCEEDED' WHERE workflow_id=$1`, id); err != nil {
		t.Fatal(err)
	}
	trace, err := Load(ctx, store, []string{id})
	if err != nil {
		t.Fatal(err)
	}
	if len(trace.History) != 1001 {
		t.Fatalf("1001 history rows truncated: %d", len(trace.History))
	}
	if v := Check(trace); !v.Valid {
		t.Fatal(v.Violations)
	}
	trace.History = trace.History[:1000]
	if Check(trace).Valid {
		t.Fatal("truncated history passed")
	}
	// Corrupt the final row beyond the old cap. It must affect the verdict.
	trace, err = Load(ctx, store, []string{id})
	if err != nil {
		t.Fatal(err)
	}
	trace.History[1000].OldState = "WAITING_TIMER"
	if Check(trace).Valid {
		t.Fatal("1001st corrupt history row passed")
	}
	if _, err = Load(ctx, store, []string{id, "missing"}); err == nil {
		t.Fatal("missing terminal workflow loaded")
	}
	table := trace.Snapshot["engine.transition_history"]
	table.Rows = table.Rows[:len(table.Rows)-1]
	trace.Snapshot["engine.transition_history"] = table
	if SnapshotIntegrity(trace) == nil {
		t.Fatal("incomplete snapshot export passed")
	}
}

func TestK2cMissingTerminal(t *testing.T) {
	trace := validTrace()
	trace.History = nil
	if v := Check(trace); v.Valid || !strings.Contains(strings.Join(v.Violations, " "), "accepted workflow missing") {
		t.Fatal("missing terminal workflow passed")
	}
	trace = validTrace()
	trace.History = trace.History[:1]
	v := Check(trace)
	if !v.Valid || len(v.NonTerminal) != 1 {
		t.Fatal("non-terminal accepted work silently omitted")
	}
}

func campaignFixture() (Trace, Arrivals, WitnessExport) {
	trace := validTrace()
	trace.Workflows = []WorkflowRecord{{"wf", "SUCCEEDED", 2}}
	trace.Attempts = []AttemptRecord{{WorkflowID: "wf", NodeID: "root", Iteration: 0, AttemptNumber: 1, State: "SUCCEEDED", ClaimToken: "token", WorkerID: "worker", WorkerRequestID: "req", ResultRecorded: true}}
	trace.Snapshot = map[string]SnapshotTable{}
	for _, name := range snapshotRequiredTables {
		trace.Snapshot[name] = SnapshotTable{Rows: []json.RawMessage{}}
	}
	trace.Snapshot["engine.node_instances"] = SnapshotTable{Rows: []json.RawMessage{json.RawMessage(`{"workflow_id":"wf","node_id":"root","iteration":0,"accepted_result":5}`)}}
	trace.Snapshot["engine.workflow_executions"] = SnapshotTable{Rows: []json.RawMessage{json.RawMessage(`{"workflow_id":"wf","namespace":"default","submission_key":"k","state":"SUCCEEDED","revision":2}`)}}
	trace.Snapshot["engine.transition_history"] = SnapshotTable{Rows: []json.RawMessage{json.RawMessage(`{"workflow_id":"wf","node_id":"root","iteration":0,"attempt_number":1,"reason":"ATTEMPT_RESULT_RECORDED"}`)}}
	for name, table := range trace.Snapshot {
		b, _ := json.Marshal(table.Rows)
		sum := sha256.Sum256(b)
		table.Count = len(table.Rows)
		table.SHA256 = hex.EncodeToString(sum[:])
		trace.Snapshot[name] = table
	}
	arrivals := Arrivals{Offered: 1, Schema: "dur053.arrivals.v1", Status: "PASS", Namespace: "default", Rows: []Arrival{{Sequence: 0, Key: "k", WorkflowID: "wf", Activity: "root", Outcome: "ACCEPTED", Expected: json.RawMessage(`5`)}}}
	zero := int64(0)
	one := int64(1)
	at := time.Date(2026, 11, 4, 0, 0, 0, 0, time.UTC)
	arrivals.Rate = 1
	arrivals.Rows[0].ScheduledAt = at
	arrivals.Rows[0].SentAt = &at
	arrivals.Rows[0].SendNS = &zero
	arrivals.Rows[0].FinishedNS = &one
	arrivals.Rows[0].Request = json.RawMessage(`{}`)
	arrivals.Rows[0].Response = json.RawMessage(`{}`)
	now := time.Date(2026, 11, 4, 0, 0, 0, 0, time.UTC)
	events := []WitnessEvent{
		{Event: "session", Session: "session", Sequence: 1, PodName: "pod", PodUID: "uid", Timestamp: now, MonotonicNS: 1},
		{Event: "start", Session: "session", Sequence: 2, PodName: "pod", PodUID: "uid", Timestamp: now, MonotonicNS: 2, InvocationID: "invocation", WorkflowID: "wf", NodeID: "root", Iteration: 0, AttemptNumber: 1, ClaimToken: "token", WorkerID: "worker"},
		{Event: "end", Session: "session", Sequence: 3, PodName: "pod", PodUID: "uid", Timestamp: now.Add(time.Second), MonotonicNS: 1000000002, InvocationID: "invocation", WorkflowID: "wf", NodeID: "root", Iteration: 0, AttemptNumber: 1, ClaimToken: "token", WorkerID: "worker", Outcome: "SUCCEEDED"},
	}
	var data []byte
	for _, e := range events {
		b, _ := json.Marshal(e)
		data = append(data, append(b, '\n')...)
	}
	sum := sha256.Sum256(data)
	export := WitnessExport{ExpectedPods: []string{"uid"}, Ledgers: []WitnessLedger{{Inventory: WitnessInventory{Session: "session", PodName: "pod", PodUID: "uid", File: "session.jsonl", Attempted: 3, Written: 3, SHA256: hex.EncodeToString(sum[:]), Complete: true}, Data: data}}}
	return trace, arrivals, export
}

func TestK2cCampaignWitness(t *testing.T) {
	trace, arrivals, export := campaignFixture()
	if r := CheckCampaign(trace, arrivals, export); !r.Valid || r.AcceptedResults != 1 || r.ExecutedIterations != 1 || r.DuplicateExecutions != 0 || r.AcceptedResultsPerExecutedIteration == nil || *r.AcceptedResultsPerExecutedIteration != 1 || len(r.InvariantsNotCovered) == 0 || len(r.InvariantOutcomes) != 13 {
		t.Fatalf("valid campaign rejected/metrics missing: %+v", r)
	}
	cases := []struct {
		name   string
		mutate func(*Trace, *Arrivals, *WitnessExport)
	}{
		{"incomplete-export", func(_ *Trace, _ *Arrivals, w *WitnessExport) {
			w.Ledgers[0].Data = w.Ledgers[0].Data[:len(w.Ledgers[0].Data)-1]
		}},
		{"write-error", func(_ *Trace, _ *Arrivals, w *WitnessExport) { w.Ledgers[0].Inventory.Errors = 1 }},
		{"missing-pod", func(_ *Trace, _ *Arrivals, w *WitnessExport) { w.ExpectedPods = append(w.ExpectedPods, "lost-pod") }},
		{"missing-workflow", func(t *Trace, _ *Arrivals, _ *WitnessExport) { t.Submissions = nil; t.History = nil }},
		{"non-terminal", func(t *Trace, _ *Arrivals, _ *WitnessExport) { t.History = t.History[:1] }},
		{"missing-winner", func(t *Trace, _ *Arrivals, _ *WitnessExport) { t.Attempts[0].ResultRecorded = false }},
		{"missing-claim", func(t *Trace, _ *Arrivals, _ *WitnessExport) { t.Attempts = nil }},
		{"stale-token", func(t *Trace, _ *Arrivals, _ *WitnessExport) { t.Attempts[0].ClaimToken = "replacement" }},
		{"missing-snapshot", func(t *Trace, _ *Arrivals, _ *WitnessExport) { t.Snapshot = nil }},
		{"arrival-omitted", func(_ *Trace, a *Arrivals, _ *WitnessExport) { a.Rows = nil }},
		{"unexpected-result", func(_ *Trace, a *Arrivals, _ *WitnessExport) { a.Rows[0].Expected = json.RawMessage(`6`) }},
		{"uncertain-unresolved", func(t *Trace, a *Arrivals, _ *WitnessExport) { t.Submissions = nil; a.Rows[0].Outcome = "UNCERTAIN" }},
		{"key-mismatch", func(_ *Trace, a *Arrivals, _ *WitnessExport) { a.Rows[0].Key = "other" }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			trace, a, w := campaignFixture()
			c.mutate(&trace, &a, &w)
			if r := CheckCampaign(trace, a, w); r.Valid {
				t.Fatalf("campaign guard bypassed: %s", c.name)
			}
		})
	}
}
