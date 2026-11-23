package k6v1

import (
	"encoding/json"
	"slices"
	"testing"
)

func TestK6CoverageUniqueTerminal(t *testing.T) {
	m := Manifest{Cell: "hook-0", Namespace: "ns", Seed: 530001}
	wf := Row{"workflow_id": "id", "namespace": "ns", "submission_key": "id", "revision": json.Number("1"), "state": "RUNNABLE"}
	started := Row{"workflow_id": "id", "revision": json.Number("1"), "old_state": nil, "new_state": "RUNNABLE",
		"actor_kind": "client", "actor_id": "actor", "reason": "registered"}
	for _, tc := range []struct {
		name, reason string
		issues       []Finding
		kind         FindingKind
	}{
		{"Pending", "required_proof_missing:UniqueTerminal", nil, MissingProof},
		{"Censored", "TERMINAL_DEADLINE_CENSORED", []Finding{*integrity("CUT_DEADLINE", BarrierPath)}, Censor},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := evaluate(m, Snapshot{NamespaceCount: 1}, map[string][]Row{executions: {wf}, history: {started}},
				Plan{}, Arrivals{}, false, false, "", tc.issues)
			label := r.Labels[10]
			if r.ExitCode() != Fail || r.Safety == "FAIL" || r.Completeness != "FAIL" || label.Outcome != "NOT COVERED" ||
				label.Reason != "required_proof_missing:UniqueTerminal" || !slices.ContainsFunc(r.Findings, func(f Finding) bool {
				return f.Predicate == "UniqueTerminal" && f.Kind == tc.kind && f.Code == tc.reason &&
					f.Witness != nil && slices.Equal(f.Witness.RowKey, []any{"id"})
			}) {
				t.Fatalf("terminal finding: %+v %s %+v", label, r.Safety, r.Findings)
			}
		})
	}
}

func TestK6CoverageTerminalHistoryGap(t *testing.T) {
	m := Manifest{Cell: "hook-0", Namespace: "ns"}
	wf := Row{"workflow_id": "id", "namespace": "ns", "submission_key": "id", "revision": json.Number("3"), "state": "SUCCEEDED"}
	start := Row{"workflow_id": "id", "revision": json.Number("1"), "old_state": nil, "new_state": "RUNNABLE",
		"actor_kind": "client", "actor_id": "actor", "reason": "registered"}
	end := Row{"workflow_id": "id", "revision": json.Number("3"), "old_state": "RUNNABLE", "new_state": "SUCCEEDED",
		"actor_kind": "scheduler", "actor_id": "actor", "reason": "finished"}
	r := evaluate(m, Snapshot{NamespaceCount: 1}, map[string][]Row{executions: {wf}, history: {start, end}},
		Plan{}, Arrivals{}, false, false, "", nil)
	if r.Completeness != "FAIL" || !slices.ContainsFunc(r.Findings, func(f Finding) bool {
		return f.Kind == MissingProof && f.Predicate == "UniqueTerminal" && f.Code == "required_proof_missing:UniqueTerminal"
	}) {
		t.Fatalf("terminal history gap: %+v", r.Findings)
	}
}

func TestK6CoverageCeiling(t *testing.T) {
	r := newResult(Manifest{Cell: "hook-0", Ordinal: 0}, "")
	r.Labels[6].Outcome = "covered/pass"
	r.record(Violation, "LowerOffsetsDisposed", "LOWER_OFFSET_UNDISPOSED", BrokerPath,
		&Witness{Path: BrokerPath, RowKey: []any{1}, SHA256: Hash(nil)})
	r.finish()
	if r.ExitCode() != Fail || r.Safety != "FAIL" || r.Labels[6].Outcome != "NOT COVERED" ||
		slices.Contains(r.InvariantsCovered, "I7") || len(r.Labels[6].Witnesses) != 1 {
		t.Fatalf("ceiling verdict: %+v", r.Labels[6])
	}
}

func TestK6CoverageSubmissionMissingOffer(t *testing.T) {
	m := Manifest{Cell: "hook-0", Namespace: "ns", Seed: 530001}
	body := SubmissionBody{
		DefinitionID: "test", DefinitionVersion: 1, InitialNodeID: "echo",
		InitialInput: []byte("{}"), Payload: []byte("{}"),
	}
	entry := Submission{Key: "dur053-schedule-530001-000000", Target: "echo", Body: body, BodySHA256: body.Digest()}
	plan := Plan{Windows: []PlanWindow{{Name: "warmup", LoadRunID: "schedule", Submissions: []Submission{entry}}}}
	rows := map[string][]Row{executions: {}, history: {}, "engine.workflow_definitions": {}}
	r := evaluate(m, Snapshot{}, rows, plan, Arrivals{}, true, true, "", nil)
	if r.ExitCode() != Fail || r.Safety == "FAIL" || r.Completeness != "FAIL" || len(r.Findings) != 1 ||
		r.Findings[0].Kind != MissingProof || r.Findings[0].Code != "required_proof_missing:SubmissionIdentity" ||
		r.Labels[0].Outcome != "NOT COVERED" || r.Labels[10].Outcome != "NOT COVERED" {
		t.Fatalf("missing offer verdict: %+v", r)
	}
}

func TestK6CoverageGroupMembership(t *testing.T) {
	m, ext, _ := fixtureExternal(t, false)
	broker := &ext.Broker.Rows[0].Data
	m.Cell, m.Ordinal = "medium-10", 80
	for _, tc := range []struct {
		name, reason string
		change       func(*BrokerRow)
		kind         FindingKind
	}{
		{"Clean", "", func(*BrokerRow) {}, ""},
		{"NoDescription", "required_proof_missing:GroupMembership", func(b *BrokerRow) { b.State = "empty"; b.Members = nil }, MissingProof},
		{"NoCoordinator", "required_proof_missing:GroupMembership", func(b *BrokerRow) { b.Coordinator = nil }, MissingProof},
		{"MemberID", "GROUP_COORDINATOR_DESCRIPTION_MISMATCH", func(b *BrokerRow) { b.Coordinator.Members[0].ID = "other" }, Violation},
		{"ClientID", "GROUP_COORDINATOR_DESCRIPTION_MISMATCH", func(b *BrokerRow) { b.Coordinator.Members[0].ClientID = "other" }, Violation},
		{"Generation", "GROUP_COORDINATOR_DESCRIPTION_MISMATCH", func(b *BrokerRow) { b.Coordinator.Generation++ }, Violation},
		{"MemberGeneration", "GROUP_COORDINATOR_DESCRIPTION_MISMATCH", func(b *BrokerRow) { b.Coordinator.Members[0].Generation++ }, Violation},
		{"Assignment", "GROUP_COORDINATOR_DESCRIPTION_MISMATCH", func(b *BrokerRow) { b.Coordinator.Members[0].Assignments = []PartitionIdentity{} }, Violation},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := *broker
			b.Coordinator = &CoordinatorObservation{Generation: b.Generation, Members: slices.Clone(b.Members)}
			tc.change(&b)
			stream := *ext.Broker
			stream.Rows = slices.Clone(stream.Rows)
			stream.Rows[0].Data = b
			for _, manifest := range []Manifest{m, {Cell: "hook-0"}} {
				r := newResult(manifest, "")
				r.checkGroupMembership(manifest, &stream)
				r.finish()
				wantReason := tc.reason
				registered := manifest.Cell == m.Cell
				if !registered && tc.kind == MissingProof {
					wantReason = ""
				}
				if wantReason == "" {
					if r.ExitCode() != Incomplete || len(r.Findings) != 0 {
						t.Fatalf("group finding: %+v", r.Findings)
					}
					continue
				}
				if r.ExitCode() != Fail || len(r.Findings) != 1 || r.Findings[0].Code != wantReason || r.Findings[0].Kind != tc.kind {
					t.Fatalf("group finding: %+v", r.Findings)
				}
				if !registered && (r.Safety != "FAIL" || r.Labels[11].Outcome != "NOT COVERED" || len(r.Labels[11].Witnesses) != 1) {
					t.Fatalf("out-of-ceiling finding: %+v", r)
				}
			}
		})
	}
}

func TestK6PredicateSinkRevision(t *testing.T) {
	empty, seeded := Hash([]byte("{}")), Hash([]byte("seeded"))
	for _, tc := range []struct {
		name, reason string
		baseline     []ResourceBaseline
		journal      []Row
		kind         FindingKind
	}{
		{"JournalEmpty", "", nil, []Row{{"old_revision": json.Number("0"), "old_state_hash": empty}}, ""},
		{"PreseededHash", "SINK_INITIAL_ENDPOINT", nil, []Row{{"old_revision": json.Number("0"), "old_state_hash": seeded}}, Violation},
		{"PreseededRevision", "SINK_INITIAL_ENDPOINT", nil, []Row{{"old_revision": json.Number("1"), "old_state_hash": empty}}, Violation},
		{"NoEndpointProof", "required_proof_missing:RevisionChain", nil, []Row{}, MissingProof},
		{"ResetInventory", "", []ResourceBaseline{{"resource", 0, seeded}}, []Row{}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := Manifest{Cell: "medium-12", Workload: "COOPERATING_EFFECT", Sink: &DatabaseIdentity{}, SinkResetResources: tc.baseline}
			resource := Row{"resource_id": "resource", "revision": json.Number("0"), "latest_fence": json.Number("0"), "final_state_hash": seeded}
			for _, write := range tc.journal {
				revision := json.Number("1")
				if write["old_revision"] == json.Number("1") {
					revision = json.Number("2")
				}
				write["resource_id"], write["revision"], write["state_hash"] = "resource", revision, seeded
				resource["revision"] = revision
			}
			rows := map[string][]Row{SinkResources: {resource}, SinkJournal: tc.journal}
			r := evaluate(m, Snapshot{}, rows, Plan{}, Arrivals{}, false, false, "", nil)
			if tc.reason == "" {
				if r.ExitCode() != Incomplete || len(r.Findings) != 0 {
					t.Fatalf("revision finding: %+v", r.Findings)
				}
			} else if r.ExitCode() != Fail || len(r.Findings) != 1 || r.Findings[0].Predicate != "RevisionChain" ||
				r.Findings[0].Code != tc.reason || r.Findings[0].Kind != tc.kind {
				t.Fatalf("revision finding: %+v", r.Findings)
			}
		})
	}
}

func TestK6PredicateSinkDuplicateMutation(t *testing.T) {
	m := Manifest{Cell: "medium-14", Workload: "COOPERATING_EFFECT", Sink: &DatabaseIdentity{}}
	journal := []Row{
		{"resource_id": "resource", "revision": json.Number("1"), "intent_id": "intent", "logical_key": "key"},
		{"resource_id": "resource", "revision": json.Number("2"), "intent_id": "intent", "logical_key": "key"},
	}
	rows := map[string][]Row{SinkJournal: journal, "engine.approval_action_intents": {
		{"workflow_id": "workflow", "intent_id": "intent", "target": "resource"},
	}}
	r := evaluate(m, Snapshot{}, rows, Plan{}, Arrivals{}, false, false, "", nil)
	if r.ExitCode() != Fail || len(r.Findings) != 1 || r.Findings[0].Code != "SINK_KEY_MUTATED_TWICE" || r.Labels[7].Outcome != "covered/fail" ||
		r.Findings[0].Witness == nil || !slices.Equal(r.Findings[0].Witness.RowKey, []any{"resource", json.Number("2")}) {
		t.Fatalf("duplicate write verdict: %+v", r.Findings)
	}
}
