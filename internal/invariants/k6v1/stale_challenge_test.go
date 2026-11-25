package k6v1

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestK6StaleChallenge(t *testing.T) {
	for _, tc := range []struct {
		name, code string
		change     func(*StaleObservation, *engineOrder, *Manifest, *Plan)
	}{
		{name: "Clean"},
		{"N5HarnessPID", "STALE_CHALLENGE_ORIGIN", func(s *StaleObservation, _ *engineOrder, _ *Manifest, _ *Plan) { s.Call.PID++ }},
		{"N5HarnessStart", "STALE_CHALLENGE_ORIGIN", func(s *StaleObservation, _ *engineOrder, _ *Manifest, _ *Plan) { s.Call.Start = "1:999" }},
		{"N5HarnessContainer", "STALE_CHALLENGE_ORIGIN", func(s *StaleObservation, _ *engineOrder, _ *Manifest, _ *Plan) { s.Call.ContainerID = "harness" }},
		{"DifferentHeldCall", "STALE_CHALLENGE_ORIGIN", func(s *StaleObservation, _ *engineOrder, _ *Manifest, _ *Plan) { s.Call.Invocation = "another-call" }},
		{"Continuity", "STALE_CHALLENGE_ORIGIN", func(s *StaleObservation, _ *engineOrder, _ *Manifest, _ *Plan) { s.After.Start = "1:999" }},
		{"WrongOrder", "STALE_CHALLENGE_ORDER", func(s *StaleObservation, _ *engineOrder, _ *Manifest, _ *Plan) { s.Call.AtNS = s.Takeover.EndNS }},
		{"Unexpired", "STALE_CHALLENGE_ORDER", func(s *StaleObservation, _ *engineOrder, _ *Manifest, _ *Plan) { s.Takeover.Updated = s.Hold.DBTime }},
		{"ControlPlan", "STALE_CHALLENGE_ORDER", func(_ *StaleObservation, _ *engineOrder, _ *Manifest, p *Plan) { p.FaultMode = "control" }},
		{"WrongOffer", "STALE_CHALLENGE_ORDER", func(_ *StaleObservation, _ *engineOrder, _ *Manifest, p *Plan) {
			p.Windows[0].Submissions[0].Key = "other"
		}},
		{"Committed", "STALE_CHALLENGE_COMMITTED", func(s *StaleObservation, _ *engineOrder, _ *Manifest, _ *Plan) { s.Call.Outcome = "COMMITTED" }},
		{"CommittedAttempt", "STALE_CHALLENGE_COMMITTED", func(s *StaleObservation, _ *engineOrder, _ *Manifest, _ *Plan) { s.Rejected.Attempts++ }},
		{"MissingRejection", "required_proof_missing:stale_challenge_rejection", func(s *StaleObservation, _ *engineOrder, _ *Manifest, _ *Plan) { s.Call.Outcome = "OTHER_ERROR" }},
		{"MissingCall", "required_proof_missing:stale_challenge_call", func(s *StaleObservation, _ *engineOrder, _ *Manifest, _ *Plan) { s.Call = nil }},
		{"MissingBackend", "required_proof_missing:stale_challenge_backend", func(s *StaleObservation, _ *engineOrder, _ *Manifest, _ *Plan) { s.Hold.Backends = nil }},
		{"OpenTransaction", "required_proof_missing:stale_challenge_backend", func(s *StaleObservation, _ *engineOrder, _ *Manifest, _ *Plan) { s.Hold.Backends[0].Transaction = true }},
		{"ReusedBackend", "required_proof_missing:stale_challenge_backend", func(s *StaleObservation, _ *engineOrder, _ *Manifest, _ *Plan) { s.Rejected.Backends[0].StartUS++ }},
		{"MissingACK", "required_proof_missing:stale_challenge_ack", func(s *StaleObservation, _ *engineOrder, _ *Manifest, _ *Plan) { s.Journal = "" }},
		{"WrongACKDigest", "required_proof_missing:stale_challenge_ack", func(s *StaleObservation, _ *engineOrder, _ *Manifest, _ *Plan) {
			s.Journal = strings.Replace(s.Journal, `"journal_sha256":"`, `"journal_sha256":"0`, 1)
		}},
		{"MissingAcquisition", "required_proof_missing:stale_challenge_acquisition", func(_ *StaleObservation, e *engineOrder, _ *Manifest, _ *Plan) {
			e.rows[commitAudit] = e.rows[commitAudit][:1]
		}},
		{"WrongWorkflow", "required_proof_missing:stale_challenge_successor_progress", func(_ *StaleObservation, e *engineOrder, _ *Manifest, _ *Plan) {
			e.rows[history][0]["workflow_id"] = "another-workflow"
		}},
		{"MissingProgress", "required_proof_missing:stale_challenge_successor_progress", func(s *StaleObservation, _ *engineOrder, _ *Manifest, _ *Plan) { s.Progress = "" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, p, arrivals, s, e := staleFixture(t)
			if tc.change != nil {
				tc.change(&s, &e, &m, &p)
			}
			e.checkStaleChallenge(m, p, arrivals, nil, &s)
			r := e.result
			if tc.code == "" {
				if r.Confirmation != (Confirmation{"confirmed", "cell_predicate_satisfied"}) || r.Labels[2].Outcome != "covered/pass" || len(r.Findings) != 0 {
					t.Fatalf("stale challenge verdict: %+v %+v %+v", r.Confirmation, r.Labels[2], r.Findings)
				}
			} else if r.Confirmation.Reason != tc.code || len(r.Findings) != 1 || r.Findings[0].Code != tc.code {
				t.Fatalf("stale challenge verdict: got %+v %+v, want %s", r.Confirmation, r.Findings, tc.code)
			}
		})
	}
	t.Run("OwnerGate", func(t *testing.T) {
		m, p, arrivals, s, e := staleFixture(t)
		e.result.record(Violation, "OwnerAtCommit", "OWNER_NOT_CURRENT_AT_COMMIT", commitAudit, nil)
		e.checkStaleChallenge(m, p, arrivals, nil, &s)
		e.result.finish()
		if e.result.Confirmation.Status == "confirmed" || e.result.Labels[2].Outcome != "covered/fail" || e.result.Safety != "FAIL" {
			t.Fatalf("stale challenge verdict: %+v", e.result)
		}
	})
	t.Run("OutsideCeiling", func(t *testing.T) {
		m, _, _, _, e := staleFixture(t)
		m.Cell = "hook-0"
		r := newResult(m, "")
		e.result = &r
		old := e.rows[commitAudit][0]
		progress := e.rows[commitAudit][2]
		progress["owner_id"], progress["epoch"] = old["owner_id"], old["epoch"]
		e.violate = func(predicate, code, relation string, a Row) {
			r.record(Violation, predicate, code, RelationPath(relation), nil)
		}
		e.checkOwnerAtCommit()
		r.finish()
		assertNoPromotion(t, r.Labels[2])
		if len(r.Findings) != 1 || r.Findings[0].Code != "OWNER_NOT_CURRENT_AT_COMMIT" || r.Safety != "FAIL" || r.ExitCode() != Fail {
			t.Fatalf("stale challenge verdict: %+v", r)
		}
	})
	t.Run("SourceBinding", func(t *testing.T) {
		m, _, _, s, _ := staleFixture(t)
		if !s.valid(m) {
			t.Fatal("stale challenge verdict: matching source rejected")
		}
		s.Binding.Episode = "hook-5-1"
		if s.valid(m) {
			t.Fatal("stale challenge verdict: foreign source accepted")
		}
	})
}

func staleFixture(t *testing.T) (Manifest, Plan, Arrivals, StaleObservation, engineOrder) {
	return staleFixtureFor(t, "hook-5")
}

func staleFixtureFor(t *testing.T, cell string) (Manifest, Plan, Arrivals, StaleObservation, engineOrder) {
	t.Helper()
	m := Manifest{Cell: cell, Episode: cell + "-0", Namespace: "ns", Workload: "PURE", Instrumentation: []string{WriteCapture}}
	base := time.Date(2026, 11, 24, 0, 0, 0, 0, time.UTC)
	at := func(seconds int) time.Time { return base.Add(time.Duration(seconds) * time.Second) }
	stamp := func(seconds int) string { return UTC(at(seconds)) }
	n := func(s string) json.Number { return json.Number(s) }
	start := StaleProcess{"old-pod", "old-container", 1, "1:101", "00000000-0000-0000-0000-000000000001"}
	peer := StaleProcess{"new-pod", "new-container", 1, "1:102", "00000000-0000-0000-0000-000000000002"}
	x := map[string]string{"cell": cell, "partition": "0", "owner": "old-owner", "epoch": "1", "node": "pure.echo", "iteration": "0", "revision": "2", "backend_pid": "5", "backend_start_us": "100"}
	y := map[string]string{"cell": cell, "partition": "0", "owner": "new-owner", "epoch": "2", "revision": "2", "transaction": "2", "backend_pid": "6", "backend_start_us": "110"}
	end := stamp(50)
	p := Plan{FaultMode: "injected", HoldDeadlineUTC: &end, Windows: []PlanWindow{
		{Name: "measured", Submissions: []Submission{{Key: "wf", Body: SubmissionBody{InitialNodeID: "pure.echo"}}}},
	}}
	removed := stamp(30)
	var journal strings.Builder
	seq := 0
	add := func(event, arm string, second int, fields Row) (int, string) {
		seq++
		fields["sequence"], fields["at_ns"], fields["event"], fields["arm"] = seq, at(second).UnixNano(), event, arm
		raw, err := Canonical(fields)
		if err != nil {
			t.Fatal(err)
		}
		journal.Write(raw)
		return seq, Hash(raw[:len(raw)-1])
	}
	ack := func(arm, boundary, invocation, role string, process StaleProcess, data map[string]string, armAt, reachAt int) staleACK {
		target := staleTarget{role, process.PodUID, process.Start, strings.ReplaceAll(process.Session, "-", "")}
		add("arm", arm, armAt, Row{"episode": m.Episode, "boundary": boundary, "target": target, "until_ms": at(50).UnixMilli(), "selector": map[string]string{"key": "wf", "epoch": data["epoch"]}})
		message := Hash([]byte(invocation))
		index, hash := add("reach", arm, reachAt, Row{"target": target, "boundary": boundary, "message_sha256": message,
			"fields": Row{"key": "wf", "invocation": invocation, "objects": data}})
		return staleACK{int64(index), message, hash}
	}
	oldACK := ack("old", "lease_takeover", "call", "predecessor", start, x, 1, 2)
	add("held", "old", 3, Row{"ack": oldACK})
	add("prolonged", "old", 5, Row{"ack": oldACK})
	newACK := ack("new", "lease_takeover_acquired", "peer-call", "successor", peer, y, 6, 17)
	add("held", "new", 18, Row{"ack": newACK})
	add("action", "old", 21, Row{"ack": oldACK, "command": "release"})
	add("command_issued", "old", 22, Row{"ack": oldACK, "command": "release"})
	add("release", "old", 23, Row{"ack": oldACK})
	add("rejection_observed", "new", 27, Row{"ack": newACK})
	add("action", "new", 28, Row{"ack": newACK, "command": "release"})
	add("command_issued", "new", 29, Row{"ack": newACK, "command": "release"})
	add("release", "new", 30, Row{"ack": newACK})
	backend := func(pid, start int64, process StaleProcess) []StaleBackend {
		return []StaleBackend{{pid, start, "dur053-e2-" + process.Session, "idle", false}}
	}
	s := StaleObservation{Schema: StaleVersion, Binding: Binding(m), Journal: journal.String(), Before: start, After: start, Peer: peer, Progress: "3",
		Hold:     StaleSample{at(4).UnixNano(), at(4).UnixNano(), stamp(4), "old-owner", 1, stamp(1), stamp(16), 2, 0, backend(5, 100, start)},
		Takeover: StaleSample{at(19).UnixNano(), at(20).UnixNano(), stamp(19), "new-owner", 2, stamp(16), stamp(31), 2, 0, backend(6, 110, peer)},
		Rejected: StaleSample{at(25).UnixNano(), at(26).UnixNano(), stamp(25), "new-owner", 2, stamp(16), stamp(31), 2, 0, backend(5, 100, start)},
		Call:     &StaleCall{start, at(24).UnixNano(), "CreateAttempt", "ErrLeaseNotOwned", "wf", "call", x},
	}
	audit := func(kind, order, epoch, owner string, proc StaleProcess, second int) Row {
		return Row{"kind": kind, "event_order": n(order), "epoch": n(epoch), "partition_id": n("0"), "owner_id": owner,
			"session_id": proc.Session, "pod_uid": proc.PodUID, "container_id": proc.PodUID, "finalized_at": stamp(second), "transaction_id": order}
	}
	old, newOwner, progress := audit("acquire", "1", "1", "old-owner", start, 1), audit("acquire", "2", "2", "new-owner", peer, 16), audit("history", "3", "2", "new-owner", peer, 31)
	progress["transition_id"] = "attempt"
	r := newResult(m, "")
	r.Labels[2].PopulationCount = 1
	e := engineOrder{result: &r, captureReady: true, workflows: map[string]Row{"wf": {"state": "SUCCEEDED", "partition_id": n("0")}},
		rows:         map[string][]Row{commitAudit: {old, newOwner, progress}, history: {{"transition_id": "attempt", "workflow_id": "wf", "reason": "ATTEMPT_CREATED", "node_id": "pure.echo", "iteration": n("0"), "revision": n("3")}}},
		byTransition: map[any]Row{"attempt": progress},
	}
	workflow := "wf"
	return m, p, Arrivals{RemovalUTC: &removed, Rows: []Arrival{
		{Key: workflow, Window: "measured", Attempts: []Send{{Outcome: "accepted", WorkflowID: &workflow, SentUTC: stamp(2)}}},
	}}, s, e
}
