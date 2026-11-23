package k6v1

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"testing"
	"testing/fstest"
	"time"
)

var fixed = time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)

const (
	firstSession       = "00000000-0000-0000-0000-000000000001"
	deadSession        = "00000000-0000-0000-0000-000000000002"
	idleSession        = "00000000-0000-0000-0000-000000000003"
	restartedSession   = "00000000-0000-0000-0000-000000000004"
	fixtureIntent      = "00000000-0000-0000-0000-000000000005"
	fixtureApplication = "00000000-0000-0000-0000-000000000006"
)

func fixtureStream[T any](m Manifest, schema, source string, fields ...T) Stream[T] {
	s := Stream[T]{Header: StreamHeader{"dur053.k6-" + schema + ".v1", Binding(m), source, UTC(fixed)}, Rows: []Record[T]{}}
	for _, field := range fields {
		s.Rows = append(s.Rows, Record[T]{Position{int64(len(s.Rows) + 1), UTC(fixed)}, field})
	}
	_ = SealStream(&s, "terminal", UTC(fixed), int64(len(s.Rows)), 0)
	return s
}

func resealFixture[T any](s *Stream[T]) {
	_ = SealStream(s, s.Seal.Mode, s.Seal.ThroughUTC, int64(len(s.Rows)), 0)
}

func fixtureHash(s string) string { return Hash([]byte(s)) }

func fixtureExternal(t *testing.T, effect bool) (Manifest, External, map[string][]Row) {
	t.Helper()
	h := Hash(nil)
	m := Manifest{Schema: EvidenceVersion, Campaign: "test", RunID: "1", Job: "k6", Episode: "hook-0-0", Ordinal: 0,
		Seed: 530001, Cell: "hook-0", Workload: "PURE", Namespace: "ns", ResetID: "reset", BarrierID: "barrier",
		Database: "engine", EngineInstance: "engine-instance", PlanSHA256: h, TopologySHA256: h,
		SourceTreeSHA256: h, ImageSetSHA256: h, FreezeSHA256: h, RegistrySHA256: RegistrySHA256,
		Instrumentation: []string{}, RequiredSources: RequiredSources(false, effect)}
	if effect {
		m.Ordinal, m.Seed, m.Cell, m.Episode, m.Workload = 90, 530091, "medium-12", "medium-12-0", "COOPERATING_EFFECT"
		m.Sink = &DatabaseIdentity{"sink", "sink-instance"}
	}
	a := AttemptIdentity{"workflow", "node", 0, 1, fixtureHash("claim-token"), "claim-request"}
	ledgerRows := []WorkerRow{{Kind: "start", Invocation: "invocation", Attempt: &a}}
	rows := map[string][]Row{
		"engine.activity_attempts": {{"workflow_id": "workflow", "node_id": "node", "iteration": json.Number("0"), "attempt_number": json.Number("1"),
			"claim_token_hash": a.ClaimHash, "worker_id": "claimant", "worker_request_id": a.RequestID}},
		"engine.k4_audit": {{"kind": "claim", "claim_identity": map[string]any{"workflow_id": "workflow", "node_id": "node",
			"iteration": json.Number("0"), "attempt_number": json.Number("1"), "token_hash": a.ClaimHash, "worker_id": "claimant", "worker_request_id": a.RequestID}}},
	}
	e := External{Sessions: map[string]Stream[WorkerRow]{}}
	roster := []WatchRow{{Kind: "baseline", Cursor: "100"}}
	watch := func(row WatchRow) {
		row.From, row.Cursor = roster[len(roster)-1].Cursor, fmt.Sprint(100+len(roster))
		roster = append(roster, row)
	}
	if effect {
		var ingress []IngressRow
		d := DecisionRow{fixtureIntent, fixtureHash("proposal"), fixtureHash("grant"), "system:serviceaccount:ns:k6-approver",
			"sa-uid", "secret-uid", fixtureHash("https://kubernetes.default.svc.cluster.local"), "dur053-k6-approval", "APPROVED", UTC(fixed)}
		decisions := fixtureStream(m, "decisions", "adapter", d)
		e.Decisions = &decisions
		rows[intentsRelation] = []Row{{"intent_id": fixtureIntent, "workflow_id": "workflow", "target": "resource",
			"proposal_hash": d.ProposalHash, "grant_token_hash": d.GrantHash, "approver_id": d.Subject, "decision": d.Outcome,
			"decided_at": d.DecidedUTC, "k6_logical_key": "logical"}}
		for idx, op := range []string{"allocate", "apply", "lookup"} {
			id := "request-" + op
			wire := WireDigest{9, fixtureHash("wire" + op)}
			traffic := Traffic{Method: "POST", Operation: op, MessageHash: fixtureHash("invocation"),
				RequestIDHash: fixtureHash(id), KeyHash: fixtureHash("logical"), Intent: fixtureIntent, ResourceHash: fixtureHash("resource"),
				GrantHash: d.GrantHash, Attempt: 1, WorkerBinding: fixtureHash("worker" + op),
				Request: WireDigest{5, fixtureHash("body" + op)}, Wire: &wire}
			reply := WireDigest{5, fixtureHash("reply" + op)}
			if op != "allocate" {
				ledgerRows = append(ledgerRows, WorkerRow{Kind: "send", Invocation: "invocation", Traffic: &traffic},
					WorkerRow{Kind: "receive", Invocation: "invocation", Traffic: &traffic, Response: &reply})
			}
			call := Row{"call_id": int64(idx + 1), "operation": op, "workflow_id": "workflow", "logical_key": "logical",
				"intent_id": fixtureIntent, "resource_id": "resource", "message_id": "invocation", "attempt": json.Number("1"),
				"request_id": id, "body_hash": traffic.Request.SHA256, "connection_hash": fixtureHash("sink" + op),
				"outcome": []string{"ALLOCATED", "APPLIED", "FOUND"}[idx], "response_hash": reply.SHA256}
			app := ""
			if op != "allocate" {
				app = fixtureApplication
				call["application_id"] = app
			}
			rows[SinkCalls] = append(rows[SinkCalls], call)
			rows[SinkBindings] = append(rows[SinkBindings], Row{"request_id": id, "body_hash": fixtureHash(op + "\x00" + traffic.Request.SHA256)})
			ingress = append(ingress, IngressRow{Traffic: traffic, Ordinal: int64(idx + 1), Copy: 1, PeerHash: fixtureHash("peer"),
				ConnectionHash: call["connection_hash"].(string), Outcome: "delivered", Response: &reply, ApplicationID: app, Acknowledged: true})
		}
		s := fixtureStream(m, "ingress", "relay-receiver", ingress...)
		e.Ingress = &s
		rows[SinkReceipts] = []Row{{"application_id": fixtureApplication}}
		rows[SinkAllocations] = []Row{{"intent_id": fixtureIntent, "grant_hash": d.GrantHash, "attempt": json.Number("1"), "fence": json.Number("1")}}
		rows[SinkLookupMisses] = []Row{}
	} else {
		components := []Component{{"database", "engine", "engine"}, {"deployment", "worker", "worker"}, {"service", "api", "engine"}}
		raw, _ := Canonical(components)
		m.TopologySHA256 = Hash(raw)
		s := fixtureStream(m, "topology", "collector", TopologyRow{"rendered", "renderer", "plan", components, topologyKinds},
			TopologyRow{"observed", "apiserver", "rv", components, topologyKinds})
		e.Topology = &s
	}
	ledgerRows = append(ledgerRows, WorkerRow{Kind: "finish", Invocation: "invocation", Attempt: &a})
	for i, id := range []string{firstSession, deadSession, idleSession, restartedSession} {
		s := WorkerSession{id, "pod-" + fmt.Sprint(i), "container-" + fmt.Sprint(i), 0, "boot-" + fmt.Sprint(i), UTC(fixed), UTC(fixed), "claimant-" + fmt.Sprint(i)}
		if i == 0 {
			s.Claimant = "claimant"
		}
		if i == 3 {
			s.PodUID, s.Restart = "pod-1", 1
		}
		runtime := s.Runtime()
		watch(WatchRow{Kind: "runtime", Runtime: &runtime})
		watch(WatchRow{Kind: "session", Session: &s, Registration: &Registration{
			PodUID: s.PodUID, Claimant: s.Claimant, ServiceAccountUID: "worker-sa", Audience: "durable-worker-control", Observer: "token-review",
		}})
		events := []WorkerRow{{Kind: "session", Session: &s}}
		if i == 0 {
			events = append(events, ledgerRows...)
		}
		l := fixtureStream(m, "worker", "receiver", events...)
		if i == 1 || i == 3 {
			l.Seal.Mode = "receiver_final"
			watch(WatchRow{Kind: "death", Session: &s, Death: &Death{"watcher", UTC(fixed), "receiver", l.Seal.Last, l.Seal.SHA256}})
		}
		e.Sessions[id] = l
	}
	e.Roster = fixtureStream(m, "roster", "watcher", roster...)
	offset := int64(7)
	broker := fixtureStream(m, "broker", "broker-reader", BrokerRow{"cluster", "group", 2, "stable", UTC(fixed), UTC(fixed),
		[]BrokerPartition{{"events", "topic-id", 0, 0, 7, &offset}}, []BrokerMember{{"member", "client", 2, []PartitionIdentity{{"topic-id", 0}}}}, nil})
	e.Broker = &broker
	return m, e, rows
}

func mapExternal(t *testing.T, e External) fstest.MapFS {
	t.Helper()
	files, err := e.Files()
	if err != nil {
		t.Fatal(err)
	}
	out := fstest.MapFS{}
	for path, raw := range files {
		out[path] = &fstest.MapFile{Data: raw}
	}
	return out
}

func externalVerdict(t *testing.T, m Manifest, original External, rows map[string][]Row, mutate func(fstest.MapFS)) Result {
	t.Helper()
	files := mapExternal(t, original)
	if mutate != nil {
		mutate(files)
	}
	e, issues := ReadExternal(files, m)
	r := newResult(m, "")
	for _, issue := range issues {
		r.failIntegrity(&issue)
	}
	checkExternal(&r, m, e, rows)
	r.finish()
	return r
}

func wantExternal(t *testing.T, r Result, code, path string) {
	t.Helper()
	if !slices.ContainsFunc(r.Findings, func(f Finding) bool { return f.Code == code && f.Source == path }) {
		t.Fatalf("expected %s for %s, got %+v", code, path, r.Findings)
	}
}

func TestK6RosterPositive(t *testing.T) {
	for _, effect := range []bool{false, true} {
		m, e, rows := fixtureExternal(t, effect)
		r := externalVerdict(t, m, e, rows, nil)
		if r.ExitCode() != Incomplete || len(r.Findings) != 0 || len(r.InvariantsCovered) != 0 {
			t.Fatalf("clean external evidence failed: %+v", r)
		}
	}
}

func TestK6RosterCompleteness(t *testing.T) {
	m, e, rows := fixtureExternal(t, false)
	for _, id := range []string{deadSession, idleSession, restartedSession} {
		path := SessionPath(id)
		r := externalVerdict(t, m, e, rows, func(files fstest.MapFS) { delete(files, path) })
		wantExternal(t, r, "required_proof_missing:"+path, path)
	}
	for i, row := range e.Roster.Rows {
		if row.Data.Kind == "session" && row.Data.Session.ID == restartedSession {
			e.Roster.Rows = slices.Delete(e.Roster.Rows, i, i+1)
			break
		}
	}
	for i := range e.Roster.Rows {
		e.Roster.Rows[i].Position.Sequence = int64(i + 1)
		if i > 0 {
			e.Roster.Rows[i].Data.From = e.Roster.Rows[i-1].Data.Cursor
		}
	}
	resealFixture(&e.Roster)
	delete(e.Sessions, restartedSession)
	wantExternal(t, externalVerdict(t, m, e, rows, nil), "ROSTER_RUNTIME_WITHOUT_SESSION", RosterPath)
}

func TestK6SessionDeath(t *testing.T) {
	m, e, rows := fixtureExternal(t, false)
	for i := range e.Roster.Rows {
		row := &e.Roster.Rows[i].Data
		if row.Death != nil {
			row.Death.FinalSHA256 = fixtureHash("wrong")
		}
	}
	resealFixture(&e.Roster)
	wantExternal(t, externalVerdict(t, m, e, rows, nil), "SESSION_DEATH_UNCORROBORATED", SessionPath(deadSession))
}

func TestK6SessionSequence(t *testing.T) {
	m, e, rows := fixtureExternal(t, false)
	session := e.Sessions[firstSession]
	session.Rows = slices.Delete(session.Rows, 1, 2)
	resealFixture(&session)
	e.Sessions[firstSession] = session
	wantExternal(t, externalVerdict(t, m, e, rows, nil), "EXTERNAL_SEQUENCE_GAP", SessionPath(firstSession))
}

func TestK6IngressGap(t *testing.T) {
	t.Run("Sequence", func(t *testing.T) {
		m, e, rows := fixtureExternal(t, true)
		e.Ingress.Rows = slices.Delete(e.Ingress.Rows, 1, 2)
		resealFixture(e.Ingress)
		wantExternal(t, externalVerdict(t, m, e, rows, nil), "EXTERNAL_SEQUENCE_GAP", IngressPath)
	})
	t.Run("Ordinal", func(t *testing.T) {
		m, e, rows := fixtureExternal(t, true)
		e.Ingress.Rows = slices.Delete(e.Ingress.Rows, 1, 2)
		e.Ingress.Rows[1].Position.Sequence = 2
		resealFixture(e.Ingress)
		wantExternal(t, externalVerdict(t, m, e, rows, nil), "INGRESS_ORDINAL_GAP", IngressPath)
	})
}

func TestK6IngressUnacknowledged(t *testing.T) {
	m, e, rows := fixtureExternal(t, true)
	e.Ingress.Rows[1].Data.Acknowledged = false
	resealFixture(e.Ingress)
	r := externalVerdict(t, m, e, rows, nil)
	wantExternal(t, r, "INGRESS_UNACKNOWLEDGED", IngressPath)
	if r.Safety != "FAIL" || r.Labels[7].Outcome != "covered/fail" {
		t.Fatalf("unacknowledged ingress escaped safety: %+v", r)
	}
}

func TestK6IngressRequestBindingDomain(t *testing.T) {
	m, e, rows := fixtureExternal(t, true)
	if r := externalVerdict(t, m, e, rows, nil); len(r.Findings) != 0 {
		t.Fatalf("operation-bound sink request rejected: %+v", r.Findings)
	}
	apply := e.Ingress.Rows[1].Data.Traffic
	for _, wrong := range []string{apply.Request.SHA256, fixtureHash("lookup\x00" + apply.Request.SHA256)} {
		rows[SinkBindings][1]["body_hash"] = wrong
		wantExternal(t, externalVerdict(t, m, e, rows, nil), "SINK_BINDING_WITHOUT_INGRESS", RelationPath(SinkBindings))
	}
}

func TestK6IngressCopies(t *testing.T) {
	m, e, rows := fixtureExternal(t, true)
	duplicate := e.Ingress.Rows[1]
	duplicate.Position.Sequence = 4
	duplicate.Data.Copy, duplicate.Data.Outcome = 2, "duplicate_reply"
	e.Ingress.Rows = append(e.Ingress.Rows, duplicate)
	resealFixture(e.Ingress)
	wantExternal(t, externalVerdict(t, m, e, rows, nil), "INGRESS_CALL_COPY", IngressPath)
}

func fixtureApply(t *testing.T, outcome, workerOutcome string) (Manifest, External, map[string][]Row) {
	t.Helper()
	m, e, rows := fixtureExternal(t, true)
	e.Ingress.Rows = e.Ingress.Rows[:2]
	rows[SinkCalls] = rows[SinkCalls][:2]
	rows[SinkBindings] = rows[SinkBindings][:2]
	session := e.Sessions[firstSession]
	session.Rows = slices.Delete(session.Rows, 4, 6) // Remove the lookup send and receive.
	session.Rows[4].Position.Sequence = 5
	if outcome != "delivered" {
		apply := &e.Ingress.Rows[1].Data
		apply.Outcome, apply.Response, apply.ApplicationID = outcome, nil, ""
		worker := &session.Rows[3].Data
		worker.Kind, worker.Response = workerOutcome, nil
	}
	resealFixture(e.Ingress)
	resealFixture(&session)
	e.Sessions[firstSession] = session
	return m, e, rows
}

func omitFixtureApplyCall(rows map[string][]Row) {
	rows[SinkCalls] = rows[SinkCalls][:1]
	rows[SinkBindings] = rows[SinkBindings][:1]
	rows[SinkReceipts] = nil
}

func TestK6IngressCloseWithoutSinkCall(t *testing.T) {
	for _, tc := range []struct {
		name, outcome, workerOutcome string
		connected                    bool
	}{
		{"NoConnection", "close", "close", false},
		{"Connected", "close", "timeout", true},
		{"Timeout", "timeout", "timeout", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, e, rows := fixtureApply(t, tc.outcome, tc.workerOutcome)
			omitFixtureApplyCall(rows)
			if !tc.connected {
				e.Ingress.Rows[1].Data.ConnectionHash = ""
				resealFixture(e.Ingress)
			}
			r := externalVerdict(t, m, e, rows, nil)
			if len(r.Findings) != 0 {
				t.Fatalf("call-free ingress terminal rejected: %+v", r.Findings)
			}
			if r.ExitCode() != Incomplete || r.Safety != "NOT COVERED" || len(r.InvariantsCovered) != 0 ||
				r.Labels[7].Outcome != "NOT COVERED" || r.Labels[7].Reason != "predicate_pending:G3a2" {
				t.Fatalf("call-free ingress terminal changed coverage: %+v", r)
			}
		})
	}
}

func TestK6IngressCloseCallCardinality(t *testing.T) {
	t.Run("MultipleCalls", func(t *testing.T) {
		m, e, rows := fixtureApply(t, "close", "close")
		call := rows[SinkCalls][1]
		call["outcome"], call["application_id"], call["response_hash"] = "STALE_FENCE", nil, nil
		rows[SinkReceipts] = nil
		if r := externalVerdict(t, m, e, rows, nil); len(r.Findings) != 0 {
			t.Fatalf("one matching call rejected: %+v", r.Findings)
		}
		second := maps.Clone(call)
		second["call_id"] = int64(3)
		rows[SinkCalls] = append(rows[SinkCalls], second)
		wantExternal(t, externalVerdict(t, m, e, rows, nil), "INGRESS_CALL_COPY", IngressPath)
	})
	t.Run("ReusedConnectionWithoutCall", func(t *testing.T) {
		m, e, rows := fixtureApply(t, "close", "close")
		omitFixtureApplyCall(rows)
		duplicate := e.Ingress.Rows[1]
		duplicate.Position.Sequence, duplicate.Data.Copy = 3, 2
		e.Ingress.Rows = append(e.Ingress.Rows, duplicate)
		resealFixture(e.Ingress)
		wantExternal(t, externalVerdict(t, m, e, rows, nil), "INGRESS_CALL_COPY", IngressPath)
	})
	t.Run("CallWithoutConnectionWitness", func(t *testing.T) {
		m, e, rows := fixtureApply(t, "close", "close")
		e.Ingress.Rows[1].Data.ConnectionHash = ""
		resealFixture(e.Ingress)
		r := externalVerdict(t, m, e, rows, nil)
		wantExternal(t, r, "INGRESS_CALL_COPY", IngressPath)
		wantExternal(t, r, "SINK_CALL_WITHOUT_INGRESS", RelationPath(SinkCalls))
		wantExternal(t, r, "SINK_RECEIPT_WITHOUT_APPLICATION", RelationPath(SinkReceipts))
		wantExternal(t, r, "SINK_BINDING_WITHOUT_INGRESS", RelationPath(SinkBindings))
	})
	t.Run("DeliveredWithoutCall", func(t *testing.T) {
		m, e, rows := fixtureApply(t, "delivered", "receive")
		omitFixtureApplyCall(rows)
		wantExternal(t, externalVerdict(t, m, e, rows, nil), "INGRESS_CALL_COPY", IngressPath)
	})
}

func TestK6OrphanClosure(t *testing.T) {
	t.Run("SuccessAfterCallFreeClose", func(t *testing.T) {
		m, e, rows := fixtureApply(t, "close", "timeout")
		omitFixtureApplyCall(rows)
		attempt := rows["engine.activity_attempts"][0]
		attempt["effect_class"], attempt["state"], attempt["logical_effect_key"] = "COOPERATING_EFFECT", "SUCCEEDED", "logical"
		r := externalVerdict(t, m, e, rows, nil)
		wantExternal(t, r, "EFFECT_SUCCESS_WITHOUT_DELIVERY", RelationPath("engine.activity_attempts"))
		if len(r.Findings) != 1 {
			t.Fatalf("expected only the unresolved success, got %+v", r.Findings)
		}
	})
	t.Run("ReverseCall", func(t *testing.T) {
		m, e, rows := fixtureExternal(t, true)
		e.Ingress.Rows = e.Ingress.Rows[:2]
		resealFixture(e.Ingress)
		r := externalVerdict(t, m, e, rows, nil)
		wantExternal(t, r, "SINK_CALL_WITHOUT_INGRESS", RelationPath(SinkCalls))
		wantExternal(t, r, "WORKER_SEND_WITHOUT_INGRESS", SessionPath(firstSession))
	})
	t.Run("SuppressedLookup", func(t *testing.T) {
		for _, outcome := range []string{"FOUND", "MISSING"} {
			t.Run(outcome, func(t *testing.T) {
				m, e, rows := fixtureExternal(t, true)
				if outcome == "MISSING" {
					row := &e.Ingress.Rows[2].Data
					const body = "K6_SINK_MISSING\n"
					reply := WireDigest{Length: int64(len(body)), SHA256: fixtureHash(body)}
					row.ApplicationID, row.Response = "", &reply
					call := rows[SinkCalls][2]
					call["outcome"], call["application_id"], call["response_hash"] = "MISSING", nil, nil
					session := e.Sessions[firstSession]
					session.Rows[len(session.Rows)-2].Data.Response = &reply
					resealFixture(&session)
					e.Sessions[firstSession] = session
					resealFixture(e.Ingress)
					if r := externalVerdict(t, m, e, rows, nil); len(r.Findings) > 0 {
						t.Fatalf("complete MISSING exchange rejected: %+v", r.Findings)
					}
				}
				rows[SinkCalls] = rows[SinkCalls][:2]
				wantExternal(t, externalVerdict(t, m, e, rows, nil), "INGRESS_CALL_COPY", IngressPath)
			})
		}
	})
	t.Run("DifferentRecoveryKey", func(t *testing.T) {
		m, e, rows := fixtureExternal(t, true)
		e.Ingress.Rows[2].Data.Traffic.KeyHash = fixtureHash("wrong-recovery")
		resealFixture(e.Ingress)
		wantExternal(t, externalVerdict(t, m, e, rows, nil), "DELIVERY_LOGICAL_KEY", IngressPath)
	})
}

func TestK6DecisionReverse(t *testing.T) {
	m, e, rows := fixtureExternal(t, true)
	e.Decisions.Rows = e.Decisions.Rows[:0]
	resealFixture(e.Decisions)
	wantExternal(t, externalVerdict(t, m, e, rows, nil), "DECISION_MISSING_FOR_INTENT", RelationPath(intentsRelation))
}

func TestK6BrokerRegression(t *testing.T) {
	m, e, rows := fixtureExternal(t, false)
	copy := e.Broker.Rows[0]
	copy.Position.Sequence = 2
	copy.Data.Generation--
	copy.Data.Members = slices.Clone(copy.Data.Members)
	copy.Data.Members[0].Generation--
	e.Broker.Rows = append(e.Broker.Rows, copy)
	resealFixture(e.Broker)
	wantExternal(t, externalVerdict(t, m, e, rows, nil), "EXTERNAL_REGRESSION", BrokerPath)
}

func TestK6TopologyPure(t *testing.T) {
	m, e, rows := fixtureExternal(t, false)
	e.Topology.Rows[1].Data.Components = slices.Insert(slices.Clone(e.Topology.Rows[1].Data.Components), 1, Component{"deployment", "sink", "sink"})
	resealFixture(e.Topology)
	wantExternal(t, externalVerdict(t, m, e, rows, nil), "PURE_EFFECT_COMPONENT", TopologyPath)
}

func TestK6SessionNestedCredentials(t *testing.T) {
	m, e, rows := fixtureExternal(t, true)
	for _, tc := range []struct{ path, before, after string }{
		{SessionPath(firstSession), `"claim_hash":"` + fixtureHash("claim-token") + `"`, `"claim_hash":"eyJLeVNlY3JldA.sentinel.signature"`},
		{IngressPath, `"grant_hash":"` + fixtureHash("grant") + `"`, `"grant_hash":{"authorization":"eyJLeVNlY3JldA.sentinel.signature"}`},
		{DecisionsPath, `"service_account_uid":"sa-uid"`, `"service_account_uid":"{\"token\":\"sentinel\"}"`},
	} {
		r := externalVerdict(t, m, e, rows, func(files fstest.MapFS) {
			file := files[tc.path]
			file.Data = bytes.ReplaceAll(file.Data, []byte(tc.before), []byte(tc.after))
		})
		raw, _ := Canonical(r)
		if r.ExitCode() != Fail || bytes.Contains(raw, []byte("sentinel")) {
			t.Fatalf("nested credential escaped in %s: %s", tc.path, raw)
		}
	}
}
