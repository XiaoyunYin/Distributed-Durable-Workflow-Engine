package k6v1

import (
	"encoding/json"
	"maps"
	"slices"
	"testing"
)

func testNumber(n string) json.Number { return json.Number(n) }

func transportOrder(t *testing.T, m Manifest, rows map[string][]Row, run func(engineOrder)) Result {
	t.Helper()
	r := newResult(m, "")
	keys := map[string][]string{}
	for _, spec := range EngineRelations(false, true) {
		keys[spec.Name] = spec.Keys
	}
	witness := func(relation string, row Row) Witness {
		key := make([]any, 0, len(keys[relation]))
		for _, col := range keys[relation] {
			key = append(key, row[col])
		}
		raw, err := Canonical(row)
		if err != nil {
			t.Fatal(err)
		}
		return Witness{RelationPath(relation), key, Hash(raw)}
	}
	violate := func(predicate, code, relation string, row Row) {
		w := witness(relation, row)
		r.record(Violation, predicate, code, w.Path, &w)
	}
	run(newEngineOrder(&r, rows, nil, witness, violate))
	r.finish()
	return r
}

func requireTransportFinding(t *testing.T, r Result, predicate, code string, kind FindingKind, path string, key []any) {
	t.Helper()
	for _, finding := range r.Findings {
		w := finding.Witness
		if finding.Predicate == predicate && finding.Code == code && finding.Kind == kind && finding.Source == path &&
			w != nil && w.Path == path && slices.Equal(w.RowKey, key) && digest.MatchString(w.SHA256) {
			return
		}
	}
	t.Fatalf("expected %s %s %s at %s %v, got %+v", kind, predicate, code, path, key, r.Findings)
}

func dispatchFixture() map[string][]Row {
	n := testNumber
	return map[string][]Row{
		history:     {{"workflow_id": "wf", "revision": n("1"), "transition_id": "transition", "reason": "WORKFLOW_CREATED"}},
		commitAudit: {{"kind": "history", "transition_id": "transition", "transaction_id": "write", "finalized_at": "commit"}},
		outbox: {{"event_id": "event", "workflow_id": "wf", "aggregate_revision": n("1"), "event_type": "workflow.created",
			"payload": map[string]any{"workflow_id": "wf"}, "topic": eventTopic, "publish_state": "PUBLISHED", "relay_attempts": n("2")}},
		publications: {
			{"publication_id": "first", "event_id": "event", "relay_attempt": n("1"), "broker_acknowledged": false},
			{"publication_id": "retry", "event_id": "event", "relay_attempt": n("2"), "broker_acknowledged": true},
		},
		inbox: {{"consumer_id": schedulerGroup, "event_id": "event", "disposition": "ACCEPTED", "topic": eventTopic,
			"kafka_partition": n("0"), "kafka_offset": n("0")}},
		wakeups:    {{"wakeup_id": "wake", "event_id": "event", "workflow_id": "wf", "reason": "EVENT:workflow.created"}},
		offsets:    {{"consumer_id": schedulerGroup, "topic": eventTopic, "kafka_partition": n("0"), "next_offset": n("1")}},
		quarantine: {}, reconciliation: {},
	}
}

func TestK6DispatchDisposition(t *testing.T) {
	historyKey := []any{"wf", testNumber("1"), "transition"}
	eventKey := []any{"event"}
	for _, tc := range []struct {
		name, code, relation string
		key                  []any
		change               func(map[string][]Row)
	}{
		{"Clean", "", "", nil, func(map[string][]Row) {}},
		{"MissingOutbox", "DISPATCH_OUTBOX_MISSING", history, historyKey, func(rows map[string][]Row) { rows[outbox] = nil }},
		{"ExtraOutbox", "DISPATCH_OUTBOX_UNMATCHED", outbox, eventKey, func(rows map[string][]Row) {
			rows[outbox][0]["aggregate_revision"] = testNumber("2")
		}},
		{"RetryEvent", "DISPATCH_MULTIPLE_EVENTS", outbox, []any{"new-event"}, func(rows map[string][]Row) {
			newEvent := maps.Clone(rows[outbox][0])
			newEvent["event_id"] = "new-event"
			rows[outbox] = append(rows[outbox], newEvent)
			rows[publications][1]["event_id"] = "new-event"
		}},
		{"WrongEventType", "DISPATCH_EVENT_TYPE", outbox, eventKey, func(rows map[string][]Row) {
			rows[outbox][0]["event_type"] = "graph.advanced"
		}},
		{"WrongTopic", "DISPATCH_EVENT_TOPIC", outbox, eventKey, func(rows map[string][]Row) {
			rows[outbox][0]["topic"] = taskTopic
		}},
		{"WrongPayload", "DISPATCH_EVENT_PAYLOAD", outbox, eventKey, func(rows map[string][]Row) {
			rows[outbox][0]["payload"] = map[string]any{"workflow_id": "different"}
		}},
		{"ExtraPublication", "DISPATCH_PUBLICATION_UNMATCHED", publications, []any{"retry"}, func(rows map[string][]Row) {
			rows[publications][1]["event_id"] = "extra-event"
		}},
		{"WrongPublicationAttempt", "DISPATCH_PUBLICATION_BINDING", publications, []any{"retry"}, func(rows map[string][]Row) {
			rows[publications][1]["relay_attempt"] = testNumber("3")
		}},
		{"MissingPublication", "DISPATCH_PUBLICATION_MISSING", outbox, eventKey, func(rows map[string][]Row) {
			rows[publications] = rows[publications][:1]
		}},
		{"ExtraInbox", "DISPATCH_INBOX_UNMATCHED", inbox, []any{"unregistered", "event"}, func(rows map[string][]Row) {
			row := maps.Clone(rows[inbox][0])
			row["consumer_id"] = "unregistered"
			rows[inbox] = append(rows[inbox], row)
		}},
		{"MissingDisposition", "DISPATCH_DISPOSITION_MISSING", outbox, eventKey, func(rows map[string][]Row) { rows[inbox] = nil }},
		{"MissingOffset", "DISPATCH_OFFSET_MISSING", inbox, []any{schedulerGroup, "event"}, func(rows map[string][]Row) {
			rows[offsets] = nil
		}},
		{"ExtraWakeup", "DISPATCH_WAKEUP_UNMATCHED", wakeups, []any{"extra"}, func(rows map[string][]Row) {
			row := maps.Clone(rows[wakeups][0])
			row["wakeup_id"] = "extra"
			rows[wakeups] = append(rows[wakeups], row)
		}},
		{"MissingWakeup", "DISPATCH_WAKEUP_MISSING", outbox, eventKey, func(rows map[string][]Row) { rows[wakeups] = nil }},
		{"ExtraQuarantine", "DISPATCH_QUARANTINE_UNMATCHED", quarantine, []any{"extra"}, func(rows map[string][]Row) {
			rows[quarantine] = []Row{{"quarantine_id": "extra", "consumer_id": schedulerGroup, "topic": eventTopic,
				"kafka_partition": testNumber("0"), "kafka_offset": testNumber("0")}}
		}},
		{"MissingReconciliation", "DISPATCH_RECONCILIATION_MISSING", outbox, eventKey, func(rows map[string][]Row) {
			rows[outbox][0]["publish_state"] = "QUARANTINED"
			rows[publications][1]["broker_acknowledged"] = false
		}},
		{"ExtraReconciliation", "DISPATCH_RECONCILIATION_UNMATCHED", reconciliation, []any{"extra"}, func(rows map[string][]Row) {
			rows[reconciliation] = []Row{{"item_id": "extra", "kind": "PENDING_OUTBOX", "reference": "outbox/other"}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := dispatchFixture()
			tc.change(rows)
			r := transportOrder(t, Manifest{Cell: "hook-0"}, rows, engineOrder.checkDispatch)
			label := r.Labels[5]
			if tc.code != "" {
				requireTransportFinding(t, r, dispatchPredicate, tc.code, Violation, RelationPath(tc.relation), tc.key)
				if r.Safety != "FAIL" || label.Outcome != "covered/fail" || r.ExitCode() != Fail {
					t.Fatalf("dispatch violation lost in roll-up: %+v", r)
				}
				return
			}
			for _, gap := range []struct {
				name, relation string
				key            []any
			}{
				{"outbox_creation_transactions", history, historyKey},
				{"publication_attempt_identities", outbox, eventKey},
				{"broker_event_copies", outbox, eventKey},
				{"dispatch_disposition_transactions", outbox, eventKey},
			} {
				requireTransportFinding(t, r, dispatchPredicate, "required_proof_missing:"+gap.name, MissingProof, RelationPath(gap.relation), gap.key)
			}
			if len(r.Findings) != 4 || r.Safety == "FAIL" || r.Completeness != "FAIL" || r.ExitCode() != Fail ||
				label.Outcome != "NOT COVERED" || label.Reason != "required_proof_missing:dispatch_disposition_transactions" {
				t.Fatalf("dispatch missing proof lost in roll-up: %+v", r)
			}
		})
	}
}
