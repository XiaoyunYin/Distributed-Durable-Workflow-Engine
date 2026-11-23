package k6v1

import (
	"fmt"
	"strings"
)

const (
	dispatchPredicate = "DispatchDisposition"
	outbox            = "engine.outbox"
	publications      = "engine.outbox_publications"
	inbox             = "engine.event_inbox"
	offsets           = "engine.consumer_offsets"
	wakeups           = "engine.scheduler_wakeups"
	quarantine        = "engine.transport_quarantine"
	reconciliation    = "engine.reconciliation_items"
	eventTopic        = "durable-agent.events.v1"
	taskTopic         = "durable-agent.tasks.v1"
	workerGroup       = "runtime-workers-v1"
	schedulerGroup    = "runtime-schedulers-v1"
)

// These history reasons create an outbox obligation in the same transaction.
// The worker result event type can be supplied by the caller.
var dispatchEvents = map[any]string{
	"WORKFLOW_CREATED": "workflow.created", attemptCreated: "attempt.dispatch",
	attemptReplaced: "attempt.dispatch", "REDISPATCH_UNCLAIMED": "attempt.redispatch",
	resultRecorded: "*", resultConsumed: "workflow.result_consumed",
	"GRAPH_ADVANCED": "graph.advanced", "TIMER_SCHEDULED": "timer.scheduled",
	canceled: "workflow.canceled", "APPROVAL_REQUESTED": "approval.requested",
	"APPROVAL_REJECTED": "approval.rejected", "APPROVAL_GRANTED": "approval.granted",
	"TIMEOUT_RECONCILIATION_REQUIRED": "reconciliation.required",
}

func dispatchTopic(event any) string {
	switch event {
	case "attempt.dispatch", "attempt.redispatch":
		return taskTopic
	case "workflow.created", "activity.result", "workflow.result_consumed", "graph.advanced",
		"timer.scheduled", "workflow.canceled", "approval.requested", "approval.decided",
		"approval.granted", "approval.rejected", "cancellation.requested", "effect.applied",
		"reconciliation.required", "reconciliation.resolved":
		return eventTopic
	}
	return ""
}

type dispatchRevision struct{ workflow, revision any }

func (e engineOrder) checkDispatch() {
	label := &e.result.Labels[5]
	label.PopulationUnit, label.Reason = "outbox_obligation", "no_positive_population"
	fail := func(code, relation string, row Row) { e.violate(dispatchPredicate, code, relation, row) }
	events := map[any]Row{}
	histories, byRevision := map[dispatchRevision]Row{}, map[dispatchRevision]Row{}
	for _, h := range e.rows[history] {
		histories[dispatchRevision{h["workflow_id"], h["revision"]}] = h
	}
	for _, row := range e.rows[outbox] {
		key := dispatchRevision{row["workflow_id"], row["aggregate_revision"]}
		h, topic := histories[key], dispatchTopic(row["event_type"])
		expected := dispatchEvents[h["reason"]]
		_, haveHistory := e.rows[history]
		switch {
		case haveHistory && expected == "":
			fail("DISPATCH_OUTBOX_UNMATCHED", outbox, row)
		case byRevision[key] != nil:
			fail("DISPATCH_MULTIPLE_EVENTS", outbox, row)
		case expected != "" && expected != "*" && row["event_type"] != expected:
			fail("DISPATCH_EVENT_TYPE", outbox, row)
		case topic == "" && row["publish_state"] != "QUARANTINED" || topic != "" && row["topic"] != topic:
			fail("DISPATCH_EVENT_TOPIC", outbox, row)
		case h != nil && !dispatchPayloadMatches(row, h):
			fail("DISPATCH_EVENT_PAYLOAD", outbox, row)
		}
		events[row["event_id"]], byRevision[key] = row, row
	}
	_, haveOutbox := e.rows[outbox]
	for _, h := range e.rows[history] {
		if dispatchEvents[h["reason"]] == "" {
			continue
		}
		row := byRevision[dispatchRevision{h["workflow_id"], h["revision"]}]
		if row == nil && !committed(e.byTransition[h["transition_id"]]) {
			// Without the audit link, the observed history alone cannot
			// identify the obligation's creating transaction.
			continue
		}
		label.PopulationCount++
		e.require(dispatchPredicate, "outbox_creation_transactions", history, h)
		if row == nil && haveOutbox {
			fail("DISPATCH_OUTBOX_MISSING", history, h)
		}
	}
	acknowledged := map[any]bool{}
	for _, p := range e.rows[publications] {
		id := p["event_id"]
		event := events[id]
		if haveOutbox && event == nil {
			fail("DISPATCH_PUBLICATION_UNMATCHED", publications, p)
		}
		if event != nil && (number(p["relay_attempt"]) < 1 ||
			number(p["relay_attempt"]) > number(event["relay_attempts"]) ||
			p["broker_acknowledged"] == true && (acknowledged[id] ||
				event["publish_state"] != "PUBLISHED" || p["relay_attempt"] != event["relay_attempts"])) {
			fail("DISPATCH_PUBLICATION_BINDING", publications, p)
		}
		acknowledged[id] = acknowledged[id] || p["broker_acknowledged"] == true
	}
	e.checkDispatchDispositions(events, acknowledged)
}

func dispatchPayloadMatches(event, h Row) bool {
	payload, _ := event["payload"].(map[string]any)
	if workflow := payload["workflow_id"]; workflow != nil && workflow != h["workflow_id"] {
		return false
	}
	if event["topic"] != taskTopic {
		return true
	}
	if h["attempt_number"] == nil {
		return false
	}
	attempt := number(h["attempt_number"])
	if h["reason"] == attemptReplaced {
		attempt++
	}
	return payload["node_id"] == h["node_id"] && payload["iteration"] == h["iteration"] &&
		payload["attempt_number"] != nil && number(payload["attempt_number"]) == attempt
}

type consumerPartition struct{ consumer, topic, partition any }

type deliveryPosition struct {
	consumerPartition
	offset any
}

func delivery(row Row) deliveryPosition {
	return deliveryPosition{consumerPartition{row["consumer_id"], row["topic"], row["kafka_partition"]}, row["kafka_offset"]}
}

func dispatchConsumer(topic any) string {
	if topic == taskTopic {
		return workerGroup
	}
	return schedulerGroup
}

func (e engineOrder) checkDispatchDispositions(events map[any]Row, acknowledged map[any]bool) {
	fail := func(code, relation string, row Row) { e.violate(dispatchPredicate, code, relation, row) }
	_, haveOutbox := e.rows[outbox]
	watermarks := map[consumerPartition]Row{}
	for _, row := range e.rows[offsets] {
		watermarks[delivery(row).consumerPartition] = row
	}
	type dispositionKey struct{ consumer, event any }
	disposed, accepted := map[dispositionKey]bool{}, map[any]bool{}
	positions := map[deliveryPosition]bool{}
	for _, row := range e.rows[inbox] {
		id, pos := row["event_id"], delivery(row)
		event := events[id]
		if haveOutbox && event == nil || event != nil && (row["topic"] != event["topic"] ||
			pos.consumer != dispatchConsumer(event["topic"])) ||
			positions[pos] || !validInboxDisposition(row["disposition"]) {
			fail("DISPATCH_INBOX_UNMATCHED", inbox, row)
			continue
		}
		positions[pos], disposed[dispositionKey{pos.consumer, id}] = true, true
		if row["disposition"] == "ACCEPTED" && pos.consumer == schedulerGroup && pos.topic == eventTopic {
			accepted[id] = true
		}
		if _, have := e.rows[offsets]; have && pos.offset != nil && watermarks[pos.consumerPartition] == nil {
			fail("DISPATCH_OFFSET_MISSING", inbox, row)
		}
	}
	woken, wakeupIDs := map[any]bool{}, map[string]bool{}
	_, haveInbox := e.rows[inbox]
	for _, row := range e.rows[wakeups] {
		id := row["event_id"]
		event := events[id]
		if haveOutbox && event == nil || haveInbox && !accepted[id] || woken[id] ||
			event != nil && (row["workflow_id"] != event["workflow_id"] || row["reason"] != "EVENT:"+event["event_type"].(string)) {
			fail("DISPATCH_WAKEUP_UNMATCHED", wakeups, row)
		}
		woken[id] = true
		wakeupIDs["wakeup/"+row["wakeup_id"].(string)] = true
	}
	poisonItems := map[string]bool{}
	for _, row := range e.rows[reconciliation] {
		if row["kind"] == "POISON_RECORD" {
			poisonItems[row["reference"].(string)] = true
		}
	}
	poison := map[string]bool{}
	for _, row := range e.rows[quarantine] {
		id, pos := row["event_id"], delivery(row)
		ref := fmt.Sprintf("transport/%s/%s/%d/%d", pos.consumer, pos.topic, number(pos.partition), number(pos.offset))
		poison[ref] = true
		_, haveReconciliation := e.rows[reconciliation]
		if positions[pos] || haveReconciliation && !poisonItems[ref] {
			fail("DISPATCH_QUARANTINE_UNMATCHED", quarantine, row)
		}
		if event := events[id]; event != nil && event["topic"] == pos.topic {
			disposed[dispositionKey{pos.consumer, id}] = true
		}
		if _, have := e.rows[offsets]; have && watermarks[pos.consumerPartition] == nil {
			fail("DISPATCH_OFFSET_MISSING", quarantine, row)
		}
		positions[pos] = true
	}
	_, haveQuarantine := e.rows[quarantine]
	_, haveWakeups := e.rows[wakeups]
	_, havePublications := e.rows[publications]
	for _, row := range e.rows[outbox] {
		id := row["event_id"]
		e.require(dispatchPredicate, "publication_attempt_identities", outbox, row)
		e.require(dispatchPredicate, "broker_event_copies", outbox, row)
		e.require(dispatchPredicate, "dispatch_disposition_transactions", outbox, row)
		switch row["publish_state"] {
		case "PUBLISHED":
			group := dispatchConsumer(row["topic"])
			if havePublications && !acknowledged[id] {
				fail("DISPATCH_PUBLICATION_MISSING", outbox, row)
			}
			if haveInbox && haveQuarantine && !disposed[dispositionKey{group, id}] {
				fail("DISPATCH_DISPOSITION_MISSING", outbox, row)
			}
			if haveWakeups && accepted[id] && !woken[id] {
				fail("DISPATCH_WAKEUP_MISSING", outbox, row)
			}
		case "QUARANTINED":
			if _, have := e.rows[reconciliation]; have && !poisonItems["outbox/"+id.(string)] {
				fail("DISPATCH_RECONCILIATION_MISSING", outbox, row)
			}
		}
	}
	for _, row := range e.rows[reconciliation] {
		ref := row["reference"].(string)
		unmatched := false
		switch row["kind"] {
		case "POISON_RECORD":
			eventID, isOutbox := strings.CutPrefix(ref, "outbox/")
			if isOutbox {
				unmatched = haveOutbox && (events[eventID] == nil || events[eventID]["publish_state"] != "QUARANTINED")
			} else {
				unmatched = haveQuarantine && !poison[ref]
			}
		case "PENDING_OUTBOX":
			eventID, _ := strings.CutPrefix(ref, "outbox/")
			event := events[eventID]
			unmatched = haveOutbox && (event == nil || row["status"] == "OPEN" && event["publish_state"] == "PUBLISHED")
		case "LOST_WAKEUP":
			unmatched = haveWakeups && !wakeupIDs[ref]
		}
		if unmatched {
			fail("DISPATCH_RECONCILIATION_UNMATCHED", reconciliation, row)
		}
	}
}

func validInboxDisposition(disposition any) bool {
	switch disposition {
	case "ACCEPTED", "ALREADY_HANDLED", "STALE", "QUARANTINED":
		return true
	}
	return false
}
