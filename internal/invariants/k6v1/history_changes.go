package k6v1

import (
	"encoding/json"
	"slices"
	"strconv"
	"time"
)

type lifecycleKey struct{ node, iteration, attempt any }

type historyChangeRule func(historyChanges, Row) bool

// This allowlist mirrors the insertHistory call sites in internal/state and
// the named callers of the two free-form transition writers. An unknown
// reason has no contract from which to infer the required writes.
var historyChangeRules = map[string]historyChangeRule{
	"WORKFLOW_CREATED":                               createdWorkflow,
	attemptCreated:                                   createdAttempt,
	resultRecorded:                                   recordedResult,
	resultConsumed:                                   consumedResult,
	"REDISPATCH_UNCLAIMED":                           redispatchedAttempt,
	"TIMEOUT_RECONCILIATION_REQUIRED":                timedOutUncertain,
	attemptReplaced:                                  replacedAttempt,
	"GRAPH_ADVANCED":                                 advancedGraph,
	canceled:                                         canceledWorkflow,
	"TIMER_SCHEDULED":                                scheduledTimer,
	"APPROVAL_REQUESTED":                             requestedApproval,
	"APPROVAL_GRANTED":                               grantedApproval,
	"APPROVAL_REJECTED":                              rejectedApproval,
	"ACTIVITY_SCHEDULED":                             scheduledActivity,
	"DUR027_FIXTURE_ACTIVITY":                        scheduledActivity,
	"DUR027_STALE_OWNER":                             scheduledActivity,
	"DUR034_CHECK_TO_COMMIT":                         scheduledActivity,
	"M5_FIXTURE_ACTIVITY":                            scheduledActivity,
	"TIMER_DUE":                                      firedTimer,
	"K6_GRANT_EXPIRED_OUTCOME_UNKNOWN":               expiredGrant,
	"LOCAL_ACTIVITY_UNSUPPORTED":                     pausedUnsupported,
	"STALE_EPOCH_PROBE":                              canceledProbe,
	"DUR043_STALE_OWNER_PROBE":                       canceledProbe,
	"DUR049_SAME_OWNER_STALE_EPOCH_PROBE_CLEANUP":    canceledProbe,
	"DUR049_SAME_OWNER_STALE_EPOCH_NEGATIVE_CONTROL": canceledProbe,
}

type historyChanges struct {
	before, after map[lifecycleKey]Row
	// Only insertions in this transaction can bind a graph event to history.
	graphs     map[any]Row
	definition Row
}

func (c captureClosure) checkHistoryTransactions(transactions []any) {
	histories := map[any][]Row{}
	writes := map[any][]Row{}
	events := map[any]Row{}
	definitions := map[[2]any]Row{}
	current := map[any]map[lifecycleKey]Row{}
	for _, d := range c.rows["engine.workflow_definitions"] {
		definitions[[2]any{d["definition_id"], d["version"]}] = d
	}
	for _, event := range c.rows[outbox] {
		if event["event_type"] == "graph.advanced" {
			events[event["event_id"]] = event
		}
	}
	for _, h := range c.rows[history] {
		if a := c.byTransition[h["transition_id"]]; committed(a) {
			xid := a["transaction_id"]
			histories[xid] = append(histories[xid], h)
		}
	}
	for _, w := range c.rows[WriteCapture] {
		switch w["subject"] {
		case nodeRows, activityAttempts, outbox:
			xid := w["transaction_id"]
			writes[xid] = append(writes[xid], w)
		}
	}
	for _, xid := range transactions {
		byWorkflow := map[any]historyChanges{}
		for _, w := range writes[xid] {
			workflow := w["workflow_id"]
			tx := byWorkflow[workflow]
			key := w["row_key"].([]any)
			if w["subject"] == outbox {
				if event := events[key[0]]; w["operation"] == "insert" && event != nil && event["workflow_id"] == workflow {
					if tx.graphs == nil {
						tx.graphs = map[any]Row{}
					}
					tx.graphs[event["aggregate_revision"]] = event
				}
			} else {
				row := lifecycleKey{key[1], key[2], nil}
				if w["subject"] == activityAttempts {
					row.attempt = key[3]
				}
				if tx.after == nil {
					tx.after = map[lifecycleKey]Row{}
				}
				tx.after[row] = captureImage(w, "after_image")
			}
			byWorkflow[workflow] = tx
		}
		for _, h := range histories[xid] {
			workflow := h["workflow_id"].(string)
			tx := byWorkflow[workflow]
			tx.before = current[workflow]
			wf := c.workflows[workflow]
			tx.definition = definitions[[2]any{wf["definition_id"], wf["definition_version"]}]
			c.checkHistoryChanges(h, tx)
		}
		// Apply the net writes only after checking every history in the commit.
		for workflow, tx := range byWorkflow {
			if current[workflow] == nil && len(tx.after) != 0 {
				current[workflow] = map[lifecycleKey]Row{}
			}
			for key, image := range tx.after {
				if image == nil {
					delete(current[workflow], key)
				} else {
					current[workflow][key] = image
				}
			}
		}
	}
}

func (c captureClosure) checkHistoryChanges(h Row, tx historyChanges) {
	reason, _ := h["reason"].(string)
	rule := historyChangeRules[reason]
	if rule == nil {
		c.require("AtomicHistory", "node_attempt_write_transactions", history, h)
		return
	}
	if !rule(tx, h) {
		c.violate("AtomicHistory", "CAPTURE_HISTORY_CHANGES", history, h)
	}
}

func historyNode(h Row) lifecycleKey {
	return lifecycleKey{h["node_id"], h["iteration"], nil}
}

func historyAttempt(h Row) lifecycleKey {
	return lifecycleKey{h["node_id"], h["iteration"], h["attempt_number"]}
}

func unscopedHistory(h Row) bool {
	return h["node_id"] == nil && h["iteration"] == nil && h["attempt_number"] == nil
}

func createdWorkflow(tx historyChanges, h Row) bool {
	key := lifecycleKey{h["node_id"], json.Number("0"), nil}
	return h["node_id"] != nil && h["iteration"] == nil && h["attempt_number"] == nil &&
		h["new_state"] == "RUNNABLE" && insertedNode(tx, key)
}

func insertedNode(tx historyChanges, key lifecycleKey) bool {
	after := tx.after[key]
	return tx.before[key] == nil && after["state"] == "RUNNABLE" && after["current_attempt_number"] == nil &&
		after["revision"] == json.Number("0")
}

func nodeRevision(before, after Row, advance int64) bool {
	return before["revision"] != nil && after["revision"] != nil &&
		number(after["revision"]) == number(before["revision"])+advance
}

func movedNode(tx historyChanges, key lifecycleKey, from, to string) bool {
	before, after := tx.before[key], tx.after[key]
	return before["state"] == from && after["state"] == to &&
		nodeRevision(before, after, 1) && before["current_attempt_number"] == after["current_attempt_number"]
}

func nodeTransition(tx historyChanges, h Row, from, to string) bool {
	return h["attempt_number"] == nil && movedNode(tx, historyNode(h), from, to)
}

func scheduledActivity(tx historyChanges, h Row) bool {
	return nodeTransition(tx, h, "RUNNABLE", "WAITING_ACTIVITY")
}

func scheduledTimer(tx historyChanges, h Row) bool {
	return nodeTransition(tx, h, "RUNNABLE", "WAITING_TIMER")
}

func firedTimer(tx historyChanges, h Row) bool {
	return nodeTransition(tx, h, "WAITING_TIMER", "RUNNABLE")
}

func requestedApproval(tx historyChanges, h Row) bool {
	return nodeTransition(tx, h, "RUNNABLE", "WAITING_APPROVAL") ||
		nodeTransition(tx, h, "WAITING_APPROVAL", "WAITING_APPROVAL")
}

func grantedApproval(tx historyChanges, h Row) bool {
	return nodeTransition(tx, h, "WAITING_APPROVAL", "RUNNABLE")
}

func rejectedApproval(tx historyChanges, h Row) bool {
	return nodeTransition(tx, h, "WAITING_APPROVAL", "REJECTED")
}

func pausedUnsupported(tx historyChanges, h Row) bool {
	return unscopedHistory(h) && len(tx.after) == 0 &&
		h["old_state"] == "RUNNABLE" && h["new_state"] == "PAUSED_UNSUPPORTED_VERSION"
}

func canceledProbe(tx historyChanges, h Row) bool {
	return unscopedHistory(h) && len(tx.after) == 0 && h["old_state"] != "WAITING_ACTIVITY" && h["new_state"] == "CANCELED"
}

func insertedAttempt(tx historyChanges, key lifecycleKey) bool {
	return key.attempt != nil && tx.before[key] == nil && tx.after[key]["state"] == "DISPATCHABLE" &&
		tx.after[key]["is_current"] == true
}

func changedPointer(tx historyChanges, node lifecycleKey, from, to any) bool {
	before, after := tx.before[node], tx.after[node]
	return before["state"] == "WAITING_ACTIVITY" && after["state"] == before["state"] &&
		before["current_attempt_number"] == from && after["current_attempt_number"] == to && nodeRevision(before, after, 0)
}

func createdAttempt(tx historyChanges, h Row) bool {
	key := historyAttempt(h)
	return changedPointer(tx, historyNode(h), nil, key.attempt) && insertedAttempt(tx, key)
}

func retiredAttempt(tx historyChanges, key lifecycleKey, from, to string) bool {
	before, after := tx.before[key], tx.after[key]
	return key.attempt != nil && before["state"] == from && before["is_current"] == true &&
		after["state"] == to && after["is_current"] == false
}

func recordedResult(tx historyChanges, h Row) bool {
	key := historyAttempt(h)
	return retiredAttempt(tx, key, "CLAIMED", "SUCCEEDED") ||
		retiredAttempt(tx, key, "CLAIMED", "FAILED_RETRYABLE") || retiredAttempt(tx, key, "CLAIMED", "FAILED_FINAL")
}

func consumedResult(tx historyChanges, h Row) bool {
	key, attempt := historyNode(h), historyAttempt(h)
	before, after := tx.before[key], tx.after[key]
	if attempt.attempt == nil || tx.before[attempt]["is_current"] != false || before["state"] != "WAITING_ACTIVITY" ||
		before["current_attempt_number"] != attempt.attempt || after["current_attempt_number"] != nil ||
		!nodeRevision(before, after, 1) {
		return false
	}
	switch tx.before[attempt]["state"] {
	case "SUCCEEDED":
		return after["state"] == "SUCCEEDED" && (h["new_state"] == "RUNNABLE" || h["new_state"] == "SUCCEEDED")
	case "FAILED_RETRYABLE":
		return after["state"] == "WAITING_TIMER" && h["new_state"] == "WAITING_TIMER" ||
			after["state"] == "FAILED" && h["new_state"] == "FAILED"
	case "FAILED_FINAL":
		return after["state"] == "FAILED" && h["new_state"] == "FAILED"
	default:
		return false
	}
}

func redispatchedAttempt(tx historyChanges, h Row) bool {
	key := historyAttempt(h)
	before, after := tx.before[key], tx.after[key]
	if key.attempt == nil || tx.before[historyNode(h)]["current_attempt_number"] != key.attempt ||
		before["state"] != "DISPATCHABLE" || after["state"] != "DISPATCHABLE" ||
		before["is_current"] != true || after["is_current"] != true {
		return false
	}
	oldDeadline, _ := before["heartbeat_deadline"].(string)
	newDeadline, _ := after["heartbeat_deadline"].(string)
	prior, priorErr := time.Parse(time.RFC3339Nano, oldDeadline)
	next, nextErr := time.Parse(time.RFC3339Nano, newDeadline)
	return nextErr == nil && priorErr == nil && next.After(prior)
}

func timedOutUncertain(tx historyChanges, h Row) bool {
	key := historyAttempt(h)
	return tx.before[historyNode(h)]["current_attempt_number"] == key.attempt &&
		retiredAttempt(tx, key, "CLAIMED", "TIMED_OUT") &&
		tx.before[key]["effect_class"] == "NON_COOPERATING_EFFECT" && tx.after[key]["outcome_disposition"] == "OUTCOME_UNKNOWN"
}

func replacedAttempt(tx historyChanges, h Row) bool {
	old := historyAttempt(h)
	if !retiredAttempt(tx, old, "CLAIMED", "TIMED_OUT") || tx.before[old]["effect_class"] == "NON_COOPERATING_EFFECT" {
		return false
	}
	next := lifecycleKey{old.node, old.iteration, json.Number(strconv.FormatInt(number(old.attempt)+1, 10))}
	return changedPointer(tx, historyNode(h), old.attempt, next.attempt) && insertedAttempt(tx, next) &&
		tx.before[old]["effect_class"] == tx.after[next]["effect_class"]
}

func expiredGrant(tx historyChanges, h Row) bool {
	node, attempt := historyNode(h), historyAttempt(h)
	before, after := tx.before[node], tx.after[node]
	switch before["state"] {
	case "RUNNABLE", "WAITING_APPROVAL", "WAITING_ACTIVITY":
	default:
		return false
	}
	if after["state"] != "RECONCILIATION_REQUIRED" || !nodeRevision(before, after, 1) ||
		before["current_attempt_number"] != attempt.attempt || after["current_attempt_number"] != attempt.attempt {
		return false
	}
	if attempt.attempt == nil {
		return true
	}
	if tx.before[attempt]["is_current"] == false {
		return true // The writer locks a previously retired attempt but leaves it intact.
	}
	return (retiredAttempt(tx, attempt, "CLAIMED", "OUTCOME_UNKNOWN") ||
		retiredAttempt(tx, attempt, "DISPATCHABLE", "OUTCOME_UNKNOWN")) && tx.after[attempt]["outcome_disposition"] == "OUTCOME_UNKNOWN"
}

func canceledWorkflow(tx historyChanges, h Row) bool {
	if !unscopedHistory(h) || h["new_state"] != "CANCELED" {
		return false
	}
	for key, before := range tx.before {
		if key.attempt == nil {
			switch before["state"] {
			case "SUCCEEDED", "FAILED", "REJECTED", "CANCELED", "ABANDONED":
				continue
			}
			after := tx.after[key]
			if after["state"] != "CANCELED" || after["current_attempt_number"] != nil || !nodeRevision(before, after, 1) {
				return false
			}
		} else if before["is_current"] == true {
			disposition := "NONE"
			if before["state"] == "CLAIMED" && before["effect_class"] != "PURE_ACTIVITY" {
				disposition = "OUTCOME_UNKNOWN"
			}
			if !(retiredAttempt(tx, key, "CLAIMED", "CANCELED") || retiredAttempt(tx, key, "DISPATCHABLE", "CANCELED")) ||
				tx.after[key]["outcome_disposition"] != disposition {
				return false
			}
		}
	}
	return true
}

func advancedGraph(tx historyChanges, h Row) bool {
	key := historyNode(h)
	return h["attempt_number"] == nil && tx.before[key]["current_attempt_number"] == nil &&
		(movedNode(tx, key, "SUCCEEDED", "SUCCEEDED") ||
			movedNode(tx, key, "RUNNABLE", "SUCCEEDED") || movedNode(tx, key, "RUNNABLE", "FAILED")) &&
		graphSuccessors(tx, h)
}

func graphSuccessors(tx historyChanges, h Row) bool {
	// The same transaction's event identifies the selected successor for an
	// activity with alternatives. The sealed definition bounds the selection.
	payload, _ := tx.graphs[h["revision"]]["payload"].(map[string]any)
	if payload == nil || payload["workflow_id"] != h["workflow_id"] || payload["from_node_id"] != h["node_id"] {
		return false
	}
	rawNext, present := payload["next"]
	if !present {
		return false
	}
	next, _ := rawNext.([]any)
	graph, _ := tx.definition["graph"].(map[string]any)
	nodes, _ := graph["nodes"].([]any)
	var source Row
	for _, item := range nodes {
		switch node := item.(type) {
		case string:
			if node == h["node_id"] {
				source = Row{"id": node}
			}
		case map[string]any:
			if node["id"] == h["node_id"] {
				source = node
			}
		}
	}
	if source == nil {
		return false
	}
	state := tx.before[historyNode(h)]["state"]
	activity := source["kind"] == nil || source["kind"] == "" || source["kind"] == "activity"
	if activity && state != "SUCCEEDED" || !activity && state != "RUNNABLE" {
		return false
	}
	var expected []any
	switch declared := source["next"].(type) {
	case string:
		expected = []any{declared}
	case []any:
		expected = slices.Clone(declared)
	}
	if source["kind"] == "fanout" || source["kind"] == "fan_out" {
		branches, _ := source["branches"].([]any)
		expected = append(expected, branches...)
		if join := source["join"]; join != nil {
			expected = append(expected, join)
		}
	}
	choice := activity && len(expected) > 1
	if choice && len(next) != 1 || !choice && len(next) != len(expected) {
		return false
	}
	for i, raw := range next {
		node, _ := raw.(map[string]any)
		id := node["NodeID"]
		if node["Iteration"] != h["iteration"] || choice && !slices.Contains(expected, id) || !choice && id != expected[i] {
			return false
		}
		key := lifecycleKey{id, node["Iteration"], nil}
		if tx.before[key] == nil && !insertedNode(tx, key) {
			return false
		}
	}
	return true
}
