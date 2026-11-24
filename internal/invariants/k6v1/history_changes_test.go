package k6v1

import (
	"maps"
	"slices"
	"testing"
)

func TestK6HistoryChanges(t *testing.T) {
	n := testNumber
	node := lifecycleKey{"node", n("0"), nil}
	sibling := lifecycleKey{"sibling", n("0"), nil}
	done := lifecycleKey{"done", n("0"), nil}
	a := lifecycleKey{"node", n("0"), n("1")}
	next := lifecycleKey{"node", n("0"), n("2")}
	nodeImage := func(state, revision string, attempt any) Row {
		return Row{"state": state, "revision": n(revision), "current_attempt_number": attempt}
	}
	attemptImage := func(state string, current bool, effect, disposition string) Row {
		return Row{"state": state, "is_current": current, "effect_class": effect, "outcome_disposition": disposition}
	}
	runnable := nodeImage("RUNNABLE", "0", nil)
	waiting := nodeImage("WAITING_ACTIVITY", "1", nil)
	working := nodeImage("WAITING_ACTIVITY", "1", a.attempt)
	dispatchable := attemptImage("DISPATCHABLE", true, "PURE_ACTIVITY", "NONE")
	claimed := attemptImage("CLAIMED", true, "PURE_ACTIVITY", "NONE")
	succeeded := attemptImage("SUCCEEDED", false, "PURE_ACTIVITY", "NONE")
	external := attemptImage("CLAIMED", true, "NON_COOPERATING_EFFECT", "NONE")
	type images = map[lifecycleKey]Row
	revision := n("2")
	graph := Row{"graph": map[string]any{"nodes": []any{map[string]any{"id": "node", "next": "done"}, "done"}}}
	graphEvent := Row{"payload": map[string]any{"workflow_id": "workflow", "from_node_id": "node",
		"next": []any{map[string]any{"NodeID": "done", "Iteration": node.iteration}}}}
	expired := maps.Clone(dispatchable)
	expired["heartbeat_deadline"] = "2026-01-02T03:04:05Z"
	renewed := maps.Clone(expired)
	renewed["heartbeat_deadline"] = "2026-01-02T03:04:06Z"

	cases := []struct {
		name    string
		reasons []string
		h       Row
		tx      historyChanges
	}{
		{"Creation", []string{"WORKFLOW_CREATED"}, Row{"node_id": node.node, "new_state": "RUNNABLE"},
			historyChanges{after: images{node: runnable}}},
		{"AttemptCreation", []string{attemptCreated}, Row{"attempt_number": a.attempt},
			historyChanges{before: images{node: waiting}, after: images{node: working, a: dispatchable}}},
		{"ResultRecord", []string{resultRecorded}, Row{"attempt_number": a.attempt},
			historyChanges{before: images{node: working, a: claimed}, after: images{a: succeeded}}},
		{"ResultConsume", []string{resultConsumed}, Row{"attempt_number": a.attempt, "new_state": "RUNNABLE"},
			historyChanges{before: images{node: working, a: succeeded}, after: images{node: nodeImage("SUCCEEDED", "2", nil)}}},
		{"Redispatch", []string{"REDISPATCH_UNCLAIMED"}, Row{"attempt_number": a.attempt},
			historyChanges{before: images{node: working, a: expired}, after: images{a: renewed}}},
		{"TimeoutReconciliation", []string{"TIMEOUT_RECONCILIATION_REQUIRED"}, Row{"attempt_number": a.attempt},
			historyChanges{before: images{node: working, a: external},
				after: images{a: attemptImage("TIMED_OUT", false, "NON_COOPERATING_EFFECT", "OUTCOME_UNKNOWN")}}},
		{"TimeoutReplacement", []string{attemptReplaced}, Row{"attempt_number": a.attempt},
			historyChanges{before: images{node: working, a: claimed}, after: images{
				node: nodeImage("WAITING_ACTIVITY", "1", next.attempt),
				a:    attemptImage("TIMED_OUT", false, "PURE_ACTIVITY", "NONE"), next: dispatchable}}},
		{"Graph", []string{"GRAPH_ADVANCED"}, nil,
			historyChanges{before: images{node: nodeImage("SUCCEEDED", "2", nil)},
				after:  images{node: nodeImage("SUCCEEDED", "3", nil), done: runnable},
				graphs: map[any]Row{revision: graphEvent}, definition: graph}},
		{"Cancellation", []string{canceled}, Row{"new_state": "CANCELED"},
			historyChanges{before: images{node: working, sibling: runnable, done: nodeImage("SUCCEEDED", "2", nil), a: external},
				after: images{node: nodeImage("CANCELED", "2", nil), sibling: nodeImage("CANCELED", "1", nil),
					a: attemptImage("CANCELED", false, "NON_COOPERATING_EFFECT", "OUTCOME_UNKNOWN")}}},
		{"TimerSchedule", []string{"TIMER_SCHEDULED"}, nil,
			historyChanges{before: images{node: runnable}, after: images{node: nodeImage("WAITING_TIMER", "1", nil)}}},
		{"ApprovalRequest", []string{"APPROVAL_REQUESTED"}, nil,
			historyChanges{before: images{node: runnable}, after: images{node: nodeImage("WAITING_APPROVAL", "1", nil)}}},
		{"ApprovalGrant", []string{"APPROVAL_GRANTED"}, nil,
			historyChanges{before: images{node: nodeImage("WAITING_APPROVAL", "1", nil)},
				after: images{node: nodeImage("RUNNABLE", "2", nil)}}},
		{"ApprovalReject", []string{"APPROVAL_REJECTED"}, nil,
			historyChanges{before: images{node: nodeImage("WAITING_APPROVAL", "1", nil)},
				after: images{node: nodeImage("REJECTED", "2", nil)}}},
		{"ActivitySchedule", []string{"ACTIVITY_SCHEDULED", "DUR027_FIXTURE_ACTIVITY", "DUR027_STALE_OWNER",
			"DUR034_CHECK_TO_COMMIT", "M5_FIXTURE_ACTIVITY"}, nil, historyChanges{before: images{node: runnable},
			after: images{node: waiting}}},
		{"TimerDue", []string{"TIMER_DUE"}, nil,
			historyChanges{before: images{node: nodeImage("WAITING_TIMER", "1", nil)},
				after: images{node: nodeImage("RUNNABLE", "2", nil)}}},
		{"GrantExpiry", []string{"K6_GRANT_EXPIRED_OUTCOME_UNKNOWN"}, Row{"attempt_number": a.attempt},
			historyChanges{before: images{node: working, a: claimed},
				after: images{node: nodeImage("RECONCILIATION_REQUIRED", "2", a.attempt),
					a: attemptImage("OUTCOME_UNKNOWN", false, "PURE_ACTIVITY", "OUTCOME_UNKNOWN")}}},
		{"Unsupported", []string{"LOCAL_ACTIVITY_UNSUPPORTED"},
			Row{"old_state": "RUNNABLE", "new_state": "PAUSED_UNSUPPORTED_VERSION"}, historyChanges{}},
		{"Probe", []string{"STALE_EPOCH_PROBE", "DUR043_STALE_OWNER_PROBE", "DUR049_SAME_OWNER_STALE_EPOCH_PROBE_CLEANUP",
			"DUR049_SAME_OWNER_STALE_EPOCH_NEGATIVE_CONTROL"}, Row{"old_state": "RUNNABLE", "new_state": "CANCELED"}, historyChanges{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, reason := range tc.reasons {
				historyRow := Row{"reason": reason, "workflow_id": "workflow", "revision": revision}
				switch tc.name {
				case "Creation", "Cancellation", "Unsupported", "Probe":
				default:
					historyRow["node_id"], historyRow["iteration"] = node.node, node.iteration
				}
				maps.Copy(historyRow, tc.h)
				c := newClosureFixture()
				c.witness = func(path string, _ Row) Witness { return Witness{Path: RelationPath(path)} }
				c.checkHistoryChanges(historyRow, tc.tx)
				if len(c.result.Findings) != 0 {
					t.Fatalf("valid %s history failed: %+v", reason, c.result.Findings)
				}
				if len(tc.tx.after) == 0 {
					bad := tc.tx
					bad.after = images{node: waiting}
					requireHistoryChangeFailure(t, historyRow, bad)
					continue
				}
				for key := range tc.tx.after {
					bad := tc.tx
					bad.after = maps.Clone(tc.tx.after)
					delete(bad.after, key)
					requireHistoryChangeFailure(t, historyRow, bad)
					// A write for a different iteration cannot fill the same obligation.
					bad.after[lifecycleKey{key.node, n("9"), key.attempt}] = tc.tx.after[key]
					requireHistoryChangeFailure(t, historyRow, bad)
				}
				if tc.name == "Graph" {
					bad := tc.tx
					bad.graphs = nil
					requireHistoryChangeFailure(t, historyRow, bad)
				}
			}
		})
	}
	t.Run("Unknown", func(t *testing.T) {
		c := newClosureFixture()
		c.witness = func(path string, row Row) Witness {
			return Witness{Path: RelationPath(path), RowKey: []any{row["workflow_id"], row["revision"], row["transition_id"]}}
		}
		h := Row{"reason": "WORKFLOW_ADVANCED", "workflow_id": "workflow", "revision": revision, "transition_id": "unknown"}
		c.checkHistoryChanges(h, historyChanges{})
		if !slices.ContainsFunc(c.result.Findings, func(f Finding) bool {
			return f.Predicate == "AtomicHistory" && f.Kind == MissingProof && f.Code == "required_proof_missing:node_attempt_write_transactions" &&
				f.Witness != nil && f.Witness.Path == RelationPath(history) &&
				slices.Equal(f.Witness.RowKey, []any{"workflow", revision, "unknown"})
		}) {
			t.Fatalf("unknown history reason was not typed missing proof: %+v", c.result.Findings)
		}
	})
}

func requireHistoryChangeFailure(t *testing.T, h Row, tx historyChanges) {
	t.Helper()
	c := newClosureFixture()
	c.checkHistoryChanges(h, tx)
	if len(c.result.Findings) != 1 || c.result.Findings[0].Predicate != "AtomicHistory" ||
		c.result.Findings[0].Kind != Violation || c.result.Findings[0].Code != "CAPTURE_HISTORY_CHANGES" {
		t.Fatalf("expected capture violation CAPTURE_HISTORY_CHANGES for %s, got %+v", h["reason"], c.result.Findings)
	}
}

func TestK6CancellationCommitPopulation(t *testing.T) {
	n := testNumber
	makeNode := func(order, tx, state, revision string) Row {
		w := capturedRow(nodeRows, "update", order, tx, []any{"wf", "node", n("0")},
			nil, Row{"state": state, "revision": n(revision)})
		w["workflow_id"] = "wf"
		return w
	}
	for _, tc := range []struct {
		name, settleTx, order, state string
		violation                    bool
	}{
		{"SameCommit", "cancel", "3", "CANCELED", false},
		{"SettledEarlier", "earlier", "2", "SUCCEEDED", false},
		{"SettledLater", "later", "5", "CANCELED", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const reason = "CAPTURE_HISTORY_CHANGES"
			c := newClosureFixture()
			h := Row{"transition_id": "cancel-history", "workflow_id": "wf", "reason": canceled, "new_state": "CANCELED"}
			audit := Row{"transition_id": h["transition_id"], "transaction_id": "cancel", "event_order": n("4"), "finalized_at": "now"}
			c.rows[history], c.rows[commitAudit] = []Row{h}, []Row{audit}
			c.byTransition[h["transition_id"]] = audit
			c.rows[WriteCapture] = []Row{makeNode("1", "created", "RUNNABLE", "0"), makeNode(tc.order, tc.settleTx, tc.state, "1")}
			c.checkHistoryTransactions(c.checkOrder())
			if tc.violation {
				wantCaptureCode(t, c, reason)
			} else if len(c.result.Findings) != 0 {
				t.Fatalf("cancellation population was wrong: %+v", c.result.Findings)
			}
		})
	}
}
