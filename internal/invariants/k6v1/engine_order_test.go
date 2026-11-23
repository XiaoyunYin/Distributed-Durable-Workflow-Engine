package k6v1

import (
	"encoding/json"
	"maps"
	"slices"
	"testing"
)

func orderFixture() map[string][]Row {
	n := func(v string) json.Number { return json.Number(v) }
	h := func(id, revision, reason, attempt string) Row {
		row := Row{"workflow_id": "wf", "revision": n(revision), "transition_id": id, "actor_kind": "scheduler",
			"actor_id": "owner", "scheduler_epoch": n("1"), "node_id": "node", "iteration": n("0"),
			"old_state": "WAITING_ACTIVITY", "new_state": "WAITING_ACTIVITY", "reason": reason}
		if attempt != "" {
			row["attempt_number"] = n(attempt)
		}
		return row
	}
	a := func(order, kind, epoch, owner, transition string) Row {
		row := Row{"event_order": n(order), "kind": kind, "epoch": n(epoch), "owner_id": owner,
			"partition_id": n("0"), "transaction_id": "txn-" + order, "finalized_at": "date"}
		if transition != "" {
			row["transition_id"] = transition
		}
		return row
	}
	d := func(attempt, state string) Row {
		return Row{"workflow_id": "wf", "node_id": "node", "iteration": n("0"), "attempt_number": n(attempt),
			"claim_token_hash": "token-" + attempt, "worker_id": "worker", "worker_request_id": "request-" + attempt,
			"state": state, "result": n("5"), "is_current": attempt == "1"}
	}
	start := h("created", "1", "WORKFLOW_CREATED", "")
	start["actor_kind"], start["actor_id"], start["scheduler_epoch"], start["old_state"], start["new_state"] = "client", "api", nil, nil, "RUNNABLE"
	schedule := h("scheduled", "2", "ACTIVITY_SCHEDULED", "")
	schedule["old_state"] = "RUNNABLE"
	created, replacement := h("attempt", "3", attemptCreated, "1"), h("replacement", "4", attemptReplaced, "1")
	result := h("result", "5", resultRecorded, "2")
	result["actor_kind"], result["actor_id"], result["scheduler_epoch"] = "worker", "worker", nil
	end := h("end", "6", resultConsumed, "2")
	end["new_state"] = "RUNNABLE"
	final := h("final", "7", "WORKFLOW_ADVANCED", "")
	final["old_state"], final["new_state"] = "RUNNABLE", "SUCCEEDED"
	old, winner := d("1", "TIMED_OUT"), d("2", "SUCCEEDED")
	claim := func(order string, db Row) Row {
		row := a(order, "claim", "0", "", "")
		id := maps.Clone(db)
		id["token_hash"] = id["claim_token_hash"]
		row["claim_identity"] = map[string]any(id)
		return row
	}
	return map[string][]Row{
		executions:       {{"workflow_id": "wf", "namespace": "ns", "submission_key": "key", "partition_id": n("0"), "state": "SUCCEEDED", "revision": n("7")}},
		nodeRows:         {{"workflow_id": "wf", "node_id": "node", "iteration": n("0"), "state": "SUCCEEDED", "accepted_result": n("5")}},
		activityAttempts: {old, winner},
		history:          {start, schedule, created, replacement, result, end, final},
		commitAudit: {
			a("10", "acquire", "1", "owner", ""), a("20", "history", "0", "", "created"),
			a("30", "history", "1", "owner", "scheduled"), a("40", "history", "1", "owner", "attempt"),
			claim("50", old), a("60", "history", "1", "owner", "replacement"), claim("70", winner),
			a("80", "history", "0", "", "result"), a("90", "history", "1", "owner", "end"),
			a("100", "history", "1", "owner", "final"), a("110", "acquire", "2", "successor", ""),
		},
	}
}

func TestK6PredicateEngineOrder(t *testing.T) {
	type test struct {
		name, predicate, code string
		kind                  FindingKind
		change                func(map[string][]Row)
	}
	for _, tc := range []test{
		{"MissingUnexportedNodeWrites", "AtomicHistory", "required_proof_missing:node_attempt_write_transactions", MissingProof, func(map[string][]Row) {}},
		{"MissingUnexportedResultIdentity", "CurrentClaimWinner", "required_proof_missing:result_commit_identity", MissingProof, func(map[string][]Row) {}},
		{"MissingUnexportedOwnerWrites", "OwnerAtCommit", "required_proof_missing:owner_write_transactions", MissingProof, func(map[string][]Row) {}},
		{"ReasonActor", "AtomicHistory", "HISTORY_REASON_ACTOR", Violation, func(rows map[string][]Row) { rows[history][0]["actor_kind"] = "worker" }},
		{"AuditMissing", "AtomicHistory", "required_proof_missing:" + RelationPath(commitAudit), MissingProof,
			func(rows map[string][]Row) { rows[commitAudit][1]["transaction_id"] = "" }},
		{"AuditOrphan", "AtomicHistory", "HISTORY_AUDIT_UNMATCHED", Violation,
			func(rows map[string][]Row) { rows[commitAudit][1]["transition_id"] = "unknown" }},
		{"AuditPartition", "AtomicHistory", "HISTORY_AUDIT_BINDING", Violation,
			func(rows map[string][]Row) { rows[commitAudit][1]["partition_id"] = json.Number("1") }},
		{"AuditOrder", "AtomicHistory", "HISTORY_AUDIT_BINDING", Violation,
			func(rows map[string][]Row) { rows[commitAudit][3]["event_order"] = json.Number("29") }},
		{"AuditTransactionOrder", "AtomicHistory", "HISTORY_AUDIT_TRANSACTION_ORDER", Violation,
			func(rows map[string][]Row) {
				rows[commitAudit][3]["transaction_id"] = rows[commitAudit][1]["transaction_id"]
			}},
		{"AttemptMissing", "AtomicHistory", "HISTORY_ATTEMPT_MISSING", Violation,
			func(rows map[string][]Row) { rows[activityAttempts] = rows[activityAttempts][:1] }},
		{"NodeMissing", "AtomicHistory", "HISTORY_NODE_MISSING", Violation,
			func(rows map[string][]Row) { rows[nodeRows] = nil }},
		{"N5OldOwner", "OwnerAtCommit", "OWNER_NOT_CURRENT_AT_COMMIT", Violation,
			func(rows map[string][]Row) {
				a := rows[commitAudit][10]
				a["event_order"] = json.Number("59")
				rows[commitAudit] = slices.Insert(rows[commitAudit][:10], 5, a)
			}},
		{"Release", "OwnerAtCommit", "OWNER_NOT_CURRENT_AT_COMMIT", Violation,
			func(rows map[string][]Row) {
				a := rows[commitAudit][10]
				a["event_order"], a["epoch"], a["kind"], a["owner_id"] = json.Number("59"), json.Number("1"), "release", "owner"
				rows[commitAudit] = slices.Insert(rows[commitAudit][:10], 5, a)
			}},
		{"RegressiveAcquisition", "OwnerAtCommit", "OWNER_ACQUISITION_ORDER", Violation,
			func(rows map[string][]Row) { rows[commitAudit][10]["epoch"] = json.Number("0") }},
		{"N6SupersededResult", "CurrentClaimWinner", "RESULT_SUPERSEDED_AT_COMMIT", Violation,
			func(rows map[string][]Row) {
				rows[history][4]["attempt_number"] = json.Number("1")
				rows[activityAttempts][0]["state"] = "SUCCEEDED"
			}},
		{"N6CanceledResult", "CurrentClaimWinner", "RESULT_AFTER_CANCELLATION", Violation,
			func(rows map[string][]Row) { rows[history][3]["new_state"] = "CANCELED" }},
		{"ClaimBinding", "CurrentClaimWinner", "CLAIM_ATTEMPT_BINDING", Violation,
			func(rows map[string][]Row) { rows[activityAttempts][1]["claim_token_hash"] = "other" }},
		{"ClaimPartition", "CurrentClaimWinner", "CLAIM_PARTITION_BINDING", Violation,
			func(rows map[string][]Row) { rows[commitAudit][6]["partition_id"] = json.Number("1") }},
		{"ClaimAfterReplacement", "CurrentClaimWinner", "CLAIM_NOT_CURRENT_AT_COMMIT", Violation,
			func(rows map[string][]Row) {
				a := rows[commitAudit][4]
				a["event_order"] = json.Number("65")
				rows[commitAudit][4], rows[commitAudit][5] = rows[commitAudit][5], a
			}},
		{"MissingClaim", "CurrentClaimWinner", "required_proof_missing:" + RelationPath(commitAudit), MissingProof,
			func(rows map[string][]Row) { rows[commitAudit][6]["claim_identity"] = nil }},
		{"MissingResultAttempt", "CurrentClaimWinner", "RESULT_ATTEMPT_IDENTITY", Violation,
			func(rows map[string][]Row) { rows[history][4]["attempt_number"] = nil }},
		{"ResultOutcome", "CurrentClaimWinner", "RESULT_WITHOUT_OUTCOME", Violation,
			func(rows map[string][]Row) { rows[activityAttempts][1]["state"] = "CLAIMED" }},
		{"ResultActor", "CurrentClaimWinner", "RESULT_CLAIM_ACTOR", Violation,
			func(rows map[string][]Row) { rows[history][4]["actor_id"] = "other" }},
		{"ResultValue", "CurrentClaimWinner", "ITERATION_WINNER_RESULT", Violation,
			func(rows map[string][]Row) { rows[nodeRows][0]["accepted_result"] = json.Number("4") }},
		{"MissingWinner", "CurrentClaimWinner", "required_proof_missing:CurrentClaimWinner", MissingProof,
			func(rows map[string][]Row) { rows[history][4]["reason"] = "UNRELATED" }},
		{"MultipleWinners", "CurrentClaimWinner", "ITERATION_MULTIPLE_WINNERS", Violation,
			func(rows map[string][]Row) {
				h := maps.Clone(rows[history][4])
				h["transition_id"] = "second-result"
				a := maps.Clone(rows[commitAudit][7])
				a["event_order"], a["transition_id"] = json.Number("85"), "second-result"
				rows[history] = slices.Insert(rows[history], 5, h)
				rows[commitAudit] = slices.Insert(rows[commitAudit], 8, a)
			}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := orderFixture()
			tc.change(rows)
			r := evaluate(Manifest{Cell: "hook-5", Namespace: "ns"}, Snapshot{NamespaceCount: 1}, rows,
				Plan{}, Arrivals{}, false, false, "", nil)
			found := slices.ContainsFunc(r.Findings, func(f Finding) bool {
				return f.Predicate == tc.predicate && f.Code == tc.code && f.Kind == tc.kind && f.Witness != nil && len(f.Witness.RowKey) > 0 &&
					(tc.name != "MissingClaim" || f.Witness.Path == RelationPath(commitAudit))
			})
			if !found || r.ExitCode() != Fail {
				t.Fatalf("ordering verdict %s %s: %+v", tc.predicate, tc.code, r.Findings)
			}
			if tc.kind == MissingProof && tc.name != "MissingWinner" && r.Safety == "FAIL" {
				t.Fatalf("missing proof changed safety: %+v", r.Findings)
			}
		})
	}
}

func TestK6PredicateFailureStrata(t *testing.T) {
	for _, outcome := range []string{"FAILED_RETRYABLE", "FAILED_FINAL"} {
		t.Run(outcome, func(t *testing.T) {
			rows := orderFixture()
			rows[activityAttempts][1]["state"], rows[nodeRows][0]["state"] = outcome, "FAILED"
			rows[history][5]["new_state"] = "FAILED"
			rows[history] = rows[history][:6]
			rows[commitAudit] = slices.Delete(rows[commitAudit], 9, 10)
			rows[executions][0]["state"], rows[executions][0]["revision"] = "FAILED", json.Number("6")
			rows["engine.attempt_result_evidence"] = []Row{{"workflow_id": "wf", "node_id": "node", "attempt_number": json.Number("1"),
				"iteration": json.Number("0"), "disposition": "RECORDED_AS_EVIDENCE"}}
			r := evaluate(Manifest{Cell: "hook-0", Namespace: "ns"}, Snapshot{NamespaceCount: 1}, rows, Plan{}, Arrivals{}, false, false, "", nil)
			if r.Safety == "FAIL" || r.Labels[3].PopulationCount != 1 || r.Labels[3].Reason != "required_proof_missing:result_commit_identity" {
				t.Fatalf("failure strata: %+v %+v", r.Labels[3], r.Findings)
			}
		})
	}
}

func TestK6ConfirmationAllCoveredPure(t *testing.T) {
	for _, status := range []string{"pending", "confirmed"} {
		t.Run(status, func(t *testing.T) {
			r := newResult(Manifest{Cell: "hook-0", Workload: "PURE"}, "")
			for i := range r.Labels {
				if r.Labels[i].Registered {
					r.Labels[i].Outcome, r.Labels[i].Reason = "covered/pass", "predicate_satisfied"
				}
			}
			if status == "confirmed" {
				r.Confirmation = Confirmation{Status: status}
			}
			r.finish()
			want := Incomplete
			if status == "confirmed" {
				want = Pass
			}
			if r.ExitCode() != want || !slices.Equal(r.InvariantsCovered, []string{"I1", "I2", "I4", "I6", "I11"}) ||
				(status == "pending" && r.Confirmation != (Confirmation{"pending", "confirmation_pending:G3a4"})) {
				t.Fatalf("confirmation verdict: %+v", r)
			}
		})
	}
}
