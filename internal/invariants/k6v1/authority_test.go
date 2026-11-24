package k6v1

import (
	"encoding/json"
	"testing"
	"time"
)

func authorityAt(seconds int) string { return UTC(fixed.Add(time.Duration(seconds) * time.Second)) }

func TestK6EffectAuthority(t *testing.T) {
	for _, tc := range []struct {
		name, code string
		change     func(map[string][]Row, *External, Plan, engineOrder)
	}{
		{name: "Clean"},
		{"WithheldPastExpiry", "", func(_ map[string][]Row, s *External, _ Plan, _ engineOrder) {
			s.Ingress.Rows[1].Data.Outcome, s.Ingress.Rows[1].Data.Nondelivery = "withheld", true
			s.Ingress.Rows[1].Position.UTC = authorityAt(20)
		}},
		{"ExpiredWithheld", "I10_EXPIRED_WRITE", func(r map[string][]Row, s *External, _ Plan, _ engineOrder) {
			s.Ingress.Rows[1].Data.Outcome, s.Ingress.Rows[1].Data.Nondelivery = "withheld", true
			s.Ingress.Rows[1].Position.UTC = authorityAt(20)
			r[SinkCalls][1]["committed_at"] = authorityAt(10)
		}},
		{"Unapproved", "I10_UNAPPROVED_WRITE", func(r map[string][]Row, _ *External, _ Plan, _ engineOrder) {
			r[intentsRelation][0]["decision"] = "REJECTED"
		}},
		{"WrongActor", "I10_WRONG_ACTOR", func(r map[string][]Row, _ *External, _ Plan, _ engineOrder) {
			r[intentsRelation][0]["approver_id"] = "other-actor"
		}},
		{"GrantDecisionBinding", "I10_GRANT_DECISION_BINDING", func(r map[string][]Row, _ *External, _ Plan, _ engineOrder) {
			captureImage(r[WriteCapture][0], "before_image")["approver_id"] = "other-actor"
		}},
		{"WrongToken", "I10_WRONG_TOKEN", callFault("grant_hash", fixtureHash("other-token"))},
		{"WrongTarget", "I10_WRONG_TARGET", callFault("resource_id", "other-resource")},
		{"WrongArgument", "I10_WRONG_ARGUMENT", callFault("arguments", Row{"position": json.Number("4")})},
		{"WrongKey", "I10_WRONG_KEY", callFault("logical_key", "other-key")},
		{"WrongScope", "I10_WRONG_SCOPE", callFault("scope_hash", fixtureHash("other-scope"))},
		{"WrongRevision", "I10_WRONG_REVISION", callFault("expected_revision", "1")},
		{"WrongOperation", "I10_WRONG_OPERATION", callFault("node_id", "other-node")},
		{"StaleFence", "I10_STALE_FENCE", callFault("fence", json.Number("2"))},
		{"ReplacedGrant", "I10_GRANT_REPLACED", func(r map[string][]Row, _ *External, _ Plan, _ engineOrder) {
			captureImage(r[WriteCapture][2], "after_image")["grant_expires_at"] = authorityAt(50)
		}},
		{"LookupGrantChanged", "I10_GRANT_CONTINUITY", func(r map[string][]Row, _ *External, _ Plan, _ engineOrder) {
			r[SinkCalls][2]["grant_hash"] = fixtureHash("other-grant")
		}},
		{"CancellationOrder", "I10_CANCELLATION_ORDER", func(r map[string][]Row, _ *External, _ Plan, _ engineOrder) {
			r[cancellations] = []Row{{"workflow_id": "workflow", "status": "APPLIED", "created_at": authorityAt(3), "applied_at": authorityAt(4)}}
		}},
		{"GrantOrder", "I10_GRANT_ORDER", func(_ map[string][]Row, s *External, _ Plan, _ engineOrder) {
			s.Decisions.Rows[0].Data.DecidedUTC = authorityAt(3)
		}},
		{"ClaimOrder", "I10_WRITE_CLAIM", func(r map[string][]Row, _ *External, _ Plan, _ engineOrder) {
			r[commitAudit][2]["finalized_at"] = authorityAt(7)
		}},
		{"ClaimRetired", "I10_WRITE_CLAIM", func(r map[string][]Row, _ *External, _ Plan, _ engineOrder) {
			captureImage(r[WriteCapture][1], "after_image")["is_current"] = false
		}},
		{"ReadAfterWrite", "I10_WRITE_BRACKET", callFault("grant_read_at", authorityAt(7))},
		{"CommitBeforeOpen", "I10_WRITE_BRACKET", func(_ map[string][]Row, s *External, _ Plan, _ engineOrder) {
			s.Ingress.Rows[1].Data.OpenedUTC = authorityAt(7)
		}},
		{"CommitTimeMissing", "required_proof_missing:sink_commit_transaction", callFault("committed_at", nil)},
		{"ReadMissing", "required_proof_missing:exact_grant_read", callFault("grant_read_at", nil)},
		{"OpenMissing", "required_proof_missing:relay_ingress_open", func(_ map[string][]Row, s *External, _ Plan, _ engineOrder) {
			s.Ingress.Rows[1].Data.OpenSequence = 0
		}},
		{"WrongTransaction", "I10_WRITE_TRANSACTION", callFault("transaction_id", "2")},
		{"Unacknowledged", "I10_DISPATCH_UNACKNOWLEDGED", func(r map[string][]Row, _ *External, _ Plan, _ engineOrder) {
			r[intentsRelation][0]["dispatch_status"] = "GRANTED"
		}},
		{"AckWrongReceipt", "I10_DISPATCH_RECEIPT", func(r map[string][]Row, _ *External, _ Plan, _ engineOrder) {
			r[activityAttempts][0]["k6_proof"] = map[string]any{"receipt": Row{"application_id": "other"}}
		}},
		{"AckWrongOrder", "I10_DISPATCH_ACK_ORDER", func(_ map[string][]Row, _ *External, _ Plan, e engineOrder) {
			e.byTransition["result"]["finalized_at"] = authorityAt(4)
		}},
		{"AckMissingProof", "required_proof_missing:dispatch_ack_transaction", func(r map[string][]Row, _ *External, _ Plan, _ engineOrder) {
			r[WriteCapture] = r[WriteCapture][:2]
		}},
		{"IssueProofMissing", "required_proof_missing:owner_grant_transaction", func(r map[string][]Row, _ *External, _ Plan, _ engineOrder) {
			r[OwnerWrites] = nil
		}},
		{"WrongIssueRevision", "I10_GRANT_ORDER", func(r map[string][]Row, _ *External, _ Plan, _ engineOrder) {
			r[commitAudit][1]["event_order"] = json.Number("1")
		}},
		{"CreationProofMissing", "required_proof_missing:attempt_creation_transaction", func(_ map[string][]Row, _ *External, _ Plan, e engineOrder) {
			delete(e.byTransition, "created")
		}},
		{"ClaimProofMissing", "required_proof_missing:committed_write_claim", func(r map[string][]Row, _ *External, _ Plan, _ engineOrder) {
			delete(r[commitAudit][2], "finalized_at")
		}},
		{"CurrentClaimProofMissing", "required_proof_missing:current_claim_at_write", func(r map[string][]Row, _ *External, _ Plan, _ engineOrder) {
			captureImage(r[WriteCapture][1], "after_image")["updated_at"] = authorityAt(7)
		}},
		{"ExpiryProofMissing", "required_proof_missing:absolute_grant_expiry", func(r map[string][]Row, _ *External, _ Plan, _ engineOrder) {
			delete(r, authEpisode)
		}},
		{"CommitProofMissing", "required_proof_missing:sink_commit", func(r map[string][]Row, _ *External, _ Plan, _ engineOrder) {
			r[SinkCalls][1]["outcome"] = "DUPLICATE"
		}},
		{"PlanProofMissing", "required_proof_missing:sealed_effect_operation", func(_ map[string][]Row, _ *External, plan Plan, _ engineOrder) {
			plan.Windows[0].Submissions[0].Effect = nil
		}},
		{"DecisionProofMissing", "required_proof_missing:committed_decision_identity", func(r map[string][]Row, _ *External, _ Plan, _ engineOrder) {
			delete(captureImage(r[WriteCapture][0], "before_image"), "decided_at")
		}},
		{"RecoveryGrantProofMissing", "required_proof_missing:recovery_grant_identity", func(r map[string][]Row, _ *External, _ Plan, _ engineOrder) {
			delete(r[SinkCalls][2], "grant_hash")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, s, plan, order := fixtureAuthority(t)
			if tc.change != nil {
				tc.change(order.rows, &s, plan, order)
			}
			traffic, _ := checkSessions(order.result, m, s, order.rows)
			order.checkEffectAuthority(m, plan, s, traffic)
			order.result.finish()
			if tc.code == "" {
				if label := order.result.Labels[9]; len(order.result.Findings) != 0 || label.Outcome != "covered/pass" || label.PopulationCount != 1 {
					t.Fatalf("expected I10 award, got %+v and %+v", label, order.result.Findings)
				}
				return
			}
			for _, finding := range order.result.Findings {
				if finding.Code == tc.code {
					return
				}
			}
			t.Fatalf("expected %s, got %+v", tc.code, order.result.Findings)
		})
	}
}

func callFault(field string, value any) func(map[string][]Row, *External, Plan, engineOrder) {
	return func(rows map[string][]Row, _ *External, _ Plan, _ engineOrder) {
		rows[SinkCalls][1][field] = value
	}
}

func fixtureAuthority(t *testing.T) (Manifest, External, Plan, engineOrder) {
	t.Helper()
	m, sources, rows := fixtureExternal(t, true)
	zero, one := json.Number("0"), json.Number("1")
	args, _ := parse([]byte(`{"position":3}`))
	argumentHash := fixtureHash(`{"position":3}`)
	proposal := fixtureHash("resource\x00" + argumentHash + "\x000")
	scope := fixtureHash(proposal + "\x000\x00logical\x00resource")
	decision := &sources.Decisions.Rows[0].Data
	decision.ProposalHash = proposal
	intent := rows[intentsRelation][0]
	intent["node_id"], intent["iteration"] = "node", zero
	intent["canonical_arguments"], intent["proposal_hash"], intent["grant_scope_hash"] = args, proposal, scope
	intent["grant_expires_at"], intent["valid_until"], intent["expected_resource_revision"] = authorityAt(10), authorityAt(10), "0"
	intent["dispatch_status"], intent["created_at"] = "DISPATCHED", authorityAt(-1)
	rows[authEpisode] = []Row{{"valid_until": authorityAt(10)}}
	call := rows[SinkCalls][1]
	call["node_id"], call["iteration"], call["scope_hash"], call["fence"] = "node", zero, scope, one
	call["grant_hash"], call["arguments"], call["expected_revision"] = decision.GrantHash, args, "0"
	call["committed_at"], call["grant_read_at"], call["transaction_id"] = authorityAt(6), authorityAt(5), "1"
	rows[SinkCalls][2]["grant_hash"] = decision.GrantHash
	sources.Ingress.Rows[1].Data.OpenedUTC, sources.Ingress.Rows[1].Data.OpenSequence = authorityAt(4), 2
	receipt := rows[SinkReceipts][0]
	receipt["applied_revision"], receipt["expected_revision"], receipt["scope_hash"], receipt["canonical_hash"] = one, zero, scope, argumentHash
	rows[SinkJournal] = []Row{{"resource_id": "resource", "revision": one, "old_revision": zero, "logical_key": "logical", "intent_id": fixtureIntent, "transaction_id": "1"}}
	attempt := rows[activityAttempts][0]
	attempt["state"], attempt["logical_effect_key"], attempt["k6_proof"] = "SUCCEEDED", "logical", map[string]any{"receipt": receipt}
	claim := rows[commitAudit][0]
	claim["finalized_at"], claim["transaction_id"], claim["event_order"] = authorityAt(3), "claim", json.Number("3")
	rows[commitAudit] = []Row{
		{"kind": "history", "transition_id": "grant", "transaction_id": "grant", "event_order": one, "finalized_at": authorityAt(1)},
		{"kind": "history", "transition_id": "created", "transaction_id": "created", "event_order": json.Number("2"), "finalized_at": authorityAt(2)},
		claim,
		{"kind": "history", "transition_id": "result", "transaction_id": "result", "event_order": json.Number("8"), "finalized_at": authorityAt(8)},
	}
	rows[history] = []Row{
		{"transition_id": "grant", "workflow_id": "workflow", "reason": "APPROVAL_GRANTED", "node_id": "node", "iteration": zero, "revision": json.Number("2"), "actor_kind": "scheduler", "actor_id": "owner", "scheduler_epoch": one},
		{"transition_id": "created", "workflow_id": "workflow", "reason": attemptCreated, "node_id": "node", "iteration": zero, "attempt_number": one},
		{"transition_id": "result", "workflow_id": "workflow", "reason": resultRecorded},
	}
	grant := map[string]any{"dispatch_status": "GRANTED", "decision": "APPROVED", "grant_token_hash": decision.GrantHash,
		"grant_expires_at": authorityAt(10), "grant_scope_hash": scope, "approver_id": decision.Subject, "decided_at": decision.DecidedUTC, "proposal_hash": proposal}
	pendingGrant := map[string]any{"dispatch_status": "NOT_GRANTED", "decision": "APPROVED", "approver_id": decision.Subject,
		"decided_at": decision.DecidedUTC, "proposal_hash": proposal}
	ack := map[string]any{"dispatch_status": "DISPATCHED", "grant_token_hash": decision.GrantHash, "grant_expires_at": authorityAt(10)}
	attemptID := []any{"workflow", "node", zero, one}
	rows[WriteCapture] = []Row{
		{"subject": intentsRelation, "row_key": []any{fixtureIntent}, "event_order": int64(1), "transaction_id": "grant", "before_image": pendingGrant, "after_image": grant},
		{"subject": activityAttempts, "row_key": attemptID, "after_image": map[string]any{"state": "CLAIMED", "is_current": true, "k6_fence": one,
			"updated_at": authorityAt(3), "heartbeat_deadline": authorityAt(18), "claim_token_hash": attempt["claim_token_hash"],
			"worker_id": attempt["worker_id"], "worker_request_id": attempt["worker_request_id"]}},
		{"subject": intentsRelation, "row_key": []any{fixtureIntent}, "transaction_id": "result", "before_image": grant, "after_image": ack},
	}
	rows[OwnerWrites] = []Row{{"event_order": int64(1), "epoch": one}}
	r := newResult(m, "")
	r.Labels[7].Outcome = "covered/pass"
	order := newEngineOrder(&r, rows, map[string]Row{"workflow": {"submission_key": "submission"}},
		func(relation string, _ Row) Witness { return Witness{Path: RelationPath(relation)} },
		func(predicate, code, relation string, _ Row) {
			r.record(Violation, predicate, code, RelationPath(relation), nil)
		})
	order.captureReady = true
	order.resultCommits["result"] = capturedResult{request: Row{"row_key": attemptID, "after_image": map[string]any{"receipt_id": fixtureApplication}}}
	plan := Plan{Windows: []PlanWindow{{Submissions: []Submission{{Key: "submission", Target: "resource",
		Effect: &PlannedEffect{Node: "node", LogicalKey: "logical", ExpectedRevision: "0", Arguments: json.RawMessage(`{"position":3}`)}}}}}}
	return m, sources, plan, order
}
