package k6v1

import (
	"slices"
	"strings"
)

const (
	replayPredicate      = "LostResultReplay"
	uncertaintyPredicate = "UnknownEffectVisible"
	shutdownPredicate    = "ShutdownDisposition"
)

func (e engineOrder) checkReplayAndUncertainty() {
	if e.captureReady {
		e.checkCapturedReplayAndUncertainty()
		return
	}
	type uncertainty struct {
		attempt int64
		open    bool
	}
	unresolved := map[iterationKey]uncertainty{}
	expired := map[attemptKey]Row{}
	for _, row := range e.rows[reconciliation] {
		if row["kind"] == "EXPIRED_ATTEMPT" && row["iteration"] != nil && row["attempt_number"] != nil {
			expired[fromDBAttempt(row)] = row
		}
	}
	writes := map[attemptKey]any{}
	for _, audit := range e.rows[commitAudit] {
		h := e.transitions[audit["transition_id"]]
		if !committed(audit) || audit["kind"] != "history" || h == nil ||
			h["node_id"] == nil || h["iteration"] == nil || h["attempt_number"] == nil {
			continue
		}
		key := fromDBAttempt(h)
		attempt := e.attempts[key]
		switch h["reason"] {
		case "TIMEOUT_RECONCILIATION_REQUIRED":
			if attempt["effect_class"] == "NON_COOPERATING_EFFECT" {
				e.require(uncertaintyPredicate, "effect_uncertainty_resolution_transactions", history, h)
				item := expired[key]
				unresolved[fromDBIteration(h)] = uncertainty{key.attempt, item["status"] == "OPEN"}
			}
		case resultRecorded:
			if prior := writes[key]; prior != nil && prior != h["transition_id"] {
				// This transition records a state-changing result write.
				// Rejected or evidence-only requests have no such history.
				e.require(replayPredicate, "result_commit_identity", history, h)
				e.violate(replayPredicate, "RESULT_REAPPLIED", history, h)
			}
			writes[key] = h["transition_id"]
			if prior := unresolved[fromDBIteration(h)]; prior.open && key.attempt > prior.attempt &&
				attempt["effect_class"] == "NON_COOPERATING_EFFECT" {
				e.violate(uncertaintyPredicate, "UNRESOLVED_EFFECT_REAPPLIED", history, h)
			}
		}
	}
}

func (e engineOrder) checkCapturedReplayAndUncertainty() {
	type effectKey struct{ workflow, logicalKey any }
	type requestKey struct {
		attempt        attemptKey
		request, token any
	}
	pending := map[attemptKey]effectKey{}
	writes := map[requestKey]bool{}
	receipts := map[[2]any]bool{}
	for _, row := range e.rows[WriteCapture] {
		tx := captureTransaction{row["transaction_id"], row["workflow_id"]}
		switch row["subject"] {
		case history:
			h := e.transitions[row["row_key"].([]any)[0]]
			if h == nil || h["node_id"] == nil || h["iteration"] == nil || h["attempt_number"] == nil {
				continue
			}
			key := fromDBAttempt(h)
			attempt := e.attempts[key]
			switch h["reason"] {
			case "TIMEOUT_RECONCILIATION_REQUIRED":
				if attempt["effect_class"] == "NON_COOPERATING_EFFECT" {
					if !uncertaintyTransaction(h, e.captureWrites[tx]) {
						e.require(uncertaintyPredicate, "effect_uncertainty_resolution_transactions", history, h)
					}
					pending[key] = effectKey{h["workflow_id"], attempt["logical_effect_key"]}
				}
			case resultRecorded:
				commit := e.resultCommits[h["transition_id"]]
				if commit.request == nil {
					e.require(replayPredicate, "result_commit_identity", history, h)
				} else {
					request := captureImage(commit.request, "after_image")
					identity := requestKey{key, request["worker_request_id"], request["token_hash"]}
					id := [2]any{h["workflow_id"], commit.attempt["receipt_id"]}
					if writes[identity] || id[1] != nil && receipts[id] {
						e.violate(replayPredicate, "RESULT_REAPPLIED", history, h)
					}
					writes[identity], receipts[id] = true, true
				}
				for prior := range pending {
					if prior.workflow == key.workflow && prior.node == key.node && prior.iteration == key.iteration &&
						prior.attempt < key.attempt && attempt["effect_class"] == "NON_COOPERATING_EFFECT" {
						e.violate(uncertaintyPredicate, "UNRESOLVED_EFFECT_REAPPLIED", history, h)
						break
					}
				}
			}
		case "effects.effect_resolution_audit", "engine.effect_resolution_audit":
			after := captureImage(row, "after_image")
			if row["operation"] != "insert" || after["disposition"] != "CONFIRMED_APPLIED" {
				continue
			}
			schema, _, _ := strings.Cut(row["subject"].(string), ".")
			effect := effectKey{row["workflow_id"], after["logical_effect_key"]}
			for prior, unresolved := range pending {
				if unresolved != effect {
					continue
				}
				if slices.ContainsFunc(e.captureWrites[tx], func(write Row) bool {
					key := write["row_key"].([]any)
					before, resolved := captureImage(write, "before_image"), captureImage(write, "after_image")
					return write["subject"] == schema+".effect_records" && key[1] == effect.logicalKey &&
						before["outcome"] == "OUTCOME_UNKNOWN" && resolved["outcome"] == "APPLIED" &&
						before["attempt_number"] == resolved["attempt_number"] && number(resolved["attempt_number"]) == prior.attempt
				}) {
					delete(pending, prior)
				} else {
					e.require(uncertaintyPredicate, "effect_uncertainty_resolution_transactions", WriteCapture, row)
				}
			}
		}
	}
}

func uncertaintyTransaction(h Row, writes []Row) bool {
	return slices.ContainsFunc(writes, func(w Row) bool {
		after := captureImage(w, "after_image")
		return w["subject"] == activityAttempts && captureMatchesAttempt(w, h) &&
			after["effect_class"] == "NON_COOPERATING_EFFECT" && after["outcome_disposition"] == "OUTCOME_UNKNOWN"
	}) && slices.ContainsFunc(writes, func(w Row) bool {
		after := captureImage(w, "after_image")
		return w["subject"] == reconciliation && after["kind"] == "EXPIRED_ATTEMPT" && after["status"] == "OPEN" &&
			after["node_id"] == h["node_id"] && after["iteration"] == h["iteration"] && after["attempt_number"] == h["attempt_number"]
	})
}

func (e engineOrder) checkShutdown(aborts []observedWorker) {
	if len(aborts) == 0 {
		return
	}
	results := map[attemptKey]Row{}
	for _, h := range e.rows[history] {
		if h["reason"] == resultRecorded && h["iteration"] != nil && h["attempt_number"] != nil &&
			committed(e.byTransition[h["transition_id"]]) {
			results[fromDBAttempt(h)] = h
		}
	}
	for _, observed := range aborts {
		session, row := observed.session, observed.row
		a := row.Data.Attempt
		key := a.key()
		db := e.attempts[key]
		if db["worker_id"] != session.Claimant || db["worker_request_id"] != a.RequestID ||
			db["claim_token_hash"] != a.ClaimHash {
			continue
		}
		raw, _ := Canonical(row)
		ref := Witness{SessionPath(session.ID), []any{row.Position.Sequence}, Hash(raw)}
		// The pure worker writes an abort only when it produced no
		// activity outcome. The effect runner can abort after sending a
		// request, so its abort alone cannot disprove a recorded failure.
		h := results[key]
		state := db["state"]
		if e.captureReady {
			if h != nil {
				commit := e.resultCommits[h["transition_id"]]
				if commit.request == nil || commit.attempt["worker_id"] != session.Claimant ||
					commit.attempt["worker_request_id"] != a.RequestID || commit.attempt["claim_token_hash"] != a.ClaimHash {
					e.requireWitness(shutdownPredicate, "shutdown_result_binding", &ref)
					continue
				}
				state = captureImage(commit.request, "after_image")["attempt_state"]
			}
			e.requireWitness(shutdownPredicate, "shutdown_signal_or_exit", &ref)
		} else {
			e.requireWitness(shutdownPredicate, "shutdown_result_binding", &ref)
		}
		if db["effect_class"] == "PURE_ACTIVITY" && h != nil && h["actor_id"] == session.Claimant &&
			(state == "FAILED_RETRYABLE" || state == "FAILED_FINAL") {
			e.violate(shutdownPredicate, "SHUTDOWN_RECORDED_AS_FAILURE", history, h)
		}
	}
}
