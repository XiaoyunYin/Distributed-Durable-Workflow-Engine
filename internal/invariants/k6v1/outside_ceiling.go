package k6v1

const (
	replayPredicate      = "LostResultReplay"
	uncertaintyPredicate = "UnknownEffectVisible"
	shutdownPredicate    = "ShutdownDisposition"
)

func (e engineOrder) checkReplayAndUncertainty() {
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
		e.requireWitness(shutdownPredicate, "shutdown_result_binding", &ref)
		// The pure worker writes an abort only when it produced no
		// activity outcome. The effect runner can abort after sending a
		// request, so its abort alone cannot disprove a recorded failure.
		h := results[key]
		if db["effect_class"] == "PURE_ACTIVITY" && h != nil && h["actor_id"] == session.Claimant &&
			(db["state"] == "FAILED_RETRYABLE" || db["state"] == "FAILED_FINAL") {
			e.violate(shutdownPredicate, "SHUTDOWN_RECORDED_AS_FAILURE", history, h)
		}
	}
}
