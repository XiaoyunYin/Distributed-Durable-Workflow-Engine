package k6v1

import (
	"reflect"
	"slices"
)

const (
	commitAudit     = "engine.k4_audit"
	nodeRows        = "engine.node_instances"
	resultRecorded  = "ATTEMPT_RESULT_RECORDED"
	attemptCreated  = "ATTEMPT_CREATED"
	attemptReplaced = "TIMEOUT_REPLACEMENT"
	resultConsumed  = "RESULT_CONSUMED"
	canceled        = "CANCELED_ALL_ACTIVE_NODES"
)

type iterationKey struct {
	workflow, node string
	iteration      int64
}

func fromDBIteration(row Row) iterationKey {
	return iterationKey{row["workflow_id"].(string), row["node_id"].(string), number(row["iteration"])}
}

type engineOrder struct {
	result        *Result
	rows          map[string][]Row
	workflows     map[string]Row
	witness       func(string, Row) Witness
	violate       rowViolation
	byTransition  map[any]Row
	transitions   map[any]Row
	attempts      map[attemptKey]Row
	nodes         map[iterationKey]Row
	missing       map[[2]string]bool
	capture       bool
	captureReady  bool
	resultCommits map[any]capturedResult
	captureWrites map[captureTransaction][]Row
}

func newEngineOrder(r *Result, rows map[string][]Row, workflows map[string]Row, witness func(string, Row) Witness, violate rowViolation) engineOrder {
	e := engineOrder{
		result: r, rows: rows, workflows: workflows, witness: witness, violate: violate,
		byTransition: map[any]Row{}, transitions: map[any]Row{}, attempts: map[attemptKey]Row{},
		nodes: map[iterationKey]Row{}, missing: map[[2]string]bool{},
		resultCommits: map[any]capturedResult{},
		captureWrites: map[captureTransaction][]Row{},
	}
	for _, row := range rows[history] {
		e.transitions[row["transition_id"]] = row
	}
	for _, row := range rows[commitAudit] {
		if row["kind"] == "history" && row["transition_id"] != nil {
			e.byTransition[row["transition_id"]] = row
		}
	}
	for _, row := range rows[activityAttempts] {
		if row["attempt_number"] != nil {
			e.attempts[fromDBAttempt(row)] = row
		}
	}
	for _, row := range rows[nodeRows] {
		if row["iteration"] != nil {
			e.nodes[fromDBIteration(row)] = row
		}
	}
	return e
}

// Name the missing collection and retain the first affected row in export order.
func (e engineOrder) require(predicate, source, relation string, row Row) {
	if e.missing[[2]string{predicate, source}] {
		return
	}
	var ref *Witness
	if row != nil {
		w := e.witness(relation, row)
		ref = &w
	}
	e.requireWitness(predicate, source, ref)
}

func (e engineOrder) requireWitness(predicate, source string, ref *Witness) {
	key := [2]string{predicate, source}
	if e.missing[key] {
		return
	}
	e.missing[key] = true
	path := source
	if ref != nil {
		path = ref.Path
	}
	e.result.record(MissingProof, predicate, "required_proof_missing:"+source, path, ref)
}

func committed(row Row) bool {
	return row != nil && row["finalized_at"] != nil && row["transaction_id"] != "" && row["transaction_id"] != nil
}

func validHistoryActor(row Row) bool {
	actor, reason := row["actor_kind"], row["reason"]
	if row["actor_id"] == nil || row["actor_id"] == "" || reason == nil || reason == "" {
		return false
	}
	switch reason {
	case "WORKFLOW_CREATED":
		return actor == "client" && number(row["revision"]) == 1 && row["scheduler_epoch"] == nil
	case resultRecorded:
		return actor == "worker" && row["scheduler_epoch"] == nil && row["old_state"] == row["new_state"]
	default:
		return actor == "scheduler" && number(row["revision"]) > 1 && row["scheduler_epoch"] != nil
	}
}

func (e engineOrder) checkAtomicHistory() {
	const predicate = "AtomicHistory"
	last := map[string]int64{}
	seen := map[any]bool{}
	finishedTransactions := map[any]bool{}
	var currentTransaction any
	_, haveHistory := e.rows[history]
	for _, a := range e.rows[commitAudit] {
		if committed(a) && a["transaction_id"] != currentTransaction {
			finishedTransactions[currentTransaction] = true
			currentTransaction = a["transaction_id"]
			if finishedTransactions[currentTransaction] {
				e.violate(predicate, "HISTORY_AUDIT_TRANSACTION_ORDER", commitAudit, a)
			}
		}
		if a["kind"] != "history" {
			continue
		}
		id := a["transition_id"]
		if haveHistory && (e.transitions[id] == nil || id == nil || seen[id]) {
			e.violate(predicate, "HISTORY_AUDIT_UNMATCHED", commitAudit, a)
		}
		seen[id] = true
	}
	for _, h := range e.rows[history] {
		if !validHistoryActor(h) {
			e.violate(predicate, "HISTORY_REASON_ACTOR", history, h)
		}
		id := h["workflow_id"].(string)
		wf, a := e.workflows[id], e.byTransition[h["transition_id"]]
		if !committed(a) {
			e.require(predicate, RelationPath(commitAudit), history, h)
		} else {
			order := number(a["event_order"])
			if order <= last[id] || !auditMatchesHistory(a, h, wf) {
				e.violate(predicate, "HISTORY_AUDIT_BINDING", commitAudit, a)
			}
			last[id] = order
		}
		if h["node_id"] != nil {
			iteration := int64(0)
			if h["iteration"] != nil {
				iteration = number(h["iteration"])
			}
			node := iterationKey{id, h["node_id"].(string), iteration}
			if _, have := e.rows[nodeRows]; have && e.nodes[node] == nil {
				e.violate(predicate, "HISTORY_NODE_MISSING", history, h)
			}
			if _, have := e.rows[activityAttempts]; have && h["attempt_number"] != nil &&
				e.attempts[attemptKey{id, node.node, iteration, number(h["attempt_number"])}] == nil {
				e.violate(predicate, "HISTORY_ATTEMPT_MISSING", history, h)
			}
		}
		if !e.capture && (h["node_id"] != nil || h["reason"] == canceled) || e.capture && !e.captureReady {
			e.require(predicate, "node_attempt_write_transactions", history, h)
		}
	}
}

func auditMatchesHistory(a, h, wf Row) bool {
	if wf != nil && a["partition_id"] != wf["partition_id"] {
		return false
	}
	if h["scheduler_epoch"] == nil {
		return number(a["epoch"]) == 0 && a["owner_id"] == ""
	}
	return a["epoch"] == h["scheduler_epoch"] && number(a["epoch"]) > 0 && a["owner_id"] != ""
}

func (e engineOrder) checkOwnerAtCommit() {
	const predicate = "OwnerAtCommit"
	type owner struct {
		id       any
		epoch    int64
		released bool
	}
	owners := map[any]owner{}
	type acquisition struct{ partition, owner, epoch any }
	unmatched := map[acquisition]int{}
	required := e.result.Labels[2].Registered
	for _, a := range e.rows[commitAudit] {
		kind := a["kind"]
		if kind != "acquire" && kind != "release" && (kind != "history" || number(a["epoch"]) == 0) {
			continue
		}
		if !committed(a) {
			if required {
				e.require(predicate, RelationPath(commitAudit), commitAudit, a)
			}
			continue
		}
		partition, epoch := a["partition_id"], number(a["epoch"])
		prior := owners[partition]
		matches := prior.epoch == epoch && prior.id == a["owner_id"] && !prior.released
		if kind == "acquire" {
			if epoch <= 0 || a["owner_id"] == "" || epoch < prior.epoch || epoch == prior.epoch && !matches {
				e.violate(predicate, "OWNER_ACQUISITION_ORDER", commitAudit, a)
				continue
			}
			owners[partition] = owner{id: a["owner_id"], epoch: epoch}
			unmatched[acquisition{partition, a["owner_id"], a["epoch"]}]++
			continue
		}
		if prior.epoch < epoch {
			if required {
				e.require(predicate, RelationPath(commitAudit), commitAudit, a)
			}
			continue
		}
		if !matches {
			e.violate(predicate, "OWNER_NOT_CURRENT_AT_COMMIT", commitAudit, a)
		} else if kind == "release" {
			prior.released = true
			owners[partition] = prior
		}
	}
	if required {
		for _, row := range e.rows["engine.lease_acquisitions"] {
			key := acquisition{row["partition_id"], row["owner_id"], row["epoch"]}
			if unmatched[key] == 0 {
				e.require(predicate, RelationPath(commitAudit), "engine.lease_acquisitions", row)
			} else {
				unmatched[key]--
			}
		}
		for _, h := range e.rows[history] {
			if h["scheduler_epoch"] != nil {
				if !committed(e.byTransition[h["transition_id"]]) {
					e.require(predicate, RelationPath(commitAudit), history, h)
				}
				if !e.captureReady {
					e.require(predicate, "owner_write_transactions", history, h)
				}
			}
		}
		label := &e.result.Labels[2]
		if e.captureReady {
			label.PopulationUnit, label.PopulationCount = "owner_write", len(e.rows[OwnerWrites])
			label.Outcome, label.Reason = "covered/pass", "predicate_satisfied"
		} else if e.capture && len(e.rows[OwnerWrites]) > 0 {
			e.require(predicate, "owner_write_transactions", OwnerWrites, e.rows[OwnerWrites][0])
		} else if e.capture && len(e.workflows) > 0 {
			e.require(predicate, RelationPath(OwnerWrites), "", nil)
		} else if e.capture {
			label.Reason = "no_positive_population"
		}
	}
}

type claimProgress struct {
	number        int64
	claimed       bool
	retired       bool
	successes     int
	finalFailures int
	resolvedRetry bool
	winner        Row
}

func (e engineOrder) checkCurrentClaimWinner() {
	const predicate = "CurrentClaimWinner"
	progress := map[iterationKey]claimProgress{}
	cancellations := map[any]bool{}
	claims := map[attemptKey]Row{}
	results := map[attemptKey]bool{}
	receipts := map[any]Row{}
	for _, row := range e.rows[SinkReceipts] {
		receipts[row["application_id"]] = row
	}
	for _, a := range e.rows[commitAudit] {
		if !committed(a) {
			continue
		}
		if a["kind"] == "claim" {
			e.advanceClaim(a, progress, claims, cancellations)
			continue
		}
		h := e.transitions[a["transition_id"]]
		if a["kind"] != "history" || h == nil {
			continue
		}
		if h["new_state"] == "CANCELED" {
			cancellations[h["workflow_id"]] = true
		}
		if h["node_id"] == nil || h["iteration"] == nil || h["attempt_number"] == nil {
			if h["reason"] == resultRecorded {
				e.violate(predicate, "RESULT_ATTEMPT_IDENTITY", history, h)
			}
			continue
		}
		key := fromDBIteration(h)
		attempt := number(h["attempt_number"])
		state := progress[key]
		switch h["reason"] {
		case attemptCreated, attemptReplaced:
			if h["reason"] == attemptReplaced {
				attempt++
			}
			state.number, state.claimed, state.retired = attempt, false, false
		case "TIMEOUT_RECONCILIATION_REQUIRED":
			state.number, state.retired = attempt, true
		case resultRecorded:
			dbKey := fromDBAttempt(h)
			switch {
			case cancellations[key.workflow]:
				e.violate(predicate, "RESULT_AFTER_CANCELLATION", history, h)
			case state.number > attempt || state.number == attempt && state.retired:
				e.violate(predicate, "RESULT_SUPERSEDED_AT_COMMIT", history, h)
			case !state.claimed || state.number != attempt:
				e.require(predicate, RelationPath(commitAudit), history, h)
			case claims[dbKey]["worker_id"] != h["actor_id"]:
				e.violate(predicate, "RESULT_CLAIM_ACTOR", history, h)
			}
			// The history names the durable attempt but does not retain the
			// token submitted by the result request in that transaction.
			commit := e.resultCommits[h["transition_id"]]
			if db := e.attempts[dbKey]; db != nil {
				results[dbKey] = true
				outcome := db["state"]
				if commit.request != nil {
					outcome = captureImage(commit.request, "after_image")["attempt_state"]
				}
				switch outcome {
				case "SUCCEEDED":
					state.successes++
					state.winner = db
				case "FAILED_FINAL":
					state.finalFailures++
				case "FAILED_RETRYABLE":
					// A retryable failure never supplies the iteration winner.
				default:
					e.violate(predicate, "RESULT_WITHOUT_OUTCOME", history, h)
				}
			}
			if !e.captureReady || commit.request == nil {
				e.require(predicate, "result_commit_identity", history, h)
			} else {
				e.checkWinnerReceipt(h, e.attempts[dbKey], commit, receipts)
			}
		case resultConsumed:
			dbKey := fromDBAttempt(h)
			if db := e.attempts[dbKey]; db != nil && results[dbKey] &&
				db["state"] == "FAILED_RETRYABLE" && h["new_state"] == "FAILED" {
				state.resolvedRetry = true
			}
		}
		progress[key] = state
	}
	for _, row := range e.rows[activityAttempts] {
		if row["claim_token_hash"] != nil && claims[fromDBAttempt(row)] == nil {
			e.require(predicate, RelationPath(commitAudit), activityAttempts, row)
		}
	}
	for _, h := range e.rows[history] {
		if slices.Contains([]any{attemptCreated, attemptReplaced, resultRecorded, canceled, "TIMEOUT_RECONCILIATION_REQUIRED"}, h["reason"]) &&
			!committed(e.byTransition[h["transition_id"]]) {
			e.require(predicate, RelationPath(commitAudit), history, h)
		}
	}
	label := &e.result.Labels[3]
	label.PopulationUnit = "executed_iteration"
	for _, row := range e.rows[nodeRows] {
		if row["iteration"] == nil {
			continue
		}
		key := fromDBIteration(row)
		state := progress[key]
		if !state.claimed && state.successes+state.finalFailures == 0 {
			continue
		}
		switch row["state"] {
		case "SUCCEEDED", "FAILED":
			label.PopulationCount++
			winners := state.successes
			if row["state"] == "FAILED" {
				winners = state.finalFailures
				if state.resolvedRetry {
					winners++
				}
			}
			switch {
			case winners > 1 || state.successes+state.finalFailures > 1:
				e.violate(predicate, "ITERATION_MULTIPLE_WINNERS", nodeRows, row)
			case winners == 0:
				e.require(predicate, predicate, nodeRows, row)
			case row["state"] == "SUCCEEDED" && !reflect.DeepEqual(row["accepted_result"], state.winner["result"]):
				e.violate(predicate, "ITERATION_WINNER_RESULT", nodeRows, row)
			}
		}
	}
	if label.PopulationCount == 0 {
		label.Reason = "no_positive_population"
	} else if e.captureReady {
		label.Outcome, label.Reason = "covered/pass", "predicate_satisfied"
	}
}

func (e engineOrder) checkWinnerReceipt(h, attempt Row, commit capturedResult, receipts map[any]Row) {
	const predicate = "CurrentClaimWinner"
	if attempt["effect_class"] != "COOPERATING_EFFECT" || attempt["state"] != "SUCCEEDED" {
		return
	}
	id := commit.attempt["receipt_id"]
	proof, _ := attempt["k6_proof"].(map[string]any)
	signed, _ := proof["receipt"].(map[string]any)
	if id == nil || signed["application_id"] != id || receipts[id] == nil ||
		slices.ContainsFunc(e.result.Findings, func(f Finding) bool {
			return f.Predicate == "ReceiptSignature" && f.Witness != nil && f.Witness.SHA256 == e.witness(activityAttempts, attempt).SHA256
		}) {
		e.require(predicate, "result_receipt_binding", history, h)
	} else if receipts[id]["workflow_id"] != h["workflow_id"] || receipts[id]["logical_key"] != attempt["logical_effect_key"] {
		e.violate(predicate, "RESULT_RECEIPT_BINDING", history, h)
	}
}

func (e engineOrder) advanceClaim(a Row, progress map[iterationKey]claimProgress, claims map[attemptKey]Row, cancellations map[any]bool) {
	const predicate = "CurrentClaimWinner"
	claim, ok := a["claim_identity"].(map[string]any)
	if !ok {
		e.require(predicate, RelationPath(commitAudit), commitAudit, a)
		return
	}
	key, dbKey := fromDBIteration(claim), fromDBAttempt(claim)
	state := progress[key]
	if cancellations[key.workflow] || state.number > dbKey.attempt || claims[dbKey] != nil {
		e.violate(predicate, "CLAIM_NOT_CURRENT_AT_COMMIT", commitAudit, a)
		return
	}
	db := e.attempts[dbKey]
	wf := e.workflows[key.workflow]
	if wf != nil && wf["partition_id"] != a["partition_id"] {
		e.violate(predicate, "CLAIM_PARTITION_BINDING", commitAudit, a)
	}
	if _, have := e.rows[activityAttempts]; have && (db == nil || db["claim_token_hash"] != claim["token_hash"] ||
		db["worker_id"] != claim["worker_id"] || db["worker_request_id"] != claim["worker_request_id"]) {
		e.violate(predicate, "CLAIM_ATTEMPT_BINDING", commitAudit, a)
	}
	state.number, state.claimed, state.retired = dbKey.attempt, true, false
	progress[key], claims[dbKey] = state, claim
}
