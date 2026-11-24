package k6v1

import (
	"encoding/json"
	"slices"
	"strconv"
	"time"
)

const (
	writeAuthority = "AuthorizedAtWrite"
	dispatchAck    = "DispatchAcknowledged"
	cancellations  = "engine.cancellation_requests"
	authEpisode    = "k6_auth.episode"
)

func validResourceRevision(s string) bool {
	n, err := strconv.ParseInt(s, 10, 64)
	return err == nil && n >= 0 && strconv.FormatInt(n, 10) == s
}

// M4 hashes number-preserving Go JSON, with default JSON escaping and no LF.
func authorityArgumentHash(value any) string {
	raw, _ := json.Marshal(value)
	return Hash(raw)
}

type authorityPlan struct {
	PlannedEffect
	target, args, proposal, scope string
}

type effectAuthority struct {
	engineOrder
	manifest   Manifest
	plans      map[any]authorityPlan
	intents    map[any]Row
	decisions  map[string]DecisionRow
	grants     map[any]Row
	acks       map[any]Row
	owners     map[any]Row
	claims     map[attemptKey][]Row
	attemptLog map[attemptKey][]Row
	history    map[any][]Row
	canceled   map[any][]Row
	ingress    map[string]IngressRow
	traffic    map[string]observedSend
	allocs     map[grantAttempt]Row
	callFence  map[any]int64
}

func (e engineOrder) checkEffectAuthority(m Manifest, plan Plan, sources External, traffic map[string]observedSend) {
	a := effectAuthority{engineOrder: e, manifest: m, traffic: traffic,
		plans: map[any]authorityPlan{}, intents: map[any]Row{}, decisions: map[string]DecisionRow{},
		grants: map[any]Row{}, acks: map[any]Row{}, owners: map[any]Row{}, claims: map[attemptKey][]Row{},
		attemptLog: map[attemptKey][]Row{}, history: map[any][]Row{}, canceled: map[any][]Row{},
		ingress: map[string]IngressRow{}, allocs: map[grantAttempt]Row{}, callFence: map[any]int64{}}
	for _, window := range plan.Windows {
		for _, entry := range window.Submissions {
			if op := entry.Effect; op != nil {
				args, _ := parse(op.Arguments)
				argHash := authorityArgumentHash(args)
				proposal := Hash([]byte(entry.Target + "\x00" + argHash + "\x00" + op.ExpectedRevision))
				scope := Hash([]byte(proposal + "\x00" + op.ExpectedRevision + "\x00" + op.LogicalKey + "\x00" + entry.Target))
				a.plans[entry.Key] = authorityPlan{*op, entry.Target, argHash, proposal, scope}
			}
		}
	}
	for _, row := range e.rows[intentsRelation] {
		a.intents[row["intent_id"]] = row
	}
	for _, row := range sources.Decisions.Rows {
		if row.Data.Outcome != "PENDING" {
			a.decisions[row.Data.Intent] = row.Data
		}
	}
	for _, row := range sources.Ingress.Rows {
		a.ingress[row.Data.ConnectionHash] = row.Data
	}
	for _, row := range e.rows[commitAudit] {
		if claim, ok := row["claim_identity"].(map[string]any); ok && row["kind"] == "claim" && committed(row) {
			key := fromDBAttempt(claim)
			a.claims[key] = append(a.claims[key], row)
		}
	}
	for _, row := range e.rows[history] {
		a.history[row["workflow_id"]] = append(a.history[row["workflow_id"]], row)
	}
	for _, row := range e.rows[cancellations] {
		a.canceled[row["workflow_id"]] = append(a.canceled[row["workflow_id"]], row)
	}
	for _, row := range e.rows[OwnerWrites] {
		a.owners[row["event_order"]] = row
	}
	for _, row := range e.rows[WriteCapture] {
		a.indexAuthorityChange(row)
	}
	for _, row := range e.rows[SinkAllocations] {
		a.allocs[dbGrantAttempt(row)] = row
	}
	a.checkApplications()
}

func (a effectAuthority) indexAuthorityChange(row Row) {
	key := row["row_key"].([]any)
	before, after := captureImage(row, "before_image"), captureImage(row, "after_image")
	switch row["subject"] {
	case activityAttempts:
		id := attemptKey{key[0].(string), key[1].(string), number(key[2]), number(key[3])}
		a.attemptLog[id] = append(a.attemptLog[id], row)
	case intentsRelation:
		intent := key[0]
		if a.intents[intent]["k6_logical_key"] == nil {
			return
		}
		if a.grants[intent] != nil && (before["grant_token_hash"] != after["grant_token_hash"] ||
			before["grant_expires_at"] != after["grant_expires_at"]) {
			a.violate(writeAuthority, "I10_GRANT_REPLACED", WriteCapture, row)
		}
		if before["dispatch_status"] == "NOT_GRANTED" && after["dispatch_status"] == "GRANTED" && a.grants[intent] == nil {
			a.grants[intent] = row
		}
		if before["dispatch_status"] == "GRANTED" && after["dispatch_status"] == "DISPATCHED" {
			a.acks[intent] = row
		}
	}
}

func (a effectAuthority) checkApplications() {
	receipts, calls := map[resourceRevision]Row{}, map[any][]Row{}
	latest := map[any]int64{}
	for _, row := range a.rows[SinkReceipts] {
		receipts[resourceRevision{row["resource_id"].(string), number(row["applied_revision"])}] = row
	}
	for _, row := range a.rows[SinkCalls] {
		resource := row["resource_id"]
		i := a.ingress[row["connection_hash"].(string)]
		if row["outcome"] == "ALLOCATED" && callRequestMatches(row, i.Traffic, row["workflow_id"].(string)) {
			latest[resource] = max(latest[resource], number(a.allocs[i.Traffic.grantAttempt()]["fence"]))
		}
		if row["outcome"] == "APPLIED" {
			calls[row["application_id"]] = append(calls[row["application_id"]], row)
			a.callFence[row["call_id"]] = latest[resource]
		}
		if grant := a.grants[row["intent_id"]]; grant != nil && (row["operation"] == "lookup" || number(row["attempt"]) > 1) {
			switch {
			case row["grant_hash"] == nil:
				a.require(writeAuthority, "recovery_grant_identity", SinkCalls, row)
			case row["grant_hash"] != captureImage(grant, "after_image")["grant_token_hash"] ||
				i.Traffic.GrantHash != "" && row["grant_hash"] != i.Traffic.GrantHash:
				a.violate(writeAuthority, "I10_GRANT_CONTINUITY", SinkCalls, row)
			}
		}
	}
	label := &a.result.Labels[9]
	label.PopulationUnit = "applied_cooperating_key"
	seen := map[cooperatingKey]bool{}
	for _, write := range a.rows[SinkJournal] {
		intent := a.intents[write["intent_id"]]
		key := cooperatingKey{intent["workflow_id"], write["logical_key"]}
		if !seen[key] {
			label.PopulationCount++
			seen[key] = true
		}
		receipt := receipts[resourceRevision{write["resource_id"].(string), number(write["revision"])}]
		applied := calls[receipt["application_id"]]
		var first Row
		if len(applied) == 0 {
			a.require(writeAuthority, "sink_commit", SinkJournal, write)
		} else {
			first = applied[0]
			for _, call := range applied {
				a.checkAuthorizedWrite(call, write, receipt, intent)
				label.Witnesses = append(label.Witnesses, a.witness(SinkCalls, call))
			}
		}
		a.checkDispatchAcknowledged(key, write, receipt, first, intent)
	}
	if label.PopulationCount == 0 {
		label.Reason = "no_positive_population"
		return
	}
	if label.Registered && a.captureReady && a.result.Labels[7].Outcome == "covered/pass" &&
		!slices.ContainsFunc(a.result.Findings, func(f Finding) bool {
			return slices.Contains(label.Predicates, f.Predicate) || f.Predicate == "OwnerAtCommit" || f.Predicate == "CurrentClaimWinner" && f.Kind == Violation
		}) {
		label.Outcome, label.Reason = "covered/pass", "predicate_satisfied"
	}
}

func (a effectAuthority) checkAuthorizedWrite(call, write, receipt, intent Row) {
	flag := func(code string) { a.violate(writeAuthority, code, SinkCalls, call) }
	need := func(source string) { a.require(writeAuthority, source, SinkCalls, call) }
	workflow, _ := call["workflow_id"].(string)
	op := a.plans[a.workflows[workflow]["submission_key"]]
	if op.LogicalKey == "" {
		need("sealed_effect_operation")
		return
	}
	if call["committed_at"] == nil || call["transaction_id"] == nil || write["transaction_id"] == nil {
		need("sink_commit_transaction")
	} else if call["transaction_id"] != write["transaction_id"] {
		flag("I10_WRITE_TRANSACTION")
	}
	if call["grant_read_at"] == nil || call["grant_hash"] == nil || call["node_id"] == nil || call["iteration"] == nil ||
		call["scope_hash"] == nil || call["arguments"] == nil || call["expected_revision"] == nil || call["fence"] == nil {
		need("exact_grant_read")
		return
	}
	i := a.ingress[call["connection_hash"].(string)]
	grant := a.grants[call["intent_id"]]
	issued := captureImage(grant, "after_image")
	prior := captureImage(grant, "before_image")
	d := a.decisions[call["intent_id"].(string)]
	if intent["decision"] != "APPROVED" || d.Outcome != "APPROVED" ||
		(issued != nil && (issued["decision"] != "APPROVED" || prior["decision"] != "APPROVED")) {
		flag("I10_UNAPPROVED_WRITE")
	} else if d.Subject != "system:serviceaccount:"+a.manifest.Namespace+":k6-approver" ||
		intent["approver_id"] != d.Subject || intent["decided_at"] != d.DecidedUTC {
		flag("I10_WRONG_ACTOR")
	}
	if issued != nil {
		if issued["decided_at"] == nil || prior["decided_at"] == nil {
			need("committed_decision_identity")
		} else if prior["approver_id"] != d.Subject || issued["approver_id"] != d.Subject ||
			evidenceTime(prior["decided_at"]) != d.DecidedUTC || evidenceTime(issued["decided_at"]) != d.DecidedUTC ||
			prior["proposal_hash"] != d.ProposalHash || issued["proposal_hash"] != d.ProposalHash {
			flag("I10_GRANT_DECISION_BINDING")
		}
	}
	if call["grant_hash"] != intent["grant_token_hash"] || issued != nil && call["grant_hash"] != issued["grant_token_hash"] ||
		i.Traffic.Intent != "" && call["grant_hash"] != i.Traffic.GrantHash || d.GrantHash != "" && call["grant_hash"] != d.GrantHash {
		flag("I10_WRONG_TOKEN")
	}
	if call["resource_id"] != op.target || intent["target"] != op.target || write["resource_id"] != op.target {
		flag("I10_WRONG_TARGET")
	}
	if authorityArgumentHash(call["arguments"]) != op.args || authorityArgumentHash(intent["canonical_arguments"]) != op.args ||
		receipt["canonical_hash"] != op.args {
		flag("I10_WRONG_ARGUMENT")
	}
	if call["logical_key"] != op.LogicalKey || intent["k6_logical_key"] != op.LogicalKey || write["logical_key"] != op.LogicalKey {
		flag("I10_WRONG_KEY")
	}
	if call["scope_hash"] != op.scope || intent["grant_scope_hash"] != op.scope || receipt["scope_hash"] != op.scope ||
		issued != nil && issued["grant_scope_hash"] != op.scope || d.ProposalHash != op.proposal || intent["proposal_hash"] != op.proposal {
		flag("I10_WRONG_SCOPE")
	}
	if call["expected_revision"] != op.ExpectedRevision || intent["expected_resource_revision"] != op.ExpectedRevision ||
		strconv.FormatInt(number(write["old_revision"]), 10) != op.ExpectedRevision || receipt["expected_revision"] != write["old_revision"] {
		flag("I10_WRONG_REVISION")
	}
	if intent["workflow_id"] != workflow || call["node_id"] != op.Node || intent["node_id"] != op.Node ||
		number(call["iteration"]) != op.Iteration || number(intent["iteration"]) != op.Iteration {
		flag("I10_WRONG_OPERATION")
	}
	issue := a.checkWriteOrder(call, intent, grant, d, i)
	a.checkWriteClaim(call, i, op, issue)
}

func evidenceTime(value any) string {
	s, _ := value.(string)
	if at, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return UTC(at)
	}
	return ""
}

func (a effectAuthority) checkWriteOrder(call, intent, grant Row, d DecisionRow, ingress IngressRow) Row {
	flag := func(code string) { a.violate(writeAuthority, code, SinkCalls, call) }
	need := func(source string) { a.require(writeAuthority, source, SinkCalls, call) }
	read, written := evidenceTime(call["grant_read_at"]), evidenceTime(call["committed_at"])
	expires := evidenceTime(intent["grant_expires_at"])
	if expires == "" || len(a.rows[authEpisode]) == 0 {
		need("absolute_grant_expiry")
	} else if expires <= read || written >= expires || intent["grant_expires_at"] != intent["valid_until"] ||
		intent["grant_expires_at"] != a.rows[authEpisode][0]["valid_until"] {
		flag("I10_EXPIRED_WRITE")
	}
	if ingress.OpenSequence == 0 {
		need("relay_ingress_open")
	} else if !callRequestMatches(call, ingress.Traffic, call["workflow_id"].(string)) ||
		ingress.OpenedUTC > read || written != "" && read > written {
		flag("I10_WRITE_BRACKET")
	}
	var issue Row
	for _, h := range a.history[call["workflow_id"]] {
		commit := a.byTransition[h["transition_id"]]
		at := evidenceTime(commit["finalized_at"])
		if h["reason"] == "APPROVAL_GRANTED" && grant != nil && committed(commit) &&
			grant["transaction_id"] == commit["transaction_id"] && h["node_id"] == call["node_id"] &&
			h["iteration"] == call["iteration"] && h["scheduler_epoch"] == a.owners[grant["event_order"]]["epoch"] && validHistoryActor(h) {
			issue = commit
		}
		if h["new_state"] == "CANCELED" && at != "" && written != "" && at <= written {
			flag("I10_CANCELLATION_ORDER")
		}
	}
	issuedAt := evidenceTime(issue["finalized_at"])
	for _, c := range a.canceled[call["workflow_id"]] {
		created, applied := evidenceTime(c["created_at"]), evidenceTime(c["applied_at"])
		if issuedAt != "" && created <= issuedAt && (applied == "" || applied >= issuedAt) ||
			c["status"] == "APPLIED" && applied != "" && written != "" && applied <= written {
			flag("I10_CANCELLATION_ORDER")
		}
	}
	if issuedAt == "" {
		need("owner_grant_transaction")
	} else if d.DecidedUTC < evidenceTime(intent["created_at"]) || issuedAt < d.DecidedUTC || issuedAt > read ||
		grantExpiry(grant) != expires {
		flag("I10_GRANT_ORDER")
	}
	return issue
}

func grantExpiry(grant Row) string {
	return evidenceTime(captureImage(grant, "after_image")["grant_expires_at"])
}

func (a effectAuthority) checkWriteClaim(call Row, ingress IngressRow, op authorityPlan, issue Row) {
	flag := func(code string) { a.violate(writeAuthority, code, SinkCalls, call) }
	need := func(source string) { a.require(writeAuthority, source, SinkCalls, call) }
	sent := a.traffic[ingress.Traffic.exchange()]
	if sent.claim.ClaimHash == "" {
		need("authenticated_write_claim")
		return
	}
	claim := sent.claim
	if claim.Workflow != call["workflow_id"] || claim.Node != op.Node || claim.Iteration != op.Iteration || claim.Attempt != number(call["attempt"]) {
		flag("I10_WRITE_CLAIM")
	}
	key := claim.key()
	read, written := evidenceTime(call["grant_read_at"]), evidenceTime(call["committed_at"])
	var creation Row
	for _, h := range a.history[claim.Workflow] {
		if h["node_id"] == claim.Node && number(h["iteration"]) == claim.Iteration &&
			(h["reason"] == attemptCreated && number(h["attempt_number"]) == claim.Attempt ||
				h["reason"] == attemptReplaced && number(h["attempt_number"])+1 == claim.Attempt) {
			creation = a.byTransition[h["transition_id"]]
		}
	}
	if !committed(creation) {
		need("attempt_creation_transaction")
	} else if issue != nil && number(creation["event_order"]) <= number(issue["event_order"]) {
		flag("I10_GRANT_ORDER")
	}
	if len(a.claims[key]) == 0 {
		need("committed_write_claim")
	} else if !slices.ContainsFunc(a.claims[key], func(row Row) bool {
		id := row["claim_identity"].(map[string]any)
		return id["token_hash"] == claim.ClaimHash && id["worker_request_id"] == claim.RequestID &&
			id["worker_id"] == sent.sent.session.Claimant && evidenceTime(row["finalized_at"]) <= read &&
			(creation == nil || number(creation["event_order"]) < number(row["event_order"]))
	}) {
		flag("I10_WRITE_CLAIM")
	}
	var current Row
	for _, row := range a.attemptLog[key] {
		image := captureImage(row, "after_image")
		if at := evidenceTime(image["updated_at"]); at != "" && at <= written {
			current = image
		}
	}
	if current == nil && written != "" {
		need("current_claim_at_write")
	} else if current != nil && (current["state"] != "CLAIMED" || current["is_current"] != true || current["claim_token_hash"] != claim.ClaimHash ||
		current["worker_request_id"] != claim.RequestID || current["worker_id"] != sent.sent.session.Claimant ||
		evidenceTime(current["heartbeat_deadline"]) <= written) {
		flag("I10_WRITE_CLAIM")
	}
	alloc := a.allocs[ingress.Traffic.grantAttempt()]
	if number(call["fence"]) <= 0 || alloc["resource_id"] != call["resource_id"] || alloc["fence"] != call["fence"] ||
		a.callFence[call["call_id"]] != number(call["fence"]) ||
		current != nil && current["k6_fence"] != call["fence"] {
		flag("I10_STALE_FENCE")
	}
}

func (a effectAuthority) checkDispatchAcknowledged(key cooperatingKey, write, receipt, call, intent Row) {
	if intent["dispatch_status"] != "DISPATCHED" {
		a.violate(dispatchAck, "I10_DISPATCH_UNACKNOWLEDGED", SinkJournal, write)
		return
	}
	var winner Row
	for _, row := range a.rows[activityAttempts] {
		if row["workflow_id"] == key.workflow && row["logical_effect_key"] == key.key && row["state"] == "SUCCEEDED" {
			winner = row
			break
		}
	}
	proof, _ := winner["k6_proof"].(map[string]any)
	want, _ := Canonical(receipt)
	got, _ := Canonical(proof["receipt"])
	if winner == nil || proof == nil || receipt == nil || string(want) != string(got) {
		a.violate(dispatchAck, "I10_DISPATCH_RECEIPT", SinkJournal, write)
		return
	}
	ack := a.acks[intent["intent_id"]]
	if ack == nil {
		a.require(dispatchAck, "dispatch_ack_transaction", SinkJournal, write)
		return
	}
	for _, h := range a.history[key.workflow] {
		audit := a.byTransition[h["transition_id"]]
		result := a.resultCommits[h["transition_id"]]
		if h["reason"] == resultRecorded && committed(audit) && audit["transaction_id"] == ack["transaction_id"] &&
			result.request != nil && captureMatchesAttempt(result.request, winner) &&
			captureImage(result.request, "after_image")["receipt_id"] == receipt["application_id"] &&
			evidenceTime(audit["finalized_at"]) >= evidenceTime(call["committed_at"]) {
			return
		}
	}
	a.violate(dispatchAck, "I10_DISPATCH_ACK_ORDER", WriteCapture, ack)
}
