package k6v1

import (
	"fmt"
	"slices"
)

func externalViolation(r *Result, label int, predicate, code, path string, key []any, raw []byte) {
	ref := Witness{Path: path, RowKey: key, SHA256: Hash(raw)}
	r.Findings = append(r.Findings, Finding{Predicate: predicate, Code: code, Source: path, Witness: &ref})
	r.Safety = "FAIL"
	if label >= 0 && r.Labels[label].Registered {
		entry := &r.Labels[label]
		entry.Outcome, entry.Reason = "covered/fail", "contract_violation"
		entry.Witnesses = append(entry.Witnesses, ref)
	}
}

func flagExternal[T any](r *Result, label int, predicate, code, path string, row Record[T]) {
	raw, _ := Canonical(row)
	externalViolation(r, label, predicate, code, path, []any{row.Position.Sequence}, raw)
}

func databaseOrphan(r *Result, label int, predicate, code, relation string, row Row) {
	for _, spec := range append(EngineRelations(false), SinkRelations()...) {
		if spec.Name != relation {
			continue
		}
		keys := make([]any, 0, len(spec.Keys))
		for _, key := range spec.Keys {
			keys = append(keys, row[key])
		}
		raw, _ := Canonical(row)
		externalViolation(r, label, predicate, code, RelationPath(relation), keys, raw)
		return
	}
}

type attemptKey struct {
	workflow, node     string
	iteration, attempt int64
}

func fromDBAttempt(row map[string]any) attemptKey {
	w, _ := row["workflow_id"].(string)
	n, _ := row["node_id"].(string)
	return attemptKey{w, n, number(row["iteration"]), number(row["attempt_number"])}
}

func (a AttemptIdentity) key() attemptKey {
	return attemptKey{a.Workflow, a.Node, a.Iteration, a.Attempt}
}

type observedWorker struct {
	session WorkerSession
	row     Record[WorkerRow]
}

type observedSend struct {
	sent     observedWorker
	outcome  *observedWorker
	workflow string
}

func checkExternal(r *Result, m Manifest, e External, rows map[string][]Row) {
	traffic := checkSessions(r, m, e, rows)
	if e.Broker != nil {
		if len(e.Broker.Rows) == 0 {
			r.failIntegrity(integrity("BROKER_BASELINE_MISSING", BrokerPath))
		}
		for i := 1; i < len(e.Broker.Rows); i++ {
			if brokerRegression(e.Broker.Rows[i-1].Data, e.Broker.Rows[i].Data) {
				r.failIntegrity(integrity("EXTERNAL_REGRESSION", BrokerPath))
			}
		}
	}
	if e.Topology != nil {
		if code := pureTopology(m, *e.Topology); code != "" {
			if code == "PURE_EFFECT_COMPONENT" {
				for _, row := range e.Topology.Rows {
					if slices.ContainsFunc(row.Data.Components, pureEffectComponent) {
						flagExternal(r, -1, "PureTopology", code, TopologyPath, row)
					}
				}
			} else {
				r.failIntegrity(integrity(code, TopologyPath))
			}
		}
	}
	if m.Sink == nil {
		for _, binding := range sortedKeys(traffic) {
			send := traffic[binding].sent
			flagExternal(r, -1, "PureTopology", "PURE_EFFECT_SEND", SessionPath(send.session.ID), send.row)
		}
		return
	}
	approved := checkDecisions(r, m, e.Decisions, rows)
	if e.Ingress != nil {
		checkDeliveries(r, *e.Ingress, traffic, approved, e.Decisions != nil, rows)
	}
}

func expectedSessions(r *Result, roster Stream[WatchRow]) (map[string]WorkerSession, map[string]Death) {
	expected, deaths := map[string]WorkerSession{}, map[string]Death{}
	if roster.Header.Source == "" {
		return expected, deaths
	}
	if len(roster.Rows) == 0 || roster.Rows[0].Data.Kind != "baseline" {
		r.failIntegrity(integrity("ROSTER_BASELINE_MISSING", RosterPath))
		return expected, deaths
	}
	last := ""
	processes := map[[3]string]bool{}
	restarts := map[string]int64{}
	runtimes, registered := map[RuntimeIdentity]bool{}, map[RuntimeIdentity]bool{}
	var runtimeRows []Record[WatchRow]
	for i, row := range roster.Rows {
		w := row.Data
		if w.From != last || w.Kind == "gap" || i > 0 && w.Kind == "baseline" {
			r.failIntegrity(integrity("ROSTER_WATCH_GAP", RosterPath))
		}
		last = w.Cursor
		if w.Kind == "runtime" {
			previous, observed := restarts[w.Runtime.PodUID]
			if !observed && w.Runtime.Restart > 0 || observed && w.Runtime.Restart > previous+1 {
				r.failIntegrity(integrity("ROSTER_RESTART_GAP", RosterPath))
			}
			restarts[w.Runtime.PodUID] = max(previous, w.Runtime.Restart)
			runtimes[*w.Runtime] = true
			runtimeRows = append(runtimeRows, row)
		}
		if w.Session == nil {
			continue
		}
		s := *w.Session
		process := [3]string{s.PodUID, s.ContainerID, s.BootID}
		switch w.Kind {
		case "session":
			if expected[s.ID].ID != "" || processes[process] {
				flagExternal(r, 3, "SessionClosure", "ROSTER_DUPLICATE_PROCESS", RosterPath, row)
			}
			expected[s.ID], processes[process] = s, true
			registered[s.Runtime()] = true
		case "death":
			if expected[s.ID] != s || deaths[s.ID].Observer != "" || w.Death.ObservedUTC < s.StartedUTC ||
				w.Death.ObservedUTC > row.Position.UTC || w.Death.Observer != roster.Header.Source ||
				w.Death.Observer == w.Death.Receiver {
				r.failIntegrity(integrity("SESSION_DEATH_UNCORROBORATED", SessionPath(s.ID)))
				continue
			}
			deaths[s.ID] = *w.Death
		}
	}
	for _, row := range runtimeRows {
		if !registered[*row.Data.Runtime] {
			flagExternal(r, 3, "SessionClosure", "ROSTER_RUNTIME_WITHOUT_SESSION", RosterPath, row)
		}
	}
	if len(runtimeRows) == 0 {
		r.failIntegrity(integrity("ROSTER_RUNTIME_EMPTY", RosterPath))
	}
	for _, id := range sortedKeys(expected) {
		if !runtimes[expected[id].Runtime()] {
			r.failIntegrity(integrity("SESSION_RUNTIME_UNCORROBORATED", SessionPath(id)))
		}
	}
	return expected, deaths
}

func corroboratedDeath(s Stream[WorkerRow], d Death) bool {
	return d.Observer != "" && d.Receiver == s.Header.Source &&
		d.FinalSequence == s.Seal.Last && d.FinalSHA256 == s.Seal.SHA256 && d.ObservedUTC <= s.Seal.ThroughUTC
}

func checkSessions(r *Result, m Manifest, e External, rows map[string][]Row) map[string]observedSend {
	expected, deaths := expectedSessions(r, e.Roster)
	attemptRows, haveAttempts := rows["engine.activity_attempts"]
	auditRows, haveAudit := rows["engine.k4_audit"]
	attempts := map[attemptKey]Row{}
	claims := map[attemptKey][]map[string]any{}
	for _, a := range attemptRows {
		attempts[fromDBAttempt(a)] = a
	}
	for _, row := range auditRows {
		if claim, ok := row["claim_identity"].(map[string]any); ok && row["kind"] == "claim" {
			key := fromDBAttempt(claim)
			claims[key] = append(claims[key], claim)
		}
	}
	started := map[attemptKey]bool{}
	invocations := map[string]bool{}
	traffic := map[string]observedSend{}
	type deadProcess struct{ started, died string }
	deadClaimants := map[string][]deadProcess{}
	for _, id := range sortedKeys(e.Sessions) {
		ledger, session := e.Sessions[id], expected[id]
		path := SessionPath(id)
		if session.ID == "" {
			if e.Roster.Header.Source != "" {
				r.failIntegrity(integrity("SESSION_UNROSTERED", path))
			}
			continue
		}
		if len(ledger.Rows) == 0 || ledger.Rows[0].Data.Kind != "session" || *ledger.Rows[0].Data.Session != session ||
			ledger.Header.OpenedUTC != session.StartedUTC {
			r.failIntegrity(integrity("SESSION_HEADER_BINDING", path))
			continue
		}
		death := corroboratedDeath(ledger, deaths[id])
		if ledger.Seal.Mode == "receiver_final" && !death {
			r.failIntegrity(integrity("SESSION_DEATH_UNCORROBORATED", path))
		}
		if death {
			deadClaimants[session.Claimant] = append(deadClaimants[session.Claimant], deadProcess{session.StartedUTC, deaths[id].ObservedUTC})
		}
		active := map[string]Record[WorkerRow]{}
		for _, row := range ledger.Rows[1:] {
			w := row.Data
			flag := func(code string) { flagExternal(r, 3, "SessionClosure", code, path, row) }
			switch w.Kind {
			case "session":
				flag("SESSION_DUPLICATE_HEADER")
			case "start":
				a, key := *w.Attempt, w.Attempt.key()
				db := attempts[key]
				matchedClaim := slices.ContainsFunc(claims[key], func(claim map[string]any) bool {
					return claim["worker_id"] == session.Claimant && claim["token_hash"] == a.ClaimHash && claim["worker_request_id"] == a.RequestID
				})
				if invocations[w.Invocation] || started[key] {
					flag("SESSION_DUPLICATE_START")
				}
				if haveAttempts && (db == nil || db["worker_id"] != session.Claimant ||
					db["claim_token_hash"] != a.ClaimHash || db["worker_request_id"] != a.RequestID) || haveAudit && !matchedClaim {
					flag("SESSION_START_CLAIM")
				}
				invocations[w.Invocation], started[key], active[w.Invocation] = true, true, row
			case "finish", "abort":
				if start, ok := active[w.Invocation]; !ok || *start.Data.Attempt != *w.Attempt {
					flag("SESSION_OUTCOME_WITHOUT_START")
				}
				delete(active, w.Invocation)
			case "send", "receive", "timeout", "close":
				start, ok := active[w.Invocation]
				if !ok || start.Data.Attempt.Attempt != w.Traffic.Attempt || Hash([]byte(w.Invocation)) != w.Traffic.MessageHash {
					flag("WORKER_TRAFFIC_INVOCATION")
					continue
				}
				joinWorkerTraffic(r, session, row, start.Data.Attempt.Workflow, traffic)
			}
		}
		for _, invocation := range sortedKeys(active) {
			if !death {
				flagExternal(r, 3, "SessionClosure", "SESSION_START_UNCLOSED", path, active[invocation])
			}
		}
	}
	if e.Roster.Header.Source != "" {
		for _, row := range attemptRows {
			if row["claim_token_hash"] == nil || started[fromDBAttempt(row)] {
				continue
			}
			claimant, _ := row["worker_id"].(string)
			bounded := slices.ContainsFunc(auditRows, func(audit Row) bool {
				claim, ok := audit["claim_identity"].(map[string]any)
				at, timeKnown := audit["at"].(string)
				return ok && timeKnown && audit["kind"] == "claim" && fromDBAttempt(claim) == fromDBAttempt(row) &&
					claim["token_hash"] == row["claim_token_hash"] && claim["worker_request_id"] == row["worker_request_id"] &&
					claim["worker_id"] == claimant && slices.ContainsFunc(deadClaimants[claimant], func(p deadProcess) bool {
					return p.started <= at && at <= p.died
				})
			})
			if !bounded {
				databaseOrphan(r, 3, "SessionClosure", "SESSION_CLAIM_WITHOUT_ENTRY", "engine.activity_attempts", row)
			}
		}
	}
	return traffic
}

func trafficEqual(a, b Traffic) bool {
	wire := a.Wire == nil || b.Wire == nil || *a.Wire == *b.Wire
	binding := a.WorkerBinding == "" || b.WorkerBinding == "" || a.WorkerBinding == b.WorkerBinding
	a.Wire, b.Wire = nil, nil
	a.WorkerBinding, b.WorkerBinding = "", ""
	return a == b && wire && binding
}

func (t Traffic) exchange() string { return t.MessageHash + t.RequestIDHash }

func joinWorkerTraffic(r *Result, session WorkerSession, row Record[WorkerRow], workflow string, traffic map[string]observedSend) {
	w := row.Data
	binding := w.Traffic.exchange()
	old, exists := traffic[binding]
	observation := observedWorker{session, row}
	if w.Kind == "send" && !exists {
		traffic[binding] = observedSend{sent: observation, workflow: workflow}
		return
	}
	if !exists || w.Kind == "send" || old.outcome != nil || old.sent.session != session ||
		old.sent.row.Data.Invocation != w.Invocation || !trafficEqual(*old.sent.row.Data.Traffic, *w.Traffic) {
		flagExternal(r, 7, "DeliveryClosure", "WORKER_TRAFFIC_ORDER", SessionPath(session.ID), row)
		return
	}
	old.outcome = &observation
	traffic[binding] = old
}

const intentsRelation = "engine.approval_action_intents"

func checkDecisions(r *Result, m Manifest, stream *Stream[DecisionRow], rows map[string][]Row) map[string]DecisionRow {
	approved := map[string]DecisionRow{}
	if stream == nil {
		return approved
	}
	intents, haveIntents := rows[intentsRelation]
	byID, seen := map[any]Row{}, map[string]bool{}
	for _, row := range intents {
		byID[row["intent_id"]] = row
	}
	for _, row := range stream.Rows {
		d := row.Data
		intent := byID[d.Intent]
		if d.Subject != "system:serviceaccount:"+m.Namespace+":k6-approver" ||
			d.IssuerHash != Hash([]byte("https://kubernetes.default.svc.cluster.local")) ||
			haveIntents && (intent == nil || intent["proposal_hash"] != d.ProposalHash ||
				d.Outcome != "PENDING" && (intent["decision"] != d.Outcome || intent["approver_id"] != d.Subject ||
					intent["decided_at"] != d.DecidedUTC || d.GrantHash != "" && intent["grant_token_hash"] != d.GrantHash)) {
			flagExternal(r, 9, "AuthorizedAtWrite", "DECISION_INTENT_BINDING", DecisionsPath, row)
		}
		if d.Outcome != "PENDING" {
			seen[d.Intent] = true
		}
		if d.Outcome == "APPROVED" {
			grant, _ := intent["grant_token_hash"].(string)
			if grant != "" {
				approved[grant] = d
			}
		}
	}
	for _, row := range intents {
		if row["decided_at"] != nil && !seen[row["intent_id"].(string)] {
			databaseOrphan(r, 9, "AuthorizedAtWrite", "DECISION_MISSING_FOR_INTENT", intentsRelation, row)
		}
	}
	return approved
}

func callMatches(c Row, i IngressRow, workflow string) bool {
	t := i.Traffic
	return c["operation"] == t.Operation && c["workflow_id"] == workflow && c["intent_id"] == t.Intent &&
		hashField(c, "logical_key") == t.KeyHash && hashField(c, "resource_id") == t.ResourceHash &&
		hashField(c, "request_id") == t.RequestIDHash && hashField(c, "message_id") == t.MessageHash &&
		number(c["attempt"]) == t.Attempt && c["body_hash"] == t.Request.SHA256 &&
		(i.Response == nil || c["response_hash"] == nil || c["response_hash"] == i.Response.SHA256) &&
		(c["application_id"] == nil && i.ApplicationID == "" || c["application_id"] == i.ApplicationID)
}

func hashField(row Row, field string) string {
	if value, ok := row[field].(string); ok {
		return Hash([]byte(value))
	}
	return ""
}

type grantAttempt struct {
	intent, grant string
	attempt       int64
}

func (t Traffic) grantAttempt() grantAttempt { return grantAttempt{t.Intent, t.GrantHash, t.Attempt} }

func dbGrantAttempt(row Row) grantAttempt {
	return grantAttempt{row["intent_id"].(string), row["grant_hash"].(string), number(row["attempt"])}
}

func deliveryOutcome(i IngressRow, send observedSend) bool {
	if send.outcome == nil {
		return false
	}
	w := send.outcome.row.Data
	switch i.Outcome {
	case "delivered", "denied":
		return w.Kind == "receive" && *w.Response == *i.Response &&
			(i.ResponseWire == nil || w.ResponseWire == nil || *i.ResponseWire == *w.ResponseWire)
	case "duplicate_reply":
		return i.Copy > 1
	case "withheld", "timeout":
		return w.Kind == "timeout" || w.Kind == "close"
	case "close":
		return w.Kind == "close" || w.Kind == "timeout"
	}
	return false
}

func checkDeliveries(r *Result, stream Stream[IngressRow], traffic map[string]observedSend, approved map[string]DecisionRow,
	haveDecisions bool, rows map[string][]Row) {
	calls, haveCalls := rows[SinkCalls]
	byConn, used := map[any][]Row{}, map[any]bool{}
	for _, c := range calls {
		byConn[c["connection_hash"]] = append(byConn[c["connection_hash"]], c)
	}
	receiptMatches := map[any]bool{}
	allocations, misses := map[grantAttempt]bool{}, map[grantAttempt]bool{}
	fences := map[grantAttempt]int64{}
	allocRows, haveAlloc := rows[SinkAllocations]
	missRows, haveMiss := rows[SinkLookupMisses]
	for _, row := range allocRows {
		key := dbGrantAttempt(row)
		allocations[key] = true
		fences[key] = number(row["fence"])
	}
	for _, row := range missRows {
		misses[dbGrantAttempt(row)] = true
	}
	requests := map[string]map[any]bool{}
	matchedAlloc, matchedMiss := map[grantAttempt]bool{}, map[grantAttempt]bool{}
	type copyKey struct{ ordinal, copy int64 }
	byCopy, byOrdinal := map[copyKey]bool{}, map[int64]string{}
	first := map[string]bool{}
	intents := map[string]Row{}
	for _, intent := range rows[intentsRelation] {
		intents[intent["intent_id"].(string)] = intent
	}
	inventory := map[copyKey]bool{}
	for _, row := range stream.Rows {
		inventory[copyKey{row.Data.Ordinal, row.Data.Copy}] = true
	}
	for _, row := range stream.Rows {
		i, t := row.Data, row.Data.Traffic
		flag := func(code string) { flagExternal(r, 7, "DeliveryClosure", code, IngressPath, row) }
		key := copyKey{i.Ordinal, i.Copy}
		if byCopy[key] || i.Copy > 1 && !inventory[copyKey{i.Ordinal, i.Copy - 1}] ||
			byOrdinal[i.Ordinal] != "" && byOrdinal[i.Ordinal] != t.WorkerBinding ||
			i.Copy == 1 && first[t.exchange()] {
			flag("INGRESS_COPY_ORDER")
		}
		byCopy[key], byOrdinal[i.Ordinal] = true, t.WorkerBinding
		if i.Copy == 1 {
			first[t.exchange()] = true
		}
		if i.Copy == 1 && i.Ordinal > 1 && !inventory[copyKey{i.Ordinal - 1, 1}] {
			flag("INGRESS_ORDINAL_GAP")
		}
		if !i.Acknowledged {
			flag("INGRESS_UNACKNOWLEDGED")
		}
		sent, ok := traffic[t.exchange()]
		intent := intents[t.Intent]
		workflow := sent.workflow
		switch {
		case t.Operation == "allocate":
			workflow, _ = intent["workflow_id"].(string)
			if ok || i.Copy != 1 {
				flag("ALLOCATOR_CHANNEL")
			}
		case !ok || !trafficEqual(*sent.sent.row.Data.Traffic, t):
			flag("INGRESS_WITHOUT_WORKER_SEND")
		case !deliveryOutcome(i, sent):
			flag("INGRESS_WORKER_OUTCOME")
		}
		if want := hashField(intent, "k6_logical_key"); want != "" && want != t.KeyHash {
			flag("DELIVERY_LOGICAL_KEY")
		}
		if haveDecisions && i.Outcome != "denied" && approved[t.GrantHash].Intent != t.Intent {
			flag("DELIVERY_WITHOUT_GRANT")
		}
		matches := byConn[i.ConnectionHash]
		if !haveCalls || i.Outcome == "denied" && len(matches) == 0 {
			continue
		}
		if len(matches) != 1 || used[i.ConnectionHash] || !callMatches(matches[0], i, workflow) {
			flag("INGRESS_CALL_COPY")
			continue
		}
		used[i.ConnectionHash] = true
		c := matches[0]
		if c["response_hash"] == nil && i.Response != nil && !unrecordedReply(c, i, fences) {
			flag("INGRESS_CALL_RESPONSE")
		}
		switch c["outcome"] {
		case "APPLIED":
			receiptMatches[c["application_id"]] = true
		case "ALLOCATED":
			matchedAlloc[t.grantAttempt()] = true
			if haveAlloc && !allocations[t.grantAttempt()] {
				flag("DELIVERY_ALLOCATION_MISSING")
			}
		case "MISSING":
			if t.Attempt > 1 {
				matchedMiss[t.grantAttempt()] = true
				if haveMiss && !misses[t.grantAttempt()] {
					flag("DELIVERY_LOOKUP_MISS_MISSING")
				}
			}
		}
		hashesForRequest := requests[t.RequestIDHash]
		if hashesForRequest == nil {
			hashesForRequest = map[any]bool{}
			requests[t.RequestIDHash] = hashesForRequest
		}
		hashesForRequest[t.Request.SHA256] = true
	}
	for _, id := range sortedKeys(traffic) {
		if !first[id] {
			s := traffic[id].sent
			flagExternal(r, 7, "DeliveryClosure", "WORKER_SEND_WITHOUT_INGRESS", SessionPath(s.session.ID), s.row)
		}
	}
	for _, row := range calls {
		if !used[row["connection_hash"]] {
			databaseOrphan(r, 7, "DeliveryClosure", "SINK_CALL_WITHOUT_INGRESS", SinkCalls, row)
		}
	}
	for _, row := range rows[SinkReceipts] {
		if !receiptMatches[row["application_id"]] {
			databaseOrphan(r, 7, "DeliveryClosure", "SINK_RECEIPT_WITHOUT_APPLICATION", SinkReceipts, row)
		}
	}
	for _, row := range allocRows {
		if !matchedAlloc[dbGrantAttempt(row)] {
			databaseOrphan(r, 7, "DeliveryClosure", "SINK_ALLOCATION_WITHOUT_INGRESS", SinkAllocations, row)
		}
	}
	for _, row := range missRows {
		if !matchedMiss[dbGrantAttempt(row)] {
			databaseOrphan(r, 7, "DeliveryClosure", "SINK_LOOKUP_WITHOUT_INGRESS", SinkLookupMisses, row)
		}
	}
	for _, row := range rows[SinkBindings] {
		if !requests[hashField(row, "request_id")][row["body_hash"]] {
			databaseOrphan(r, 7, "DeliveryClosure", "SINK_BINDING_WITHOUT_INGRESS", SinkBindings, row)
		}
	}
}

func unrecordedReply(call Row, i IngressRow, fences map[grantAttempt]int64) bool {
	outcome, _ := call["outcome"].(string)
	var body string
	switch outcome {
	case "ALLOCATED":
		fence := fences[i.Traffic.grantAttempt()]
		if fence <= 0 {
			return false
		}
		body = fmt.Sprintf(`{"fence":%d}`, fence)
	case "MISSING", "REQUEST_CONFLICT", "KEY_CONFLICT", "STALE_FENCE", "REVISION_CONFLICT":
		body = "K6_SINK_" + outcome + "\n"
	default:
		return false
	}
	return *i.Response == (WireDigest{Length: int64(len(body)), SHA256: Hash([]byte(body))})
}
