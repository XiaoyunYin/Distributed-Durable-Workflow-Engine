package k6v1

import (
	"maps"
	"slices"
	"sort"
	"strconv"
	"strings"
)

const (
	Pass       = 0
	Fail       = 1
	Usage      = 2
	Incomplete = 3
)

type Witness struct {
	Path   string `json:"path"`
	RowKey []any  `json:"row_key"`
	SHA256 string `json:"sha256"`
}

type Finding struct {
	Kind      FindingKind `json:"kind"`
	Predicate string      `json:"predicate"`
	Code      string      `json:"code"`
	Source    string      `json:"source"`
	Witness   *Witness    `json:"witness"`
}

type FindingKind string

const (
	MissingProof FindingKind = "missing"
	Violation    FindingKind = "violation"
	Censor       FindingKind = "censor"
)

type Label struct {
	ID              string    `json:"id"`
	Registered      bool      `json:"registered"`
	Outcome         string    `json:"outcome"`
	Predicates      []string  `json:"predicates"`
	Reason          string    `json:"reason"`
	PopulationUnit  string    `json:"population_unit"`
	PopulationCount int       `json:"population_count"`
	Witnesses       []Witness `json:"witnesses"`
}

type NamespaceCounts struct {
	Executions                     int   `json:"executions"`
	NamespaceExecutions            int   `json:"namespace_executions"`
	SQLNamespaceExecutions         int64 `json:"sql_namespace_executions"`
	ForeignExecutions              int   `json:"foreign_executions"`
	OrphanRows                     int   `json:"orphan_rows"`
	TerminalExecutions             int   `json:"terminal_executions"`
	UnsuccessfulTerminalExecutions int   `json:"unsuccessful_terminal_executions"`
}

type Result struct {
	Schema               string           `json:"schema"`
	Status               string           `json:"status"`
	Safety               string           `json:"safety"`
	Completeness         string           `json:"completeness"`
	RelationIntegrity    string           `json:"relation_integrity"`
	Confirmation         Confirmation     `json:"confirmation"`
	IndexSHA256          string           `json:"index_sha256"`
	MeasurementClaim     bool             `json:"measurement_claim"`
	ChargedSeconds       int              `json:"charged_seconds"`
	Namespace            *NamespaceCounts `json:"namespace"`
	Labels               []Label          `json:"labels"`
	InvariantsCovered    []string         `json:"invariants_covered"`
	InvariantsNotCovered []string         `json:"invariants_not_covered"`
	Findings             []Finding        `json:"findings"`
}

type Confirmation struct {
	Status string `json:"status"`
	Reason string `json:"reason"`
}

func (r Result) ExitCode() int {
	switch r.Status {
	case "failed":
		return Fail
	case "passed":
		return Pass
	}
	return Incomplete
}

func integrity(code, source string) *Finding {
	return &Finding{Kind: Violation, Predicate: "RelationIntegrity", Code: code, Source: source}
}

func missing(path string) *Finding {
	return &Finding{Kind: MissingProof, Predicate: "RelationIntegrity", Code: "required_proof_missing:" + path, Source: path}
}

func newResult(m Manifest, indexHash string) Result {
	r := Result{
		Schema: "dur053.k6-checker.v1", Status: "incomplete", Safety: "NOT COVERED",
		Completeness: "incomplete", RelationIntegrity: "covered/pass", IndexSHA256: indexHash,
		Confirmation:      Confirmation{Status: "pending", Reason: "confirmation_pending:G3a4"},
		InvariantsCovered: []string{}, InvariantsNotCovered: []string{}, Findings: []Finding{},
	}
	for i, predicates := range [][]string{
		{"SubmissionIdentity"}, {"AtomicHistory"}, {"OwnerAtCommit", "DeployedStaleChallenge"},
		{"CurrentClaimWinner", "SessionClosure"}, {"LostResultReplay"}, {"DispatchDisposition"},
		{"LowerOffsetsDisposed"}, {"OneMutationPerKey", "RevisionChain", "DeliveryClosure", "ReceiptSignature"},
		{"UnknownEffectVisible"}, {"AuthorizedAtWrite", "DispatchAcknowledged", "ReceiptSignature"}, {"UniqueTerminal"},
		{"SignedProcessSkew", "DatabaseDeadline", "GroupMembership"}, {"ShutdownDisposition"},
	} {
		n := i + 1
		inCeiling := m.Cell != "" && registered(n, m)
		reason := "outside_registry_ceiling"
		if inCeiling {
			reason = pendingReason(n)
		}
		r.Labels = append(r.Labels, Label{
			ID: "I" + strconv.Itoa(n), Registered: inCeiling, Outcome: "NOT COVERED",
			Predicates: predicates, Reason: reason, Witnesses: []Witness{},
		})
	}
	return r
}

func pendingReason(label int) string {
	switch label {
	case 8:
		return "required_proof_missing:effect_closure"
	case 10:
		return "predicate_pending:I10"
	case 12:
		return "predicate_pending:G3a4"
	default:
		return "predicate_pending"
	}
}

func registered(n int, m Manifest) bool {
	switch n {
	case 1, 2, 4, 6, 11:
		return true
	case 3:
		return m.Cell == "hook-5" || m.Cell == "medium-2" || m.Cell == "medium-3"
	case 7:
		return m.Cell == "hook-3"
	case 8, 10:
		return m.Workload == "COOPERATING_EFFECT"
	case 12:
		return m.Ordinal >= 70 && m.Ordinal < 90
	default:
		return false
	}
}

func (r *Result) finish() {
	for i := range r.Labels {
		label := &r.Labels[i]
		for _, f := range r.Findings {
			if !slices.Contains(label.Predicates, f.Predicate) {
				continue
			}
			if f.Witness != nil && !slices.ContainsFunc(label.Witnesses, func(w Witness) bool {
				return w.Path == f.Witness.Path && w.SHA256 == f.Witness.SHA256
			}) {
				label.Witnesses = append(label.Witnesses, *f.Witness)
			}
			if !label.Registered {
				continue
			}
			switch f.Kind {
			case Violation:
				label.Outcome, label.Reason = "covered/fail", "contract_violation"
			case MissingProof:
				if label.Outcome != "covered/fail" {
					label.Outcome, label.Reason = "NOT COVERED", f.Code
				}
			case Censor:
				if label.Outcome != "covered/fail" {
					label.Outcome, label.Reason = "NOT COVERED", "required_proof_missing:"+f.Predicate
				}
			}
		}
		if !label.Registered {
			label.Outcome, label.Reason = "NOT COVERED", "outside_registry_ceiling"
		}
		if label.Outcome == "NOT COVERED" {
			r.InvariantsNotCovered = append(r.InvariantsNotCovered, label.ID)
		} else {
			r.InvariantsCovered = append(r.InvariantsCovered, label.ID)
		}
	}
	if len(r.Findings) > 0 || r.Safety == "FAIL" || r.Completeness == "FAIL" {
		r.Status = "failed"
	} else if r.Confirmation.Status == "confirmed" && r.RelationIntegrity == "covered/pass" &&
		!slices.ContainsFunc(r.Labels, func(l Label) bool { return l.Registered && l.Outcome != "covered/pass" }) {
		r.Status, r.Safety, r.Completeness = "passed", "PASS", "PASS"
	}
}

func (r *Result) record(kind FindingKind, predicate, code, source string, ref *Witness) {
	r.Findings = append(r.Findings, Finding{Kind: kind, Predicate: predicate, Code: code, Source: source, Witness: ref})
	if kind == Violation {
		r.Safety = "FAIL"
	} else {
		r.Completeness = "FAIL"
	}
}

func (r *Result) failIntegrity(issue *Finding) {
	r.Completeness = "FAIL"
	r.RelationIntegrity = "covered/fail"
	r.Findings = append(r.Findings, *issue)
}

func Failed(issue *Finding) Result {
	r := newResult(Manifest{}, "")
	r.failIntegrity(issue)
	r.finish()
	return r
}

func AcquisitionFailed() Result {
	return Failed(missing(IndexPath))
}

const (
	executions = "engine.workflow_executions"
	history    = "engine.transition_history"
)

func evaluate(m Manifest, s Snapshot, rows map[string][]Row, plan Plan, arrivals Arrivals, planOK, arrivalsOK bool, indexHash string, issues []Finding, external ...External) Result {
	r := newResult(m, indexHash)
	for i := range issues {
		r.failIntegrity(&issues[i])
	}
	workflows, haveWorkflows := rows[executions]
	histories, haveHistory := rows[history]
	specs := m.Relations("engine")
	keys := map[string][]string{}
	for _, spec := range append(slices.Clone(specs), SinkRelations()...) {
		keys[spec.Name] = spec.Keys
	}
	witness := func(relation string, row Row) Witness {
		key := make([]any, 0, len(keys[relation]))
		for _, name := range keys[relation] {
			key = append(key, row[name])
		}
		raw, _ := encode(row)
		return Witness{Path: RelationPath(relation), RowKey: key, SHA256: Hash(raw)}
	}
	violate := func(predicate, code, relation string, row Row) {
		ref := witness(relation, row)
		r.record(Violation, predicate, code, ref.Path, &ref)
	}

	byID := make(map[string]Row, len(workflows))
	counts := NamespaceCounts{Executions: len(workflows), SQLNamespaceExecutions: s.NamespaceCount}
	if haveWorkflows {
		r.Namespace = &counts
		for _, row := range workflows {
			byID[row["workflow_id"].(string)] = row
			if row["namespace"] == m.Namespace {
				counts.NamespaceExecutions++
			} else {
				counts.ForeignExecutions++
				violate("WholeNamespace", "FOREIGN_EXECUTION", executions, row)
			}
			if isTerminal(row["state"]) {
				counts.TerminalExecutions++
				if row["state"] != "SUCCEEDED" {
					counts.UnsuccessfulTerminalExecutions++
				}
			}
		}
		if int64(counts.NamespaceExecutions) != s.NamespaceCount {
			r.failIntegrity(integrity("NAMESPACE_COUNT", RelationPath(executions)))
		}
		// Iterate the pinned order, including foreign and dangling rows from
		// relations without an engine-side workflow foreign key.
		for _, spec := range specs {
			for _, row := range rows[spec.Name] {
				id, present := row["workflow_id"].(string)
				if present && byID[id] == nil {
					counts.OrphanRows++
					violate("WholeNamespace", "ORPHAN_WORKFLOW_ROW", spec.Name, row)
				}
			}
		}
	}
	if m.Workload == "PURE" {
		for _, spec := range specs {
			_, table, _ := strings.Cut(spec.Name, ".")
			switch table {
			case "effect_records", "effect_call_attempts", "effect_resolution_audit":
				for _, row := range rows[spec.Name] {
					violate("PureEngineEffects", "PURE_EFFECT_ROW", spec.Name, row)
				}
			case "activity_attempts":
				for _, row := range rows[spec.Name] {
					if row["k6_proof"] != nil {
						violate("PureEngineEffects", "PURE_EFFECT_ROW", spec.Name, row)
					}
				}
			}
		}
	}
	for _, row := range rows["engine.k4_audit_queue"] {
		violate("RelationIntegrity", "AUDIT_QUEUE_NOT_DRAINED", "engine.k4_audit_queue", row)
		r.Completeness = "FAIL"
		r.RelationIntegrity = "covered/fail"
	}
	r.checkSubmissions(m, rows, plan, arrivals, planOK, arrivalsOK, witness, violate)
	if m.Sink != nil {
		r.checkSink(m, rows, byID, haveWorkflows, witness, violate)
		r.checkReceiptSignatures(m, rows[activityAttempts], witness)
	}

	groups := make(map[string][]Row, len(workflows))
	for _, row := range histories {
		id := row["workflow_id"].(string)
		groups[id] = append(groups[id], row)
	}
	terminal := &r.Labels[10]
	terminal.PopulationUnit = "durable_accept"
	timedOut := slices.ContainsFunc(issues, func(f Finding) bool { return f.Code == "CUT_DEADLINE" })
	checkHistory := func(group []Row) bool {
		var previous any
		contiguous := true
		for i, row := range group {
			if number(row["revision"]) != int64(i+1) {
				contiguous = false
				violate("AtomicHistory", "HISTORY_REVISION_GAP", history, row)
			}
			if row["old_state"] != previous || !legalEdge(row["old_state"], row["new_state"], i == 0) {
				violate("AtomicHistory", "HISTORY_EDGE", history, row)
			}
			previous = row["new_state"]
		}
		return contiguous
	}
	for _, wf := range workflows {
		id := wf["workflow_id"].(string)
		group := groups[id]
		delete(groups, id)
		contiguous := checkHistory(group)
		if wf["namespace"] != m.Namespace {
			continue
		}
		terminal.PopulationCount++
		ref := witness(executions, wf)
		terminal.Witnesses = append(terminal.Witnesses, ref)
		if !haveHistory {
			continue
		}
		final := number(wf["revision"])
		aligned := len(group) > 0 && number(group[len(group)-1]["revision"]) == final &&
			group[len(group)-1]["new_state"] == wf["state"]
		if !aligned {
			violate("AtomicHistory", "HISTORY_FINAL_REVISION", executions, wf)
		}
		terminals := 0
		for _, row := range group {
			if isTerminal(row["new_state"]) {
				terminals++
			}
		}
		if terminals > 1 || !aligned || (terminals != 0) != isTerminal(wf["state"]) {
			violate("UniqueTerminal", "TERMINAL_FINAL_UNIQUE", executions, wf)
		} else if terminals == 0 {
			if timedOut {
				r.record(Censor, "UniqueTerminal", "TERMINAL_DEADLINE_CENSORED", ref.Path, &ref)
			} else {
				r.record(MissingProof, "UniqueTerminal", "required_proof_missing:UniqueTerminal", ref.Path, &ref)
			}
		} else if !contiguous {
			r.record(MissingProof, "UniqueTerminal", "required_proof_missing:UniqueTerminal", ref.Path, &ref)
		}
	}
	for _, id := range slices.Sorted(maps.Keys(groups)) {
		checkHistory(groups[id])
	}
	label := &r.Labels[1]
	label.PopulationUnit = "workflow"
	label.PopulationCount = len(workflows)
	for _, row := range workflows {
		label.Witnesses = append(label.Witnesses, witness(executions, row))
	}
	for _, entry := range []*Label{label, terminal} {
		if entry.Outcome == "covered/fail" {
			continue
		}
		if !haveWorkflows {
			entry.Reason = "required_proof_missing:" + RelationPath(executions)
		} else if !haveHistory {
			entry.Reason = "required_proof_missing:" + RelationPath(history)
		}
	}
	if haveWorkflows && haveHistory {
		label.Reason = "no_positive_population"
		if len(workflows) > 0 {
			label.Outcome, label.Reason = "covered/pass", "predicate_satisfied"
		}
		switch {
		case r.Labels[0].Outcome == "covered/pass" && terminal.PopulationCount > 0:
			terminal.Outcome, terminal.Reason = "covered/pass", "predicate_satisfied"
		case r.Labels[0].Outcome == "covered/fail":
			terminal.Reason = "required_proof_missing:SubmissionIdentity"
		case r.Labels[0].Reason == "no_positive_population" || terminal.PopulationCount == 0 && r.Labels[0].Outcome == "covered/pass":
			terminal.Reason = "no_positive_population"
		default:
			terminal.Reason = r.Labels[0].Reason
		}
	}
	ordering := newEngineOrder(&r, rows, byID, witness, violate)
	ordering.capture = m.CaptureEnabled()
	if ordering.capture {
		ordering.captureReady = ordering.checkCaptureClosure()
	}
	ordering.checkAtomicHistory()
	ordering.checkOwnerAtCommit()
	ordering.checkCurrentClaimWinner()
	for _, sources := range external {
		ordering.brokerCopies = sources.BrokerCopies != nil && sources.Broker != nil && len(sources.Broker.Rows) > 0
	}
	ordering.checkDispatch()
	ordering.checkReplayAndUncertainty()
	for _, sources := range external {
		copied := ordering.checkBrokerCopies(sources.Broker, sources.BrokerCopies)
		aborts := checkExternal(&r, m, sources, rows)
		ordering.checkLowerOffsets(sources.Broker, copied)
		ordering.checkShutdown(aborts, sources)
		if m.Sink != nil && planOK && sources.Ingress != nil && sources.Decisions != nil {
			r.awardEffectClosure(rows, plan, sources, witness, violate)
		}
	}
	if ordering.capture && r.Labels[3].Outcome == "covered/pass" {
		for _, issue := range issues {
			if issue.Source == RosterPath || isSessionPath(issue.Source) {
				ordering.require("SessionClosure", issue.Source, "", nil)
			}
		}
	}
	if r.Safety != "FAIL" && r.Completeness != "FAIL" {
		r.Safety = "incomplete"
	}
	r.finish()
	return r
}

func isTerminal(state any) bool {
	switch state {
	case "SUCCEEDED", "FAILED", "REJECTED", "CANCELED", "ABANDONED":
		return true
	default:
		return false
	}
}

var workflowEdges = map[string]string{
	"RUNNABLE":                   "CANCELED FAILED PAUSED_UNSUPPORTED_VERSION RUNNABLE SUCCEEDED WAITING_ACTIVITY WAITING_APPROVAL WAITING_TIMER",
	"WAITING_ACTIVITY":           "CANCELED FAILED RECONCILIATION_REQUIRED RUNNABLE WAITING_ACTIVITY WAITING_TIMER",
	"WAITING_TIMER":              "CANCELED RUNNABLE WAITING_TIMER",
	"WAITING_APPROVAL":           "CANCELED REJECTED RUNNABLE WAITING_APPROVAL",
	"PAUSED_UNSUPPORTED_VERSION": "ABANDONED CANCELED PAUSED_UNSUPPORTED_VERSION RUNNABLE",
	"RECONCILIATION_REQUIRED":    "ABANDONED CANCELED RECONCILIATION_REQUIRED RUNNABLE",
}

func legalEdge(from, to any, first bool) bool {
	if first {
		return from == nil && to == "RUNNABLE"
	}
	old, ok := from.(string)
	if !ok {
		return false
	}
	allowed := strings.Fields(workflowEdges[old])
	i := sort.SearchStrings(allowed, to.(string))
	return i < len(allowed) && allowed[i] == to
}
