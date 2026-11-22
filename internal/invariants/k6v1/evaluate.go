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
	Predicate string   `json:"predicate"`
	Code      string   `json:"code"`
	Source    string   `json:"source"`
	Witness   *Witness `json:"witness"`
}

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
	IndexSHA256          string           `json:"index_sha256"`
	MeasurementClaim     bool             `json:"measurement_claim"`
	ChargedSeconds       int              `json:"charged_seconds"`
	Namespace            *NamespaceCounts `json:"namespace"`
	Labels               []Label          `json:"labels"`
	InvariantsCovered    []string         `json:"invariants_covered"`
	InvariantsNotCovered []string         `json:"invariants_not_covered"`
	Findings             []Finding        `json:"findings"`
}

func (r Result) ExitCode() int {
	if r.Status == "failed" {
		return Fail
	}
	return Incomplete
}

func integrity(code, source string) *Finding {
	return &Finding{Predicate: "RelationIntegrity", Code: code, Source: source}
}

func missing(path string) *Finding {
	return integrity("required_proof_missing:"+path, path)
}

func newResult(m Manifest, indexHash string) Result {
	r := Result{
		Schema: "dur053.k6-checker.v1", Status: "incomplete", Safety: "NOT COVERED",
		Completeness: "incomplete", RelationIntegrity: "covered/pass", IndexSHA256: indexHash,
		InvariantsCovered: []string{}, InvariantsNotCovered: []string{}, Findings: []Finding{},
	}
	for i, predicates := range [][]string{
		{"SubmissionIdentity"}, {"AtomicHistory"}, {"OwnerAtCommit", "DeployedStaleChallenge"},
		{"CurrentClaimWinner", "SessionClosure"}, {"LostResultReplay"}, {"DispatchDisposition"},
		{"LowerOffsetsDisposed"}, {"OneMutationPerKey", "RevisionChain", "DeliveryClosure"},
		{"UnknownEffectVisible"}, {"AuthorizedAtWrite", "DispatchAcknowledged"}, {"UniqueTerminal"},
		{"SignedProcessSkew", "DatabaseDeadline"}, {"ShutdownDisposition"},
	} {
		n := i + 1
		reason := "slice_not_implemented:G2b"
		if n == 1 || n == 2 || n == 4 || n == 11 || ((n == 8 || n == 10) && m.Sink != nil) {
			reason = "predicate_partial:G2b"
		}
		r.Labels = append(r.Labels, Label{
			ID: "I" + strconv.Itoa(n), Registered: m.Cell != "" && registered(n, m), Outcome: "NOT COVERED",
			Predicates: predicates, Reason: reason, Witnesses: []Witness{},
		})
	}
	return r
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
	for _, label := range r.Labels {
		if label.Outcome == "NOT COVERED" {
			r.InvariantsNotCovered = append(r.InvariantsNotCovered, label.ID)
		} else {
			r.InvariantsCovered = append(r.InvariantsCovered, label.ID)
		}
	}
	if len(r.Findings) > 0 {
		r.Status = "failed"
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
	specs := EngineRelations(m.Instrumented())
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
		r.Findings = append(r.Findings, Finding{Predicate: predicate, Code: code, Source: ref.Path, Witness: &ref})
		r.Safety = "FAIL"
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
		r.checkSink(rows, byID, haveWorkflows, witness, violate)
	}

	groups := make(map[string][]Row, len(workflows))
	for _, row := range histories {
		id := row["workflow_id"].(string)
		groups[id] = append(groups[id], row)
	}
	historyFailed, terminalFailed := false, false
	checkHistory := func(group []Row) {
		var previous any
		for i, row := range group {
			if number(row["revision"]) != int64(i+1) {
				historyFailed = true
				violate("AtomicHistory", "HISTORY_REVISION_GAP", history, row)
			}
			if row["old_state"] != previous || !legalEdge(row["old_state"], row["new_state"], i == 0) ||
				row["actor_kind"] == "" || row["actor_id"] == "" || row["reason"] == "" {
				historyFailed = true
				violate("AtomicHistory", "HISTORY_EDGE", history, row)
			}
			previous = row["new_state"]
		}
	}
	for _, wf := range workflows {
		id := wf["workflow_id"].(string)
		group := groups[id]
		delete(groups, id)
		checkHistory(group)
		if !haveHistory {
			continue
		}
		final := number(wf["revision"])
		aligned := len(group) > 0 && number(group[len(group)-1]["revision"]) == final &&
			group[len(group)-1]["new_state"] == wf["state"]
		if !aligned {
			historyFailed = true
			violate("AtomicHistory", "HISTORY_FINAL_REVISION", executions, wf)
		}
		terminals := 0
		for _, row := range group {
			if isTerminal(row["new_state"]) {
				terminals++
			}
		}
		if terminals != 1 || !aligned || !isTerminal(wf["state"]) {
			terminalFailed = true
			violate("UniqueTerminal", "TERMINAL_FINAL_UNIQUE", executions, wf)
		}
	}
	for _, id := range slices.Sorted(maps.Keys(groups)) {
		checkHistory(groups[id])
	}
	for _, item := range []struct {
		index  int
		failed bool
	}{{1, historyFailed}, {10, terminalFailed}} {
		label := &r.Labels[item.index]
		label.PopulationUnit = "workflow"
		label.PopulationCount = len(workflows)
		for _, row := range workflows {
			label.Witnesses = append(label.Witnesses, witness(executions, row))
		}
		if item.failed {
			label.Outcome, label.Reason = "covered/fail", "contract_violation"
		} else if !haveWorkflows {
			label.Reason = "required_proof_missing:" + RelationPath(executions)
		} else if !haveHistory {
			label.Reason = "required_proof_missing:" + RelationPath(history)
		}
	}
	for _, sources := range external {
		checkExternal(&r, m, sources, rows)
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
