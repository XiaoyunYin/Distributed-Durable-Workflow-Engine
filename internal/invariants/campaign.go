package invariants

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Arrival struct {
	Sequence    int             `json:"sequence"`
	Key         string          `json:"key"`
	WorkflowID  string          `json:"workflow_id"`
	Activity    string          `json:"activity"`
	ScheduledNS int64           `json:"scheduled_ns"`
	LatenessNS  int64           `json:"lateness_ns"`
	SendNS      *int64          `json:"send_ns"`
	ScheduledAt time.Time       `json:"scheduled_at"`
	SentAt      *time.Time      `json:"sent_at"`
	FinishedNS  *int64          `json:"finished_ns"`
	HTTPStatus  int             `json:"http_status"`
	Outcome     string          `json:"outcome"`
	Response    json.RawMessage `json:"response"`
	Error       string          `json:"error,omitempty"`
	Request     json.RawMessage `json:"request"`
	Expected    json.RawMessage `json:"expected"`
}

type Arrivals struct {
	Offered     int        `json:"offered"`
	Schema      string     `json:"schema"`
	Status      string     `json:"status"`
	Namespace   string     `json:"namespace"`
	RunID       string     `json:"run_id"`
	Arm         string     `json:"arm"`
	Seed        int64      `json:"seed"`
	Rate        float64    `json:"rate"`
	Concurrency int        `json:"concurrency"`
	CPUUnits    int        `json:"cpu_units"`
	Rows        []Arrival  `json:"rows"`
	Windows     []Arrivals `json:"windows,omitempty"`
}

type WitnessInventory struct {
	Session   string `json:"session"`
	PodName   string `json:"pod_name"`
	PodUID    string `json:"pod_uid"`
	File      string `json:"file"`
	Attempted int    `json:"attempted"`
	Written   int    `json:"written"`
	Errors    int    `json:"errors"`
	SHA256    string `json:"sha256"`
	Complete  bool   `json:"complete"`
}
type WitnessLedger struct {
	Inventory WitnessInventory `json:"inventory"`
	Data      []byte           `json:"data"`
}
type WitnessExport struct {
	ExpectedPods []string        `json:"expected_pods"`
	Ledgers      []WitnessLedger `json:"ledgers"`
}
type WitnessEvent struct {
	Event         string    `json:"event"`
	Session       string    `json:"session"`
	Sequence      int       `json:"sequence"`
	PodName       string    `json:"pod_name"`
	PodUID        string    `json:"pod_uid"`
	InvocationID  string    `json:"invocation_id"`
	WorkflowID    string    `json:"workflow_id"`
	NodeID        string    `json:"node_id"`
	Iteration     int       `json:"iteration"`
	AttemptNumber int64     `json:"attempt_number"`
	ClaimToken    string    `json:"claim_token"`
	WorkerID      string    `json:"worker_id"`
	Timestamp     time.Time `json:"timestamp"`
	MonotonicNS   int64     `json:"monotonic_ns"`
	Outcome       string    `json:"outcome"`
}
type ExecutionMetric struct {
	WorkflowID  string `json:"workflow_id"`
	NodeID      string `json:"node_id"`
	Iteration   int    `json:"iteration"`
	Invocations int    `json:"invocations"`
	Winners     int    `json:"winners"`
	Canceled    bool   `json:"canceled"`
}
type CampaignReport struct {
	Valid                               bool              `json:"valid"`
	Violations                          []string          `json:"violations"`
	Offered                             int               `json:"offered"`
	Accepted                            int               `json:"accepted"`
	Rejected                            int               `json:"rejected"`
	Uncertain                           int               `json:"uncertain"`
	NonTerminal                         []string          `json:"non_terminal"`
	DuplicateExecutions                 int               `json:"duplicate_executions"`
	ExecutedIterations                  int               `json:"executed_iterations"`
	CanceledIterations                  int               `json:"canceled_iterations"`
	RetryableReceipts                   int               `json:"intermediate_retryable_receipts"`
	FinalFailureReceipts                int               `json:"final_failure_receipts"`
	AcceptedResults                     int               `json:"accepted_results"`
	AcceptedResultsPerExecutedIteration *float64          `json:"accepted_results_per_executed_non_canceled_iteration"`
	BusySeconds                         float64           `json:"busy_seconds"`
	ReexecutedSeconds                   float64           `json:"reexecuted_seconds"`
	DuplicateEffects                    string            `json:"duplicate_effects"`
	Executions                          []ExecutionMetric `json:"executions"`
	InvariantsCovered                   []string          `json:"invariants_covered"`
	InvariantsNotCovered                []string          `json:"invariants_not_covered"`
	InvariantOutcomes                   map[string]string `json:"invariant_outcomes"`
}

// PureCoverage names the evidence actually checked, rather than treating an
// unexercised race, fault or external effect as a passing invariant.
func PureCoverage() ([]string, []string, map[string]string) {
	covered := []string{"I1 submission identity reconciliation", "I2 contiguous atomic history evidence", "I4 stale-result/current-attempt evidence", "I6 history/outbox obligations", "I11 unique terminal history"}
	missing := []string{"I3 no confirmed takeover/lease-order fault", "I5 no lost-result-response drill", "I7 no independent broker commit-order witness", "I8 PURE: cooperating effects NOT COVERED", "I9 PURE: uncertain non-cooperating effects NOT COVERED", "I10 PURE: approval/grant effects NOT COVERED", "I12 no process clock-skew drill", "I13 no shutdown drill", "cancellation races NOT COVERED", "storage loss NOT COVERED"}
	outcomes := map[string]string{}
	for i := 1; i <= 13; i++ {
		outcomes[fmt.Sprintf("I%d", i)] = "not-covered"
	}
	for _, i := range []int{1, 2, 4, 6, 11} {
		outcomes[fmt.Sprintf("I%d", i)] = "covered/pass"
	}
	return covered, missing, outcomes
}

func CheckCampaign(t Trace, arrivals Arrivals, witness WitnessExport) CampaignReport {
	return ReconcileCampaign(t, arrivals, witness, Check(t), SnapshotIntegrity(t))
}

// ReconcileCampaign adds campaign evidence checks to the results of Check and
// SnapshotIntegrity for the same trace. The CLI also uses the structural verdict
// in its top-level report.
func ReconcileCampaign(t Trace, arrivals Arrivals, witness WitnessExport, v Verdict, integrityErr error) CampaignReport {
	r := CampaignReport{Violations: append([]string{}, v.Violations...), NonTerminal: v.NonTerminal, Offered: len(arrivals.Rows), DuplicateEffects: "NOT COVERED (PURE)", Executions: []ExecutionMetric{}}
	r.InvariantsCovered, r.InvariantsNotCovered, r.InvariantOutcomes = PureCoverage()
	fail := func(s string) { r.Violations = append(r.Violations, s) }
	if integrityErr != nil {
		fail(integrityErr.Error())
	}
	if arrivals.Schema != "dur053.arrivals.v1" || arrivals.Status != "PASS" || len(arrivals.Rows) == 0 || len(arrivals.Rows) != arrivals.Offered || arrivals.Namespace == "" {
		fail("arrival manifest incomplete")
	}
	// Raw snapshot and projected accepted set must independently agree for
	// every namespace key, including acceptance after an uncertain response.
	rawAccepted := make(map[string]string, len(t.Submissions))
	type workflowEvidence struct {
		ID        string `json:"workflow_id"`
		Namespace string `json:"namespace"`
		Key       string `json:"submission_key"`
	}
	var w workflowEvidence
	for _, raw := range t.Snapshot["engine.workflow_executions"].Rows {
		w = workflowEvidence{}
		if json.Unmarshal(raw, &w) != nil {
			fail("invalid workflow snapshot")
			continue
		}
		if w.Namespace == arrivals.Namespace {
			rawAccepted[w.Key] = w.ID
		}
	}
	durable := make(map[string]SubmissionRecord, len(t.Submissions))
	for _, s := range t.Submissions {
		if s.Namespace != arrivals.Namespace {
			fail("snapshot includes wrong namespace")
		}
		if _, ok := durable[s.Key]; ok {
			fail("duplicate durable accepted key")
		}
		durable[s.Key] = s
	}
	if len(rawAccepted) != len(durable) {
		fail("raw snapshot accepted-count reconciliation failed")
	}
	for key, id := range rawAccepted {
		if durable[key].WorkflowID != id {
			fail("raw snapshot accepted identity missing from projection")
		}
	}
	keys := make(map[string]bool, len(arrivals.Rows))
	workflows := make(map[string]bool, len(arrivals.Rows))
	sequences, rates := make([]int, len(arrivals.Rows)), make([]float64, len(arrivals.Rows))
	for i := range arrivals.Rows {
		sequences[i], rates[i] = i, arrivals.Rate
	}
	if len(arrivals.Windows) > 0 {
		position := 0
		windowIDs := map[string]bool{}
		for _, window := range arrivals.Windows {
			if len(window.Windows) != 0 || window.Offered != len(window.Rows) || window.Offered < 1 || window.Schema != "dur053.arrivals.v1" || window.Status != "PASS" || window.Namespace != arrivals.Namespace || window.Arm != arrivals.Arm || window.Concurrency != arrivals.Concurrency || window.CPUUnits != arrivals.CPUUnits || windowIDs[window.RunID] {
				fail("invalid campaign window metadata")
			}
			windowIDs[window.RunID] = true
			for i, row := range window.Rows {
				if position >= len(arrivals.Rows) {
					fail("campaign windows exceed combined arrivals")
					break
				}
				want, _ := json.Marshal(row)
				got, _ := json.Marshal(arrivals.Rows[position])
				if !bytes.Equal(want, got) {
					fail("campaign window bytes differ from combined arrivals")
				}
				sequences[position], rates[position] = i, window.Rate
				position++
			}
		}
		if position != len(arrivals.Rows) {
			fail("campaign windows do not cover combined arrivals")
		}
	}
	for i, a := range arrivals.Rows {
		if a.Sequence != sequences[i] || a.Key == "" || keys[a.Key] || workflows[a.WorkflowID] {
			fail("arrival identity duplicate or incomplete")
		}
		keys[a.Key] = true
		workflows[a.WorkflowID] = true
		if rates[i] <= 0 || math.IsNaN(rates[i]) || math.IsInf(rates[i], 0) || a.ScheduledNS != int64(float64(sequences[i])*1e9/rates[i]) || a.ScheduledAt.IsZero() || a.SendNS == nil || a.SentAt == nil || a.FinishedNS == nil || *a.SendNS < a.ScheduledNS || *a.FinishedNS < *a.SendNS || a.LatenessNS != *a.SendNS-a.ScheduledNS || !json.Valid(a.Request) || !json.Valid(a.Response) {
			fail("arrival schedule/send/response evidence incomplete")
		}
		s, found := durable[a.Key]
		if found {
			r.Accepted++
			if s.WorkflowID != a.WorkflowID {
				fail("submission key maps to unexpected workflow")
			}
			if a.Outcome != "ACCEPTED" && a.Outcome != "UNCERTAIN" {
				fail("rejected/unsent arrival found durably accepted")
			}
		} else if a.Outcome == "ACCEPTED" {
			fail("accepted workflow missing from durable submissions: " + a.WorkflowID)
		}
		if a.Outcome == "UNCERTAIN" {
			r.Uncertain++
			if !found {
				fail("uncertain submission unresolved: " + a.Key)
			}
		}
		if a.Outcome == "REJECTED" {
			r.Rejected++
		}
		if a.Outcome != "ACCEPTED" && a.Outcome != "UNCERTAIN" {
			fail("prerequisites require every offered arrival accepted")
		}
	}
	for key := range durable {
		if !keys[key] {
			fail("extra durable accepted submission: " + key)
		}
	}
	if len(r.NonTerminal) > 0 {
		fail("accepted workflows remain non-terminal")
	}
	for _, h := range t.History {
		if isTerminal(h.NewState) && h.NewState != "SUCCEEDED" {
			fail("unexpected unsuccessful terminal workflow: " + h.WorkflowID)
		}
	}
	// Validate expected application results independently of worker receipt claims.
	expected := make(map[string]json.RawMessage, len(arrivals.Rows))
	for _, a := range arrivals.Rows {
		expected[a.WorkflowID+"\x00"+a.Activity] = a.Expected
	}
	resultSeen := make(map[string]bool, len(expected))
	type nodeEvidence struct {
		WorkflowID string          `json:"workflow_id"`
		NodeID     string          `json:"node_id"`
		Result     json.RawMessage `json:"accepted_result"`
	}
	var n nodeEvidence
	var compactGot, compactWant bytes.Buffer
	for _, raw := range t.Snapshot["engine.node_instances"].Rows {
		n = nodeEvidence{}
		if err := json.Unmarshal(raw, &n); err != nil {
			fail("invalid node snapshot")
			continue
		}
		k := n.WorkflowID + "\x00" + n.NodeID
		want, ok := expected[k]
		if !ok {
			continue
		}
		if !equalCampaignResult(n.Result, want, &compactGot, &compactWant) {
			fail("unexpected accepted activity result: " + n.WorkflowID)
		}
		resultSeen[k] = true
	}
	for k := range expected {
		if !resultSeen[k] {
			fail("missing accepted activity result: " + k)
		}
	}
	reconcileWitness(t, witness, &r)
	r.Valid = len(r.Violations) == 0
	if !r.Valid {
		for k, v := range r.InvariantOutcomes {
			if v == "covered/pass" {
				r.InvariantOutcomes[k] = "covered/fail (safety or evidence completeness)"
			}
		}
	}
	return r
}

func equalCampaignResult(got, want json.RawMessage, compactGot, compactWant *bytes.Buffer) bool {
	if bytes.Equal(got, want) {
		return json.Valid(got)
	}
	compactGot.Reset()
	compactWant.Reset()
	if json.Compact(compactGot, got) != nil || json.Compact(compactWant, want) != nil {
		return false
	}
	if bytes.Equal(compactGot.Bytes(), compactWant.Bytes()) {
		return true
	}
	gotHash, gotErr := canonicalJSONHash(got)
	wantHash, wantErr := canonicalJSONHash(want)
	return gotErr == nil && wantErr == nil && gotHash == wantHash
}

func iterationKey(w, n string, i int) string { return w + "/" + n + "/" + strconv.Itoa(i) }
func attemptKey(w, n string, i int, a int64) string {
	return w + "/" + n + "/" + strconv.Itoa(i) + "/" + strconv.FormatInt(a, 10)
}
func reconcileWitness(t Trace, export WitnessExport, r *CampaignReport) {
	fail := func(s string) { r.Violations = append(r.Violations, s) }
	expected := map[string]bool{}
	seenPods := map[string]bool{}
	sessions := map[string]bool{}
	for _, pod := range export.ExpectedPods {
		if pod == "" || expected[pod] {
			fail("invalid expected witness pod inventory")
		}
		expected[pod] = true
	}
	if len(expected) == 0 {
		fail("missing expected witness pod inventory")
	}
	attemptCount := len(t.Attempts)
	starts := make(map[string]WitnessEvent, attemptCount)
	ends := make(map[string]WitnessEvent, attemptCount)
	var e WitnessEvent
	for _, ledger := range export.Ledgers {
		inv := ledger.Inventory
		sum := sha256.Sum256(ledger.Data)
		if !inv.Complete || inv.Errors != 0 || inv.Attempted != inv.Written || inv.Written < 1 || inv.SHA256 != hex.EncodeToString(sum[:]) {
			fail("incomplete witness export: count/hash/write failure")
		}
		if !expected[inv.PodUID] || sessions[inv.Session] || inv.Session == "" {
			fail("unexpected or duplicate witness session")
		}
		sessions[inv.Session] = true
		seenPods[inv.PodUID] = true
		if (len(ledger.Data) > 0 && !bytes.HasSuffix(ledger.Data, []byte{'\n'})) || bytes.Count(ledger.Data, []byte{'\n'}) != inv.Written {
			fail("incomplete witness export: line count")
		}
		remaining := ledger.Data
		for index := 0; ; index++ {
			line, rest, found := bytes.Cut(remaining, []byte{'\n'})
			if !found {
				break
			}
			remaining = rest
			e = WitnessEvent{}
			if json.Unmarshal(line, &e) != nil {
				fail("invalid witness JSON")
				continue
			}
			if e.Sequence != index+1 || e.Session != inv.Session || e.PodUID != inv.PodUID || e.PodName != inv.PodName || e.Timestamp.IsZero() || e.MonotonicNS <= 0 {
				fail("incomplete witness event identity or sequence")
			}
			if index == 0 {
				if e.Event != "session" {
					fail("missing witness session header")
				}
				continue
			}
			if e.InvocationID == "" || e.WorkflowID == "" || e.NodeID == "" || e.Iteration < 0 || e.AttemptNumber < 1 || e.ClaimToken == "" || e.WorkerID == "" {
				fail("incomplete invocation identity")
			}
			switch e.Event {
			case "start":
				if _, ok := starts[e.InvocationID]; ok {
					fail("duplicate invocation ID")
				}
				starts[e.InvocationID] = e
			case "end":
				if _, ok := ends[e.InvocationID]; ok {
					fail("duplicate invocation end")
				}
				ends[e.InvocationID] = e
			default:
				fail("unknown witness event")
			}
		}
	}
	for pod := range expected {
		if !seenPods[pod] {
			fail("missing witness pod export: " + pod)
		}
	}
	attempts := make(map[string]*AttemptRecord, attemptCount)
	witnessed := make(map[string]bool, len(starts))
	metrics := make(map[string]*ExecutionMetric, len(starts))
	first := make(map[string]string, len(starts))
	firstAt := make(map[string]int64, len(starts))
	for i := range t.Attempts {
		a := &t.Attempts[i]
		attempts[attemptKey(a.WorkflowID, a.NodeID, a.Iteration, a.AttemptNumber)] = a
	}
	for id, e := range starts {
		key := attemptKey(e.WorkflowID, e.NodeID, e.Iteration, e.AttemptNumber)
		a, ok := attempts[key]
		if !ok || a.ClaimToken != e.ClaimToken || !(a.WorkerID == e.WorkerID || strings.HasSuffix(a.WorkerID, ":uid:"+e.PodUID)) {
			fail("invocation does not match engine claim: " + id)
		}
		witnessed[key] = true
		end, done := ends[id]
		if !done {
			fail("unfinished invocation in prerequisites: " + id)
		} else if end.Session != e.Session || end.WorkflowID != e.WorkflowID || end.NodeID != e.NodeID || end.Iteration != e.Iteration || end.AttemptNumber != e.AttemptNumber || end.ClaimToken != e.ClaimToken || end.WorkerID != e.WorkerID || end.Outcome != "SUCCEEDED" || end.MonotonicNS < e.MonotonicNS || end.Timestamp.Before(e.Timestamp) {
			fail("invocation end identity/time mismatch")
		}
		k := iterationKey(e.WorkflowID, e.NodeID, e.Iteration)
		if metrics[k] == nil {
			metrics[k] = &ExecutionMetric{WorkflowID: e.WorkflowID, NodeID: e.NodeID, Iteration: e.Iteration}
		}
		metrics[k].Invocations++
		// Compare wall starts across sessions only for descriptive ordering on
		// this registered single-host topology; elapsed busy time is monotonic.
		ns := e.Timestamp.UnixNano()
		if old, ok := firstAt[k]; !ok || ns < old || (ns == old && id < first[k]) {
			firstAt[k] = ns
			first[k] = id
		}
		if done {
			r.BusySeconds += float64(end.MonotonicNS-e.MonotonicNS) / 1e9
		}
	}
	for id := range ends {
		if _, ok := starts[id]; !ok {
			fail("invocation end without start")
		}
	}
	receipts := make(map[string]int, attemptCount)
	type receiptEvidence struct {
		WorkflowID    string `json:"workflow_id"`
		NodeID        string `json:"node_id"`
		Iteration     int    `json:"iteration"`
		AttemptNumber int64  `json:"attempt_number"`
		Reason        string `json:"reason"`
	}
	var h receiptEvidence
	for _, raw := range t.Snapshot["engine.transition_history"].Rows {
		h = receiptEvidence{}
		if json.Unmarshal(raw, &h) != nil {
			fail("invalid receipt history")
			continue
		}
		if h.Reason == "ATTEMPT_RESULT_RECORDED" {
			key := attemptKey(h.WorkflowID, h.NodeID, h.Iteration, h.AttemptNumber)
			if _, ok := attempts[key]; ok {
				receipts[key]++
			}
		}
	}
	for key, a := range attempts {
		if a.ClaimToken != "" && !witnessed[key] {
			fail("claimed attempt missing invocation witness: " + key)
		}
		k := iterationKey(a.WorkflowID, a.NodeID, a.Iteration)
		m := metrics[k]
		if m == nil {
			if a.ResultRecorded {
				fail("result lacks externally executed iteration")
			}
			continue
		}
		if a.State == "CANCELED" {
			m.Canceled = true
		}
		if a.ResultRecorded && receipts[key] != 1 {
			fail("accepted result receipt/history reconciliation failed")
		}
		if !a.ResultRecorded && receipts[key] != 0 {
			fail("receipt history has no matching accepted result")
		}
		if a.ResultRecorded {
			if a.State == "FAILED_RETRYABLE" {
				r.RetryableReceipts++
			}
			if a.State == "FAILED_FINAL" {
				r.FinalFailureReceipts++
			}
			if a.State == "SUCCEEDED" {
				m.Winners++
			} else {
				fail("unexpected unsuccessful result receipt")
			}
		}
	}
	for _, e := range starts {
		k := iterationKey(e.WorkflowID, e.NodeID, e.Iteration)
		if end, ok := ends[e.InvocationID]; ok && e.InvocationID != first[k] {
			r.ReexecutedSeconds += float64(end.MonotonicNS-e.MonotonicNS) / 1e9
		}
	}
	executionKeys := make([]string, 0, len(metrics))
	for key, m := range metrics {
		executionKeys = append(executionKeys, key)
		r.ExecutedIterations++
		r.DuplicateExecutions += m.Invocations - 1
		if m.Canceled {
			r.CanceledIterations++
		} else {
			r.AcceptedResults += m.Winners
			if m.Winners != 1 {
				fail("executed non-canceled iteration does not have exactly one winning successful result")
			}
		}
	}
	sort.Strings(executionKeys)
	r.Executions = make([]ExecutionMetric, 0, len(executionKeys))
	for _, key := range executionKeys {
		r.Executions = append(r.Executions, *metrics[key])
	}
	if n := r.ExecutedIterations - r.CanceledIterations; n > 0 {
		ratio := float64(r.AcceptedResults) / float64(n)
		r.AcceptedResultsPerExecutedIteration = &ratio
	}
	if r.ExecutedIterations == 0 {
		fail("no external execution witness")
	}
}
