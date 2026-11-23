package k6v1

import (
	"encoding/json"
	"fmt"
	"slices"
	"time"
)

const (
	PlanPath              = "plan.json"
	ArrivalsPath          = "arrivals.json"
	PlanVersion           = "dur053.k6-plan.v1"
	ArrivalsVersion       = "dur053.k6-arrivals.v1"
	reconciliationSeconds = 600
)

type Plan struct {
	Schema          string       `json:"schema"`
	Episode         string       `json:"episode"`
	Namespace       string       `json:"namespace"`
	ResetID         string       `json:"reset_id"`
	SealedUTC       string       `json:"sealed_utc"`
	FaultMode       string       `json:"fault_mode"`
	HoldDeadlineUTC *string      `json:"hold_deadline_utc"`
	Windows         []PlanWindow `json:"windows"`
}

type PlanWindow struct {
	Name        string       `json:"name"`
	LoadRunID   string       `json:"load_run_id"`
	Submissions []Submission `json:"submissions"`
}

type Submission struct {
	Sequence     int             `json:"sequence"`
	ScheduledUTC string          `json:"scheduled_utc"`
	Key          string          `json:"submission_key"`
	BodySHA256   string          `json:"body_sha256"`
	Request      WireDigest      `json:"request"`
	Body         SubmissionBody  `json:"body"`
	Target       string          `json:"target"`
	Expected     json.RawMessage `json:"expected"`
}

type WireDigest struct {
	Length int64  `json:"length"`
	SHA256 string `json:"sha256"`
}

func (w WireDigest) valid() bool {
	return w.Length > 0 && digest.MatchString(w.SHA256)
}

// Field order and JSON escaping are part of the API fingerprint contract.
// These are the fields the API fingerprints after applying request defaults.
type SubmissionBody struct {
	DefinitionID      string          `json:"definition_id"`
	DefinitionVersion int64           `json:"definition_version"`
	InitialNodeID     string          `json:"initial_node_id"`
	InitialInput      json.RawMessage `json:"initial_input"`
	Payload           json.RawMessage `json:"payload"`
}

func (b SubmissionBody) Digest() string {
	normalize := func(raw json.RawMessage) json.RawMessage {
		value, _ := parse(raw)
		out, _ := json.Marshal(value)
		return out
	}
	b.InitialInput, b.Payload = normalize(b.InitialInput), normalize(b.Payload)
	raw, _ := json.Marshal(b)
	return Hash(raw)
}

type Arrivals struct {
	Schema     string    `json:"schema"`
	Episode    string    `json:"episode"`
	ResetID    string    `json:"reset_id"`
	PlanSHA256 string    `json:"plan_sha256"`
	RemovalUTC *string   `json:"removal_utc"`
	SealedUTC  string    `json:"sealed_utc"`
	Rows       []Arrival `json:"rows"`
}

type Arrival struct {
	Window       string  `json:"window"`
	Sequence     int     `json:"sequence"`
	Key          string  `json:"submission_key"`
	Disposition  string  `json:"disposition"`
	UnsentReason *string `json:"unsent_reason"`
	Attempts     []Send  `json:"attempts"`
}

type Send struct {
	Attempt     int         `json:"attempt"`
	SentUTC     string      `json:"sent_utc"`
	ObservedUTC string      `json:"observed_utc"`
	BodySHA256  string      `json:"body_sha256"`
	Request     WireDigest  `json:"request"`
	Response    *WireDigest `json:"response"`
	Outcome     string      `json:"outcome"`
	HTTPStatus  *int        `json:"http_status"`
	WorkflowID  *string     `json:"workflow_id"`
	PayloadHash *string     `json:"payload_hash"`
}

func nonempty(s string) bool { return s != "" && publicIdentity(s) }

func workloadJSON(raw json.RawMessage) bool {
	value, err := parse(raw)
	return err == nil && (validValue("workload", value) ||
		object(value, "run_id:text sequence:int", true) || object(value, "work_units:int", true))
}

func (p Plan) Valid(m Manifest) bool {
	if p.Schema != PlanVersion || p.Episode != m.Episode || p.Namespace != m.Namespace ||
		p.ResetID != m.ResetID || !validTime(p.SealedUTC) || len(p.Windows) != 2 {
		return false
	}
	switch p.FaultMode {
	case "control":
		if p.HoldDeadlineUTC != nil {
			return false
		}
	case "injected":
		if p.HoldDeadlineUTC == nil || !validTime(*p.HoldDeadlineUTC) || *p.HoldDeadlineUTC < p.SealedUTC {
			return false
		}
	default:
		return false
	}
	previous := p.SealedUTC
	idle := m.Sink == nil && p.FaultMode == "control" &&
		len(p.Windows[0].Submissions) == 0 && len(p.Windows[1].Submissions) == 0
	for i, window := range p.Windows {
		if window.Name != []string{"warmup", "measured"}[i] || !nonempty(window.LoadRunID) ||
			window.Submissions == nil || (!idle && len(window.Submissions) == 0) ||
			(i == 1 && window.LoadRunID == p.Windows[0].LoadRunID) {
			return false
		}
		for seq, row := range window.Submissions {
			if row.Sequence != seq || !validTime(row.ScheduledUTC) || row.ScheduledUTC < previous ||
				!nonempty(row.Key) || !digest.MatchString(row.BodySHA256) || !row.Request.valid() || !nonempty(row.Target) ||
				!nonempty(row.Body.DefinitionID) || row.Body.DefinitionVersion < 1 || !nonempty(row.Body.InitialNodeID) ||
				!workloadJSON(row.Body.InitialInput) || !workloadJSON(row.Body.Payload) || !workloadJSON(row.Expected) {
				return false
			}
			previous = row.ScheduledUTC
		}
	}
	return true
}

func (a Arrivals) Valid(m Manifest) bool {
	if a.Schema != ArrivalsVersion || a.Episode != m.Episode || a.ResetID != m.ResetID ||
		a.PlanSHA256 != m.PlanSHA256 || !validTime(a.SealedUTC) ||
		(a.RemovalUTC != nil && (!validTime(*a.RemovalUTC) || *a.RemovalUTC > a.SealedUTC)) || a.Rows == nil {
		return false
	}
	seen := map[scheduledID]bool{}
	for _, row := range a.Rows {
		id := scheduledID{row.Window, row.Sequence}
		if !slices.Contains([]string{"warmup", "measured"}, row.Window) || row.Sequence < 0 ||
			!nonempty(row.Key) || seen[id] || row.Attempts == nil {
			return false
		}
		seen[id] = true
		if row.Disposition == "unsent" {
			if row.UnsentReason == nil || !nonempty(*row.UnsentReason) || len(row.Attempts) != 0 {
				return false
			}
			continue
		}
		if row.Disposition != "sent" || row.UnsentReason != nil || len(row.Attempts) == 0 {
			return false
		}
		previous := ""
		for i, attempt := range row.Attempts {
			if attempt.Attempt != i+1 || !validTime(attempt.SentUTC) || !validTime(attempt.ObservedUTC) ||
				attempt.SentUTC < previous || attempt.SentUTC > attempt.ObservedUTC || attempt.ObservedUTC > a.SealedUTC ||
				!digest.MatchString(attempt.BodySHA256) || !attempt.Request.valid() || !attempt.validResponse() {
				return false
			}
			previous = attempt.ObservedUTC
		}
	}
	return true
}

func (s Send) validResponse() bool {
	if s.Response != nil && !s.Response.valid() {
		return false
	}
	if s.Response != nil && s.HTTPStatus == nil {
		return false
	}
	switch s.Outcome {
	case "accepted":
		return s.Response != nil && s.HTTPStatus != nil && (*s.HTTPStatus == 200 || *s.HTTPStatus == 201) &&
			s.WorkflowID != nil && nonempty(*s.WorkflowID) && s.PayloadHash != nil &&
			len(*s.PayloadHash) == len("sub-v1:")+64 && (*s.PayloadHash)[:7] == "sub-v1:" && digest.MatchString((*s.PayloadHash)[7:])
	case "rejected":
		return s.Response != nil && s.HTTPStatus != nil && *s.HTTPStatus >= 400 && *s.HTTPStatus < 500 && s.WorkflowID == nil && s.PayloadHash == nil
	case "uncertain":
		return (s.HTTPStatus == nil || (*s.HTTPStatus >= 100 && *s.HTTPStatus <= 599)) && s.WorkflowID == nil && s.PayloadHash == nil
	default:
		return false
	}
}

func (p Plan) LastScheduledUTC() string {
	for _, window := range slices.Backward(p.Windows) {
		if len(window.Submissions) != 0 {
			return window.Submissions[len(window.Submissions)-1].ScheduledUTC
		}
	}
	return p.SealedUTC
}

// Deadline is historical evidence. Missing removal still has a bounded
// acquisition window, but can never satisfy the barrier.
func (p Plan) Deadline(a Arrivals) time.Time {
	if p.FaultMode == "injected" && a.RemovalUTC == nil {
		t, _ := time.Parse(TimeLayout, *p.HoldDeadlineUTC)
		return t
	}
	later := p.LastScheduledUTC()
	if a.RemovalUTC != nil && *a.RemovalUTC > later {
		later = *a.RemovalUTC
	}
	t, _ := time.Parse(TimeLayout, later)
	return t.Add(reconciliationSeconds * time.Second)
}

type scheduledID struct {
	window   string
	sequence int
}

type rowViolation func(predicate, code, relation string, row Row)

func (r *Result) checkSubmissions(m Manifest, rows map[string][]Row, plan Plan, arrivals Arrivals, planOK, arrivalsOK bool, witness func(string, Row) Witness, violate rowViolation) {
	const predicate = "SubmissionIdentity"
	label := &r.Labels[0]
	workflows, haveWorkflows := rows[executions]
	definitions, haveDefinitions := rows["engine.workflow_definitions"]
	intents, haveIntents := rows["engine.approval_action_intents"]
	complete := planOK && arrivalsOK && haveWorkflows && haveDefinitions && (m.Sink == nil || haveIntents)
	byKey := map[string]Row{}
	failed := false
	for _, wf := range workflows {
		key := wf["submission_key"].(string)
		if m.Namespace == wf["namespace"] {
			if byKey[key] != nil {
				violate(predicate, "SUBMISSION_DUPLICATE_KEY", executions, wf)
				failed = true
			}
			byKey[key] = wf
		}
	}
	flag := func(code, source string) {
		failed = true
		r.record(Violation, predicate, code, source, nil)
	}
	defer func() {
		if failed {
			label.Outcome, label.Reason = "covered/fail", "contract_violation"
		} else if complete {
			if label.PopulationCount > 0 {
				label.Outcome, label.Reason = "covered/pass", "predicate_satisfied"
			} else {
				label.Reason = "no_positive_population"
			}
		}
	}()
	if !complete {
		for _, source := range []struct {
			ok   bool
			path string
		}{
			{planOK, PlanPath}, {arrivalsOK, ArrivalsPath}, {haveWorkflows, RelationPath(executions)},
			{haveDefinitions, RelationPath("engine.workflow_definitions")},
			{m.Sink == nil || haveIntents, RelationPath("engine.approval_action_intents")},
		} {
			if !source.ok {
				label.Reason = "required_proof_missing:" + source.path
				break
			}
		}
	}
	if !planOK {
		return
	}
	observed := map[scheduledID]Arrival{}
	for _, row := range arrivals.Rows {
		observed[scheduledID{row.Window, row.Sequence}] = row
	}
	plannedIDs := map[scheduledID]bool{}
	plannedKeys := map[string]bool{}
	responseKeys := map[string]string{}
	type definition struct {
		id      string
		version int64
	}
	knownDefinitions := map[definition]bool{}
	for _, row := range definitions {
		knownDefinitions[definition{row["definition_id"].(string), number(row["version"])}] = true
	}
	intentsByWorkflow := map[string][]Row{}
	for _, row := range intents {
		id := row["workflow_id"].(string)
		intentsByWorkflow[id] = append(intentsByWorkflow[id], row)
	}
	label.PopulationUnit = "scheduled_submission"
	for _, window := range plan.Windows {
		for _, entry := range window.Submissions {
			label.PopulationCount++
			key := fmt.Sprintf("dur053-%s-%d-%06d", window.LoadRunID, m.Seed, entry.Sequence)
			bodyHash := entry.Body.Digest()
			if entry.Key != key || plannedKeys[entry.Key] {
				flag("SUBMISSION_PLAN_KEY", PlanPath)
			}
			if entry.BodySHA256 != bodyHash {
				flag("SUBMISSION_PLAN_BODY_HASH", PlanPath)
			}
			if m.Sink == nil && entry.Target != entry.Body.InitialNodeID {
				flag("SUBMISSION_PLAN_TARGET", PlanPath)
			}
			plannedKeys[entry.Key] = true
			id := scheduledID{window.Name, entry.Sequence}
			plannedIDs[id] = true
			arrival, present := observed[id]
			if arrivalsOK && !present {
				complete = false
				label.Reason = "required_proof_missing:SubmissionIdentity"
				raw, _ := Canonical(entry)
				ref := Witness{Path: PlanPath, RowKey: []any{window.Name, entry.Sequence}, SHA256: Hash(raw)}
				r.record(MissingProof, predicate, label.Reason, ArrivalsPath, &ref)
			}
			if present && arrival.Key != key {
				flag("SUBMISSION_ARRIVAL_KEY", ArrivalsPath)
			}
			wf := byKey[entry.Key]
			accepted, uncertain := false, false
			for _, attempt := range arrival.Attempts {
				if attempt.BodySHA256 != bodyHash {
					flag("SUBMISSION_SEND_BODY_HASH", ArrivalsPath)
				}
				if attempt.Request != entry.Request {
					flag("SUBMISSION_SEND_WIRE_HASH", ArrivalsPath)
				}
				if attempt.SentUTC < entry.ScheduledUTC {
					flag("SUBMISSION_SEND_TIME", ArrivalsPath)
				}
				if attempt.Outcome == "uncertain" {
					uncertain = true
				}
				if attempt.Outcome != "accepted" {
					continue
				}
				accepted = true
				if prior, exists := responseKeys[*attempt.WorkflowID]; exists && prior != entry.Key {
					flag("SUBMISSION_WORKFLOW_ALIAS", ArrivalsPath)
				}
				responseKeys[*attempt.WorkflowID] = entry.Key
				if *attempt.WorkflowID != key || *attempt.PayloadHash != "sub-v1:"+bodyHash {
					flag("SUBMISSION_RESPONSE_IDENTITY", ArrivalsPath)
				}
			}
			if wf == nil {
				if haveWorkflows && accepted {
					flag("SUBMISSION_ACCEPT_MISSING", ArrivalsPath)
				}
				continue
			}
			label.Witnesses = append(label.Witnesses, witness(executions, wf))
			check := func(code string) {
				violate(predicate, code, executions, wf)
				failed = true
			}
			if present && !accepted && !uncertain {
				check("SUBMISSION_DURABLE_WITHOUT_SEND")
			}
			if wf["workflow_id"] != key {
				check("SUBMISSION_WORKFLOW_IDENTITY")
			}
			if wf["submission_payload_hash"] != "sub-v1:"+bodyHash {
				check("SUBMISSION_DURABLE_BODY_HASH")
			}
			d := definition{entry.Body.DefinitionID, entry.Body.DefinitionVersion}
			if wf["definition_id"] != d.id || number(wf["definition_version"]) != d.version || (haveDefinitions && !knownDefinitions[d]) {
				check("SUBMISSION_DEFINITION")
			}
			for _, intent := range intentsByWorkflow[wf["workflow_id"].(string)] {
				if intent["target"] != entry.Target {
					check("SUBMISSION_TARGET")
				}
			}
		}
	}
	for _, row := range arrivals.Rows {
		if !plannedIDs[scheduledID{row.Window, row.Sequence}] {
			flag("SUBMISSION_ARRIVAL_UNPLANNED", ArrivalsPath)
		}
	}
	for _, wf := range workflows {
		if !plannedKeys[wf["submission_key"].(string)] || wf["namespace"] != m.Namespace {
			violate(predicate, "SUBMISSION_DURABLE_UNPLANNED", executions, wf)
			failed = true
		}
	}
}
