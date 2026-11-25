package k6v1

import (
	"encoding/json"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	StalePath    = "fault/stale-challenge.json"
	StaleVersion = "dur053.k6-stale-challenge.v1"
	staleProof   = "DeployedStaleChallenge"
)

func (m Manifest) staleChallenge() bool {
	return (m.Cell == "hook-5" || m.Cell == "medium-3") && m.CaptureEnabled()
}

type StaleObservation struct {
	Schema    string          `json:"schema"`
	Binding   SourceBinding   `json:"binding"`
	Journal   string          `json:"journal"`
	Before    StaleProcess    `json:"before"`
	After     StaleProcess    `json:"after"`
	Peer      StaleProcess    `json:"peer"`
	Hold      StaleSample     `json:"hold"`
	Takeover  StaleSample     `json:"takeover"`
	Rejected  StaleSample     `json:"rejected"`
	Call      *StaleCall      `json:"call"`
	Progress  string          `json:"progress_transaction"`
	Partition *StalePartition `json:"partition,omitempty"`
}

type StaleProcess struct {
	PodUID      string `json:"pod_uid"`
	ContainerID string `json:"container_id"`
	PID         int64  `json:"pid"`
	Start       string `json:"process_start_id"`
	Session     string `json:"session"`
}

type StaleSample struct {
	StartNS  int64          `json:"start_ns"`
	EndNS    int64          `json:"end_ns"`
	DBTime   string         `json:"db_time"`
	Owner    string         `json:"owner"`
	Epoch    int64          `json:"epoch"`
	Updated  string         `json:"updated"`
	Expiry   string         `json:"expiry"`
	Revision int64          `json:"revision"`
	Attempts int64          `json:"attempts"`
	Backends []StaleBackend `json:"backends"`
}

type StaleBackend struct {
	PID         int64  `json:"pid"`
	StartUS     int64  `json:"start_us"`
	Application string `json:"application"`
	State       string `json:"state"`
	Transaction bool   `json:"in_transaction"`
}

type StaleCall struct {
	StaleProcess
	AtNS       int64             `json:"at_ns"`
	Operation  string            `json:"operation"`
	Outcome    string            `json:"outcome"`
	Workflow   string            `json:"workflow"`
	Invocation string            `json:"invocation"`
	Objects    map[string]string `json:"objects"`
}

type staleTarget struct {
	Actor   string `json:"actor"`
	UID     string `json:"uid"`
	Start   string `json:"process_start_id"`
	Session string `json:"session"`
}

type staleACK struct {
	Sequence int64  `json:"sequence"`
	Message  string `json:"sha256"`
	Journal  string `json:"journal_sha256"`
}

type staleJournalRow struct {
	Event    string            `json:"event"`
	Arm      string            `json:"arm"`
	Episode  string            `json:"episode"`
	Boundary string            `json:"boundary"`
	Target   staleTarget       `json:"target"`
	AtNS     int64             `json:"at_ns"`
	Sequence int64             `json:"sequence"`
	UntilMS  int64             `json:"until_ms"`
	Message  string            `json:"message_sha256"`
	Command  string            `json:"command"`
	ACK      staleACK          `json:"ack"`
	Selector map[string]string `json:"selector"`
	Fields   struct {
		Key, Invocation string
		Objects         map[string]string
	} `json:"fields"`
	hash string
}

func staleJournal(raw string) ([]staleJournalRow, bool) {
	if !strings.HasSuffix(raw, "\n") {
		return nil, false
	}
	var rows []staleJournalRow
	var at int64
	for i, line := range strings.Split(strings.TrimSuffix(raw, "\n"), "\n") {
		var row staleJournalRow
		if _, err := parse([]byte(line)); err != nil {
			return nil, false
		}
		if json.Unmarshal([]byte(line), &row) != nil || row.Sequence != int64(i+1) || row.AtNS <= at {
			return nil, false
		}
		row.hash = Hash([]byte(line))
		rows, at = append(rows, row), row.AtNS
	}
	return rows, true
}

func staleACKValid(arm, reach staleJournalRow, rows ...staleJournalRow) bool {
	if arm.Arm == "" || reach.Arm != arm.Arm || reach.Boundary != arm.Boundary || reach.Target != arm.Target ||
		!digest.MatchString(reach.Message) || !identities(reach.Fields.Key, reach.Fields.Invocation) ||
		arm.Selector["key"] != reach.Fields.Key || arm.UntilMS <= 0 || arm.UntilMS > arm.AtNS/1e6+60_000 {
		return false
	}
	ack := staleACK{reach.Sequence, reach.Message, reach.hash}
	for _, row := range rows {
		if row.Arm != arm.Arm || row.ACK != ack || row.AtNS/1e6 >= arm.UntilMS ||
			((row.Event == "action" || row.Event == "command_issued") && row.Command != "release") {
			return false
		}
	}
	return true
}

func (p StaleProcess) matches(target staleTarget, actor string) bool {
	return target.Actor == actor && identities(p.PodUID, p.ContainerID) && uuid.MatchString(p.Session) &&
		p.PID > 0 && strings.HasPrefix(p.Start, strconv.FormatInt(p.PID, 10)+":") &&
		target.UID == p.PodUID && target.Start == p.Start && target.Session == strings.ReplaceAll(p.Session, "-", "")
}

func staleObject(objects map[string]string, key string) int64 {
	n, _ := strconv.ParseInt(objects[key], 10, 64)
	return n
}

func (s StaleSample) backend(p StaleProcess, objects map[string]string, idle bool) bool {
	return slices.ContainsFunc(s.Backends, func(b StaleBackend) bool {
		return b.PID > 0 && b.StartUS > 0 && b.PID == staleObject(objects, "backend_pid") &&
			b.StartUS == staleObject(objects, "backend_start_us") && b.Application == "dur053-e2-"+p.Session &&
			(!idle || b.State == "idle" && !b.Transaction)
	})
}

func (e engineOrder) checkStaleChallenge(m Manifest, plan Plan, arrivals Arrivals, issues []Finding, s *StaleObservation) {
	r := e.result
	if len(issues) > 0 {
		issue := issues[0]
		r.Confirmation = Confirmation{"failed", issue.Code}
		if issue.Kind == MissingProof {
			r.Confirmation.Status = "missing"
		}
	}
	if s == nil {
		return
	}
	code := e.staleChallenge(m, plan, arrivals, *s)
	if code != "" {
		kind, status := Violation, "failed"
		var ref *Witness
		if strings.HasPrefix(code, "required_proof_missing:") {
			kind, status = MissingProof, "missing"
		} else {
			raw, _ := Canonical(s)
			ref = &Witness{StalePath, []any{}, Hash(raw)}
		}
		r.Confirmation = Confirmation{status, code}
		r.record(kind, staleProof, code, StalePath, ref)
		return
	}
	if len(issues) != 0 {
		return
	}
	label := &r.Labels[2]
	if !label.Registered || !e.captureReady || label.PopulationCount == 0 || slices.ContainsFunc(r.Findings, func(f Finding) bool {
		return f.Predicate == "OwnerAtCommit"
	}) {
		return
	}
	label.Outcome, label.Reason = "covered/pass", "predicate_satisfied"
	r.Confirmation = Confirmation{"confirmed", "cell_predicate_satisfied"}
}

func (e engineOrder) staleChallenge(m Manifest, plan Plan, arrivals Arrivals, s StaleObservation) string {
	const missingACK = "required_proof_missing:stale_challenge_ack"
	const wrongOrder = "STALE_CHALLENGE_ORDER"
	const wrongOrigin = "STALE_CHALLENGE_ORIGIN"
	journal, ok := staleJournal(s.Journal)
	if !ok {
		return missingACK
	}
	var rows []staleJournalRow
	for _, row := range journal {
		if row.Event != "register" {
			rows = append(rows, row)
		}
	}
	events := []string{"arm", "reach", "held", "prolonged", "arm", "reach", "held", "action", "command_issued", "release", "rejection_observed", "action", "command_issued", "release"}
	if len(rows) != len(events) {
		return missingACK
	}
	for i, event := range events {
		if rows[i].Event != event {
			return wrongOrder
		}
	}
	old, peer := rows[1], rows[5]
	if rows[0].Episode != m.Episode || rows[4].Episode != m.Episode || rows[0].Arm == rows[4].Arm ||
		old.Boundary != "lease_takeover" || peer.Boundary != "lease_takeover_acquired" ||
		!staleACKValid(rows[0], old, rows[2], rows[3], rows[7], rows[8], rows[9]) ||
		!staleACKValid(rows[4], peer, rows[6], rows[10], rows[11], rows[12], rows[13]) {
		return missingACK
	}
	if s.Call == nil {
		return "required_proof_missing:stale_challenge_call"
	}
	x, y := old.Fields.Objects, peer.Fields.Objects
	call := s.Call
	if !s.Before.matches(old.Target, "predecessor") || s.After != s.Before || !s.Peer.matches(peer.Target, "successor") ||
		s.Peer.PodUID == s.Before.PodUID || s.Peer.ContainerID == s.Before.ContainerID || s.Peer.Session == s.Before.Session ||
		call.StaleProcess != s.Before || call.Workflow != old.Fields.Key || call.Invocation != old.Fields.Invocation ||
		call.Operation != "CreateAttempt" || !maps.Equal(call.Objects, x) || peer.Fields.Key != old.Fields.Key {
		return wrongOrigin
	}
	if !s.Hold.backend(s.Before, x, true) || !s.Rejected.backend(s.Before, x, false) || !s.Takeover.backend(s.Peer, y, true) {
		return "required_proof_missing:stale_challenge_backend"
	}
	partition, oldEpoch, newEpoch := staleObject(x, "partition"), staleObject(x, "epoch"), staleObject(y, "epoch")
	oldCommit := e.staleAcquisition(s.Before, partition, oldEpoch, x["owner"], "")
	peerCommit := e.staleAcquisition(s.Peer, partition, newEpoch, y["owner"], y["transaction"])
	if oldCommit == nil || peerCommit == nil {
		return "required_proof_missing:stale_challenge_acquisition"
	}
	if !staleOrder(m, plan, arrivals, s, rows, oldCommit, peerCommit) {
		return wrongOrder
	}
	if m.Cell == "hook-5" && call.Outcome == "COMMITTED" {
		return "STALE_CHALLENGE_COMMITTED"
	}
	if s.Rejected.Revision != s.Hold.Revision || s.Rejected.Attempts != 0 {
		return "STALE_CHALLENGE_COMMITTED"
	}
	if call.Outcome != "ErrLeaseNotOwned" {
		return "required_proof_missing:stale_challenge_rejection"
	}
	if m.Cell == "medium-3" {
		if code := checkStalePartition(m, s, rows); code != "" {
			return code
		}
	}
	wf := e.workflows[call.Workflow]
	for _, h := range e.rows[history] {
		a := e.byTransition[h["transition_id"]]
		if h["workflow_id"] == call.Workflow && h["reason"] == "ATTEMPT_CREATED" &&
			h["node_id"] == x["node"] && h["iteration"] != nil && number(h["iteration"]) == staleObject(x, "iteration") &&
			number(h["revision"]) == s.Hold.Revision+1 && committed(a) &&
			s.Progress != "" && a["transaction_id"] == s.Progress && evidenceTime(a["finalized_at"]) > *arrivals.RemovalUTC &&
			number(a["event_order"]) > number(peerCommit["event_order"]) &&
			a["epoch"] == peerCommit["epoch"] && a["owner_id"] == peerCommit["owner_id"] &&
			a["session_id"] == s.Peer.Session && a["pod_uid"] == s.Peer.PodUID &&
			wf["state"] == "SUCCEEDED" && number(wf["partition_id"]) == partition {
			return ""
		}
	}
	return "required_proof_missing:stale_challenge_successor_progress"
}

func staleOffer(plan Plan, arrivals Arrivals, workflow, node string, armNS, heldNS int64) bool {
	for _, window := range plan.Windows {
		if window.Name != "measured" || !slices.ContainsFunc(window.Submissions, func(sub Submission) bool {
			return sub.Key == workflow && sub.Body.InitialNodeID == node && sub.Effect == nil
		}) {
			continue
		}
		return slices.ContainsFunc(arrivals.Rows, func(row Arrival) bool {
			return row.Key == workflow && row.Window == window.Name && slices.ContainsFunc(row.Attempts, func(send Send) bool {
				return send.Outcome == "accepted" && send.WorkflowID != nil && *send.WorkflowID == workflow &&
					send.SentUTC >= UTC(time.Unix(0, armNS)) && send.SentUTC < UTC(time.Unix(0, heldNS))
			})
		})
	}
	return false
}

func (e engineOrder) staleAcquisition(p StaleProcess, partition, epoch int64, owner, xid string) Row {
	for _, a := range e.rows[commitAudit] {
		if a["kind"] == "acquire" && committed(a) && a["session_id"] == p.Session && a["pod_uid"] == p.PodUID &&
			a["container_id"] == p.PodUID && // the G audit uses the downward API pod UID
			number(a["partition_id"]) == partition && a["owner_id"] == owner && number(a["epoch"]) == epoch &&
			(xid == "" || a["transaction_id"] == xid) {
			return a
		}
	}
	return nil
}

func staleOrder(m Manifest, plan Plan, arrivals Arrivals, s StaleObservation, rows []staleJournalRow, old, peer Row) bool {
	x, y := rows[1].Fields.Objects, rows[5].Fields.Objects
	boot, held, rejected := s.Hold, s.Takeover, s.Rejected
	for _, sample := range []StaleSample{boot, held, rejected} {
		if sample.StartNS <= 0 || sample.StartNS > sample.EndNS || !validTime(sample.DBTime) ||
			!validTime(sample.Updated) || !validTime(sample.Expiry) {
			return false
		}
	}
	updated, _ := time.Parse(TimeLayout, boot.Updated)
	expiry, _ := time.Parse(TimeLayout, boot.Expiry)
	dbAt, _ := time.Parse(TimeLayout, boot.DBTime)
	return m.staleChallenge() && plan.FaultMode == "injected" && arrivals.RemovalUTC != nil &&
		staleOffer(plan, arrivals, s.Call.Workflow, x["node"], rows[0].AtNS, rows[2].AtNS) &&
		*arrivals.RemovalUTC == UTC(time.Unix(0, rows[13].AtNS)) && plan.HoldDeadlineUTC != nil &&
		UTC(time.UnixMilli(rows[0].UntilMS)) <= *plan.HoldDeadlineUTC && rows[0].UntilMS == rows[4].UntilMS &&
		x["cell"] == m.Cell && y["cell"] == m.Cell && x["partition"] == y["partition"] &&
		boot.Owner != held.Owner && boot.Owner == x["owner"] && held.Owner == y["owner"] &&
		boot.Epoch > 0 && boot.Epoch == staleObject(x, "epoch") && held.Epoch == boot.Epoch+1 && held.Epoch == staleObject(y, "epoch") &&
		y["transaction"] != "" && rows[4].Selector["epoch"] == y["epoch"] &&
		boot.Revision == staleObject(x, "revision") && held.Revision == boot.Revision &&
		boot.Attempts == 0 && held.Attempts == 0 && rejected.Owner == held.Owner && rejected.Epoch == held.Epoch &&
		expiry.Sub(updated) >= 14_950*time.Millisecond && expiry.Sub(updated) <= 15_050*time.Millisecond &&
		boot.Updated <= boot.DBTime && boot.DBTime < boot.Expiry &&
		boot.Expiry <= held.Updated && held.Updated <= evidenceTime(peer["finalized_at"]) && held.DBTime < held.Expiry &&
		number(old["event_order"]) < number(peer["event_order"]) && held.DBTime >= evidenceTime(peer["finalized_at"]) &&
		rows[2].AtNS < boot.StartNS && boot.EndNS < rows[3].AtNS && rows[3].AtNS < boot.StartNS+expiry.Sub(dbAt).Nanoseconds() &&
		rows[6].AtNS < held.StartNS && held.EndNS < rows[7].AtNS && rows[9].AtNS < s.Call.AtNS &&
		s.Call.AtNS < rejected.StartNS && rejected.EndNS < rows[10].AtNS &&
		held.DBTime <= rejected.DBTime && rejected.DBTime < held.Expiry
}

func (s StaleObservation) valid(m Manifest) bool {
	return s.Schema == StaleVersion && s.Binding == Binding(m)
}
