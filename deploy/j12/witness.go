package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
)

// The observer reads committed state with a SELECT-only login. The projection
// contains identifiers and state, with only presence bits for bearer secrets.
// Outbox relay status is deliberately excluded from equality during probes.
const witnessSQL = `SELECT jsonb_build_object(
 'workflows', COALESCE((SELECT jsonb_agg(to_jsonb(x) ORDER BY workflow_id) FROM
   (SELECT workflow_id,namespace,state,revision FROM engine.workflow_executions) x),'[]'),
 'requesters', COALESCE((SELECT jsonb_agg(to_jsonb(x) ORDER BY workflow_id) FROM
   (SELECT workflow_id,issuer,subject FROM engine.j12_workflow_requesters) x),'[]'),
 'history', COALESCE((SELECT jsonb_agg(to_jsonb(x) ORDER BY workflow_id,revision) FROM
   (SELECT workflow_id,revision,actor_id,reason FROM engine.transition_history) x),'[]'),
 'approvals', COALESCE((SELECT jsonb_agg(to_jsonb(x) ORDER BY intent_id) FROM
   (SELECT workflow_id,intent_id,decision,approver_id,dispatch_status,
     grant_token IS NOT NULL AS has_grant,grant_scope_hash IS NOT NULL AS has_scope,
     grant_expires_at IS NOT NULL AS has_expiry,decided_at IS NOT NULL AS has_decision
    FROM engine.approval_action_intents) x),'[]'),
 'cancellations', COALESCE((SELECT jsonb_agg(to_jsonb(x) ORDER BY request_id) FROM
   (SELECT workflow_id,request_id,status,observed_revision FROM engine.cancellation_requests) x),'[]'),
 'attempts', COALESCE((SELECT jsonb_agg(to_jsonb(x) ORDER BY workflow_id,node_id,iteration,attempt_number) FROM
   (SELECT workflow_id,node_id,iteration,attempt_number,state,worker_id,heartbeat_deadline,
     claim_token IS NOT NULL AS has_claim,result IS NOT NULL AS has_result FROM engine.activity_attempts) x),'[]'),
 'inbox', COALESCE((SELECT jsonb_agg(to_jsonb(x) ORDER BY consumer_id,event_id) FROM
   (SELECT consumer_id,event_id,disposition,topic,kafka_partition,kafka_offset FROM engine.event_inbox) x),'[]'),
 'offsets', COALESCE((SELECT jsonb_agg(to_jsonb(x) ORDER BY consumer_id,topic,kafka_partition) FROM
   (SELECT consumer_id,topic,kafka_partition,next_offset FROM engine.consumer_offsets) x),'[]'),
 'outbox_events', COALESCE((SELECT jsonb_agg(to_jsonb(x) ORDER BY event_id) FROM
   (SELECT event_id,workflow_id,event_type FROM engine.outbox) x),'[]'),
 'quarantined', (SELECT count(*) FROM engine.transport_quarantine),
 'result_evidence', (SELECT count(*) FROM engine.attempt_result_evidence),
 'effects', jsonb_build_object(
   'engine_records',(SELECT count(*) FROM engine.effect_records),
   'engine_calls',(SELECT count(*) FROM engine.effect_call_attempts),
   'engine_state',(SELECT count(*) FROM engine.sandbox_effect_state),
   'engine_fences',(SELECT count(*) FROM engine.effect_resource_fences),
   'engine_resolution',(SELECT count(*) FROM engine.effect_resolution_audit),
   'service_records',(SELECT count(*) FROM effects.effect_records),
   'service_calls',(SELECT count(*) FROM effects.effect_call_attempts),
   'service_state',(SELECT count(*) FROM effects.sandbox_effect_state),
   'service_fences',(SELECT count(*) FROM effects.effect_resource_fences),
   'service_resolution',(SELECT count(*) FROM effects.effect_resolution_audit)))`

type snapshot struct {
	Workflows []struct {
		Workflow         string `json:"workflow_id"`
		Namespace, State string
		Revision         int
	}
	Requesters []struct {
		Workflow        string `json:"workflow_id"`
		Issuer, Subject string
	}
	History []struct {
		Workflow string `json:"workflow_id"`
		Actor    string `json:"actor_id"`
		Revision int
		Reason   string
	}
	Approvals     []approvalRow
	Cancellations []struct {
		Workflow string `json:"workflow_id"`
		ID       string `json:"request_id"`
		Status   string
	}
	Attempts []struct {
		Workflow  string `json:"workflow_id"`
		Worker    string `json:"worker_id"`
		State     string
		Heartbeat string `json:"heartbeat_deadline"`
		Claim     bool   `json:"has_claim"`
		Result    bool   `json:"has_result"`
	}
	Inbox []struct {
		Event       string `json:"event_id"`
		Disposition string
		Partition   int   `json:"kafka_partition"`
		Offset      int64 `json:"kafka_offset"`
	}
	Effects map[string]int
}

type approvalRow struct {
	Workflow string `json:"workflow_id"`
	ID       string `json:"intent_id"`
	Decision string
	Approver string `json:"approver_id"`
	Dispatch string `json:"dispatch_status"`
	Grant    bool   `json:"has_grant"`
	Scope    bool   `json:"has_scope"`
	Expiry   bool   `json:"has_expiry"`
	Decided  bool   `json:"has_decision"`
}

func readSnapshot(ctx context.Context, reader *pgx.Conn) (snapshot, json.RawMessage, error) {
	var raw json.RawMessage
	var value snapshot
	if err := reader.QueryRow(ctx, witnessSQL).Scan(&raw); err != nil {
		return value, nil, errors.New("read J12 durable witness")
	}
	if err := json.Unmarshal(raw, &value); err != nil || len(value.Effects) != 10 {
		return value, nil, errors.New("decode J12 durable witness")
	}
	return value, raw, nil
}

func (s snapshot) noEffects() bool {
	if len(s.Effects) != 10 {
		return false
	}
	for _, count := range s.Effects {
		if count != 0 {
			return false
		}
	}
	return true
}

func (s snapshot) requester(workflow, namespace, subject string) bool {
	var workflowOK, requesterOK, historyOK bool
	for _, w := range s.Workflows {
		if w.Workflow == workflow && w.Namespace == namespace {
			workflowOK = true
		}
	}
	for _, r := range s.Requesters {
		if r.Workflow == workflow && r.Issuer == issuer && r.Subject == subject {
			requesterOK = true
		}
	}
	for _, h := range s.History {
		if h.Workflow == workflow && h.Revision == 1 && h.Actor == subject && h.Reason == "WORKFLOW_CREATED" {
			historyOK = true
		}
	}
	return workflowOK && requesterOK && historyOK
}

func (s snapshot) intent(workflow, id string) approvalRow {
	for _, a := range s.Approvals {
		if a.Workflow == workflow && (id == "" || a.ID == id) {
			return a
		}
	}
	return approvalRow{}
}

func pending(s snapshot, workflow, id string) bool {
	row := s.intent(workflow, id)
	return row.ID != "" && row.Decision == "PENDING" && row.Approver == "" && row.Dispatch == "NOT_GRANTED" &&
		!row.Decided && !row.Grant && !row.Scope && !row.Expiry && s.noEffects()
}

func (s snapshot) approved(workflow, id, subject string, granted bool) bool {
	row := s.intent(workflow, id)
	wantDispatch := "NOT_GRANTED"
	if granted {
		wantDispatch = "GRANTED"
	}
	return row.ID == id && row.Decision == "APPROVED" && row.Approver == subject && row.Decided && row.Dispatch == wantDispatch &&
		row.Grant == granted && row.Scope == granted && row.Expiry == granted && s.noEffects()
}

func (s snapshot) cancelled(workflow, id, status string) bool {
	for _, row := range s.Cancellations {
		if row.Workflow == workflow && row.ID == id && row.Status == status {
			return true
		}
	}
	return false
}

type campaign struct {
	ctx    context.Context
	host   *host
	report *report
}

func (q campaign) mark(row result) error {
	i := slices.IndexFunc(q.report.Results, func(want result) bool { return want.ID == row.ID })
	if i < 0 || q.report.Results[i].Verdict != "unperformed" {
		return errors.New("unexpected or repeated J12 control")
	}
	q.report.Results[i] = row
	if row.Verdict == "fail" {
		return contractMismatch{}
	}
	return nil
}

func (q campaign) snapshot() (snapshot, string, error) {
	s, raw, err := readSnapshot(q.ctx, q.host.observer)
	if err != nil {
		return s, "", err
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, raw); err != nil {
		return s, "", err
	}
	key := hexDigest(compact.Bytes())
	q.report.SQL[key] = compact.Bytes()
	return s, key, nil
}

func (q campaign) offlineControls() error {
	for _, test := range []struct{ id, pkg, name string }{
		{"offline/fixed-clock", "./internal/api", "TestJ12TenantJWTClaims"},
		{"offline/unset-zero", "./cmd/runtime", "TestJ12LegacyEquivalence"},
		{"offline/switch-values", "./cmd/runtime", "TestJ12SwitchExactValues"},
	} {
		ctx, cancel := context.WithTimeout(q.ctx, 3*time.Minute)
		output, err := execute(ctx, q.host.env, nil, "go", "test", "-p", "1", "-count=1", "-json", test.pkg, "-run", "^"+test.name+"$")
		cancel()
		row := result{ID: test.id, Verdict: "invalid"}
		if err == nil && sawGoPass(output, test.name) {
			row.Verdict = "pass"
		}
		if markErr := q.mark(row); markErr != nil {
			return markErr
		}
		if row.Verdict != "pass" {
			return fmt.Errorf("J12 offline control lacks passing evidence: %s", test.id)
		}
	}
	return nil
}

func sawGoPass(output []byte, name string) bool {
	for _, line := range bytes.Split(output, []byte{'\n'}) {
		var event struct{ Action, Test string }
		if json.Unmarshal(line, &event) == nil && event.Action == "pass" && event.Test == name {
			return true
		}
	}
	return false
}

func errorCode(reply response) string {
	var envelope struct{ Error struct{ Code string } }
	_ = json.Unmarshal(reply.Body, &envelope)
	return envelope.Error.Code
}

type observation func(before, after snapshot, reply response) bool

func (q campaign) http(id, caller string, request probeRequest, want int, code string, check observation) (response, error) {
	row := result{ID: id, Verdict: "invalid", Want: want, Target: request.Path}
	before, beforeID, err := q.snapshot()
	row.Before = beforeID
	var reply response
	if err == nil {
		reply, err = q.host.request(q.ctx, caller, request)
	}
	if err == nil {
		var after snapshot
		after, row.After, err = q.snapshot()
		if err == nil {
			row.Status, row.Code = reply.Status, errorCode(reply)
			row.Verdict = "pass"
			readOrDeny := want >= 400 || request.Method == "GET" || request.Method == "HEAD" || id == "approval/replay-after-grant"
			if row.Status != want || row.Code != code || !after.noEffects() ||
				((readOrDeny || check == nil) && row.After != row.Before) || (check != nil && !check(before, after, reply)) {
				row.Verdict = "fail"
			}
		}
	}
	if markErr := q.mark(row); markErr != nil {
		return reply, fmt.Errorf("%s: %w", id, markErr)
	}
	if err != nil {
		return reply, fmt.Errorf("%s: %w", id, err)
	}
	return reply, nil
}

func (q campaign) startup(id, role, marker string) error {
	reply, err := q.host.request(q.ctx, "operator", probeRequest{Startup: role, Marker: marker})
	row := result{ID: id, Verdict: "pass"}
	if err != nil {
		row.Verdict = "invalid"
	} else if reply.Exit != 1 || !reply.Marker {
		row.Verdict = "fail"
	}
	if markErr := q.mark(row); markErr != nil {
		return fmt.Errorf("%s: %w", id, markErr)
	}
	return err
}
