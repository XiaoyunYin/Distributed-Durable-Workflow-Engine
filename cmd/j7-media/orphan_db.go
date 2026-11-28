package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/netip"
	"time"

	"durable-agent-execution-engine/internal/restoreledger"
	"durable-agent-execution-engine/internal/state"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	grantWindow        = 20 * time.Minute
	clockBound         = 100 * time.Millisecond
	orphanObserverRole = "j7_e1_observer"
)

type orphanClock struct {
	StartNS  int64     `json:"drill_start_ns"`
	StartUTC time.Time `json:"drill_start_utc"`
}

func (c orphanClock) wall(ns int64) time.Time {
	return c.StartUTC.Add(time.Duration(ns - c.StartNS))
}

type orphanGrant struct {
	Intent, Subject, Decision, Status, Resource, Key, Canonical, Scope, TokenSHA, ExpectedRevision string
	DecidedAt, Expires, WindowEnd, IntentEnd                                                       time.Time
	UncertaintyUS                                                                                  int64
}

type readerSession struct {
	PID       int       `json:"pid"`
	Client    string    `json:"client_address"`
	StartedAt time.Time `json:"backend_start"`
	TLS       bool      `json:"tls"`
}

type orphanDB struct {
	SystemID   uint64            `json:"system_identifier"`
	Timeline   uint32            `json:"timeline"`
	Address    string            `json:"server_address"`
	Database   string            `json:"database"`
	FlushLSN   string            `json:"flush_lsn"`
	DBTime     time.Time         `json:"database_time"`
	ReadOnly   bool              `json:"read_only"`
	StartedNS  int64             `json:"started_ns"`
	EndedNS    int64             `json:"completed_ns"`
	Workflow   string            `json:"workflow_id"`
	Namespace  string            `json:"namespace"`
	State      string            `json:"workflow_state"`
	Revision   int64             `json:"workflow_revision"`
	Grant      orphanGrant       `json:"grant"`
	Sessions   []readerSession   `json:"reader_sessions"`
	Attempts   []orphanAttempt   `json:"attempts"`
	History    []dispatchHistory `json:"history"`
	NodeState  string            `json:"node_state"`
	Current    *int64            `json:"current_attempt_number"`
	Accepted   json.RawMessage   `json:"accepted_result"`
	ResultSHA  string            `json:"accepted_result_sha256,omitempty"`
	Unresolved int               `json:"unresolved_reconciliation"`
}

type orphanAttempt struct {
	WorkflowID string          `json:"workflow_id"`
	NodeID     string          `json:"node_id"`
	Iteration  int             `json:"iteration"`
	Attempt    int64           `json:"attempt_number"`
	ClaimSHA   string          `json:"claim_token_sha256"`
	Worker     string          `json:"worker_id"`
	Request    string          `json:"worker_request_id"`
	Deadline   time.Time       `json:"heartbeat_deadline"`
	State      string          `json:"state"`
	Class      string          `json:"effect_class"`
	Current    bool            `json:"is_current"`
	Outcome    string          `json:"outcome_disposition"`
	Result     json.RawMessage `json:"result"`
	Proof      json.RawMessage `json:"k6_proof"`
	Fence      int64           `json:"k6_fence"`
	LogicalKey string          `json:"logical_effect_key"`
	ScopeHash  string          `json:"grant_scope_hash"`
}

// Read all relevant rows and the live reader backend in one statement. Raw
// approval and claim tokens never leave the database in this export.
const orphanSQL = `
WITH w AS (
    SELECT workflow_id, namespace, state, revision FROM engine.workflow_executions
    WHERE namespace=$1 AND workflow_id=$2
), n AS (
    SELECT ni.* FROM engine.node_instances ni JOIN w USING (workflow_id)
    WHERE node_id=$3 AND iteration=$4
), a AS (
    SELECT i.* FROM engine.approval_action_intents i JOIN n USING (workflow_id,node_id,iteration)
)
SELECT json_build_object(
  'system_identifier', (SELECT system_identifier FROM pg_control_system()),
  'timeline', (SELECT timeline_id FROM pg_control_checkpoint()),
  'server_address', host(inet_server_addr()), 'database', current_database(), 'flush_lsn', pg_current_wal_flush_lsn(),
  'database_time', clock_timestamp(), 'read_only', current_setting('transaction_read_only') = 'on',
  'workflow_id', w.workflow_id, 'namespace', w.namespace, 'workflow_state', w.state,
  'workflow_revision', w.revision,
  'grant', (SELECT json_build_object(
      'Intent', a.intent_id, 'Subject', a.approver_id, 'Decision', a.decision, 'Status', a.dispatch_status,
      'Resource', a.target, 'Key', a.k6_logical_key,
      'Scope', a.grant_scope_hash, 'TokenSHA', encode(sha256(convert_to(a.grant_token::text,'UTF8')),'hex'),
      'ExpectedRevision', a.expected_resource_revision, 'DecidedAt', a.decided_at,
      'Expires', a.grant_expires_at, 'IntentEnd', a.valid_until,
      'WindowEnd', e.valid_until, 'UncertaintyUS', e.uncertainty_us)
    FROM a JOIN k6_auth.episode e ON e.namespace=w.namespace
    WHERE a.approver_id=e.approver_subject),
  'grant_arguments', (SELECT canonical_arguments FROM a),
  'reader_sessions', (SELECT COALESCE(jsonb_agg(jsonb_build_object(
      'pid', b.pid, 'client_address', host(b.client_addr), 'backend_start', b.backend_start, 'tls', s.ssl)
      ORDER BY b.pid),'[]')
    FROM pg_stat_activity b LEFT JOIN pg_stat_ssl s USING (pid)
    WHERE b.datname=current_database() AND b.usename='k6_sink_grant_reader'
      AND b.application_name='dur053-k6-sink-grant-reader' AND b.backend_type='client backend'),
  'attempts', (SELECT COALESCE(jsonb_agg(
      (to_jsonb(t) - 'claim_token') || jsonb_build_object(
        'claim_token_sha256', encode(sha256(convert_to(t.claim_token::text,'UTF8')),'hex'))
      ORDER BY t.attempt_number),'[]')
    FROM engine.activity_attempts t JOIN n USING (workflow_id,node_id,iteration)),
  'history', (SELECT COALESCE(jsonb_agg(to_jsonb(h) ORDER BY revision),'[]')
    FROM engine.transition_history h WHERE h.workflow_id=w.workflow_id),
  'node_state', (SELECT state FROM n), 'current_attempt_number', (SELECT current_attempt_number FROM n),
  'accepted_result', (SELECT accepted_result FROM n),
  'accepted_result_sha256', (SELECT encode(sha256(convert_to(accepted_result::text,'UTF8')),'hex') FROM n),
  'unresolved_reconciliation', (SELECT count(*) FROM engine.reconciliation_items i
    WHERE i.workflow_id=w.workflow_id AND i.status='OPEN')
) FROM w;`

func readOrphanDB(ctx context.Context, dsnPath, namespace, workflow string, iteration int,
	now restoreledger.Clock) (orphanDB, error) {
	var db orphanDB
	dsn, err := readPrivateDSN(dsnPath)
	if err != nil {
		return db, err
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil || cfg.ConnConfig.User != orphanObserverRole || cfg.ConnConfig.TLSConfig == nil ||
		cfg.ConnConfig.TLSConfig.InsecureSkipVerify || cfg.ConnConfig.TLSConfig.ServerName == "" ||
		len(cfg.ConnConfig.Fallbacks) != 0 {
		return db, errors.New("E1 observation requires the TLS-verified j7_e1_observer endpoint")
	}
	store, err := state.NewCheckerFromURL(ctx, dsn)
	if err != nil {
		return db, errors.New("open independent E1 observer")
	}
	defer store.Close()
	started, err := now()
	if err != nil {
		return db, err
	}
	var raw []byte
	if err := store.Pool().QueryRow(ctx, orphanSQL, namespace, workflow, state.K6Activity, iteration).Scan(&raw); err != nil {
		return db, errors.New("query E1 approval, attempts and reader session")
	}
	ended, err := now()
	if err != nil {
		return db, err
	}
	var observed struct {
		orphanDB
		Arguments json.RawMessage `json:"grant_arguments"`
	}
	if err := json.Unmarshal(raw, &observed); err != nil {
		return db, errors.New("decode independent E1 observation")
	}
	db = observed.orphanDB
	db.Grant.Canonical, err = state.CanonicalPayloadHash(observed.Arguments)
	if err != nil {
		return db, errors.New("hash E1 authorization arguments")
	}
	db.StartedNS, db.EndedNS = started, ended
	return db, nil
}

func (d orphanDB) checkClock(clock orphanClock) error {
	lsn, err := parseLSN(d.FlushLSN)
	if err != nil || lsn == 0 || !d.ReadOnly || d.SystemID == 0 || d.Timeline == 0 || d.DBTime.IsZero() ||
		d.StartedNS < clock.StartNS || d.EndedNS < d.StartedNS || d.EndedNS-d.StartedNS > int64(clockBound) ||
		d.DBTime.Before(clock.wall(d.StartedNS).Add(-clockBound)) ||
		d.DBTime.After(clock.wall(d.EndedNS).Add(clockBound)) {
		return errors.New("E1 database identity or 100 ms clock bracket is unverified")
	}
	return nil
}

func (d orphanDB) checkGrant(clock orphanClock, hold effectAtCut, namespace, status string) error {
	if err := d.checkClock(clock); err != nil {
		return err
	}
	g := d.Grant
	if d.Workflow != hold.Attempt.WorkflowID || d.Namespace != namespace ||
		g.Intent != hold.Intent || g.Subject != "system:serviceaccount:"+namespace+":k6-approver" ||
		g.Decision != "APPROVED" || g.Status != status || g.Resource != hold.Resource ||
		g.Key != hold.LogicalKey || g.Canonical != hold.CanonicalHash || g.Scope != hold.ScopeHash ||
		g.TokenSHA != hold.GrantSHA || !restoreledger.IsSHA256(g.TokenSHA) || g.ExpectedRevision != "0" ||
		g.UncertaintyUS <= 0 || g.UncertaintyUS > clockBound.Microseconds() ||
		!g.Expires.Equal(clock.StartUTC.Add(grantWindow)) || !g.IntentEnd.Equal(g.Expires) ||
		!g.WindowEnd.Equal(g.Expires) || g.DecidedAt.Before(clock.StartUTC) || g.DecidedAt.After(d.DBTime) ||
		!d.DBTime.Before(g.Expires.Add(-time.Duration(g.UncertaintyUS)*time.Microsecond)) {
		return errors.New("E1 surviving approval differs from the sealed grant or 20-minute window")
	}
	return nil
}

func (d orphanDB) checkReader(client string, after time.Time) error {
	addr, err := netip.ParseAddr(d.Address)
	if err != nil || addr.IsUnspecified() || addr.IsLoopback() || len(d.Sessions) < 1 || len(d.Sessions) > 2 {
		return errors.New("E1 reader is absent from the private engine endpoint")
	}
	for _, s := range d.Sessions {
		if s.PID <= 0 || s.Client != client || !s.TLS || s.StartedAt.Before(after) || s.StartedAt.After(d.DBTime) {
			return errors.New("E1 grant-reader backend is outside the witnessed TLS binding")
		}
	}
	return nil
}

func (d orphanDB) checkHeld(hold effectAtCut, source bool) error {
	if d.State != "WAITING_ACTIVITY" || d.NodeState != "WAITING_ACTIVITY" || d.Current == nil ||
		*d.Current != 1 || len(d.Attempts) != 1 || present(d.Accepted) || d.ResultSHA != "" || d.Unresolved != 0 {
		return errors.New("E1 no longer matches the held first attempt")
	}
	a := d.Attempts[0]
	if !matchOrphanAttempt(a, hold) || a.Attempt != 1 || !a.Current || a.State != "CLAIMED" ||
		a.Outcome != "NONE" ||
		a.Fence != hold.Fence || a.ClaimSHA != hold.Attempt.ClaimSHA || a.Deadline.Before(hold.Attempt.Deadline) ||
		(source && !a.Deadline.After(d.DBTime)) || a.Worker == "" || a.Request == "" ||
		present(a.Result) || present(a.Proof) {
		return errors.New("E1 first claim, fence or source deadline is unproved")
	}
	return nil
}

func matchOrphanAttempt(a orphanAttempt, hold effectAtCut) bool {
	return a.WorkflowID == hold.Attempt.WorkflowID && a.NodeID == hold.Attempt.NodeID &&
		a.Iteration == hold.Attempt.Iteration && a.Class == "COOPERATING_EFFECT" &&
		a.LogicalKey == hold.LogicalKey && a.ScopeHash == hold.ScopeHash
}
