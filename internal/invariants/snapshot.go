package invariants

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"durable-agent-execution-engine/internal/state"
	"github.com/jackc/pgx/v5"
)

// Load uses one read-only repeatable-read transaction. Every query is unbounded
// by row count: PostgreSQL/context errors abort the entire export, never a prefix.
// Raw table counts/hashes and the checker projections share the same snapshot.
func Load(ctx context.Context, store *state.Store, ids []string) (Trace, error) {
	tx, err := store.Pool().BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return Trace{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	trace, err := loadSnapshot(ctx, tx, ids)
	if err != nil {
		return Trace{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Trace{}, err
	}
	return trace, nil
}

func collect[T any](ctx context.Context, tx pgx.Tx, sql string, args ...any) ([]T, error) {
	rows, err := tx.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[T])
}

func loadSnapshot(ctx context.Context, tx pgx.Tx, ids []string) (Trace, error) {
	var t Trace
	var err error
	if len(ids) == 0 {
		return t, fmt.Errorf("snapshot requires workflow identities")
	}
	t.Workflows, err = collect[WorkflowRecord](ctx, tx, `SELECT workflow_id,state,revision FROM engine.workflow_executions WHERE workflow_id=ANY($1) ORDER BY workflow_id`, ids)
	if err != nil {
		return t, err
	}
	unique := map[string]bool{}
	for _, id := range ids {
		if id == "" || unique[id] {
			return t, fmt.Errorf("duplicate or empty requested workflow")
		}
		unique[id] = true
	}
	if len(t.Workflows) != len(ids) {
		return t, fmt.Errorf("accepted workflow missing from snapshot")
	}
	queries := []struct {
		target any
		sql    string
	}{
		{&t.History, `SELECT workflow_id,revision,actor_kind,scheduler_epoch,COALESCE(old_state,''),new_state,reason FROM engine.transition_history WHERE workflow_id=ANY($1) ORDER BY workflow_id,revision`},
		{&t.Results, `SELECT workflow_id,node_id,iteration,true FROM engine.node_instances WHERE workflow_id=ANY($1) AND accepted_result IS NOT NULL ORDER BY workflow_id,node_id,iteration`},
		{&t.Submissions, `SELECT namespace,submission_key,submission_payload_hash,workflow_id FROM engine.workflow_executions WHERE workflow_id=ANY($1) ORDER BY workflow_id`},
		{&t.Attempts, `SELECT workflow_id,node_id,iteration,attempt_number,state,COALESCE(claim_token::text,''),COALESCE(worker_id,''),COALESCE(worker_request_id,''),outcome_disposition,is_current,result IS NOT NULL FROM engine.activity_attempts WHERE workflow_id=ANY($1) ORDER BY workflow_id,node_id,iteration,attempt_number`},
		{&t.Outbox, `SELECT workflow_id,event_id::text,aggregate_revision,event_type,topic,publish_state,payload IS NOT NULL FROM engine.outbox WHERE workflow_id=ANY($1) ORDER BY event_id`},
		{&t.Inbox, `SELECT i.consumer_id,i.event_id::text,COALESCE(i.topic,''),COALESCE(i.kafka_partition,0),COALESCE(i.kafka_offset,0),i.disposition FROM engine.event_inbox i JOIN engine.outbox o USING(event_id) WHERE o.workflow_id=ANY($1) ORDER BY i.consumer_id,i.event_id`},
		{&t.Wakeups, `SELECT workflow_id,event_id::text,partition_id,state FROM engine.scheduler_wakeups WHERE workflow_id=ANY($1) ORDER BY wakeup_id`},
		{&t.Reconciliation, `SELECT COALESCE(workflow_id,''),kind,reference,status FROM engine.reconciliation_items WHERE workflow_id=ANY($1) OR workflow_id IS NULL ORDER BY item_id`},
		{&t.Checkpoints, `SELECT workflow_id,node_id,iteration,sequence,schema_version,source_attempt_number,payload IS NOT NULL,payload_hash FROM engine.checkpoints WHERE workflow_id=ANY($1) ORDER BY workflow_id,node_id,iteration,sequence`},
		{&t.Effects, `SELECT workflow_id,COALESCE(intent_id::text,''),logical_effect_key,argument_hash,attempt_number,COALESCE(grant_scope_hash,''),COALESCE(resource_id,''),outcome,receipt IS NOT NULL FROM effects.effect_records WHERE workflow_id=ANY($1) ORDER BY workflow_id,logical_effect_key`},
		{&t.EffectCalls, `SELECT workflow_id,logical_effect_key,argument_hash,attempt_number,request_id,outcome FROM effects.effect_call_attempts WHERE workflow_id=ANY($1) ORDER BY call_id`},
		{&t.Timers, `SELECT workflow_id,node_id,iteration,purpose,consumed_at IS NOT NULL FROM engine.timers WHERE workflow_id=ANY($1) ORDER BY timer_id`},
	}
	for _, q := range queries {
		rows, e := tx.Query(ctx, q.sql, ids)
		if e != nil {
			return t, e
		}
		// json object names come from checker-owned fields, not production validators.
		switch dest := q.target.(type) {
		case *[]HistoryRecord:
			*dest, err = pgx.CollectRows(rows, pgx.RowToStructByPos[HistoryRecord])
		case *[]AcceptedResult:
			*dest, err = pgx.CollectRows(rows, pgx.RowToStructByPos[AcceptedResult])
		case *[]SubmissionRecord:
			*dest, err = pgx.CollectRows(rows, pgx.RowToStructByPos[SubmissionRecord])
		case *[]AttemptRecord:
			*dest, err = pgx.CollectRows(rows, pgx.RowToStructByPos[AttemptRecord])
		case *[]OutboxRecord:
			*dest, err = pgx.CollectRows(rows, pgx.RowToStructByPos[OutboxRecord])
		case *[]InboxRecord:
			*dest, err = pgx.CollectRows(rows, pgx.RowToStructByPos[InboxRecord])
		case *[]WakeupRecord:
			*dest, err = pgx.CollectRows(rows, pgx.RowToStructByPos[WakeupRecord])
		case *[]ReconciliationRecord:
			*dest, err = pgx.CollectRows(rows, pgx.RowToStructByPos[ReconciliationRecord])
		case *[]CheckpointRecord:
			*dest, err = pgx.CollectRows(rows, pgx.RowToStructByPos[CheckpointRecord])
		case *[]EffectRecordSnapshot:
			*dest, err = pgx.CollectRows(rows, pgx.RowToStructByPos[EffectRecordSnapshot])
		case *[]EffectCallRecord:
			*dest, err = pgx.CollectRows(rows, pgx.RowToStructByPos[EffectCallRecord])
		case *[]TimerRecord:
			*dest, err = pgx.CollectRows(rows, pgx.RowToStructByPos[TimerRecord])
		}
		if err != nil {
			return t, err
		}
	}
	t.ConsumerOffsets, err = collect[ConsumerOffsetRecord](ctx, tx, `SELECT consumer_id,topic,kafka_partition,next_offset FROM engine.consumer_offsets ORDER BY consumer_id,topic,kafka_partition`)
	if err != nil {
		return t, err
	}
	// Snapshot every engine table, preserving full rows (including receipt bytes,
	// definitions, cancellation and lease acquisition ordering) for offline audit.
	t.Snapshot = map[string]SnapshotTable{}
	tableRows, err := tx.Query(ctx, `SELECT schemaname||'.'||tablename FROM pg_tables WHERE schemaname IN ('engine','effects') ORDER BY schemaname,tablename`)
	if err != nil {
		return t, err
	}
	tables, err := pgx.CollectRows(tableRows, pgx.RowTo[string])
	// Scalar collection cannot use RowToStructByPos.
	if err != nil {
		return t, err
	}
	for _, table := range tables {
		parts := strings.SplitN(table, ".", 2)
		name := pgx.Identifier{parts[0], parts[1]}.Sanitize()
		rows, e := tx.Query(ctx, `SELECT row_to_json(r)::jsonb FROM `+name+` r ORDER BY row_to_json(r)::text`)
		if e != nil {
			return t, e
		}
		raw, e := pgx.CollectRows(rows, pgx.RowTo[json.RawMessage])
		if e != nil {
			return t, e
		}
		if raw == nil {
			raw = []json.RawMessage{}
		}
		b, e := json.Marshal(raw)
		if e != nil {
			return t, e
		}
		sum := sha256.Sum256(b)
		t.Snapshot[table] = SnapshotTable{len(raw), hex.EncodeToString(sum[:]), raw}
	}
	// Approvals need independent argument canonicalization before existing checks.
	for _, raw := range t.Snapshot["engine.approval_action_intents"].Rows {
		var a struct {
			IntentID     string          `json:"intent_id"`
			WorkflowID   string          `json:"workflow_id"`
			NodeID       string          `json:"node_id"`
			Iteration    int             `json:"iteration"`
			ProposalHash string          `json:"proposal_hash"`
			Target       string          `json:"target"`
			Arguments    json.RawMessage `json:"canonical_arguments"`
			Expected     string          `json:"expected_resource_revision"`
			Decision     string          `json:"decision"`
			Dispatch     string          `json:"dispatch_status"`
			Scope        string          `json:"grant_scope_hash"`
			Token        string          `json:"grant_token"`
		}
		if err := json.Unmarshal(raw, &a); err != nil {
			return t, err
		}
		if !unique[a.WorkflowID] {
			continue
		}
		hash, e := canonicalJSONHash(a.Arguments)
		if e != nil {
			return t, e
		}
		t.Approvals = append(t.Approvals, ApprovalRecord{a.IntentID, a.WorkflowID, a.NodeID, a.Iteration, a.ProposalHash, a.Target, true, hash, a.Expected, a.Decision, a.Dispatch, a.Scope, a.Token})
	}
	for _, wf := range t.Workflows {
		n := int64(0)
		last := ""
		for _, h := range t.History {
			if h.WorkflowID == wf.WorkflowID {
				n++
				last = h.NewState
			}
		}
		if n != wf.Revision || last != wf.State {
			return t, fmt.Errorf("history snapshot incomplete: %s", wf.WorkflowID)
		}
	}
	return t, nil
}

// Reconciliation is also applied to old offline traces: every accepted workflow
// must have history. In-flight work is explicit, and campaign closeout rejects it.
func reconcileSubmissions(t Trace, violations *[]string) []string {
	latest := map[string]HistoryRecord{}
	for _, h := range t.History {
		if h.Revision > latest[h.WorkflowID].Revision {
			latest[h.WorkflowID] = h
		}
	}
	pending := []string{}
	for _, s := range t.Submissions {
		h, ok := latest[s.WorkflowID]
		if !ok {
			*violations = append(*violations, "accepted workflow missing history: "+s.WorkflowID)
			continue
		}
		if !isTerminal(h.NewState) {
			pending = append(pending, s.WorkflowID)
		}
	}
	for _, wf := range t.Workflows {
		h, ok := latest[wf.WorkflowID]
		count := int64(0)
		for _, entry := range t.History {
			if entry.WorkflowID == wf.WorkflowID {
				count++
			}
		}
		if !ok || h.Revision != wf.Revision || h.NewState != wf.State || count != wf.Revision {
			*violations = append(*violations, "workflow/history snapshot incomplete: "+wf.WorkflowID)
		}
	}
	sort.Strings(pending)
	return pending
}

var snapshotRequiredTables = []string{
	"engine.schema_migrations", "engine.workflow_executions", "engine.workflow_definitions", "engine.node_instances", "engine.activity_attempts", "engine.partition_leases", "engine.transition_history", "engine.outbox", "engine.event_inbox", "engine.cancellation_requests", "engine.lease_acquisitions", "engine.timers", "engine.checkpoints", "engine.approval_action_intents", "engine.consumer_offsets", "engine.scheduler_wakeups", "engine.reconciliation_items", "engine.outbox_publications", "engine.attempt_result_evidence", "engine.effect_records", "engine.effect_call_attempts", "engine.effect_resource_fences", "engine.sandbox_effect_state", "engine.effect_resolution_audit", "engine.transport_quarantine", "engine.retry_policies", "engine.dur050_admission_slots",
	"effects.effect_records", "effects.effect_call_attempts", "effects.effect_resource_fences", "effects.sandbox_effect_state", "effects.effect_resolution_audit",
}

// SnapshotIntegrity is required by campaign/offline closeout, not legacy traces.
func SnapshotIntegrity(t Trace) error {
	for _, name := range snapshotRequiredTables {
		if _, ok := t.Snapshot[name]; !ok {
			return fmt.Errorf("missing snapshot table: %s", name)
		}
	}
	for name, table := range t.Snapshot {
		b, err := json.Marshal(table.Rows)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(b)
		if table.Count != len(table.Rows) || !strings.EqualFold(table.SHA256, hex.EncodeToString(sum[:])) {
			return fmt.Errorf("snapshot count/hash mismatch: %s", name)
		}
	}
	return nil
}
