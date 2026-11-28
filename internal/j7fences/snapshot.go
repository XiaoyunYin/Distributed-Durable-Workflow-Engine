// Package j7fences runs the J7 authority probes through the normal engine
// store and authenticated worker-control API, using the controller's ledger.
package j7fences

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"durable-agent-execution-engine/internal/restoreledger"
	"durable-agent-execution-engine/internal/state"
)

const maxClockBracket = 100 * time.Millisecond

// Scope identifies a held workflow in the sealed drill workload.
type Scope struct {
	Namespace string `json:"namespace"`
	Workflow  string `json:"workflow_id"`
	Partition int16  `json:"partition_id"`
	Node      string `json:"node_id,omitempty"`
	Iteration int    `json:"iteration,omitempty"`
}

type Database struct {
	SystemID uint64 `json:"system_identifier"`
	Timeline uint32 `json:"timeline"`
}

type Workflow struct {
	ID        string              `json:"workflow_id"`
	Namespace string              `json:"namespace"`
	Partition int16               `json:"partition_id"`
	State     state.WorkflowState `json:"state"`
	Revision  int64               `json:"revision"`
}

type Lease struct {
	Owner   string    `json:"owner_id"`
	Epoch   int64     `json:"epoch"`
	Expires time.Time `json:"lease_expires_at"`
}

func (l Lease) ref(partition int16) restoreledger.LeaseRef {
	return restoreledger.LeaseRef{PartitionID: partition, OwnerID: l.Owner, Epoch: l.Epoch}
}

type Attempt struct {
	Node      string             `json:"node_id"`
	Iteration int                `json:"iteration"`
	Number    int64              `json:"attempt_number"`
	State     state.AttemptState `json:"state"`
	Class     state.EffectClass  `json:"effect_class"`
	Current   bool               `json:"is_current"`
	TokenSHA  string             `json:"claim_token_sha256"`
	Deadline  time.Time          `json:"heartbeat_deadline"`
	Worker    string             `json:"worker_id"`
	Request   string             `json:"worker_request_id"`
	Result    json.RawMessage    `json:"result"`
	Outcome   string             `json:"outcome_disposition"`
}

func (a Attempt) ref() restoreledger.ClaimRef {
	return restoreledger.ClaimRef{NodeID: a.Node, Iteration: a.Iteration, Attempt: a.Number, TokenSHA256: a.TokenSHA}
}

type node struct {
	ID        string          `json:"node_id"`
	Iteration int             `json:"iteration"`
	Current   int64           `json:"current_attempt_number"`
	Input     json.RawMessage `json:"input"`
	Result    json.RawMessage `json:"accepted_result"`
}

type history struct {
	Revision  int64               `json:"revision"`
	Actor     string              `json:"actor_id"`
	ActorKind string              `json:"actor_kind"`
	Epoch     int64               `json:"scheduler_epoch"`
	Reason    string              `json:"reason"`
	Node      string              `json:"node_id"`
	Iteration int                 `json:"iteration"`
	Attempt   int64               `json:"attempt_number"`
	At        time.Time           `json:"created_at"`
	NewState  state.WorkflowState `json:"new_state"`
}

type outbox struct {
	ID       string          `json:"event_id"`
	Revision int64           `json:"aggregate_revision"`
	Kind     string          `json:"event_type"`
	Payload  json.RawMessage `json:"payload"`
}

type sample struct {
	Scope       Scope                      `json:"scope"`
	Database    Database                   `json:"database"`
	DBTime      time.Time                  `json:"database_time"`
	StartedNS   int64                      `json:"started_ns"`
	CompletedNS int64                      `json:"completed_ns"`
	Rows        map[string]json.RawMessage `json:"rows"`
}

func (s sample) workflow() (Workflow, error) {
	var w Workflow
	err := json.Unmarshal(s.Rows["workflow"], &w)
	if err != nil || w.ID != s.Scope.Workflow || w.Namespace != s.Scope.Namespace ||
		w.Partition != s.Scope.Partition || w.State == "" || w.Revision <= 0 {
		return w, errors.New("probe workflow is missing or has a different identity")
	}
	return w, nil
}

func (s sample) lease() (Lease, error) {
	var l Lease
	if err := json.Unmarshal(s.Rows["lease"], &l); err != nil || l.Epoch < 0 || (l.Owner == "") != l.Expires.IsZero() {
		return l, errors.New("probe lease row is missing or invalid")
	}
	return l, nil
}

func (s sample) attempts() ([]Attempt, error) {
	var a []Attempt
	err := json.Unmarshal(s.Rows["attempts"], &a)
	return a, err
}

func (s sample) node() (node, error) {
	var rows []node
	if err := json.Unmarshal(s.Rows["nodes"], &rows); err != nil {
		return node{}, err
	}
	for _, row := range rows {
		if row.ID == s.Scope.Node && row.Iteration == s.Scope.Iteration {
			return row, nil
		}
	}
	return node{}, errors.New("probe node is missing")
}

const sampleSQL = `
WITH w AS MATERIALIZED (
    SELECT * FROM engine.workflow_executions
    WHERE workflow_id = $1 AND namespace = $2 AND partition_id = $3
)
SELECT json_build_object(
    'database', json_build_object('system_identifier', (SELECT system_identifier FROM pg_control_system()),
                                  'timeline', (SELECT timeline_id FROM pg_control_checkpoint())),
    'database_time', clock_timestamp(),
    'rows', jsonb_build_object(
      'workflow', (SELECT to_jsonb(w) FROM w),
      'lease', (SELECT to_jsonb(l) FROM engine.partition_leases l WHERE $4::boolean AND l.partition_id = $3),
      'lease_acquisitions', (SELECT COALESCE(jsonb_agg(to_jsonb(l) ORDER BY acquisition_id), '[]')
                            FROM engine.lease_acquisitions l WHERE $4::boolean AND l.partition_id = $3),
      'nodes', (SELECT COALESCE(jsonb_agg(to_jsonb(n) ORDER BY n.node_id,n.iteration), '[]') FROM engine.node_instances n JOIN w USING (workflow_id)),
      'attempts', (SELECT COALESCE(jsonb_agg(
          (to_jsonb(a) - 'claim_token') || jsonb_build_object('claim_token_sha256',
              encode(sha256(convert_to(a.claim_token::text, 'UTF8')), 'hex'))
          ORDER BY a.node_id,a.iteration,a.attempt_number), '[]') FROM engine.activity_attempts a JOIN w USING (workflow_id)),
      'history', (SELECT COALESCE(jsonb_agg(to_jsonb(h) ORDER BY h.revision), '[]') FROM engine.transition_history h JOIN w USING (workflow_id)),
      'outbox', (SELECT COALESCE(jsonb_agg(to_jsonb(o) ORDER BY o.event_id), '[]') FROM engine.outbox o JOIN w USING (workflow_id)),
      'publications', (SELECT COALESCE(jsonb_agg(to_jsonb(p) ORDER BY p.publication_id), '[]')
                       FROM engine.outbox_publications p JOIN engine.outbox o USING (event_id) JOIN w USING (workflow_id)),
      'result_evidence', (SELECT COALESCE(jsonb_agg(
          (to_jsonb(e) - 'claim_token') || jsonb_build_object('claim_token_sha256',
              encode(sha256(convert_to(e.claim_token::text, 'UTF8')), 'hex'))
          ORDER BY e.evidence_id), '[]') FROM engine.attempt_result_evidence e JOIN w USING (workflow_id)),
      'reconciliation', (SELECT COALESCE(jsonb_agg(to_jsonb(r) ORDER BY r.item_id), '[]')
                         FROM engine.reconciliation_items r JOIN w USING (workflow_id)),
      'timers', (SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY t.timer_id), '[]') FROM engine.timers t JOIN w USING (workflow_id))
    )
);`

type readState func(context.Context, Scope, bool) (sample, error)

func readPostgres(store *state.Store, clock restoreledger.Clock) readState {
	return func(ctx context.Context, scope Scope, withLease bool) (sample, error) {
		s := sample{Scope: scope}
		start, err := clock()
		if err != nil {
			return s, err
		}
		var raw []byte
		if err := store.Pool().QueryRow(ctx, sampleSQL, scope.Workflow, scope.Namespace, scope.Partition, withLease).Scan(&raw); err != nil {
			return s, fmt.Errorf("read authority and workflow from PostgreSQL: %w", err)
		}
		end, err := clock()
		if err != nil {
			return s, err
		}
		if err := json.Unmarshal(raw, &s); err != nil {
			return s, err
		}
		s.StartedNS, s.CompletedNS = start, end
		return s, nil
	}
}

func checkSample(s sample, scope Scope, db Database) error {
	if s.Scope != scope || s.Database != db || db.SystemID == 0 || db.Timeline == 0 ||
		s.DBTime.IsZero() || s.StartedNS <= 0 || s.CompletedNS < s.StartedNS {
		return errors.New("probe sampled a different database, workflow, or invalid clock")
	}
	_, err := s.workflow()
	return err
}

func checkClock(s sample) error {
	if s.CompletedNS-s.StartedNS > int64(maxClockBracket) {
		return errors.New("database clock observation exceeded 100 ms")
	}
	return nil
}
