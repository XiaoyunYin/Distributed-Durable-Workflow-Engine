package state

import (
	"context"
	"encoding/json"
	"errors"
)

// RuntimeWork is a bounded, keyset-paged repair scan. Wakeups are hints; a
// missing notification or a crash after inbox commit must not strand work.
func (s *Store) RuntimeWork(ctx context.Context, namespace, after string, limit int) ([]string, error) {
	if namespace == "" || limit < 1 || limit > 1000 {
		return nil, errors.New("runtime scan requires a namespace and bounded limit")
	}
	rows, err := s.pool.Query(ctx, `SELECT workflow_id FROM engine.workflow_executions
		WHERE namespace = $1 AND workflow_id > $2 AND (state IN ('RUNNABLE','WAITING_ACTIVITY','WAITING_TIMER')
		OR EXISTS (SELECT 1 FROM engine.scheduler_wakeups sw WHERE sw.workflow_id=workflow_executions.workflow_id AND sw.state <> 'CONSUMED')
		OR EXISTS (SELECT 1 FROM engine.cancellation_requests cr WHERE cr.workflow_id=workflow_executions.workflow_id AND cr.status='PENDING'))
		ORDER BY workflow_id LIMIT $3`, namespace, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (s *Store) ApplyRuntimeCancellations(ctx context.Context, lease LeaseRef, workflowID, actor string) error {
	rows, err := s.pool.Query(ctx, `SELECT request_id::text FROM engine.cancellation_requests WHERE workflow_id=$1 AND status='PENDING' ORDER BY created_at LIMIT 100`, workflowID)
	if err != nil {
		return err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, id := range ids {
		if _, err := s.ApplyCancellationRequest(ctx, lease, id, actor); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) AcknowledgeWorkflowWakeups(ctx context.Context, lease LeaseRef, workflowID string, revision int64) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := lockLease(ctx, tx, lease); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE engine.scheduler_wakeups w SET state = 'CONSUMED',
		claim_owner = NULL, claim_epoch = NULL, claim_expires_at = NULL, consumed_at = clock_timestamp()
		FROM engine.outbox o WHERE w.event_id = o.event_id AND w.workflow_id = $1
		AND w.partition_id = $2 AND o.aggregate_revision <= $3 AND w.state <> 'CONSUMED'`,
		workflowID, lease.PartitionID, revision)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE engine.reconciliation_items r SET status = 'RESOLVED',
		resolved_at = clock_timestamp(), updated_at = clock_timestamp()
		FROM engine.scheduler_wakeups w WHERE r.reference = 'wakeup/' || w.wakeup_id::text
		AND r.kind = 'LOST_WAKEUP' AND r.status = 'OPEN' AND w.workflow_id = $1 AND w.state = 'CONSUMED'`, workflowID)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// RuntimeActivity is read from authoritative node/definition rows, never from
// caller-supplied broker input. This local deployment only allowlists pure work.
type RuntimeActivity struct {
	WorkflowID      string          `json:"workflow_id"`
	NodeID          string          `json:"node_id"`
	Iteration       int             `json:"iteration"`
	AttemptNumber   int64           `json:"attempt_number"`
	ActivityName    string          `json:"activity_name"`
	ActivityVersion string          `json:"activity_version"`
	Input           json.RawMessage `json:"input"`
}

func RuntimeVersion(definition Definition, nodeID string) (string, bool) {
	var versions map[string]string
	if json.Unmarshal(definition.ActivityVersions, &versions) != nil {
		return "", false
	}
	version := versions[nodeID]
	return version, definition.EffectClasses[nodeID] == EffectPure && version == "v1" &&
		(nodeID == "pure.echo" || nodeID == "pure.add")
}

func (s *Store) RuntimeActivity(ctx context.Context, namespace string, payload json.RawMessage) (*RuntimeActivity, error) {
	var task RuntimeActivity
	if err := json.Unmarshal(payload, &task); err != nil {
		return nil, err
	}
	wf, err := s.GetWorkflow(ctx, task.WorkflowID)
	if err != nil {
		return nil, err
	}
	if wf.Namespace != namespace || wf.State != StateWaitingActivity {
		return nil, nil
	}
	node, err := s.GetNode(ctx, task.WorkflowID, task.NodeID, task.Iteration)
	if err != nil {
		return nil, err
	}
	if node.CurrentAttempt == nil || *node.CurrentAttempt != task.AttemptNumber {
		return nil, nil
	}
	attempt, err := s.GetAttempt(ctx, task.WorkflowID, task.NodeID, task.Iteration, task.AttemptNumber)
	if err != nil {
		return nil, err
	}
	if !attempt.IsCurrent || attempt.State != AttemptDispatchable {
		return nil, nil
	}
	definition, err := s.GetDefinition(ctx, wf.DefinitionID, wf.DefinitionVersion)
	if err != nil {
		return nil, err
	}
	version, ok := RuntimeVersion(definition, task.NodeID)
	if !ok {
		return nil, errors.New("activity is not enabled in the local runtime registry")
	}
	task.ActivityName, task.ActivityVersion, task.Input = task.NodeID, version, node.Input
	return &task, nil
}
