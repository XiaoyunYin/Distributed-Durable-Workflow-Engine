package state

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
)

const defaultRetryBudget = 3

func (s *Store) SetRetryPolicy(ctx context.Context, input RetryPolicyInput) error {
	if input.WorkflowID == "" || input.NodeID == "" || input.ActorID == "" || input.MaxRetries < 0 {
		return errors.New("retry policy identity and non-negative budget are required")
	}
	if input.CheckpointSchemaVersion <= 0 {
		input.CheckpointSchemaVersion = 1
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := lockLease(ctx, tx, input.Lease); err != nil {
		return err
	}
	var partitionID int16
	var revision int64
	if err := tx.QueryRow(ctx, `
		SELECT partition_id, revision FROM engine.workflow_executions
		WHERE workflow_id = $1 FOR UPDATE`, input.WorkflowID).Scan(&partitionID, &revision); err != nil {
		return fmt.Errorf("lock workflow for retry policy: %w", err)
	}
	if partitionID != input.Lease.PartitionID {
		return fmt.Errorf("workflow partition %d does not match lease %d", partitionID, input.Lease.PartitionID)
	}
	if revision != input.ExpectedRevision {
		return fmt.Errorf("%w: expected %d, got %d", ErrRevisionConflict, input.ExpectedRevision, revision)
	}
	var nodeState WorkflowState
	if err := tx.QueryRow(ctx, `
		SELECT state FROM engine.node_instances
		WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3 FOR UPDATE`,
		input.WorkflowID, input.NodeID, input.Iteration).Scan(&nodeState); err != nil {
		return fmt.Errorf("lock node for retry policy: %w", err)
	}
	if isTerminalWorkflowState(nodeState) {
		return ErrAttemptNotCurrent
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO engine.retry_policies
			(workflow_id, node_id, iteration, max_retries, retries_used, total_deadline_at, checkpoint_schema_version)
		VALUES ($1, $2, $3, $4, 0, $5, $6)
		ON CONFLICT (workflow_id, node_id, iteration) DO UPDATE SET
			max_retries = EXCLUDED.max_retries,
			total_deadline_at = EXCLUDED.total_deadline_at,
			checkpoint_schema_version = EXCLUDED.checkpoint_schema_version,
			updated_at = clock_timestamp()`,
		input.WorkflowID, input.NodeID, input.Iteration, input.MaxRetries,
		input.TotalDeadlineAt, input.CheckpointSchemaVersion)
	if err != nil {
		return fmt.Errorf("save retry policy: %w", err)
	}
	return tx.Commit(ctx)
}

func ensureRetryPolicyTx(ctx context.Context, tx pgx.Tx, workflowID, nodeID string, iteration int) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO engine.retry_policies
			(workflow_id, node_id, iteration, max_retries, retries_used, checkpoint_schema_version)
		VALUES ($1, $2, $3, $4, 0, 1)
		ON CONFLICT (workflow_id, node_id, iteration) DO NOTHING`,
		workflowID, nodeID, iteration, defaultRetryBudget)
	return err
}

func retryPolicyTx(ctx context.Context, tx pgx.Tx, workflowID, nodeID string, iteration int) (RetryPolicy, error) {
	if err := ensureRetryPolicyTx(ctx, tx, workflowID, nodeID, iteration); err != nil {
		return RetryPolicy{}, err
	}
	var policy RetryPolicy
	err := tx.QueryRow(ctx, `
		SELECT max_retries, retries_used, total_deadline_at, checkpoint_schema_version
		FROM engine.retry_policies
		WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3
		FOR UPDATE`, workflowID, nodeID, iteration).Scan(
		&policy.MaxRetries, &policy.RetriesUsed, &policy.TotalDeadlineAt, &policy.CheckpointSchemaVersion)
	return policy, err
}

func (s *Store) GetRetryPolicy(ctx context.Context, workflowID, nodeID string, iteration int) (RetryPolicy, error) {
	var policy RetryPolicy
	err := s.pool.QueryRow(ctx, `
		SELECT max_retries, retries_used, total_deadline_at, checkpoint_schema_version
		FROM engine.retry_policies
		WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3`,
		workflowID, nodeID, iteration).Scan(
		&policy.MaxRetries, &policy.RetriesUsed, &policy.TotalDeadlineAt, &policy.CheckpointSchemaVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		return RetryPolicy{MaxRetries: defaultRetryBudget, CheckpointSchemaVersion: 1}, nil
	}
	if err != nil {
		return RetryPolicy{}, fmt.Errorf("read retry policy: %w", err)
	}
	return policy, nil
}

func canonicalPayloadHash(payload json.RawMessage) (string, error) {
	if len(payload) == 0 || !json.Valid(payload) {
		return "", errors.New("payload must be valid JSON")
	}
	var value any
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return "", fmt.Errorf("decode payload: %w", err)
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("canonicalize payload: %w", err)
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}

func (s *Store) RecordCheckpoint(ctx context.Context, input CheckpointInput) (Checkpoint, error) {
	if input.WorkflowID == "" || input.NodeID == "" || input.ClaimToken == "" || input.AttemptNumber <= 0 || input.Sequence < 0 || input.SchemaVersion <= 0 {
		return Checkpoint{}, errors.New("checkpoint identity, sequence, and schema version are required")
	}
	if len(input.Payload) == 0 || !json.Valid(input.Payload) {
		return Checkpoint{}, errors.New("checkpoint payload must be valid JSON")
	}
	payloadHash, err := canonicalPayloadHash(input.Payload)
	if err != nil {
		return Checkpoint{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Checkpoint{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var workflowState WorkflowState
	if err := tx.QueryRow(ctx, `
		SELECT state FROM engine.workflow_executions
		WHERE workflow_id = $1 FOR UPDATE`, input.WorkflowID).Scan(&workflowState); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Checkpoint{}, ErrWorkflowNotFound
		}
		return Checkpoint{}, fmt.Errorf("lock workflow for checkpoint: %w", err)
	}
	if isTerminalWorkflowState(workflowState) {
		return Checkpoint{}, ErrStaleClaim
	}
	var currentAttempt *int64
	if err := tx.QueryRow(ctx, `
		SELECT current_attempt_number FROM engine.node_instances
		WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3 FOR UPDATE`,
		input.WorkflowID, input.NodeID, input.Iteration).Scan(&currentAttempt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Checkpoint{}, ErrNodeNotFound
		}
		return Checkpoint{}, fmt.Errorf("lock node for checkpoint: %w", err)
	}
	if currentAttempt == nil || *currentAttempt != input.AttemptNumber {
		return Checkpoint{}, ErrStaleClaim
	}
	var attemptState AttemptState
	if err := tx.QueryRow(ctx, `
		SELECT state FROM engine.activity_attempts
		WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3 AND attempt_number = $4
			AND is_current AND claim_token = $5 FOR UPDATE`,
		input.WorkflowID, input.NodeID, input.Iteration, input.AttemptNumber, input.ClaimToken).Scan(&attemptState); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Checkpoint{}, ErrStaleClaim
		}
		return Checkpoint{}, fmt.Errorf("lock attempt for checkpoint: %w", err)
	}
	if attemptState != AttemptClaimed {
		return Checkpoint{}, ErrStaleClaim
	}
	policy, err := retryPolicyTx(ctx, tx, input.WorkflowID, input.NodeID, input.Iteration)
	if err != nil {
		return Checkpoint{}, fmt.Errorf("read checkpoint policy: %w", err)
	}
	if input.SchemaVersion != policy.CheckpointSchemaVersion {
		return Checkpoint{}, ErrCheckpointConflict
	}
	var latest Checkpoint
	latestErr := tx.QueryRow(ctx, `
		SELECT workflow_id, node_id, iteration, sequence, schema_version,
			source_attempt_number, payload, payload_hash, created_at
		FROM engine.checkpoints
		WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3
		ORDER BY sequence DESC LIMIT 1 FOR UPDATE`,
		input.WorkflowID, input.NodeID, input.Iteration).Scan(
		&latest.WorkflowID, &latest.NodeID, &latest.Iteration, &latest.Sequence,
		&latest.SchemaVersion, &latest.SourceAttempt, &latest.Payload,
		&latest.PayloadHash, &latest.CreatedAt)
	if latestErr == nil {
		if input.Sequence < latest.Sequence || input.Sequence > latest.Sequence+1 {
			return Checkpoint{}, ErrCheckpointStale
		}
		if input.Sequence == latest.Sequence {
			if latest.PayloadHash != payloadHash || latest.SchemaVersion != input.SchemaVersion {
				return Checkpoint{}, ErrCheckpointConflict
			}
			if err := tx.Commit(ctx); err != nil {
				return Checkpoint{}, err
			}
			return latest, nil
		}
	} else if !errors.Is(latestErr, pgx.ErrNoRows) {
		return Checkpoint{}, fmt.Errorf("read latest checkpoint: %w", latestErr)
	} else if input.Sequence != 0 {
		return Checkpoint{}, ErrCheckpointStale
	}
	var checkpoint Checkpoint
	err = tx.QueryRow(ctx, `
		INSERT INTO engine.checkpoints
			(workflow_id, node_id, iteration, sequence, schema_version,
			 source_attempt_number, payload, payload_hash)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING workflow_id, node_id, iteration, sequence, schema_version,
			source_attempt_number, payload, payload_hash, created_at`,
		input.WorkflowID, input.NodeID, input.Iteration, input.Sequence,
		input.SchemaVersion, input.AttemptNumber, input.Payload, payloadHash).Scan(
		&checkpoint.WorkflowID, &checkpoint.NodeID, &checkpoint.Iteration,
		&checkpoint.Sequence, &checkpoint.SchemaVersion, &checkpoint.SourceAttempt,
		&checkpoint.Payload, &checkpoint.PayloadHash, &checkpoint.CreatedAt)
	if err != nil {
		return Checkpoint{}, fmt.Errorf("record checkpoint: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Checkpoint{}, err
	}
	return checkpoint, nil
}

func (s *Store) GetLatestCheckpoint(ctx context.Context, workflowID, nodeID string, iteration int) (Checkpoint, error) {
	var checkpoint Checkpoint
	err := s.pool.QueryRow(ctx, `
		SELECT workflow_id, node_id, iteration, sequence, schema_version,
			source_attempt_number, payload, payload_hash, created_at
		FROM engine.checkpoints
		WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3
		ORDER BY sequence DESC LIMIT 1`, workflowID, nodeID, iteration).Scan(
		&checkpoint.WorkflowID, &checkpoint.NodeID, &checkpoint.Iteration,
		&checkpoint.Sequence, &checkpoint.SchemaVersion, &checkpoint.SourceAttempt,
		&checkpoint.Payload, &checkpoint.PayloadHash, &checkpoint.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Checkpoint{}, pgx.ErrNoRows
	}
	if err != nil {
		return Checkpoint{}, fmt.Errorf("read latest checkpoint: %w", err)
	}
	return checkpoint, nil
}

func (s *Store) ListCheckpoints(ctx context.Context, workflowID string) ([]Checkpoint, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT workflow_id, node_id, iteration, sequence, schema_version,
			source_attempt_number, payload, payload_hash, created_at
		FROM engine.checkpoints WHERE workflow_id = $1
		ORDER BY node_id, iteration, sequence`, workflowID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var checkpoints []Checkpoint
	for rows.Next() {
		var checkpoint Checkpoint
		if err := rows.Scan(&checkpoint.WorkflowID, &checkpoint.NodeID, &checkpoint.Iteration,
			&checkpoint.Sequence, &checkpoint.SchemaVersion, &checkpoint.SourceAttempt,
			&checkpoint.Payload, &checkpoint.PayloadHash, &checkpoint.CreatedAt); err != nil {
			return nil, err
		}
		checkpoints = append(checkpoints, checkpoint)
	}
	return checkpoints, rows.Err()
}

func proposalHash(target string, arguments json.RawMessage, expectedResourceRevision string) (string, error) {
	if target == "" || len(arguments) == 0 || !json.Valid(arguments) {
		return "", errors.New("approval target and valid canonical arguments are required")
	}
	hash, err := canonicalPayloadHash(arguments)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256([]byte(target + "\x00" + hash + "\x00" + expectedResourceRevision))
	return hex.EncodeToString(sum[:]), nil
}

func (s *Store) CreateApprovalIntent(ctx context.Context, input ApprovalIntentInput) (ApprovalIntent, error) {
	if input.WorkflowID == "" || input.NodeID == "" || input.ActorID == "" || input.ValidUntil.IsZero() || !input.ValidUntil.After(time.Now()) {
		return ApprovalIntent{}, errors.New("approval identity and future validity are required")
	}
	hash, err := proposalHash(input.Target, input.CanonicalArguments, input.ExpectedResourceRevision)
	if err != nil {
		return ApprovalIntent{}, err
	}
	signatureSum := sha256.Sum256([]byte(hash + "\x00" + input.ActorID))
	signature := hex.EncodeToString(signatureSum[:])
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ApprovalIntent{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := lockLease(ctx, tx, input.Lease); err != nil {
		return ApprovalIntent{}, err
	}
	var workflow Workflow
	if err := tx.QueryRow(ctx, `
		SELECT workflow_id, namespace, submission_key, submission_payload_hash,
			definition_id, definition_version, partition_id, state, revision,
		created_at, updated_at
		FROM engine.workflow_executions WHERE workflow_id = $1 FOR UPDATE`, input.WorkflowID).Scan(
		&workflow.WorkflowID, &workflow.Namespace, &workflow.SubmissionKey,
		&workflow.PayloadHash, &workflow.DefinitionID, &workflow.DefinitionVersion,
		&workflow.PartitionID, &workflow.State, &workflow.Revision,
		&workflow.CreatedAt, &workflow.UpdatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ApprovalIntent{}, ErrWorkflowNotFound
		}
		return ApprovalIntent{}, fmt.Errorf("lock workflow for approval: %w", err)
	}
	if workflow.PartitionID != input.Lease.PartitionID {
		return ApprovalIntent{}, ErrLeaseNotOwned
	}
	if workflow.Revision != input.ExpectedRevision {
		return ApprovalIntent{}, fmt.Errorf("%w: expected %d, got %d", ErrRevisionConflict, input.ExpectedRevision, workflow.Revision)
	}
	if workflow.State != StateRunnable && workflow.State != StateWaitingApproval {
		return ApprovalIntent{}, fmt.Errorf("%w: workflow is not approval-ready", ErrInvalidTransition)
	}
	var nodeState WorkflowState
	if err := tx.QueryRow(ctx, `SELECT state FROM engine.node_instances WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3 FOR UPDATE`,
		input.WorkflowID, input.NodeID, input.Iteration).Scan(&nodeState); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ApprovalIntent{}, ErrNodeNotFound
		}
		return ApprovalIntent{}, fmt.Errorf("lock node for approval: %w", err)
	}
	if nodeState != StateRunnable && nodeState != StateWaitingApproval {
		return ApprovalIntent{}, fmt.Errorf("%w: node is not approval-ready", ErrInvalidTransition)
	}
	var existing ApprovalIntent
	err = scanApproval(tx.QueryRow(ctx, approvalSelect+` WHERE intent_id = (
		SELECT intent_id FROM engine.approval_action_intents
		WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3
		  AND decision = 'PENDING' AND dispatch_status = 'NOT_GRANTED'
		ORDER BY created_at DESC LIMIT 1 FOR UPDATE)`, input.WorkflowID, input.NodeID, input.Iteration), &existing)
	if err == nil {
		if existing.ProposalHash != hash {
			return ApprovalIntent{}, ErrApprovalMismatch
		}
		if err := tx.Commit(ctx); err != nil {
			return ApprovalIntent{}, err
		}
		return existing, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return ApprovalIntent{}, err
	}
	intentID := NewID()
	newState := workflow.State
	newRevision := workflow.Revision
	if workflow.State == StateRunnable {
		newState = StateWaitingApproval
		newRevision++
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO engine.approval_action_intents
			(intent_id, workflow_id, node_id, iteration, proposal_hash,
			 proposal_signature, target, canonical_arguments, expected_resource_revision,
			 valid_until)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		intentID, input.WorkflowID, input.NodeID, input.Iteration, hash, signature,
		input.Target, input.CanonicalArguments, input.ExpectedResourceRevision, input.ValidUntil); err != nil {
		return ApprovalIntent{}, fmt.Errorf("insert approval intent: %w", err)
	}
	if newState != workflow.State {
		if _, err := tx.Exec(ctx, `
			UPDATE engine.node_instances SET state = $4, revision = revision + 1, updated_at = clock_timestamp()
			WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3`,
			input.WorkflowID, input.NodeID, input.Iteration, newState); err != nil {
			return ApprovalIntent{}, err
		}
		if _, err := tx.Exec(ctx, `UPDATE engine.workflow_executions SET state = $2, revision = $3, updated_at = clock_timestamp() WHERE workflow_id = $1`,
			input.WorkflowID, newState, newRevision); err != nil {
			return ApprovalIntent{}, err
		}
		if err := insertHistory(ctx, tx, input.WorkflowID, newRevision, "scheduler", input.ActorID,
			&input.Lease.Epoch, input.NodeID, &input.Iteration, nil, &workflow.State, newState, "APPROVAL_REQUESTED"); err != nil {
			return ApprovalIntent{}, err
		}
		if err := insertOutbox(ctx, tx, input.WorkflowID, newRevision, "approval.requested",
			json.RawMessage(fmt.Sprintf(`{"intent_id":%q,"workflow_id":%q}`, intentID, input.WorkflowID))); err != nil {
			return ApprovalIntent{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return ApprovalIntent{}, err
	}
	return ApprovalIntent{IntentID: intentID, WorkflowID: input.WorkflowID, NodeID: input.NodeID,
		Iteration: input.Iteration, ProposalHash: hash, ProposalSignature: signature,
		Target: input.Target, CanonicalArguments: input.CanonicalArguments,
		ExpectedResourceRevision: input.ExpectedResourceRevision, Decision: "PENDING",
		ValidUntil: input.ValidUntil, DispatchStatus: "NOT_GRANTED"}, nil
}

const approvalSelect = `SELECT intent_id::text, workflow_id, node_id, iteration,
	proposal_hash, COALESCE(proposal_signature,''), target, canonical_arguments,
	COALESCE(expected_resource_revision,''), decision, COALESCE(approver_id,''),
	valid_until, dispatch_status, COALESCE(grant_scope_hash,''),
	COALESCE(grant_token::text,''), grant_expires_at, COALESCE(decision_reason,'')
	FROM engine.approval_action_intents`

type rowScanner interface {
	Scan(dest ...any) error
}

func scanApproval(row rowScanner, result *ApprovalIntent) error {
	return row.Scan(&result.IntentID, &result.WorkflowID, &result.NodeID, &result.Iteration,
		&result.ProposalHash, &result.ProposalSignature, &result.Target, &result.CanonicalArguments,
		&result.ExpectedResourceRevision, &result.Decision, &result.ApproverID, &result.ValidUntil,
		&result.DispatchStatus, &result.GrantScopeHash, &result.GrantToken, &result.GrantExpiresAt,
		&result.DecisionReason)
}

func (s *Store) GetApprovalIntent(ctx context.Context, intentID string) (ApprovalIntent, error) {
	var result ApprovalIntent
	if err := scanApproval(s.pool.QueryRow(ctx, approvalSelect+` WHERE intent_id = $1`, intentID), &result); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ApprovalIntent{}, ErrApprovalNotFound
		}
		return ApprovalIntent{}, err
	}
	return result, nil
}

func (s *Store) ListApprovalIntents(ctx context.Context, workflowID string) ([]ApprovalIntent, error) {
	rows, err := s.pool.Query(ctx, approvalSelect+` WHERE workflow_id = $1 ORDER BY created_at`, workflowID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var intents []ApprovalIntent
	for rows.Next() {
		var intent ApprovalIntent
		if err := scanApproval(rows, &intent); err != nil {
			return nil, err
		}
		intents = append(intents, intent)
	}
	return intents, rows.Err()
}

func (s *Store) RecordApprovalDecision(ctx context.Context, input ApprovalDecisionInput) (ApprovalIntent, error) {
	if input.IntentID == "" || input.ApproverID == "" || (input.Decision != "APPROVED" && input.Decision != "REJECTED") || input.ProposalHash == "" {
		return ApprovalIntent{}, ErrApprovalUnauthorized
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ApprovalIntent{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var intent ApprovalIntent
	if err := scanApproval(tx.QueryRow(ctx, approvalSelect+` WHERE intent_id = $1 FOR UPDATE`, input.IntentID), &intent); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ApprovalIntent{}, ErrApprovalNotFound
		}
		return ApprovalIntent{}, err
	}
	if intent.ProposalHash != input.ProposalHash {
		return ApprovalIntent{}, ErrApprovalMismatch
	}
	if intent.Decision != "PENDING" {
		if intent.Decision != input.Decision || intent.ApproverID != input.ApproverID {
			return ApprovalIntent{}, ErrApprovalConflict
		}
		if err := tx.Commit(ctx); err != nil {
			return ApprovalIntent{}, err
		}
		return intent, nil
	}
	if !intent.ValidUntil.After(time.Now()) {
		return ApprovalIntent{}, ErrApprovalExpired
	}
	if _, err := tx.Exec(ctx, `
		UPDATE engine.approval_action_intents
		SET decision = $2, approver_id = $3, decision_reason = $4,
			decided_at = clock_timestamp()
		WHERE intent_id = $1`, input.IntentID, input.Decision, input.ApproverID, input.DecisionReason); err != nil {
		return ApprovalIntent{}, err
	}
	intent.Decision, intent.ApproverID, intent.DecisionReason = input.Decision, input.ApproverID, input.DecisionReason
	if err := tx.Commit(ctx); err != nil {
		return ApprovalIntent{}, err
	}
	return intent, nil
}

func (s *Store) ApplyApproval(ctx context.Context, input ApplyApprovalInput) (ApprovalGrant, error) {
	if input.WorkflowID == "" || input.NodeID == "" || input.IntentID == "" || input.ActorID == "" {
		return ApprovalGrant{}, errors.New("approval application identity is required")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ApprovalGrant{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := lockLease(ctx, tx, input.Lease); err != nil {
		return ApprovalGrant{}, err
	}
	var workflow Workflow
	if err := tx.QueryRow(ctx, `SELECT workflow_id, namespace, submission_key, submission_payload_hash, definition_id, definition_version, partition_id, state, revision, created_at, updated_at FROM engine.workflow_executions WHERE workflow_id = $1 FOR UPDATE`, input.WorkflowID).Scan(
		&workflow.WorkflowID, &workflow.Namespace, &workflow.SubmissionKey, &workflow.PayloadHash,
		&workflow.DefinitionID, &workflow.DefinitionVersion, &workflow.PartitionID, &workflow.State,
		&workflow.Revision, &workflow.CreatedAt, &workflow.UpdatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ApprovalGrant{}, ErrWorkflowNotFound
		}
		return ApprovalGrant{}, fmt.Errorf("lock workflow for approval application: %w", err)
	}
	if workflow.PartitionID != input.Lease.PartitionID {
		return ApprovalGrant{}, ErrLeaseNotOwned
	}
	if workflow.Revision != input.ExpectedRevision {
		return ApprovalGrant{}, fmt.Errorf("%w: expected %d, got %d", ErrRevisionConflict, input.ExpectedRevision, workflow.Revision)
	}
	if workflow.State != StateWaitingApproval {
		return ApprovalGrant{}, fmt.Errorf("%w: workflow is not waiting for approval", ErrInvalidTransition)
	}
	var intent ApprovalIntent
	if err := scanApproval(tx.QueryRow(ctx, approvalSelect+` WHERE intent_id = $1 AND workflow_id = $2 AND node_id = $3 AND iteration = $4 FOR UPDATE`, input.IntentID, input.WorkflowID, input.NodeID, input.Iteration), &intent); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ApprovalGrant{}, ErrApprovalNotFound
		}
		return ApprovalGrant{}, err
	}
	if intent.DispatchStatus != "NOT_GRANTED" {
		return ApprovalGrant{}, ErrApprovalMismatch
	}
	var cancellationPending bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM engine.cancellation_requests
			WHERE workflow_id = $1 AND status = 'PENDING'
		)`, input.WorkflowID).Scan(&cancellationPending); err != nil {
		return ApprovalGrant{}, err
	}
	if cancellationPending {
		return ApprovalGrant{}, ErrCancellationConflict
	}
	if intent.Decision == "REJECTED" {
		newRevision := workflow.Revision + 1
		if _, err := tx.Exec(ctx, `UPDATE engine.approval_action_intents SET dispatch_status = 'CANCELED' WHERE intent_id = $1`, input.IntentID); err != nil {
			return ApprovalGrant{}, err
		}
		if _, err := tx.Exec(ctx, `UPDATE engine.node_instances SET state = 'REJECTED', revision = revision + 1, updated_at = clock_timestamp() WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3`, input.WorkflowID, input.NodeID, input.Iteration); err != nil {
			return ApprovalGrant{}, err
		}
		if _, err := tx.Exec(ctx, `UPDATE engine.workflow_executions SET state = 'REJECTED', revision = $2, updated_at = clock_timestamp() WHERE workflow_id = $1`, input.WorkflowID, newRevision); err != nil {
			return ApprovalGrant{}, err
		}
		if err := insertHistory(ctx, tx, input.WorkflowID, newRevision, "scheduler", input.ActorID,
			&input.Lease.Epoch, input.NodeID, &input.Iteration, nil, &workflow.State, StateRejected, "APPROVAL_REJECTED"); err != nil {
			return ApprovalGrant{}, err
		}
		if err := insertOutbox(ctx, tx, input.WorkflowID, newRevision, "approval.rejected",
			json.RawMessage(fmt.Sprintf(`{"intent_id":%q}`, input.IntentID))); err != nil {
			return ApprovalGrant{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return ApprovalGrant{}, err
		}
		return ApprovalGrant{IntentID: input.IntentID}, nil
	}
	if intent.Decision != "APPROVED" || input.LogicalEffectKey == "" {
		return ApprovalGrant{}, ErrApprovalMismatch
	}
	if !intent.ValidUntil.After(time.Now()) {
		return ApprovalGrant{}, ErrApprovalExpired
	}
	grantToken := NewID()
	scopeSum := sha256.Sum256([]byte(intent.ProposalHash + "\x00" + intent.ExpectedResourceRevision + "\x00" + input.LogicalEffectKey))
	grantScope := hex.EncodeToString(scopeSum[:])
	if _, err := tx.Exec(ctx, `
		UPDATE engine.approval_action_intents
		SET dispatch_status = 'GRANTED', grant_token = $2, grant_scope_hash = $3,
			grant_expires_at = valid_until
		WHERE intent_id = $1`, input.IntentID, grantToken, grantScope); err != nil {
		return ApprovalGrant{}, err
	}
	newRevision := workflow.Revision + 1
	if _, err := tx.Exec(ctx, `UPDATE engine.node_instances SET state = 'RUNNABLE', revision = revision + 1, updated_at = clock_timestamp() WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3`, input.WorkflowID, input.NodeID, input.Iteration); err != nil {
		return ApprovalGrant{}, err
	}
	if _, err := tx.Exec(ctx, `UPDATE engine.workflow_executions SET state = 'RUNNABLE', revision = $2, updated_at = clock_timestamp() WHERE workflow_id = $1`, input.WorkflowID, newRevision); err != nil {
		return ApprovalGrant{}, err
	}
	if err := insertHistory(ctx, tx, input.WorkflowID, newRevision, "scheduler", input.ActorID,
		&input.Lease.Epoch, input.NodeID, &input.Iteration, nil, &workflow.State, StateRunnable, "APPROVAL_GRANTED"); err != nil {
		return ApprovalGrant{}, err
	}
	if err := insertOutbox(ctx, tx, input.WorkflowID, newRevision, "approval.granted",
		json.RawMessage(fmt.Sprintf(`{"intent_id":%q,"grant_scope_hash":%q}`, input.IntentID, grantScope))); err != nil {
		return ApprovalGrant{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ApprovalGrant{}, err
	}
	return ApprovalGrant{IntentID: input.IntentID, GrantToken: grantToken, GrantScopeHash: grantScope, ExpiresAt: intent.ValidUntil}, nil
}

func (s *Store) ValidateApprovalGrant(ctx context.Context, input GrantValidationInput) (ApprovalIntent, error) {
	var intent ApprovalIntent
	if err := scanApproval(s.pool.QueryRow(ctx, approvalSelect+` WHERE intent_id = $1 AND grant_token = $2 AND workflow_id = $3 AND node_id = $4 AND iteration = $5`, input.IntentID, input.GrantToken, input.WorkflowID, input.NodeID, input.Iteration), &intent); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ApprovalIntent{}, ErrGrantInvalid
		}
		return ApprovalIntent{}, err
	}
	if intent.DispatchStatus != "GRANTED" && intent.DispatchStatus != "DISPATCHED" || intent.GrantScopeHash != input.GrantScopeHash || intent.GrantExpiresAt == nil || !intent.GrantExpiresAt.After(time.Now()) {
		return ApprovalIntent{}, ErrGrantInvalid
	}
	if input.LogicalEffectKey != "" {
		scopeSum := sha256.Sum256([]byte(intent.ProposalHash + "\x00" + intent.ExpectedResourceRevision + "\x00" + input.LogicalEffectKey))
		if hex.EncodeToString(scopeSum[:]) != input.GrantScopeHash {
			return ApprovalIntent{}, ErrGrantInvalid
		}
	}
	return intent, nil
}

func (s *Store) RequestCancellation(ctx context.Context, input CancellationRequestInput) (CancellationRequest, error) {
	if input.WorkflowID == "" || input.ClientKey == "" || input.ObservedRevision < 0 {
		return CancellationRequest{}, errors.New("cancellation workflow, key, and revision are required")
	}
	var exists bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM engine.workflow_executions WHERE workflow_id = $1)`, input.WorkflowID).Scan(&exists); err != nil {
		return CancellationRequest{}, err
	}
	if !exists {
		return CancellationRequest{}, ErrWorkflowNotFound
	}
	var request CancellationRequest
	err := s.pool.QueryRow(ctx, `
		INSERT INTO engine.cancellation_requests (request_id, workflow_id, client_key, observed_revision)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (workflow_id, client_key) DO UPDATE SET client_key = EXCLUDED.client_key
		RETURNING request_id::text, workflow_id, client_key, observed_revision, status, created_at, applied_at`,
		NewID(), input.WorkflowID, input.ClientKey, input.ObservedRevision).Scan(
		&request.RequestID, &request.WorkflowID, &request.ClientKey, &request.ObservedRevision,
		&request.Status, &request.CreatedAt, &request.AppliedAt)
	if err != nil {
		return CancellationRequest{}, fmt.Errorf("record cancellation request: %w", err)
	}
	return request, nil
}

func (s *Store) ApplyCancellationRequest(ctx context.Context, lease LeaseRef, requestID, actorID string) (Workflow, error) {
	if requestID == "" || actorID == "" {
		return Workflow{}, errors.New("cancellation request and actor are required")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Workflow{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := lockLease(ctx, tx, lease); err != nil {
		return Workflow{}, err
	}
	var request CancellationRequest
	if err := tx.QueryRow(ctx, `SELECT request_id::text, workflow_id, client_key, observed_revision, status, created_at, applied_at FROM engine.cancellation_requests WHERE request_id = $1 FOR UPDATE`, requestID).Scan(
		&request.RequestID, &request.WorkflowID, &request.ClientKey, &request.ObservedRevision,
		&request.Status, &request.CreatedAt, &request.AppliedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Workflow{}, ErrCancellationConflict
		}
		return Workflow{}, err
	}
	if request.Status != "PENDING" {
		workflow, err := scanWorkflow(tx.QueryRow(ctx, `
			SELECT workflow_id, namespace, submission_key, submission_payload_hash,
				definition_id, definition_version, partition_id, state, revision,
				created_at, updated_at
			FROM engine.workflow_executions WHERE workflow_id = $1 FOR UPDATE`, request.WorkflowID))
		if err != nil {
			return Workflow{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return Workflow{}, err
		}
		return workflow, nil
	}
	workflow, err := scanWorkflow(tx.QueryRow(ctx, `
		SELECT workflow_id, namespace, submission_key, submission_payload_hash,
			definition_id, definition_version, partition_id, state, revision,
			created_at, updated_at
		FROM engine.workflow_executions WHERE workflow_id = $1 FOR UPDATE`, request.WorkflowID))
	if err != nil {
		return Workflow{}, err
	}
	if workflow.PartitionID != lease.PartitionID {
		return Workflow{}, ErrLeaseNotOwned
	}
	var granted bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM engine.approval_action_intents WHERE workflow_id = $1 AND dispatch_status IN ('GRANTED','DISPATCHED'))`, request.WorkflowID).Scan(&granted); err != nil {
		return Workflow{}, err
	}
	if granted {
		if _, err := tx.Exec(ctx, `UPDATE engine.cancellation_requests SET status = 'BEST_EFFORT', applied_at = clock_timestamp() WHERE request_id = $1`, requestID); err != nil {
			return Workflow{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return Workflow{}, err
		}
		return workflow, nil
	}
	if isTerminalWorkflowState(workflow.State) {
		if _, err := tx.Exec(ctx, `UPDATE engine.cancellation_requests SET status = 'APPLIED', applied_at = clock_timestamp() WHERE request_id = $1`, requestID); err != nil {
			return Workflow{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return Workflow{}, err
		}
		return workflow, nil
	}
	if err := cancelWorkflowTx(ctx, tx, lease, &workflow, actorID); err != nil {
		return Workflow{}, err
	}
	if _, err := tx.Exec(ctx, `UPDATE engine.cancellation_requests SET status = 'APPLIED', applied_at = clock_timestamp() WHERE request_id = $1`, requestID); err != nil {
		return Workflow{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Workflow{}, err
	}
	return workflow, nil
}

// cancelWorkflowTx applies the owner-side cancellation while the lease and
// workflow rows are already locked. Keeping the request and transition in one
// transaction closes the race where a grant could be issued between them.
func cancelWorkflowTx(ctx context.Context, tx pgx.Tx, lease LeaseRef, workflow *Workflow, actorID string) error {
	rows, err := tx.Query(ctx, `
		SELECT node_id, iteration, state, current_attempt_number
		FROM engine.node_instances
		WHERE workflow_id = $1
		ORDER BY iteration, node_id
		FOR UPDATE`, workflow.WorkflowID)
	if err != nil {
		return fmt.Errorf("lock workflow nodes for cancellation: %w", err)
	}
	type cancellationNode struct {
		id        string
		iteration int
		state     WorkflowState
		attempt   *int64
	}
	var nodes []cancellationNode
	for rows.Next() {
		var node cancellationNode
		if err := rows.Scan(&node.id, &node.iteration, &node.state, &node.attempt); err != nil {
			rows.Close()
			return err
		}
		nodes = append(nodes, node)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	for _, node := range nodes {
		if isTerminalWorkflowState(node.state) {
			continue
		}
		if node.attempt != nil {
			var ignored int
			if err := tx.QueryRow(ctx, `SELECT 1 FROM engine.activity_attempts
				WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3 AND attempt_number = $4 FOR UPDATE`,
				workflow.WorkflowID, node.id, node.iteration, *node.attempt).Scan(&ignored); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `UPDATE engine.activity_attempts
				SET state = 'CANCELED', is_current = false,
					outcome_disposition = CASE WHEN state = 'CLAIMED' AND effect_class <> 'PURE_ACTIVITY' THEN 'OUTCOME_UNKNOWN' ELSE 'NONE' END,
					updated_at = clock_timestamp()
				WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3 AND attempt_number = $4 AND is_current`,
				workflow.WorkflowID, node.id, node.iteration, *node.attempt); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx, `UPDATE engine.node_instances
			SET state = 'CANCELED', current_attempt_number = NULL, deadline_at = NULL,
				revision = revision + 1, updated_at = clock_timestamp()
			WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3`,
			workflow.WorkflowID, node.id, node.iteration); err != nil {
			return err
		}
	}
	newRevision := workflow.Revision + 1
	if _, err := tx.Exec(ctx, `UPDATE engine.workflow_executions
		SET state = 'CANCELED', revision = $2, updated_at = clock_timestamp()
		WHERE workflow_id = $1`, workflow.WorkflowID, newRevision); err != nil {
		return err
	}
	if err := insertHistory(ctx, tx, workflow.WorkflowID, newRevision, "scheduler", actorID,
		&lease.Epoch, "", nil, nil, &workflow.State, StateCanceled, "CANCELED_ALL_ACTIVE_NODES"); err != nil {
		return err
	}
	if err := insertOutbox(ctx, tx, workflow.WorkflowID, newRevision, "workflow.canceled",
		json.RawMessage(fmt.Sprintf(`{"workflow_id":%q}`, workflow.WorkflowID))); err != nil {
		return err
	}
	workflow.State = StateCanceled
	workflow.Revision = newRevision
	workflow.UpdatedAt = time.Now().UTC()
	return nil
}

func (s *Store) ApplyEffect(ctx context.Context, input EffectApplyInput) (EffectReceipt, error) {
	if input.WorkflowID == "" || input.NodeID == "" || input.Iteration < 0 || input.LogicalEffectKey == "" || input.ArgumentHash == "" || input.RequestID == "" || input.AttemptNumber <= 0 || input.ResourceID == "" || input.FenceToken <= 0 {
		return EffectReceipt{}, errors.New("effect identity, request, resource, and fence are required")
	}
	if input.IntentID == "" || input.GrantToken == "" || input.GrantScopeHash == "" {
		return EffectReceipt{}, ErrGrantInvalid
	}
	if len(input.State) == 0 || !json.Valid(input.State) {
		return EffectReceipt{}, errors.New("effect state must be valid JSON")
	}
	var decision, dispatchStatus, scopeHash, proposalHash, expectedResourceRevision string
	var expiresAt *time.Time
	if err := s.pool.QueryRow(ctx, `
		SELECT decision, dispatch_status, grant_scope_hash, grant_expires_at,
			proposal_hash, COALESCE(expected_resource_revision, '')
		FROM engine.approval_action_intents
		WHERE intent_id = $1 AND grant_token = $2 AND workflow_id = $3
			AND node_id = $4 AND iteration = $5`,
		input.IntentID, input.GrantToken, input.WorkflowID, input.NodeID, input.Iteration).Scan(
		&decision, &dispatchStatus, &scopeHash, &expiresAt, &proposalHash, &expectedResourceRevision); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return EffectReceipt{}, ErrGrantInvalid
		}
		return EffectReceipt{}, err
	}
	if decision != "APPROVED" || (dispatchStatus != "GRANTED" && dispatchStatus != "DISPATCHED") ||
		scopeHash != input.GrantScopeHash || expiresAt == nil || !expiresAt.After(time.Now()) {
		return EffectReceipt{}, ErrGrantInvalid
	}
	if input.ExpectedResourceRevision != expectedResourceRevision {
		return EffectReceipt{}, ErrEffectVersion
	}
	scopeSum := sha256.Sum256([]byte(proposalHash + "\x00" + expectedResourceRevision + "\x00" + input.LogicalEffectKey))
	if hex.EncodeToString(scopeSum[:]) != input.GrantScopeHash {
		return EffectReceipt{}, ErrGrantInvalid
	}

	// The effect service transaction owns only effects.*. The grant is
	// validated above, but never changed here: the engine's workflow/grant
	// transaction and the cooperating service ledger are separate boundaries.
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return EffectReceipt{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var existing EffectRecord
	var existingReceipt []byte
	err = tx.QueryRow(ctx, `
		SELECT workflow_id, logical_effect_key, argument_hash, attempt_number,
			COALESCE(grant_scope_hash,''), outcome, receipt, created_at, updated_at
		FROM effects.effect_records
		WHERE workflow_id = $1 AND logical_effect_key = $2 FOR UPDATE`,
		input.WorkflowID, input.LogicalEffectKey).Scan(&existing.WorkflowID, &existing.LogicalEffectKey,
		&existing.ArgumentHash, &existing.AttemptNumber, &existing.GrantScopeHash, &existing.Outcome,
		&existingReceipt, &existing.CreatedAt, &existing.UpdatedAt)
	if err == nil {
		if existing.ArgumentHash != input.ArgumentHash || existing.GrantScopeHash != input.GrantScopeHash {
			if callErr := recordEffectCallTx(ctx, tx, input, "CONFLICT", nil); callErr != nil {
				return EffectReceipt{}, callErr
			}
			if err := tx.Commit(ctx); err != nil {
				return EffectReceipt{}, err
			}
			return EffectReceipt{}, ErrEffectConflict
		}
		if existing.Outcome == "OUTCOME_UNKNOWN" {
			if callErr := recordEffectCallTx(ctx, tx, input, "UNKNOWN", nil); callErr != nil {
				return EffectReceipt{}, callErr
			}
			if err := tx.Commit(ctx); err != nil {
				return EffectReceipt{}, err
			}
			return EffectReceipt{}, ErrAmbiguousEffect
		}
		if err := recordEffectCallTx(ctx, tx, input, "DUPLICATE", existingReceipt); err != nil {
			return EffectReceipt{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return EffectReceipt{}, err
		}
		return EffectReceipt{WorkflowID: input.WorkflowID, LogicalEffectKey: input.LogicalEffectKey,
			ArgumentHash: existing.ArgumentHash, ResourceID: input.ResourceID, Receipt: existingReceipt}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return EffectReceipt{}, fmt.Errorf("read effect record: %w", err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO effects.effect_resource_fences (resource_id, next_token, observed_token)
		VALUES ($1, $2, $2) ON CONFLICT (resource_id) DO NOTHING`, input.ResourceID, input.FenceToken)
	if err != nil {
		return EffectReceipt{}, err
	}
	var observedToken int64
	if err := tx.QueryRow(ctx, `SELECT observed_token FROM effects.effect_resource_fences WHERE resource_id = $1 FOR UPDATE`, input.ResourceID).Scan(&observedToken); err != nil {
		return EffectReceipt{}, err
	}
	if input.FenceToken < observedToken {
		if callErr := recordEffectCallTx(ctx, tx, input, "REJECTED", nil); callErr != nil {
			return EffectReceipt{}, callErr
		}
		if err := tx.Commit(ctx); err != nil {
			return EffectReceipt{}, err
		}
		return EffectReceipt{}, ErrEffectFence
	}
	if _, err := tx.Exec(ctx, `UPDATE effects.effect_resource_fences SET next_token = GREATEST(next_token, $2), observed_token = $2, updated_at = clock_timestamp() WHERE resource_id = $1`, input.ResourceID, input.FenceToken); err != nil {
		return EffectReceipt{}, err
	}
	var resourceRevision int64
	err = tx.QueryRow(ctx, `SELECT resource_revision FROM effects.sandbox_effect_state WHERE resource_id = $1 FOR UPDATE`, input.ResourceID).Scan(&resourceRevision)
	if errors.Is(err, pgx.ErrNoRows) {
		resourceRevision = 0
		if input.ExpectedResourceRevision != "0" {
			if callErr := recordEffectCallTx(ctx, tx, input, "REJECTED", nil); callErr != nil {
				return EffectReceipt{}, callErr
			}
			if err := tx.Commit(ctx); err != nil {
				return EffectReceipt{}, err
			}
			return EffectReceipt{}, ErrEffectVersion
		}
		if _, err := tx.Exec(ctx, `INSERT INTO effects.sandbox_effect_state (resource_id, state, resource_revision) VALUES ($1, $2, 0)`, input.ResourceID, json.RawMessage(`{}`)); err != nil {
			return EffectReceipt{}, err
		}
	} else if err != nil {
		return EffectReceipt{}, err
	} else if strconv.FormatInt(resourceRevision, 10) != input.ExpectedResourceRevision {
		if callErr := recordEffectCallTx(ctx, tx, input, "REJECTED", nil); callErr != nil {
			return EffectReceipt{}, callErr
		}
		if err := tx.Commit(ctx); err != nil {
			return EffectReceipt{}, err
		}
		return EffectReceipt{}, ErrEffectVersion
	}
	resourceRevision++
	if _, err := tx.Exec(ctx, `UPDATE effects.sandbox_effect_state SET state = $2, resource_revision = $3, updated_at = clock_timestamp() WHERE resource_id = $1`, input.ResourceID, input.State, resourceRevision); err != nil {
		return EffectReceipt{}, err
	}
	receipt, _ := json.Marshal(map[string]any{"resource_id": input.ResourceID, "resource_revision": resourceRevision, "logical_effect_key": input.LogicalEffectKey})
	var storedReceipt []byte
	if err := tx.QueryRow(ctx, `
		INSERT INTO effects.effect_records (workflow_id, logical_effect_key, argument_hash, attempt_number, grant_scope_hash, outcome, receipt)
		VALUES ($1, $2, $3, $4, $5, 'APPLIED', $6)
		RETURNING receipt`, input.WorkflowID, input.LogicalEffectKey,
		input.ArgumentHash, input.AttemptNumber, nullableString(input.GrantScopeHash), receipt).Scan(&storedReceipt); err != nil {
		return EffectReceipt{}, err
	}
	receipt = storedReceipt
	if err := recordEffectCallTx(ctx, tx, input, "APPLIED", receipt); err != nil {
		return EffectReceipt{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return EffectReceipt{}, err
	}
	return EffectReceipt{WorkflowID: input.WorkflowID, LogicalEffectKey: input.LogicalEffectKey,
		ArgumentHash: input.ArgumentHash, ResourceID: input.ResourceID, ResourceRevision: resourceRevision,
		Receipt: receipt}, nil
}

func recordEffectCallTx(ctx context.Context, tx pgx.Tx, input EffectApplyInput, outcome string, receipt []byte) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO effects.effect_call_attempts
			(call_id, workflow_id, logical_effect_key, argument_hash, attempt_number, request_id, outcome, receipt)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (workflow_id, request_id) DO NOTHING`, NewID(), input.WorkflowID,
		input.LogicalEffectKey, input.ArgumentHash, input.AttemptNumber, input.RequestID, outcome, receipt)
	return err
}

func (s *Store) LookupEffect(ctx context.Context, workflowID, logicalEffectKey string) (EffectRecord, error) {
	var record EffectRecord
	var receipt []byte
	err := s.pool.QueryRow(ctx, `
		SELECT workflow_id, logical_effect_key, argument_hash, attempt_number,
			COALESCE(grant_scope_hash,''), outcome, receipt, created_at, updated_at
		FROM effects.effect_records WHERE workflow_id = $1 AND logical_effect_key = $2`,
		workflowID, logicalEffectKey).Scan(&record.WorkflowID, &record.LogicalEffectKey,
		&record.ArgumentHash, &record.AttemptNumber, &record.GrantScopeHash, &record.Outcome,
		&receipt, &record.CreatedAt, &record.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return EffectRecord{}, ErrEffectNotFound
	}
	if err != nil {
		return EffectRecord{}, err
	}
	record.Receipt = receipt
	return record, nil
}

func (s *Store) RecordUnknownEffect(ctx context.Context, workflowID, logicalEffectKey, argumentHash, grantScopeHash string, attemptNumber int64) (EffectRecord, error) {
	if workflowID == "" || logicalEffectKey == "" || argumentHash == "" || attemptNumber <= 0 {
		return EffectRecord{}, errors.New("unknown effect identity is required")
	}
	var record EffectRecord
	var receipt []byte
	err := s.pool.QueryRow(ctx, `
		INSERT INTO effects.effect_records
			(workflow_id, logical_effect_key, argument_hash, attempt_number, grant_scope_hash, outcome)
		VALUES ($1, $2, $3, $4, $5, 'OUTCOME_UNKNOWN')
		ON CONFLICT (workflow_id, logical_effect_key) DO UPDATE SET updated_at = clock_timestamp()
		RETURNING workflow_id, logical_effect_key, argument_hash, attempt_number,
			COALESCE(grant_scope_hash,''), outcome, receipt, created_at, updated_at`,
		workflowID, logicalEffectKey, argumentHash, attemptNumber, nullableString(grantScopeHash)).Scan(
		&record.WorkflowID, &record.LogicalEffectKey, &record.ArgumentHash, &record.AttemptNumber,
		&record.GrantScopeHash, &record.Outcome, &receipt, &record.CreatedAt, &record.UpdatedAt)
	if err != nil {
		return EffectRecord{}, err
	}
	record.Receipt = receipt
	return record, nil
}

func (s *Store) ResolveUnknownEffect(ctx context.Context, actorID, workflowID, logicalEffectKey, argumentHash string, receipt json.RawMessage, abandon bool) error {
	if actorID == "" || workflowID == "" || logicalEffectKey == "" || argumentHash == "" {
		return errors.New("effect resolution identity is required")
	}
	if !abandon && (len(receipt) == 0 || !json.Valid(receipt)) {
		return errors.New("verified effect receipt is required")
	}
	if abandon {
		receipt = nil
	}
	outcome := "APPLIED"
	if abandon {
		outcome = "OUTCOME_UNKNOWN"
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var existingOutcome string
	if err := tx.QueryRow(ctx, `
		SELECT outcome FROM effects.effect_records
		WHERE workflow_id = $1 AND logical_effect_key = $2 AND argument_hash = $3
		FOR UPDATE`, workflowID, logicalEffectKey, argumentHash).Scan(&existingOutcome); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrEffectNotFound
		}
		return err
	}
	if existingOutcome != "OUTCOME_UNKNOWN" {
		return ErrEffectConflict
	}
	if _, err := tx.Exec(ctx, `
		UPDATE effects.effect_records SET outcome = $4,
			receipt = CASE WHEN $4 = 'APPLIED' THEN $5 ELSE receipt END,
			updated_at = clock_timestamp()
		WHERE workflow_id = $1 AND logical_effect_key = $2 AND argument_hash = $3`,
		workflowID, logicalEffectKey, argumentHash, outcome, receipt); err != nil {
		return err
	}
	disposition := "CONFIRMED_APPLIED"
	if abandon {
		disposition = "ABANDONED_UNKNOWN"
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO effects.effect_resolution_audit
			(resolution_id, workflow_id, logical_effect_key, argument_hash, actor_id, disposition, receipt)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		NewID(), workflowID, logicalEffectKey, argumentHash, actorID, disposition, receipt); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) ListEffectRecords(ctx context.Context, workflowID string) ([]EffectRecord, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT workflow_id, logical_effect_key, argument_hash, attempt_number,
			COALESCE(grant_scope_hash,''), outcome, receipt, created_at, updated_at
		FROM effects.effect_records WHERE workflow_id = $1 ORDER BY logical_effect_key`, workflowID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var records []EffectRecord
	for rows.Next() {
		var record EffectRecord
		if err := rows.Scan(&record.WorkflowID, &record.LogicalEffectKey, &record.ArgumentHash,
			&record.AttemptNumber, &record.GrantScopeHash, &record.Outcome, &record.Receipt,
			&record.CreatedAt, &record.UpdatedAt); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, rows.Err()
}

func (s *Store) ListEffectCallAttempts(ctx context.Context, workflowID string) ([]EffectCallAttempt, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT call_id::text, workflow_id, logical_effect_key, argument_hash,
			attempt_number, request_id, outcome, receipt, created_at
		FROM effects.effect_call_attempts WHERE workflow_id = $1 ORDER BY created_at`, workflowID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var calls []EffectCallAttempt
	for rows.Next() {
		var call EffectCallAttempt
		if err := rows.Scan(&call.CallID, &call.WorkflowID, &call.LogicalEffectKey,
			&call.ArgumentHash, &call.AttemptNumber, &call.RequestID, &call.Outcome,
			&call.Receipt, &call.CreatedAt); err != nil {
			return nil, err
		}
		calls = append(calls, call)
	}
	return calls, rows.Err()
}
