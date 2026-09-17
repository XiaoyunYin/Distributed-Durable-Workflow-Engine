package state

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"durable-agent-execution-engine/internal/partition"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// NewFromURL opens a pool and verifies that PostgreSQL is reachable. The
// migration runner owns schema installation; the repository only operates on
// the already versioned schema.
func NewFromURL(ctx context.Context, databaseURL string) (*Store, error) {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database URL: %w", err)
	}
	config.MaxConns = 8
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("open PostgreSQL pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping PostgreSQL: %w", err)
	}
	return New(pool), nil
}

func NewID() string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		panic(fmt.Sprintf("generate identifier: %v", err))
	}
	raw[6] = (raw[6] & 0x0f) | 0x40
	raw[8] = (raw[8] & 0x3f) | 0x80
	return hex.EncodeToString(raw[:])[:8] + "-" +
		hex.EncodeToString(raw[:])[8:12] + "-" +
		hex.EncodeToString(raw[:])[12:16] + "-" +
		hex.EncodeToString(raw[:])[16:20] + "-" +
		hex.EncodeToString(raw[:])[20:]
}

func (s *Store) CreateDefinition(ctx context.Context, input DefinitionInput) error {
	if input.DefinitionID == "" || input.Version <= 0 || input.DefinitionHash == "" {
		return errors.New("definition ID, positive version, and hash are required")
	}
	effectClasses, err := decodeEffectClasses(input.EffectClasses)
	if err != nil {
		return err
	}
	if len(effectClasses) == 0 {
		return ErrMissingEffectClass
	}
	graph := input.Graph
	if len(graph) == 0 {
		graph = json.RawMessage(`{}`)
	}
	activityVersions := input.ActivityVersions
	if len(activityVersions) == 0 {
		activityVersions = json.RawMessage(`{}`)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	_, err = tx.Exec(ctx, `
		INSERT INTO engine.workflow_definitions
			(definition_id, version, definition_hash, graph, activity_versions, effect_classes)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (definition_id, version) DO NOTHING`,
		input.DefinitionID, input.Version, input.DefinitionHash, graph, activityVersions, input.EffectClasses)
	if err != nil {
		return fmt.Errorf("insert workflow definition: %w", err)
	}
	var existingHash string
	var existingEffectClasses []byte
	if err := tx.QueryRow(ctx, `
		SELECT definition_hash, effect_classes
		FROM engine.workflow_definitions
		WHERE definition_id = $1 AND version = $2`, input.DefinitionID, input.Version).Scan(&existingHash, &existingEffectClasses); err != nil {
		return fmt.Errorf("read workflow definition: %w", err)
	}
	var storedEffectClasses map[string]EffectClass
	if err := json.Unmarshal(existingEffectClasses, &storedEffectClasses); err != nil {
		return fmt.Errorf("decode stored effect classes: %w", err)
	}
	if existingHash != input.DefinitionHash || !reflect.DeepEqual(storedEffectClasses, effectClasses) {
		return fmt.Errorf("definition %s version %d already has a different hash", input.DefinitionID, input.Version)
	}
	return tx.Commit(ctx)
}

func (s *Store) CreateWorkflow(ctx context.Context, input CreateWorkflowInput) (CreateWorkflowResult, error) {
	if input.WorkflowID == "" || input.Namespace == "" || input.SubmissionKey == "" ||
		input.SubmissionPayloadHash == "" || input.DefinitionID == "" || input.InitialNodeID == "" {
		return CreateWorkflowResult{}, errors.New("workflow identity and initial node are required")
	}
	computedPartition, err := partition.ID(input.WorkflowID)
	if err != nil {
		return CreateWorkflowResult{}, err
	}
	if input.ActorID == "" {
		input.ActorID = "client"
	}
	if len(input.InitialInput) == 0 {
		input.InitialInput = json.RawMessage(`{}`)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return CreateWorkflowResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var workflow Workflow
	// Resolve an existing idempotency record before validating the definition.
	// This preserves the stable conflict response for a retry whose execution
	// meaning changed, even if the changed definition is not present.
	err = tx.QueryRow(ctx, `
		SELECT workflow_id, namespace, submission_key, submission_payload_hash,
			definition_id, definition_version, partition_id, state, revision,
			created_at, updated_at
		FROM engine.workflow_executions
		WHERE namespace = $1 AND submission_key = $2
		FOR UPDATE`, input.Namespace, input.SubmissionKey).Scan(
		&workflow.WorkflowID, &workflow.Namespace, &workflow.SubmissionKey,
		&workflow.PayloadHash, &workflow.DefinitionID, &workflow.DefinitionVersion,
		&workflow.PartitionID, &workflow.State, &workflow.Revision,
		&workflow.CreatedAt, &workflow.UpdatedAt)
	if err == nil {
		if workflow.PayloadHash != input.SubmissionPayloadHash {
			return CreateWorkflowResult{}, ErrSubmissionConflict
		}
		if err := tx.Commit(ctx); err != nil {
			return CreateWorkflowResult{}, err
		}
		return CreateWorkflowResult{Workflow: workflow, Created: false}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return CreateWorkflowResult{}, fmt.Errorf("read existing workflow by submission: %w", err)
	}

	// A workflow ID is independently unique. Check it before the insert so a
	// client mistake is not exposed as a database 500.
	var existingNamespace, existingSubmissionKey string
	err = tx.QueryRow(ctx, `
		SELECT namespace, submission_key
		FROM engine.workflow_executions
		WHERE workflow_id = $1
		FOR UPDATE`, input.WorkflowID).Scan(&existingNamespace, &existingSubmissionKey)
	if err == nil {
		return CreateWorkflowResult{}, ErrWorkflowIDConflict
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return CreateWorkflowResult{}, fmt.Errorf("read existing workflow ID: %w", err)
	}

	// The repository, not the HTTP caller, is the authority for the initial
	// node's declared effect class. This also turns FK/invalid-node mistakes
	// into typed client errors before creating any durable rows.
	var effectClass *string
	err = tx.QueryRow(ctx, `
		SELECT effect_classes ->> $3
		FROM engine.workflow_definitions
		WHERE definition_id = $1 AND version = $2`,
		input.DefinitionID, input.DefinitionVersion, input.InitialNodeID).Scan(&effectClass)
	if errors.Is(err, pgx.ErrNoRows) {
		return CreateWorkflowResult{}, ErrDefinitionNotFound
	}
	if err != nil {
		return CreateWorkflowResult{}, fmt.Errorf("read workflow definition: %w", err)
	}
	if effectClass == nil || *effectClass == "" {
		return CreateWorkflowResult{}, ErrUnknownNode
	}
	if int16(computedPartition) != input.PartitionID {
		return CreateWorkflowResult{}, fmt.Errorf("%w: got %d, want %d", ErrPartitionMismatch, input.PartitionID, computedPartition)
	}

	err = tx.QueryRow(ctx, `
		INSERT INTO engine.workflow_executions
			(workflow_id, namespace, submission_key, submission_payload_hash,
			 definition_id, definition_version, partition_id, state, revision)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 1)
		ON CONFLICT (namespace, submission_key) DO NOTHING
		RETURNING workflow_id, namespace, submission_key, submission_payload_hash,
			definition_id, definition_version, partition_id, state, revision,
			created_at, updated_at`,
		input.WorkflowID, input.Namespace, input.SubmissionKey, input.SubmissionPayloadHash,
		input.DefinitionID, input.DefinitionVersion, input.PartitionID, StateRunnable,
	).Scan(&workflow.WorkflowID, &workflow.Namespace, &workflow.SubmissionKey,
		&workflow.PayloadHash, &workflow.DefinitionID, &workflow.DefinitionVersion,
		&workflow.PartitionID, &workflow.State, &workflow.Revision,
		&workflow.CreatedAt, &workflow.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.QueryRow(ctx, `
			SELECT workflow_id, namespace, submission_key, submission_payload_hash,
				definition_id, definition_version, partition_id, state, revision,
				created_at, updated_at
			FROM engine.workflow_executions
			WHERE namespace = $1 AND submission_key = $2
			FOR UPDATE`, input.Namespace, input.SubmissionKey).Scan(
			&workflow.WorkflowID, &workflow.Namespace, &workflow.SubmissionKey,
			&workflow.PayloadHash, &workflow.DefinitionID, &workflow.DefinitionVersion,
			&workflow.PartitionID, &workflow.State, &workflow.Revision,
			&workflow.CreatedAt, &workflow.UpdatedAt); err != nil {
			return CreateWorkflowResult{}, fmt.Errorf("read existing workflow: %w", err)
		}
		if workflow.PayloadHash != input.SubmissionPayloadHash {
			return CreateWorkflowResult{}, ErrSubmissionConflict
		}
		if err := tx.Commit(ctx); err != nil {
			return CreateWorkflowResult{}, err
		}
		return CreateWorkflowResult{Workflow: workflow, Created: false}, nil
	}
	if err != nil {
		var constraintErr *pgconn.PgError
		if errors.As(err, &constraintErr) && constraintErr.Code == "23505" {
			return CreateWorkflowResult{}, ErrWorkflowIDConflict
		}
		return CreateWorkflowResult{}, fmt.Errorf("insert workflow: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO engine.node_instances
			(workflow_id, node_id, iteration, state, input)
		VALUES ($1, $2, 0, $3, $4)`,
		input.WorkflowID, input.InitialNodeID, StateRunnable, input.InitialInput); err != nil {
		return CreateWorkflowResult{}, fmt.Errorf("insert initial node: %w", err)
	}
	if err := insertHistory(ctx, tx, workflow.WorkflowID, 1, "client", input.ActorID,
		nil, input.InitialNodeID, nil, nil, nil, StateRunnable, "WORKFLOW_CREATED"); err != nil {
		return CreateWorkflowResult{}, err
	}
	if err := insertOutbox(ctx, tx, workflow.WorkflowID, 1, "workflow.created", json.RawMessage(fmt.Sprintf(`{"workflow_id":%q}`, workflow.WorkflowID))); err != nil {
		return CreateWorkflowResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return CreateWorkflowResult{}, fmt.Errorf("commit workflow: %w", err)
	}
	return CreateWorkflowResult{Workflow: workflow, Created: true}, nil
}

func (s *Store) GetWorkflow(ctx context.Context, workflowID string) (Workflow, error) {
	workflow, err := scanWorkflow(s.pool.QueryRow(ctx, `
		SELECT workflow_id, namespace, submission_key, submission_payload_hash,
			definition_id, definition_version, partition_id, state, revision,
			created_at, updated_at
		FROM engine.workflow_executions
		WHERE workflow_id = $1`, workflowID))
	if errors.Is(err, pgx.ErrNoRows) {
		return Workflow{}, fmt.Errorf("%w: %s", ErrWorkflowNotFound, workflowID)
	}
	return workflow, err
}

func (s *Store) GetAttempt(ctx context.Context, workflowID, nodeID string, iteration int, attemptNumber int64) (Attempt, error) {
	var attempt Attempt
	var claimToken *string
	var workerID *string
	var workerRequestID *string
	var deadline *time.Time
	var effectKey *string
	var grantScope *string
	err := s.pool.QueryRow(ctx, `
		SELECT workflow_id, node_id, iteration, attempt_number, state, effect_class,
			claim_token::text, worker_id, worker_request_id, heartbeat_deadline,
			logical_effect_key, grant_scope_hash, outcome_disposition, is_current
		FROM engine.activity_attempts
		WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3 AND attempt_number = $4`,
		workflowID, nodeID, iteration, attemptNumber).Scan(
		&attempt.WorkflowID, &attempt.NodeID, &attempt.Iteration, &attempt.AttemptNumber,
		&attempt.State, &attempt.EffectClass, &claimToken, &workerID, &workerRequestID,
		&deadline, &effectKey, &grantScope, &attempt.OutcomeDisposition, &attempt.IsCurrent)
	if err != nil {
		return Attempt{}, err
	}
	if claimToken != nil {
		attempt.ClaimToken = *claimToken
	}
	if workerID != nil {
		attempt.WorkerID = *workerID
	}
	if workerRequestID != nil {
		attempt.WorkerRequestID = *workerRequestID
	}
	if deadline != nil {
		attempt.HeartbeatDeadline = deadline
	}
	if effectKey != nil {
		attempt.LogicalEffectKey = *effectKey
	}
	if grantScope != nil {
		attempt.GrantScopeHash = *grantScope
	}
	return attempt, nil
}

func (s *Store) HistoryCount(ctx context.Context, workflowID string) (int, error) {
	var count int
	err := s.pool.QueryRow(ctx, `
		SELECT count(*) FROM engine.transition_history WHERE workflow_id = $1`, workflowID).Scan(&count)
	return count, err
}

func (s *Store) ListHistory(ctx context.Context, workflowID string, afterRevision int64, limit int) ([]TransitionRecord, error) {
	if afterRevision < 0 {
		return nil, errors.New("history revision cursor must not be negative")
	}
	if limit <= 0 || limit > 1000 {
		return nil, errors.New("history limit must be between 1 and 1000")
	}
	if _, err := s.GetWorkflow(ctx, workflowID); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `
		SELECT transition_id::text, workflow_id, revision, actor_kind, actor_id,
			scheduler_epoch, node_id, iteration, attempt_number, old_state,
			new_state, reason, created_at
		FROM engine.transition_history
		WHERE workflow_id = $1 AND revision > $2
		ORDER BY revision ASC
		LIMIT $3`, workflowID, afterRevision, limit)
	if err != nil {
		return nil, fmt.Errorf("query workflow history: %w", err)
	}
	defer rows.Close()
	history := make([]TransitionRecord, 0, limit)
	for rows.Next() {
		var record TransitionRecord
		var oldState *string
		if err := rows.Scan(&record.TransitionID, &record.WorkflowID, &record.Revision,
			&record.ActorKind, &record.ActorID, &record.SchedulerEpoch, &record.NodeID,
			&record.Iteration, &record.AttemptNumber, &oldState, &record.NewState,
			&record.Reason, &record.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan workflow history: %w", err)
		}
		if oldState != nil {
			value := WorkflowState(*oldState)
			record.OldState = &value
		}
		history = append(history, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read workflow history: %w", err)
	}
	return history, nil
}

func scanWorkflow(row pgx.Row) (Workflow, error) {
	var workflow Workflow
	err := row.Scan(&workflow.WorkflowID, &workflow.Namespace, &workflow.SubmissionKey,
		&workflow.PayloadHash, &workflow.DefinitionID, &workflow.DefinitionVersion,
		&workflow.PartitionID, &workflow.State, &workflow.Revision,
		&workflow.CreatedAt, &workflow.UpdatedAt)
	return workflow, err
}

func (s *Store) AcquireLease(ctx context.Context, partitionID int16, ownerID string, ttl time.Duration) (Lease, bool, error) {
	if ttl <= 0 || ownerID == "" {
		return Lease{}, false, errors.New("owner ID and positive lease TTL are required")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Lease{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `
		INSERT INTO engine.partition_leases (partition_id)
		VALUES ($1)
		ON CONFLICT (partition_id) DO NOTHING`, partitionID); err != nil {
		return Lease{}, false, fmt.Errorf("ensure partition lease: %w", err)
	}
	var currentOwner *string
	var currentExpiry *time.Time
	var epoch int64
	var databaseNow time.Time
	if err := tx.QueryRow(ctx, `
		SELECT owner_id::text, epoch, lease_expires_at, clock_timestamp()
		FROM engine.partition_leases
		WHERE partition_id = $1
		FOR UPDATE`, partitionID).Scan(&currentOwner, &epoch, &currentExpiry, &databaseNow); err != nil {
		return Lease{}, false, fmt.Errorf("lock partition lease: %w", err)
	}
	if currentOwner != nil && currentExpiry != nil && currentExpiry.After(databaseNow) && *currentOwner != ownerID {
		lease := Lease{PartitionID: partitionID, OwnerID: *currentOwner, Epoch: epoch, LeaseExpiresAt: *currentExpiry}
		if err := tx.Commit(ctx); err != nil {
			return Lease{}, false, err
		}
		return lease, false, nil
	}
	nextEpoch := epoch + 1
	if currentOwner != nil && *currentOwner == ownerID && currentExpiry != nil && currentExpiry.After(databaseNow) {
		nextEpoch = epoch
	}
	lease := Lease{PartitionID: partitionID, OwnerID: ownerID, Epoch: nextEpoch, LeaseExpiresAt: databaseNow.Add(ttl)}
	if _, err := tx.Exec(ctx, `
		UPDATE engine.partition_leases
		SET owner_id = $2, epoch = $3, lease_expires_at = $4, updated_at = clock_timestamp()
		WHERE partition_id = $1`, partitionID, lease.OwnerID, lease.Epoch, lease.LeaseExpiresAt); err != nil {
		return Lease{}, false, fmt.Errorf("acquire partition lease: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Lease{}, false, err
	}
	return lease, true, nil
}

func (s *Store) ReleaseLease(ctx context.Context, ref LeaseRef) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := lockLease(ctx, tx, ref); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE engine.partition_leases
		SET owner_id = NULL, lease_expires_at = NULL, updated_at = clock_timestamp()
		WHERE partition_id = $1`, ref.PartitionID); err != nil {
		return fmt.Errorf("release partition lease: %w", err)
	}
	return tx.Commit(ctx)
}

func lockLease(ctx context.Context, tx pgx.Tx, ref LeaseRef) (Lease, error) {
	var owner *string
	var expiry *time.Time
	var epoch int64
	var databaseNow time.Time
	if err := tx.QueryRow(ctx, `
		SELECT owner_id::text, epoch, lease_expires_at, clock_timestamp()
		FROM engine.partition_leases
		WHERE partition_id = $1
		FOR UPDATE`, ref.PartitionID).Scan(&owner, &epoch, &expiry, &databaseNow); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Lease{}, ErrLeaseNotOwned
		}
		return Lease{}, fmt.Errorf("lock partition lease: %w", err)
	}
	if owner == nil || expiry == nil || *owner != ref.OwnerID || epoch != ref.Epoch || !expiry.After(databaseNow) {
		return Lease{}, ErrLeaseNotOwned
	}
	return Lease{PartitionID: ref.PartitionID, OwnerID: *owner, Epoch: epoch, LeaseExpiresAt: *expiry}, nil
}

func (s *Store) ApplyOwnerTransition(ctx context.Context, input OwnerTransitionInput) error {
	if input.WorkflowID == "" || input.ActorID == "" || input.Reason == "" {
		return errors.New("workflow, actor, and reason are required")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := lockLease(ctx, tx, input.Lease); err != nil {
		return err
	}
	var state WorkflowState
	var revision int64
	var partitionID int16
	if err := tx.QueryRow(ctx, `
		SELECT state, revision, partition_id
		FROM engine.workflow_executions
		WHERE workflow_id = $1
		FOR UPDATE`, input.WorkflowID).Scan(&state, &revision, &partitionID); err != nil {
		return fmt.Errorf("lock workflow: %w", err)
	}
	if partitionID != input.Lease.PartitionID {
		return fmt.Errorf("workflow partition %d does not match lease %d", partitionID, input.Lease.PartitionID)
	}
	if revision != input.ExpectedRevision {
		return fmt.Errorf("%w: expected %d, got %d", ErrRevisionConflict, input.ExpectedRevision, revision)
	}
	if !legalTransition(state, input.NewState) {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, state, input.NewState)
	}
	var currentAttemptNumber *int64
	if input.NodeID != "" {
		if err := tx.QueryRow(ctx, `
			SELECT current_attempt_number
			FROM engine.node_instances
			WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3
			FOR UPDATE`, input.WorkflowID, input.NodeID, input.Iteration).Scan(&currentAttemptNumber); err != nil {
			return fmt.Errorf("lock node: %w", err)
		}
	}
	if input.NewState == StateCanceled && state == StateWaitingActivity {
		if input.NodeID == "" {
			return errors.New("cancellation of active work requires a node")
		}
		if currentAttemptNumber != nil {
			if input.AttemptNumber != nil && *input.AttemptNumber != *currentAttemptNumber {
				return ErrAttemptNotCurrent
			}
			attemptNumber := *currentAttemptNumber
			input.AttemptNumber = &attemptNumber
		}
	}
	var retryTimerID *string
	if state == StateWaitingTimer && input.NewState == StateRunnable {
		if input.NodeID == "" {
			return fmt.Errorf("%w: retry timer requires a node", ErrInvalidTransition)
		}
		var timerID string
		var dueAt time.Time
		var consumedAt *time.Time
		if err := tx.QueryRow(ctx, `
			SELECT timer_id::text, due_at, consumed_at
			FROM engine.timers
			WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3
				AND purpose = 'RETRY_BACKOFF' AND consumed_at IS NULL
			ORDER BY due_at ASC
			LIMIT 1
			FOR UPDATE`, input.WorkflowID, input.NodeID, input.Iteration).Scan(&timerID, &dueAt, &consumedAt); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("%w: no pending retry timer", ErrInvalidTransition)
			}
			return fmt.Errorf("lock retry timer: %w", err)
		}
		var databaseNow time.Time
		if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&databaseNow); err != nil {
			return fmt.Errorf("read timer clock: %w", err)
		}
		if dueAt.After(databaseNow) {
			return fmt.Errorf("%w: retry timer is not due", ErrInvalidTransition)
		}
		retryTimerID = &timerID
	}
	if input.AttemptNumber != nil {
		var ignored int64
		if err := tx.QueryRow(ctx, `
			SELECT attempt_number
			FROM engine.activity_attempts
			WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3 AND attempt_number = $4
			FOR UPDATE`, input.WorkflowID, input.NodeID, input.Iteration, *input.AttemptNumber).Scan(&ignored); err != nil {
			return fmt.Errorf("lock attempt: %w", err)
		}
	}
	nodeState := input.NodeState
	if nodeState == nil && input.NodeID != "" {
		nodeState = &input.NewState
	}
	newRevision := revision + 1
	if _, err := tx.Exec(ctx, `
		UPDATE engine.workflow_executions
		SET state = $2, revision = $3, updated_at = clock_timestamp()
		WHERE workflow_id = $1`, input.WorkflowID, input.NewState, newRevision); err != nil {
		return fmt.Errorf("update workflow transition: %w", err)
	}
	if input.NewState == StateCanceled && state == StateWaitingActivity && currentAttemptNumber != nil {
		if _, err := tx.Exec(ctx, `
			UPDATE engine.activity_attempts
			SET state = 'CANCELED', is_current = false,
				outcome_disposition = CASE
					WHEN state = 'CLAIMED' AND effect_class <> 'PURE_ACTIVITY' THEN 'OUTCOME_UNKNOWN'
					ELSE 'NONE'
				END,
				updated_at = clock_timestamp()
			WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3 AND attempt_number = $4
				AND is_current`, input.WorkflowID, input.NodeID, input.Iteration, *currentAttemptNumber); err != nil {
			return fmt.Errorf("settle canceled attempt: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE engine.node_instances
			SET state = $4, current_attempt_number = NULL, revision = revision + 1,
				updated_at = clock_timestamp()
			WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3`,
			input.WorkflowID, input.NodeID, input.Iteration, StateCanceled); err != nil {
			return fmt.Errorf("settle canceled node: %w", err)
		}
	} else if nodeState != nil {
		if _, err := tx.Exec(ctx, `
			UPDATE engine.node_instances
			SET state = $4, revision = revision + 1, updated_at = clock_timestamp()
			WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3`,
			input.WorkflowID, input.NodeID, input.Iteration, *nodeState); err != nil {
			return fmt.Errorf("update node transition: %w", err)
		}
	}
	if retryTimerID != nil {
		if _, err := tx.Exec(ctx, `
			UPDATE engine.timers
			SET consumed_at = clock_timestamp()
			WHERE timer_id = $1 AND consumed_at IS NULL`, *retryTimerID); err != nil {
			return fmt.Errorf("consume retry timer: %w", err)
		}
	}
	if err := insertHistory(ctx, tx, input.WorkflowID, newRevision, "scheduler", input.ActorID,
		&input.Lease.Epoch, input.NodeID, nullableIteration(input.NodeID, input.Iteration), input.AttemptNumber,
		&state, input.NewState, input.Reason); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ConsumeResult is the lease-owner boundary between a recorded worker result
// and downstream workflow progress. It is the only repository operation that
// clears the node's current attempt or permits a result-dependent workflow
// transition.
func (s *Store) ConsumeResult(ctx context.Context, input ConsumeResultInput) (ConsumeResult, error) {
	if input.WorkflowID == "" || input.NodeID == "" || input.ActorID == "" || input.AttemptNumber <= 0 {
		return ConsumeResult{}, errors.New("result-consumption identity is required")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ConsumeResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := lockLease(ctx, tx, input.Lease); err != nil {
		return ConsumeResult{}, err
	}
	var workflow Workflow
	if err := tx.QueryRow(ctx, `
		SELECT workflow_id, namespace, submission_key, submission_payload_hash,
			definition_id, definition_version, partition_id, state, revision,
			created_at, updated_at
		FROM engine.workflow_executions
		WHERE workflow_id = $1
		FOR UPDATE`, input.WorkflowID).Scan(&workflow.WorkflowID, &workflow.Namespace,
		&workflow.SubmissionKey, &workflow.PayloadHash, &workflow.DefinitionID,
		&workflow.DefinitionVersion, &workflow.PartitionID, &workflow.State,
		&workflow.Revision, &workflow.CreatedAt, &workflow.UpdatedAt); err != nil {
		return ConsumeResult{}, fmt.Errorf("lock workflow for result consumption: %w", err)
	}
	if workflow.PartitionID != input.Lease.PartitionID {
		return ConsumeResult{}, fmt.Errorf("workflow partition %d does not match lease %d", workflow.PartitionID, input.Lease.PartitionID)
	}
	if workflow.Revision != input.ExpectedRevision {
		return ConsumeResult{}, fmt.Errorf("%w: expected %d, got %d", ErrRevisionConflict, input.ExpectedRevision, workflow.Revision)
	}
	if workflow.State != StateWaitingActivity {
		return ConsumeResult{}, fmt.Errorf("workflow state %s cannot consume a result", workflow.State)
	}
	var nodeState WorkflowState
	var currentAttempt *int64
	if err := tx.QueryRow(ctx, `
		SELECT state, current_attempt_number
		FROM engine.node_instances
		WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3
		FOR UPDATE`, input.WorkflowID, input.NodeID, input.Iteration).Scan(&nodeState, &currentAttempt); err != nil {
		return ConsumeResult{}, fmt.Errorf("lock node for result consumption: %w", err)
	}
	if currentAttempt == nil || *currentAttempt != input.AttemptNumber {
		return ConsumeResult{}, ErrResultNotConsumable
	}
	var attemptState AttemptState
	var attemptCurrent bool
	var result []byte
	if err := tx.QueryRow(ctx, `
		SELECT state, is_current, result
		FROM engine.activity_attempts
		WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3 AND attempt_number = $4
		FOR UPDATE`, input.WorkflowID, input.NodeID, input.Iteration, input.AttemptNumber).Scan(
		&attemptState, &attemptCurrent, &result); err != nil {
		return ConsumeResult{}, fmt.Errorf("lock attempt for result consumption: %w", err)
	}
	if attemptCurrent || !isTerminalAttempt(attemptState) || len(result) == 0 {
		return ConsumeResult{}, ErrResultNotConsumable
	}
	var newNodeState WorkflowState
	switch attemptState {
	case AttemptSucceeded:
		if input.NewWorkflowState != StateRunnable && input.NewWorkflowState != StateSucceeded {
			return ConsumeResult{}, fmt.Errorf("%w: successful result cannot advance to %s", ErrInvalidTransition, input.NewWorkflowState)
		}
		newNodeState = StateSucceeded
	case AttemptFailedRetryable:
		if input.NewWorkflowState != StateWaitingTimer || input.RetryDueAt == nil {
			return ConsumeResult{}, fmt.Errorf("%w: retryable result requires WAITING_TIMER and a due time", ErrInvalidTransition)
		}
		newNodeState = StateWaitingTimer
	case AttemptFailedFinal:
		if input.NewWorkflowState != StateFailed {
			return ConsumeResult{}, fmt.Errorf("%w: final failure must advance to FAILED", ErrInvalidTransition)
		}
		newNodeState = StateFailed
	default:
		return ConsumeResult{}, ErrResultNotConsumable
	}
	newRevision := workflow.Revision + 1
	if _, err := tx.Exec(ctx, `
		UPDATE engine.node_instances
		SET state = $4, current_attempt_number = NULL,
			accepted_result = CASE WHEN $4 = 'SUCCEEDED' THEN $5::jsonb ELSE accepted_result END,
			retry_count = CASE WHEN $4 = 'WAITING_TIMER' THEN retry_count + 1 ELSE retry_count END,
			revision = revision + 1, updated_at = clock_timestamp()
		WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3`,
		input.WorkflowID, input.NodeID, input.Iteration, newNodeState, result); err != nil {
		return ConsumeResult{}, fmt.Errorf("consume node result: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE engine.workflow_executions
		SET state = $2, revision = $3, updated_at = clock_timestamp()
		WHERE workflow_id = $1`, input.WorkflowID, input.NewWorkflowState, newRevision); err != nil {
		return ConsumeResult{}, fmt.Errorf("advance workflow after result: %w", err)
	}
	if err := insertHistory(ctx, tx, input.WorkflowID, newRevision, "scheduler", input.ActorID,
		&input.Lease.Epoch, input.NodeID, &input.Iteration, &input.AttemptNumber,
		&workflow.State, input.NewWorkflowState, "RESULT_CONSUMED"); err != nil {
		return ConsumeResult{}, err
	}
	if attemptState == AttemptFailedRetryable {
		if _, err := tx.Exec(ctx, `
			INSERT INTO engine.timers
				(timer_id, workflow_id, node_id, iteration, due_at, purpose, owning_revision)
			VALUES ($1, $2, $3, $4, $5, 'RETRY_BACKOFF', $6)`,
			NewID(), input.WorkflowID, input.NodeID, input.Iteration, *input.RetryDueAt, newRevision); err != nil {
			return ConsumeResult{}, fmt.Errorf("create retry timer: %w", err)
		}
	}
	payload := json.RawMessage(fmt.Sprintf(`{"workflow_id":%q,"node_id":%q,"iteration":%d,"attempt_number":%d,"attempt_state":%q}`,
		input.WorkflowID, input.NodeID, input.Iteration, input.AttemptNumber, attemptState))
	if err := insertOutbox(ctx, tx, input.WorkflowID, newRevision, "workflow.result_consumed", payload); err != nil {
		return ConsumeResult{}, err
	}
	workflow.State = input.NewWorkflowState
	workflow.Revision = newRevision
	workflow.UpdatedAt = time.Now().UTC()
	if err := tx.Commit(ctx); err != nil {
		return ConsumeResult{}, err
	}
	return ConsumeResult{Workflow: workflow, NodeState: newNodeState, AttemptState: attemptState}, nil
}

func legalTransition(from, to WorkflowState) bool {
	allowed := map[WorkflowState]map[WorkflowState]bool{
		StateRunnable: {
			StateWaitingActivity: true, StateWaitingApproval: true, StateWaitingTimer: true,
			StatePausedUnsupportedVersion: true, StateSucceeded: true, StateCanceled: true,
		},
		StateWaitingActivity: {
			StateReconciliationRequired: true, StateCanceled: true,
		},
		StateWaitingTimer: {
			StateRunnable: true, StateCanceled: true,
		},
		StateWaitingApproval: {
			StateRunnable: true, StateRejected: true, StateCanceled: true,
		},
		StatePausedUnsupportedVersion: {
			StateRunnable: true, StateCanceled: true, StateAbandoned: true,
		},
		StateReconciliationRequired: {
			StateRunnable: true, StateCanceled: true, StateAbandoned: true,
		},
	}
	return allowed[from][to]
}

func nullableIteration(nodeID string, iteration int) *int {
	if nodeID == "" {
		return nil
	}
	return &iteration
}

func (s *Store) CreateAttempt(ctx context.Context, input AttemptInput) (Attempt, error) {
	if input.WorkflowID == "" || input.NodeID == "" || input.ActorID == "" {
		return Attempt{}, errors.New("attempt identity and actor are required")
	}
	if input.EffectClass != EffectPure && input.EffectClass != EffectCooperating && input.EffectClass != EffectNonCooperating {
		return Attempt{}, errors.New("invalid activity effect class")
	}
	if input.HeartbeatDeadline.IsZero() {
		input.HeartbeatDeadline = time.Now().Add(DefaultDispatchLease)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Attempt{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := lockLease(ctx, tx, input.Lease); err != nil {
		return Attempt{}, err
	}
	var workflowState WorkflowState
	var revision int64
	var partitionID int16
	var definitionID string
	var definitionVersion int
	if err := tx.QueryRow(ctx, `
		SELECT state, revision, partition_id, definition_id, definition_version
		FROM engine.workflow_executions
		WHERE workflow_id = $1
		FOR UPDATE`, input.WorkflowID).Scan(&workflowState, &revision, &partitionID, &definitionID, &definitionVersion); err != nil {
		return Attempt{}, fmt.Errorf("lock workflow for attempt: %w", err)
	}
	if partitionID != input.Lease.PartitionID {
		return Attempt{}, fmt.Errorf("workflow partition %d does not match lease %d", partitionID, input.Lease.PartitionID)
	}
	if revision != input.ExpectedRevision {
		return Attempt{}, fmt.Errorf("%w: expected %d, got %d", ErrRevisionConflict, input.ExpectedRevision, revision)
	}
	if workflowState != StateWaitingActivity {
		return Attempt{}, fmt.Errorf("workflow state %s cannot create an attempt", workflowState)
	}
	var definitionClass *string
	if err := tx.QueryRow(ctx, `
		SELECT effect_classes ->> $3
		FROM engine.workflow_definitions
		WHERE definition_id = $1 AND version = $2`, definitionID, definitionVersion, input.NodeID).Scan(&definitionClass); err != nil {
		return Attempt{}, fmt.Errorf("read activity effect class: %w", err)
	}
	if definitionClass == nil || *definitionClass == "" {
		return Attempt{}, ErrMissingEffectClass
	}
	authoritativeClass := EffectClass(*definitionClass)
	if authoritativeClass != EffectPure && authoritativeClass != EffectCooperating && authoritativeClass != EffectNonCooperating {
		return Attempt{}, fmt.Errorf("invalid stored effect class %q", *definitionClass)
	}
	if input.EffectClass != authoritativeClass {
		return Attempt{}, fmt.Errorf("%w: got %s, want %s", ErrEffectClassMismatch, input.EffectClass, authoritativeClass)
	}
	if authoritativeClass == EffectCooperating || authoritativeClass == EffectNonCooperating {
		var firstKey *string
		var firstGrantScope *string
		err := tx.QueryRow(ctx, `
			SELECT logical_effect_key, grant_scope_hash
			FROM engine.activity_attempts
			WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3
			ORDER BY attempt_number ASC
			LIMIT 1`, input.WorkflowID, input.NodeID, input.Iteration).Scan(&firstKey, &firstGrantScope)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return Attempt{}, fmt.Errorf("read first effect identity: %w", err)
		}
		if err == nil {
			if input.LogicalEffectKey != "" && (firstKey == nil || input.LogicalEffectKey != *firstKey) {
				return Attempt{}, ErrEffectIdentityMismatch
			}
			if firstKey != nil {
				input.LogicalEffectKey = *firstKey
			}
			if input.GrantScopeHash != "" && (firstGrantScope == nil || input.GrantScopeHash != *firstGrantScope) {
				return Attempt{}, ErrEffectIdentityMismatch
			}
			if firstGrantScope != nil {
				input.GrantScopeHash = *firstGrantScope
			}
		}
	}
	if authoritativeClass == EffectCooperating && input.LogicalEffectKey == "" {
		return Attempt{}, errors.New("cooperating effects require a logical effect key")
	}
	var currentAttempt int64
	var nodeState WorkflowState
	if err := tx.QueryRow(ctx, `
		SELECT state, COALESCE(current_attempt_number, 0)
		FROM engine.node_instances
		WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3
		FOR UPDATE`, input.WorkflowID, input.NodeID, input.Iteration).Scan(&nodeState, &currentAttempt); err != nil {
		return Attempt{}, fmt.Errorf("lock node for attempt: %w", err)
	}
	if nodeState != StateWaitingActivity {
		return Attempt{}, fmt.Errorf("node state %s cannot create an attempt", nodeState)
	}
	if currentAttempt != 0 {
		return Attempt{}, fmt.Errorf("node already has current attempt %d", currentAttempt)
	}
	var attemptNumber int64
	if err := tx.QueryRow(ctx, `
		SELECT COALESCE(MAX(attempt_number), 0) + 1
		FROM engine.activity_attempts
		WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3`,
		input.WorkflowID, input.NodeID, input.Iteration).Scan(&attemptNumber); err != nil {
		return Attempt{}, fmt.Errorf("allocate attempt number: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO engine.activity_attempts
			(workflow_id, node_id, iteration, attempt_number, state, effect_class,
			 heartbeat_deadline, logical_effect_key, grant_scope_hash)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		input.WorkflowID, input.NodeID, input.Iteration, attemptNumber, AttemptDispatchable,
		authoritativeClass, input.HeartbeatDeadline, nullableString(input.LogicalEffectKey), nullableString(input.GrantScopeHash)); err != nil {
		return Attempt{}, fmt.Errorf("insert attempt: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE engine.node_instances
		SET current_attempt_number = $4, updated_at = clock_timestamp()
		WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3`,
		input.WorkflowID, input.NodeID, input.Iteration, attemptNumber); err != nil {
		return Attempt{}, fmt.Errorf("set current attempt: %w", err)
	}
	newRevision := revision + 1
	if _, err := tx.Exec(ctx, `
		UPDATE engine.workflow_executions
		SET revision = $2, updated_at = clock_timestamp()
		WHERE workflow_id = $1`, input.WorkflowID, newRevision); err != nil {
		return Attempt{}, fmt.Errorf("advance workflow revision: %w", err)
	}
	if err := insertHistory(ctx, tx, input.WorkflowID, newRevision, "scheduler", input.ActorID,
		&input.Lease.Epoch, input.NodeID, &input.Iteration, &attemptNumber, &workflowState,
		workflowState, "ATTEMPT_CREATED"); err != nil {
		return Attempt{}, err
	}
	payload := json.RawMessage(fmt.Sprintf(`{"workflow_id":%q,"node_id":%q,"iteration":%d,"attempt_number":%d}`,
		input.WorkflowID, input.NodeID, input.Iteration, attemptNumber))
	if err := insertOutbox(ctx, tx, input.WorkflowID, newRevision, "attempt.dispatch", payload); err != nil {
		return Attempt{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Attempt{}, err
	}
	return Attempt{
		WorkflowID: input.WorkflowID, NodeID: input.NodeID, Iteration: input.Iteration,
		AttemptNumber: attemptNumber, State: AttemptDispatchable, EffectClass: authoritativeClass,
		HeartbeatDeadline: &input.HeartbeatDeadline, LogicalEffectKey: input.LogicalEffectKey,
		GrantScopeHash: input.GrantScopeHash, IsCurrent: true,
	}, nil
}

func (s *Store) ClaimAttempt(ctx context.Context, input ClaimInput) (ClaimResult, error) {
	if input.WorkflowID == "" || input.NodeID == "" || input.WorkerID == "" || input.RequestID == "" {
		return ClaimResult{}, errors.New("claim identity is required")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ClaimResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var workflowState WorkflowState
	if err := tx.QueryRow(ctx, `
		SELECT state FROM engine.workflow_executions
		WHERE workflow_id = $1 FOR UPDATE`, input.WorkflowID).Scan(&workflowState); err != nil {
		return ClaimResult{}, fmt.Errorf("lock workflow for claim: %w", err)
	}
	if workflowState != StateWaitingActivity {
		return ClaimResult{}, fmt.Errorf("workflow state %s cannot claim work", workflowState)
	}
	var existingWorkflowID string
	var existingNodeID string
	var existingIteration int
	var existingNumber int64
	var existingToken *string
	var existingClass EffectClass
	var existingCurrent bool
	var existingState AttemptState
	var existingDeadline *time.Time
	err = tx.QueryRow(ctx, `
		SELECT workflow_id, node_id, iteration, attempt_number, claim_token::text,
			effect_class, is_current, state, heartbeat_deadline
		FROM engine.activity_attempts
		WHERE worker_request_id = $1
		FOR UPDATE`, input.RequestID).Scan(&existingWorkflowID, &existingNodeID, &existingIteration,
		&existingNumber, &existingToken, &existingClass, &existingCurrent, &existingState, &existingDeadline)
	if err == nil {
		if existingWorkflowID != input.WorkflowID || existingNodeID != input.NodeID || existingIteration != input.Iteration {
			return ClaimResult{}, ErrClaimRequestConflict
		}
		if existingToken == nil || !existingCurrent || existingState != AttemptClaimed {
			return ClaimResult{}, ErrStaleClaim
		}
		if err := tx.Commit(ctx); err != nil {
			return ClaimResult{}, err
		}
		result := ClaimResult{AttemptNumber: existingNumber, ClaimToken: *existingToken, EffectClass: existingClass}
		if existingDeadline != nil {
			result.HeartbeatDeadline = *existingDeadline
		}
		return result, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return ClaimResult{}, fmt.Errorf("look up worker request: %w", err)
	}
	var attemptNumber int64
	if err := tx.QueryRow(ctx, `
		SELECT COALESCE(current_attempt_number, 0)
		FROM engine.node_instances
		WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3
		FOR UPDATE`, input.WorkflowID, input.NodeID, input.Iteration).Scan(&attemptNumber); err != nil {
		return ClaimResult{}, fmt.Errorf("lock node for claim: %w", err)
	}
	var token string
	var effectClass EffectClass
	if err := tx.QueryRow(ctx, `
		SELECT effect_class
		FROM engine.activity_attempts
		WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3
			AND attempt_number = $4 AND is_current AND state = 'DISPATCHABLE'
		FOR UPDATE`, input.WorkflowID, input.NodeID, input.Iteration, attemptNumber).Scan(&effectClass); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ClaimResult{}, ErrAttemptNotCurrent
		}
		return ClaimResult{}, fmt.Errorf("lock dispatchable attempt: %w", err)
	}
	token = NewID()
	attemptLease := input.AttemptLease
	if attemptLease <= 0 {
		attemptLease = DefaultAttemptLease
	}
	var heartbeatDeadline time.Time
	if err := tx.QueryRow(ctx, `
		UPDATE engine.activity_attempts
		SET state = 'CLAIMED', claim_token = $5, worker_id = $6,
			worker_request_id = $7,
			heartbeat_deadline = clock_timestamp() + ($8::double precision * interval '1 second'),
			updated_at = clock_timestamp()
		WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3 AND attempt_number = $4
		RETURNING heartbeat_deadline`,
		input.WorkflowID, input.NodeID, input.Iteration, attemptNumber, token, input.WorkerID, input.RequestID,
		durationSeconds(attemptLease)).Scan(&heartbeatDeadline); err != nil {
		return ClaimResult{}, fmt.Errorf("claim attempt: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return ClaimResult{}, err
	}
	return ClaimResult{AttemptNumber: attemptNumber, ClaimToken: token, EffectClass: effectClass,
		HeartbeatDeadline: heartbeatDeadline}, nil
}

func (s *Store) HeartbeatAttempt(ctx context.Context, input HeartbeatInput) (time.Time, error) {
	if input.WorkflowID == "" || input.NodeID == "" || input.ClaimToken == "" {
		return time.Time{}, errors.New("heartbeat identity is required")
	}
	extension := input.Extension
	if extension <= 0 {
		extension = DefaultAttemptLease
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return time.Time{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var workflowState WorkflowState
	if err := tx.QueryRow(ctx, `
		SELECT state FROM engine.workflow_executions
		WHERE workflow_id = $1 FOR UPDATE`, input.WorkflowID).Scan(&workflowState); err != nil {
		return time.Time{}, fmt.Errorf("lock workflow for heartbeat: %w", err)
	}
	if workflowState == StateSucceeded || workflowState == StateFailed || workflowState == StateRejected || workflowState == StateCanceled || workflowState == StateAbandoned {
		return time.Time{}, ErrStaleClaim
	}
	var currentAttempt int64
	if err := tx.QueryRow(ctx, `
		SELECT COALESCE(current_attempt_number, 0)
		FROM engine.node_instances
		WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3
		FOR UPDATE`, input.WorkflowID, input.NodeID, input.Iteration).Scan(&currentAttempt); err != nil {
		return time.Time{}, fmt.Errorf("lock node for heartbeat: %w", err)
	}
	if currentAttempt != input.AttemptNumber {
		return time.Time{}, ErrStaleClaim
	}
	var deadline time.Time
	if err := tx.QueryRow(ctx, `
		UPDATE engine.activity_attempts
		SET heartbeat_deadline = clock_timestamp() + ($6::double precision * interval '1 second'),
			updated_at = clock_timestamp()
		WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3 AND attempt_number = $4
			AND is_current AND state = 'CLAIMED' AND claim_token = $5
		RETURNING heartbeat_deadline`, input.WorkflowID, input.NodeID, input.Iteration,
		input.AttemptNumber, input.ClaimToken, durationSeconds(extension)).Scan(&deadline); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return time.Time{}, ErrStaleClaim
		}
		return time.Time{}, fmt.Errorf("extend attempt heartbeat: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return time.Time{}, err
	}
	return deadline, nil
}

func (s *Store) RecordResult(ctx context.Context, input ResultInput) error {
	_, err := s.RecordResultReceipt(ctx, input)
	return err
}

// RecordResultReceipt records a worker result and returns the durable receipt.
// A retry of an already committed result returns the same receipt without
// writing another outbox event.
func (s *Store) RecordResultReceipt(ctx context.Context, input ResultInput) (ResultReceipt, error) {
	if input.WorkflowID == "" || input.NodeID == "" || input.ClaimToken == "" {
		return ResultReceipt{}, errors.New("result identity is required")
	}
	if input.AttemptState != AttemptSucceeded && input.AttemptState != AttemptFailedRetryable && input.AttemptState != AttemptFailedFinal {
		return ResultReceipt{}, errors.New("result must be a terminal attempt outcome")
	}
	if input.EventType == "" {
		input.EventType = "attempt.result"
	}
	if len(input.Payload) == 0 {
		input.Payload = json.RawMessage(`{}`)
	}
	if !json.Valid(input.Payload) {
		return ResultReceipt{}, errors.New("result payload must be valid JSON")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ResultReceipt{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var workflowState WorkflowState
	if err := tx.QueryRow(ctx, `
		SELECT state FROM engine.workflow_executions
		WHERE workflow_id = $1 FOR UPDATE`, input.WorkflowID).Scan(&workflowState); err != nil {
		return ResultReceipt{}, fmt.Errorf("lock workflow for result: %w", err)
	}
	var nodeCurrentAttempt *int64
	if err := tx.QueryRow(ctx, `
		SELECT current_attempt_number
		FROM engine.node_instances
		WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3
		FOR UPDATE`, input.WorkflowID, input.NodeID, input.Iteration).Scan(&nodeCurrentAttempt); err != nil {
		return ResultReceipt{}, fmt.Errorf("lock node for result: %w", err)
	}
	var current bool
	var state AttemptState
	var effectClass EffectClass
	var outcomeDisposition string
	var storedResult []byte
	var workerID *string
	if err := tx.QueryRow(ctx, `
		SELECT is_current, state, effect_class, outcome_disposition, result, worker_id
		FROM engine.activity_attempts
		WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3 AND attempt_number = $4
			AND claim_token = $5
		FOR UPDATE`, input.WorkflowID, input.NodeID, input.Iteration, input.AttemptNumber, input.ClaimToken).Scan(
		&current, &state, &effectClass, &outcomeDisposition, &storedResult, &workerID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ResultReceipt{}, ErrAttemptNotCurrent
		}
		return ResultReceipt{}, fmt.Errorf("lock claimed attempt: %w", err)
	}
	if !current && isTerminalAttempt(state) {
		if state != input.AttemptState || !jsonEqual(storedResult, input.Payload) {
			return ResultReceipt{}, ErrResultConflict
		}
		if err := tx.Commit(ctx); err != nil {
			return ResultReceipt{}, err
		}
		return ResultReceipt{Disposition: ResultAccepted, WorkflowID: input.WorkflowID,
			NodeID: input.NodeID, Iteration: input.Iteration, AttemptNumber: input.AttemptNumber,
			AttemptState: state, Payload: json.RawMessage(storedResult)}, nil
	}
	if isLateEvidenceAttempt(effectClass, state, outcomeDisposition) {
		reconciliationRef := input.ReconciliationRef
		if reconciliationRef == "" {
			reconciliationRef = defaultReconciliationRef(input)
		}
		if _, err := insertAttemptEvidence(ctx, tx, input.WorkflowID, input.NodeID, input.Iteration,
			input.AttemptNumber, input.ClaimToken, reconciliationRef, input.Payload); err != nil {
			return ResultReceipt{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return ResultReceipt{}, err
		}
		return ResultReceipt{Disposition: ResultRecordedAsEvidence, WorkflowID: input.WorkflowID,
			NodeID: input.NodeID, Iteration: input.Iteration, AttemptNumber: input.AttemptNumber,
			AttemptState: state, Payload: input.Payload}, nil
	}
	if isTerminalWorkflowState(workflowState) {
		return ResultReceipt{}, ErrAttemptNotCurrent
	}
	if nodeCurrentAttempt == nil || *nodeCurrentAttempt != input.AttemptNumber || !current || state != AttemptClaimed {
		return ResultReceipt{}, ErrAttemptNotCurrent
	}
	disposition := "NONE"
	if input.AttemptState == AttemptSucceeded {
		disposition = "KNOWN_SUCCESS"
	}
	if _, err := tx.Exec(ctx, `
		UPDATE engine.activity_attempts
		SET state = $6, outcome_disposition = $7, result = $8, is_current = false,
			updated_at = clock_timestamp()
		WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3 AND attempt_number = $4
			AND claim_token = $5`, input.WorkflowID, input.NodeID, input.Iteration,
		input.AttemptNumber, input.ClaimToken, input.AttemptState, disposition, input.Payload); err != nil {
		return ResultReceipt{}, fmt.Errorf("record attempt result: %w", err)
	}
	var workflowRevision int64
	if err := tx.QueryRow(ctx, `SELECT revision FROM engine.workflow_executions WHERE workflow_id = $1`, input.WorkflowID).Scan(&workflowRevision); err != nil {
		return ResultReceipt{}, fmt.Errorf("read workflow revision: %w", err)
	}
	newRevision := workflowRevision + 1
	if _, err := tx.Exec(ctx, `
		UPDATE engine.workflow_executions
		SET revision = $2, updated_at = clock_timestamp()
		WHERE workflow_id = $1`, input.WorkflowID, newRevision); err != nil {
		return ResultReceipt{}, fmt.Errorf("advance result revision: %w", err)
	}
	actorID := "worker"
	if workerID != nil {
		actorID = *workerID
	}
	if err := insertHistory(ctx, tx, input.WorkflowID, newRevision, "worker", actorID,
		nil, input.NodeID, &input.Iteration, &input.AttemptNumber, &workflowState,
		workflowState, "ATTEMPT_RESULT_RECORDED"); err != nil {
		return ResultReceipt{}, err
	}
	if err := insertOutbox(ctx, tx, input.WorkflowID, newRevision, input.EventType, input.Payload); err != nil {
		return ResultReceipt{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ResultReceipt{}, err
	}
	return ResultReceipt{Disposition: ResultAccepted, WorkflowID: input.WorkflowID,
		NodeID: input.NodeID, Iteration: input.Iteration, AttemptNumber: input.AttemptNumber,
		AttemptState: input.AttemptState, Payload: input.Payload}, nil
}

func (s *Store) TimeoutAttempt(ctx context.Context, input TimeoutInput) (TimeoutResult, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return TimeoutResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := lockLease(ctx, tx, input.Lease); err != nil {
		return TimeoutResult{}, err
	}
	var workflowState WorkflowState
	var revision int64
	var partitionID int16
	if err := tx.QueryRow(ctx, `
		SELECT state, revision, partition_id
		FROM engine.workflow_executions
		WHERE workflow_id = $1 FOR UPDATE`, input.WorkflowID).Scan(&workflowState, &revision, &partitionID); err != nil {
		return TimeoutResult{}, fmt.Errorf("lock workflow for timeout: %w", err)
	}
	if partitionID != input.Lease.PartitionID {
		return TimeoutResult{}, fmt.Errorf("workflow partition %d does not match lease %d", partitionID, input.Lease.PartitionID)
	}
	if isTerminalWorkflowState(workflowState) {
		return TimeoutResult{}, ErrAttemptNotCurrent
	}
	if revision != input.ExpectedRevision {
		return TimeoutResult{}, fmt.Errorf("%w: expected %d, got %d", ErrRevisionConflict, input.ExpectedRevision, revision)
	}
	var currentAttempt int64
	if err := tx.QueryRow(ctx, `
		SELECT COALESCE(current_attempt_number, 0)
		FROM engine.node_instances
		WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3
		FOR UPDATE`, input.WorkflowID, input.NodeID, input.Iteration).Scan(&currentAttempt); err != nil {
		return TimeoutResult{}, fmt.Errorf("lock node for timeout: %w", err)
	}
	if currentAttempt != input.AttemptNumber {
		return TimeoutResult{}, ErrAttemptNotCurrent
	}
	var attemptState AttemptState
	var effectClass EffectClass
	var heartbeatDeadline *time.Time
	var logicalEffectKey *string
	var grantScopeHash *string
	if err := tx.QueryRow(ctx, `
		SELECT state, effect_class, heartbeat_deadline, logical_effect_key, grant_scope_hash
		FROM engine.activity_attempts
		WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3 AND attempt_number = $4
			AND is_current
		FOR UPDATE`, input.WorkflowID, input.NodeID, input.Iteration, input.AttemptNumber).Scan(
		&attemptState, &effectClass, &heartbeatDeadline, &logicalEffectKey, &grantScopeHash); err != nil {
		return TimeoutResult{}, fmt.Errorf("lock current attempt for timeout: %w", err)
	}
	var databaseNow time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&databaseNow); err != nil {
		return TimeoutResult{}, err
	}
	if heartbeatDeadline == nil || heartbeatDeadline.After(databaseNow) {
		return TimeoutResult{}, errors.New("attempt heartbeat deadline has not passed")
	}
	dispatchLease := input.DispatchLease
	if dispatchLease <= 0 {
		dispatchLease = DefaultDispatchLease
	}
	newRevision := revision + 1
	if attemptState == AttemptDispatchable {
		if _, err := tx.Exec(ctx, `UPDATE engine.activity_attempts SET updated_at = clock_timestamp() WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3 AND attempt_number = $4`, input.WorkflowID, input.NodeID, input.Iteration, input.AttemptNumber); err != nil {
			return TimeoutResult{}, err
		}
		if _, err := tx.Exec(ctx, `
			UPDATE engine.activity_attempts
			SET heartbeat_deadline = clock_timestamp() + ($5::double precision * interval '1 second'),
				updated_at = clock_timestamp()
			WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3 AND attempt_number = $4`,
			input.WorkflowID, input.NodeID, input.Iteration, input.AttemptNumber, durationSeconds(dispatchLease)); err != nil {
			return TimeoutResult{}, err
		}
		if err := updateRevisionAndHistory(ctx, tx, input, workflowState, newRevision, "REDISPATCH_UNCLAIMED"); err != nil {
			return TimeoutResult{}, err
		}
		payload := json.RawMessage(fmt.Sprintf(`{"workflow_id":%q,"node_id":%q,"iteration":%d,"attempt_number":%d}`, input.WorkflowID, input.NodeID, input.Iteration, input.AttemptNumber))
		if err := insertOutbox(ctx, tx, input.WorkflowID, newRevision, "attempt.redispatch", payload); err != nil {
			return TimeoutResult{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return TimeoutResult{}, err
		}
		return TimeoutResult{AttemptNumber: input.AttemptNumber, Redispatched: true}, nil
	}
	if attemptState != AttemptClaimed {
		return TimeoutResult{}, ErrAttemptNotCurrent
	}
	if effectClass == EffectNonCooperating {
		if _, err := tx.Exec(ctx, `
			UPDATE engine.activity_attempts
			SET state = 'TIMED_OUT', outcome_disposition = 'OUTCOME_UNKNOWN', updated_at = clock_timestamp()
			WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3 AND attempt_number = $4`,
			input.WorkflowID, input.NodeID, input.Iteration, input.AttemptNumber); err != nil {
			return TimeoutResult{}, err
		}
		if err := updateRevisionAndHistory(ctx, tx, input, workflowState, newRevision, "TIMEOUT_RECONCILIATION_REQUIRED"); err != nil {
			return TimeoutResult{}, err
		}
		if _, err := tx.Exec(ctx, `
			UPDATE engine.workflow_executions
			SET state = $2, updated_at = clock_timestamp()
			WHERE workflow_id = $1`, input.WorkflowID, StateReconciliationRequired); err != nil {
			return TimeoutResult{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return TimeoutResult{}, err
		}
		return TimeoutResult{AttemptNumber: input.AttemptNumber, Reconciliation: true}, nil
	}
	if _, err := tx.Exec(ctx, `
		UPDATE engine.activity_attempts
		SET state = 'TIMED_OUT', is_current = false, updated_at = clock_timestamp()
		WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3 AND attempt_number = $4`,
		input.WorkflowID, input.NodeID, input.Iteration, input.AttemptNumber); err != nil {
		return TimeoutResult{}, err
	}
	newAttempt := input.AttemptNumber + 1
	if _, err := tx.Exec(ctx, `
		INSERT INTO engine.activity_attempts
			(workflow_id, node_id, iteration, attempt_number, state, effect_class,
			 heartbeat_deadline, logical_effect_key, grant_scope_hash)
		VALUES ($1, $2, $3, $4, 'DISPATCHABLE', $5, $6, $7, $8)`,
		input.WorkflowID, input.NodeID, input.Iteration, newAttempt, effectClass,
		databaseNow.Add(dispatchLease), logicalEffectKey, grantScopeHash); err != nil {
		return TimeoutResult{}, fmt.Errorf("create replacement attempt: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE engine.node_instances
		SET current_attempt_number = $4, updated_at = clock_timestamp()
		WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3`, input.WorkflowID, input.NodeID, input.Iteration, newAttempt); err != nil {
		return TimeoutResult{}, err
	}
	if err := updateRevisionAndHistory(ctx, tx, input, workflowState, newRevision, "TIMEOUT_REPLACEMENT"); err != nil {
		return TimeoutResult{}, err
	}
	payload := json.RawMessage(fmt.Sprintf(`{"workflow_id":%q,"node_id":%q,"iteration":%d,"attempt_number":%d}`, input.WorkflowID, input.NodeID, input.Iteration, newAttempt))
	if err := insertOutbox(ctx, tx, input.WorkflowID, newRevision, "attempt.dispatch", payload); err != nil {
		return TimeoutResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return TimeoutResult{}, err
	}
	return TimeoutResult{AttemptNumber: input.AttemptNumber, ReplacementNumber: &newAttempt}, nil
}

func (s *Store) RecordLateEvidence(ctx context.Context, input LateEvidenceInput) (LateEvidence, error) {
	if input.ReconciliationRef == "" || len(input.Payload) == 0 {
		return LateEvidence{}, errors.New("reconciliation reference and evidence payload are required")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return LateEvidence{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var workflowState WorkflowState
	if err := tx.QueryRow(ctx, `
		SELECT state FROM engine.workflow_executions
		WHERE workflow_id = $1 FOR UPDATE`, input.WorkflowID).Scan(&workflowState); err != nil {
		return LateEvidence{}, fmt.Errorf("lock workflow for late evidence: %w", err)
	}
	var currentAttempt *int64
	if err := tx.QueryRow(ctx, `
		SELECT current_attempt_number
		FROM engine.node_instances
		WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3
		FOR UPDATE`, input.WorkflowID, input.NodeID, input.Iteration).Scan(&currentAttempt); err != nil {
		return LateEvidence{}, fmt.Errorf("lock node for late evidence: %w", err)
	}
	var effectClass EffectClass
	var attemptState AttemptState
	var disposition string
	var storedClaimToken *string
	if err := tx.QueryRow(ctx, `
		SELECT effect_class, state, outcome_disposition, claim_token::text
		FROM engine.activity_attempts
		WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3 AND attempt_number = $4
		FOR UPDATE`, input.WorkflowID, input.NodeID, input.Iteration, input.AttemptNumber).Scan(&effectClass, &attemptState, &disposition, &storedClaimToken); err != nil {
		return LateEvidence{}, fmt.Errorf("lock timed-out attempt: %w", err)
	}
	if !isLateEvidenceAttempt(effectClass, attemptState, disposition) {
		return LateEvidence{}, ErrNotTimedOut
	}
	if storedClaimToken == nil || input.ClaimToken == "" || *storedClaimToken != input.ClaimToken {
		return LateEvidence{}, ErrAttemptNotCurrent
	}
	if attemptState == AttemptTimedOut && (currentAttempt == nil || *currentAttempt != input.AttemptNumber) {
		return LateEvidence{}, ErrAttemptNotCurrent
	}
	if !isTerminalWorkflowState(workflowState) && currentAttempt == nil {
		return LateEvidence{}, ErrAttemptNotCurrent
	}
	evidenceID, err := insertAttemptEvidence(ctx, tx, input.WorkflowID, input.NodeID, input.Iteration,
		input.AttemptNumber, input.ClaimToken, input.ReconciliationRef, input.Payload)
	if err != nil {
		return LateEvidence{}, fmt.Errorf("record late evidence: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return LateEvidence{}, err
	}
	return LateEvidence{EvidenceID: evidenceID, WorkflowID: input.WorkflowID, NodeID: input.NodeID,
		Iteration: input.Iteration, AttemptNumber: input.AttemptNumber,
		ReconciliationRef: input.ReconciliationRef, RecordedAt: time.Now().UTC()}, nil
}

func insertAttemptEvidence(ctx context.Context, tx pgx.Tx, workflowID, nodeID string, iteration int,
	attemptNumber int64, claimToken, reconciliationRef string, payload json.RawMessage) (string, error) {
	if reconciliationRef == "" || len(payload) == 0 {
		return "", errors.New("reconciliation reference and evidence payload are required")
	}
	var existingID string
	var existingPayload []byte
	err := tx.QueryRow(ctx, `
		SELECT evidence_id::text, payload
		FROM engine.attempt_result_evidence
		WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3 AND attempt_number = $4
			AND claim_token = NULLIF($5, '')::uuid AND reconciliation_reference = $6
		FOR UPDATE`, workflowID, nodeID, iteration, attemptNumber, claimToken, reconciliationRef).Scan(&existingID, &existingPayload)
	if err == nil {
		if !jsonEqual(existingPayload, payload) {
			return "", ErrEvidenceConflict
		}
		return existingID, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", fmt.Errorf("look up existing attempt-result evidence: %w", err)
	}
	evidenceID := NewID()
	if _, err := tx.Exec(ctx, `
		INSERT INTO engine.attempt_result_evidence
			(evidence_id, workflow_id, node_id, iteration, attempt_number, claim_token,
			 reconciliation_reference, payload, disposition)
		VALUES ($1, $2, $3, $4, $5, NULLIF($6, '')::uuid, $7, $8, 'RECORDED_AS_EVIDENCE')`,
		evidenceID, workflowID, nodeID, iteration, attemptNumber, claimToken, reconciliationRef, payload); err != nil {
		return "", fmt.Errorf("insert attempt-result evidence: %w", err)
	}
	return evidenceID, nil
}

func defaultReconciliationRef(input ResultInput) string {
	return fmt.Sprintf("attempt/%s/%s/%d/%d", input.WorkflowID, input.NodeID, input.Iteration, input.AttemptNumber)
}

func durationSeconds(value time.Duration) float64 {
	return value.Seconds()
}

func isTerminalAttempt(state AttemptState) bool {
	return state == AttemptSucceeded || state == AttemptFailedRetryable || state == AttemptFailedFinal
}

func isLateEvidenceAttempt(effectClass EffectClass, attemptState AttemptState, disposition string) bool {
	if disposition != "OUTCOME_UNKNOWN" {
		return false
	}
	if effectClass == EffectNonCooperating && attemptState == AttemptTimedOut {
		return true
	}
	return effectClass != EffectPure && attemptState == AttemptCanceled
}

func isTerminalWorkflowState(state WorkflowState) bool {
	return state == StateSucceeded || state == StateFailed || state == StateRejected ||
		state == StateCanceled || state == StateAbandoned
}

func jsonEqual(left, right []byte) bool {
	var leftValue any
	var rightValue any
	if len(left) == 0 || json.Unmarshal(left, &leftValue) != nil || json.Unmarshal(right, &rightValue) != nil {
		return false
	}
	return reflect.DeepEqual(leftValue, rightValue)
}

func updateRevisionAndHistory(ctx context.Context, tx pgx.Tx, input TimeoutInput, oldState WorkflowState, newRevision int64, reason string) error {
	newState := oldState
	if reason == "TIMEOUT_RECONCILIATION_REQUIRED" {
		newState = StateReconciliationRequired
	}
	if _, err := tx.Exec(ctx, `
		UPDATE engine.workflow_executions
		SET state = $2, revision = $3, updated_at = clock_timestamp()
		WHERE workflow_id = $1`, input.WorkflowID, newState, newRevision); err != nil {
		return fmt.Errorf("update workflow revision: %w", err)
	}
	return insertHistory(ctx, tx, input.WorkflowID, newRevision, "scheduler", input.ActorID,
		&input.Lease.Epoch, input.NodeID, &input.Iteration, &input.AttemptNumber,
		&oldState, newState, reason)
}

func insertHistory(ctx context.Context, tx pgx.Tx, workflowID string, revision int64, actorKind, actorID string,
	epoch *int64, nodeID string, iteration *int, attemptNumber *int64, oldState *WorkflowState,
	newState WorkflowState, reason string) error {
	if _, err := tx.Exec(ctx, `
		INSERT INTO engine.transition_history
			(transition_id, workflow_id, revision, actor_kind, actor_id, scheduler_epoch,
			 node_id, iteration, attempt_number, old_state, new_state, reason)
		VALUES ($1, $2, $3, $4, $5, $6, NULLIF($7, ''), $8, $9, $10, $11, $12)`,
		NewID(), workflowID, revision, actorKind, actorID, epoch, nodeID, iteration,
		attemptNumber, oldState, newState, reason); err != nil {
		return fmt.Errorf("insert transition history: %w", err)
	}
	return nil
}

func insertOutbox(ctx context.Context, tx pgx.Tx, workflowID string, revision int64, eventType string, payload json.RawMessage) error {
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO engine.outbox (event_id, workflow_id, aggregate_revision, event_type, payload)
		VALUES ($1, $2, $3, $4, $5)`, NewID(), workflowID, revision, eventType, payload); err != nil {
		return fmt.Errorf("insert outbox: %w", err)
	}
	return nil
}

func nullableString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func decodeEffectClasses(raw json.RawMessage) (map[string]EffectClass, error) {
	if len(raw) == 0 {
		return nil, ErrMissingEffectClass
	}
	var classes map[string]EffectClass
	if err := json.Unmarshal(raw, &classes); err != nil {
		return nil, fmt.Errorf("decode activity effect classes: %w", err)
	}
	for nodeID, class := range classes {
		if nodeID == "" || (class != EffectPure && class != EffectCooperating && class != EffectNonCooperating) {
			return nil, fmt.Errorf("invalid effect class for activity %q", nodeID)
		}
	}
	return classes, nil
}
