package state

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
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
			(definition_id, version, definition_hash, graph, activity_versions)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (definition_id, version) DO NOTHING`,
		input.DefinitionID, input.Version, input.DefinitionHash, graph, activityVersions)
	if err != nil {
		return fmt.Errorf("insert workflow definition: %w", err)
	}
	var existingHash string
	if err := tx.QueryRow(ctx, `
		SELECT definition_hash
		FROM engine.workflow_definitions
		WHERE definition_id = $1 AND version = $2`, input.DefinitionID, input.Version).Scan(&existingHash); err != nil {
		return fmt.Errorf("read workflow definition: %w", err)
	}
	if existingHash != input.DefinitionHash {
		return fmt.Errorf("definition %s version %d already has a different hash", input.DefinitionID, input.Version)
	}
	return tx.Commit(ctx)
}

func (s *Store) CreateWorkflow(ctx context.Context, input CreateWorkflowInput) (CreateWorkflowResult, error) {
	if input.WorkflowID == "" || input.Namespace == "" || input.SubmissionKey == "" ||
		input.SubmissionPayloadHash == "" || input.DefinitionID == "" || input.InitialNodeID == "" {
		return CreateWorkflowResult{}, errors.New("workflow identity and initial node are required")
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
	if err := tx.Commit(ctx); err != nil {
		return CreateWorkflowResult{}, fmt.Errorf("commit workflow: %w", err)
	}
	return CreateWorkflowResult{Workflow: workflow, Created: true}, nil
}

func (s *Store) GetWorkflow(ctx context.Context, workflowID string) (Workflow, error) {
	return scanWorkflow(s.pool.QueryRow(ctx, `
		SELECT workflow_id, namespace, submission_key, submission_payload_hash,
			definition_id, definition_version, partition_id, state, revision,
			created_at, updated_at
		FROM engine.workflow_executions
		WHERE workflow_id = $1`, workflowID))
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
	lease := Lease{PartitionID: partitionID, OwnerID: ownerID, Epoch: epoch + 1, LeaseExpiresAt: databaseNow.Add(ttl)}
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
	if input.NodeID != "" {
		var ignored string
		if err := tx.QueryRow(ctx, `
			SELECT node_id
			FROM engine.node_instances
			WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3
			FOR UPDATE`, input.WorkflowID, input.NodeID, input.Iteration).Scan(&ignored); err != nil {
			return fmt.Errorf("lock node: %w", err)
		}
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
	if nodeState != nil {
		if _, err := tx.Exec(ctx, `
			UPDATE engine.node_instances
			SET state = $4, revision = revision + 1, updated_at = clock_timestamp()
			WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3`,
			input.WorkflowID, input.NodeID, input.Iteration, *nodeState); err != nil {
			return fmt.Errorf("update node transition: %w", err)
		}
	}
	if err := insertHistory(ctx, tx, input.WorkflowID, newRevision, "scheduler", input.ActorID,
		&input.Lease.Epoch, input.NodeID, nullableIteration(input.NodeID, input.Iteration), input.AttemptNumber,
		&state, input.NewState, input.Reason); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func legalTransition(from, to WorkflowState) bool {
	allowed := map[WorkflowState]map[WorkflowState]bool{
		StateRunnable: {
			StateWaitingActivity: true, StateWaitingApproval: true, StateWaitingTimer: true,
			StateSucceeded: true, StateCanceled: true,
		},
		StateWaitingActivity: {
			StateRunnable: true, StateWaitingTimer: true, StateReconciliationRequired: true,
			StateSucceeded: true, StateFailed: true, StateCanceled: true,
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
	if input.EffectClass == EffectCooperating && input.LogicalEffectKey == "" {
		return Attempt{}, errors.New("cooperating effects require a logical effect key")
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
	if err := tx.QueryRow(ctx, `
		SELECT state, revision, partition_id
		FROM engine.workflow_executions
		WHERE workflow_id = $1
		FOR UPDATE`, input.WorkflowID).Scan(&workflowState, &revision, &partitionID); err != nil {
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
		input.EffectClass, input.HeartbeatDeadline, nullableString(input.LogicalEffectKey), nullableString(input.GrantScopeHash)); err != nil {
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
		AttemptNumber: attemptNumber, State: AttemptDispatchable, EffectClass: input.EffectClass,
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
	var existingNumber int64
	var existingToken *string
	var existingClass EffectClass
	err = tx.QueryRow(ctx, `
		SELECT attempt_number, claim_token::text, effect_class
		FROM engine.activity_attempts
		WHERE worker_request_id = $1
		FOR UPDATE`, input.RequestID).Scan(&existingNumber, &existingToken, &existingClass)
	if err == nil {
		if existingToken == nil {
			return ClaimResult{}, ErrAttemptNotCurrent
		}
		if err := tx.Commit(ctx); err != nil {
			return ClaimResult{}, err
		}
		return ClaimResult{AttemptNumber: existingNumber, ClaimToken: *existingToken, EffectClass: existingClass}, nil
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
	if _, err := tx.Exec(ctx, `
		UPDATE engine.activity_attempts
		SET state = 'CLAIMED', claim_token = $5, worker_id = $6,
			worker_request_id = $7, updated_at = clock_timestamp()
		WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3 AND attempt_number = $4`,
		input.WorkflowID, input.NodeID, input.Iteration, attemptNumber, token, input.WorkerID, input.RequestID); err != nil {
		return ClaimResult{}, fmt.Errorf("claim attempt: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return ClaimResult{}, err
	}
	return ClaimResult{AttemptNumber: attemptNumber, ClaimToken: token, EffectClass: effectClass}, nil
}

func (s *Store) RecordResult(ctx context.Context, input ResultInput) error {
	_, err := s.RecordResultReceipt(ctx, input)
	return err
}

// RecordResultReceipt records a worker result and reports whether it was
// accepted as progress or retained only as reconciliation evidence. The
// compatibility wrapper above exposes the legacy error-only API.
func (s *Store) RecordResultReceipt(ctx context.Context, input ResultInput) (ResultDisposition, error) {
	if input.WorkflowID == "" || input.NodeID == "" || input.ClaimToken == "" {
		return "", errors.New("result identity is required")
	}
	if input.AttemptState != AttemptSucceeded && input.AttemptState != AttemptFailedRetryable && input.AttemptState != AttemptFailedFinal {
		return "", errors.New("result must be a terminal attempt outcome")
	}
	if input.EventType == "" {
		input.EventType = "attempt.result"
	}
	if len(input.Payload) == 0 {
		input.Payload = json.RawMessage(`{}`)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var workflowState WorkflowState
	if err := tx.QueryRow(ctx, `
		SELECT state FROM engine.workflow_executions
		WHERE workflow_id = $1 FOR UPDATE`, input.WorkflowID).Scan(&workflowState); err != nil {
		return "", fmt.Errorf("lock workflow for result: %w", err)
	}
	if workflowState == StateSucceeded || workflowState == StateFailed || workflowState == StateRejected || workflowState == StateCanceled || workflowState == StateAbandoned {
		return "", ErrAttemptNotCurrent
	}
	var nodeCurrentAttempt int64
	if err := tx.QueryRow(ctx, `
		SELECT COALESCE(current_attempt_number, 0)
		FROM engine.node_instances
		WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3
		FOR UPDATE`, input.WorkflowID, input.NodeID, input.Iteration).Scan(&nodeCurrentAttempt); err != nil {
		return "", fmt.Errorf("lock node for result: %w", err)
	}
	if nodeCurrentAttempt != input.AttemptNumber {
		return "", ErrAttemptNotCurrent
	}
	var current bool
	var state AttemptState
	var effectClass EffectClass
	if err := tx.QueryRow(ctx, `
		SELECT is_current, state, effect_class
		FROM engine.activity_attempts
		WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3 AND attempt_number = $4
			AND claim_token = $5
		FOR UPDATE`, input.WorkflowID, input.NodeID, input.Iteration, input.AttemptNumber, input.ClaimToken).Scan(&current, &state, &effectClass); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", ErrAttemptNotCurrent
		}
		return "", fmt.Errorf("lock claimed attempt: %w", err)
	}
	if state == AttemptTimedOut && effectClass == EffectNonCooperating {
		reconciliationRef := input.ReconciliationRef
		if reconciliationRef == "" {
			reconciliationRef = defaultReconciliationRef(input)
		}
		if _, err := insertAttemptEvidence(ctx, tx, input.WorkflowID, input.NodeID, input.Iteration,
			input.AttemptNumber, input.ClaimToken, reconciliationRef, input.Payload); err != nil {
			return "", err
		}
		if err := tx.Commit(ctx); err != nil {
			return "", err
		}
		return ResultRecordedAsEvidence, nil
	}
	if !current || state != AttemptClaimed {
		return "", ErrAttemptNotCurrent
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
		return "", fmt.Errorf("record attempt result: %w", err)
	}
	var workflowRevision int64
	if err := tx.QueryRow(ctx, `SELECT revision FROM engine.workflow_executions WHERE workflow_id = $1`, input.WorkflowID).Scan(&workflowRevision); err != nil {
		return "", fmt.Errorf("read workflow revision: %w", err)
	}
	if err := insertOutbox(ctx, tx, input.WorkflowID, workflowRevision, input.EventType, input.Payload); err != nil {
		return "", err
	}
	if err := tx.Commit(ctx); err != nil {
		return "", err
	}
	return ResultAccepted, nil
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
	newRevision := revision + 1
	if attemptState == AttemptDispatchable {
		if _, err := tx.Exec(ctx, `UPDATE engine.activity_attempts SET updated_at = clock_timestamp() WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3 AND attempt_number = $4`, input.WorkflowID, input.NodeID, input.Iteration, input.AttemptNumber); err != nil {
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
		heartbeatDeadline, logicalEffectKey, grantScopeHash); err != nil {
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
	if effectClass != EffectNonCooperating || attemptState != AttemptTimedOut || disposition != "OUTCOME_UNKNOWN" {
		return LateEvidence{}, ErrNotTimedOut
	}
	if storedClaimToken == nil || input.ClaimToken == "" || *storedClaimToken != input.ClaimToken {
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
