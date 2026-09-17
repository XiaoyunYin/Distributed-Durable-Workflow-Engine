package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

type definitionGraphNode struct {
	ID       string
	Kind     string
	Next     []string
	Branches []string
	Join     string
}

type definitionGraph struct {
	Entry string
	Nodes map[string]definitionGraphNode
}

func decodeDefinitionGraph(raw []byte) (definitionGraph, error) {
	var document struct {
		Entry string            `json:"entry"`
		Nodes []json.RawMessage `json:"nodes"`
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		return definitionGraph{}, fmt.Errorf("decode graph: %w", err)
	}
	if len(document.Nodes) == 0 {
		return definitionGraph{}, errors.New("graph has no nodes")
	}
	graph := definitionGraph{Entry: document.Entry, Nodes: make(map[string]definitionGraphNode, len(document.Nodes))}
	for _, rawNode := range document.Nodes {
		var shorthand string
		if json.Unmarshal(rawNode, &shorthand) == nil {
			if shorthand == "" {
				return definitionGraph{}, errors.New("graph node ID is empty")
			}
			if _, exists := graph.Nodes[shorthand]; exists {
				return definitionGraph{}, fmt.Errorf("duplicate graph node %q", shorthand)
			}
			graph.Nodes[shorthand] = definitionGraphNode{ID: shorthand, Kind: "activity"}
			continue
		}
		var object struct {
			ID       string          `json:"id"`
			Kind     string          `json:"kind"`
			Next     json.RawMessage `json:"next"`
			Branches []string        `json:"branches"`
			Join     string          `json:"join"`
		}
		if err := json.Unmarshal(rawNode, &object); err != nil || object.ID == "" {
			return definitionGraph{}, errors.New("graph node object requires an ID")
		}
		if object.Kind == "" {
			object.Kind = "activity"
		}
		next, err := decodeDefinitionNext(object.Next)
		if err != nil {
			return definitionGraph{}, fmt.Errorf("node %q: %w", object.ID, err)
		}
		if _, exists := graph.Nodes[object.ID]; exists {
			return definitionGraph{}, fmt.Errorf("duplicate graph node %q", object.ID)
		}
		graph.Nodes[object.ID] = definitionGraphNode{ID: object.ID, Kind: object.Kind,
			Next: next, Branches: append([]string(nil), object.Branches...), Join: object.Join}
	}
	if graph.Entry == "" {
		if len(graph.Nodes) != 1 {
			return definitionGraph{}, errors.New("graph entry is required when more than one node is declared")
		}
		for id := range graph.Nodes {
			graph.Entry = id
		}
	}
	if _, exists := graph.Nodes[graph.Entry]; !exists {
		return definitionGraph{}, fmt.Errorf("graph entry %q is not declared", graph.Entry)
	}
	for _, node := range graph.Nodes {
		for _, ref := range append(append(append([]string{}, node.Next...), node.Branches...), node.Join) {
			if ref == "" {
				continue
			}
			if _, exists := graph.Nodes[ref]; !exists {
				return definitionGraph{}, fmt.Errorf("node %q references undeclared node %q", node.ID, ref)
			}
		}
	}
	return graph, nil
}

func decodeDefinitionNext(raw json.RawMessage) ([]string, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var one string
	if json.Unmarshal(raw, &one) == nil {
		if one == "" {
			return nil, errors.New("next node ID is empty")
		}
		return []string{one}, nil
	}
	var many []string
	if err := json.Unmarshal(raw, &many); err != nil {
		return nil, errors.New("next must be a node ID or array")
	}
	for _, id := range many {
		if id == "" {
			return nil, errors.New("next node ID is empty")
		}
	}
	return many, nil
}

func (graph definitionGraph) node(id string) (definitionGraphNode, error) {
	node, ok := graph.Nodes[id]
	if !ok {
		return definitionGraphNode{}, fmt.Errorf("%w: node %q is not declared", ErrGraphViolation, id)
	}
	return node, nil
}

func validateGraphSuccessors(graph definitionGraph, source definitionGraphNode, sourceIteration int, next []GraphNodeInput) error {
	expected := append([]string(nil), source.Next...)
	if source.Kind == "fanout" || source.Kind == "fan_out" {
		expected = append(expected, source.Branches...)
		if source.Join != "" {
			expected = append(expected, source.Join)
		}
	}
	if source.Kind == "activity" && len(expected) > 1 {
		if len(next) != 1 || !containsString(expected, next[0].NodeID) {
			return fmt.Errorf("%w: activity %q selected an undeclared successor", ErrGraphViolation, source.ID)
		}
	} else if len(expected) != len(next) {
		return fmt.Errorf("%w: node %q expected %d successors, got %d", ErrGraphViolation, source.ID, len(expected), len(next))
	} else {
		for index, id := range expected {
			if next[index].NodeID != id {
				return fmt.Errorf("%w: node %q successor %q is not declared at position %d", ErrGraphViolation, source.ID, next[index].NodeID, index)
			}
		}
	}
	seen := make(map[string]struct{}, len(next))
	for _, item := range next {
		if item.Iteration < 0 || item.Iteration != sourceIteration {
			return fmt.Errorf("%w: successor %q has iteration %d, want %d", ErrGraphViolation, item.NodeID, item.Iteration, sourceIteration)
		}
		if _, exists := seen[item.NodeID]; exists {
			return fmt.Errorf("%w: duplicate successor %q", ErrGraphViolation, item.NodeID)
		}
		seen[item.NodeID] = struct{}{}
		if _, err := graph.node(item.NodeID); err != nil {
			return err
		}
	}
	if source.Kind == "fanout" || source.Kind == "fan_out" {
		for _, item := range next {
			if item.NodeID == source.Join && !sameStrings(item.Dependencies, source.Branches) {
				return fmt.Errorf("%w: join %q dependencies do not match fan-out branches", ErrGraphViolation, item.NodeID)
			}
		}
	}
	return nil
}

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func sameStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func (s *Store) GetDefinition(ctx context.Context, definitionID string, version int) (Definition, error) {
	var definition Definition
	var graph, activityVersions, effectClasses []byte
	err := s.pool.QueryRow(ctx, `
		SELECT definition_id, version, definition_hash, graph, activity_versions, effect_classes
		FROM engine.workflow_definitions
		WHERE definition_id = $1 AND version = $2`, definitionID, version).Scan(
		&definition.DefinitionID, &definition.Version, &definition.DefinitionHash,
		&graph, &activityVersions, &effectClasses)
	if errors.Is(err, pgx.ErrNoRows) {
		return Definition{}, ErrDefinitionNotFound
	}
	if err != nil {
		return Definition{}, fmt.Errorf("read workflow definition: %w", err)
	}
	definition.Graph = json.RawMessage(graph)
	definition.ActivityVersions = json.RawMessage(activityVersions)
	if err := json.Unmarshal(effectClasses, &definition.EffectClasses); err != nil {
		return Definition{}, fmt.Errorf("decode workflow effect classes: %w", err)
	}
	return definition, nil
}

func (s *Store) GetNode(ctx context.Context, workflowID, nodeID string, iteration int) (NodeInstance, error) {
	return scanNode(s.pool.QueryRow(ctx, `
		SELECT workflow_id, node_id, iteration, state, dependencies, input,
			accepted_result, current_attempt_number, retry_count, deadline_at, timer_fired, revision
		FROM engine.node_instances
		WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3`, workflowID, nodeID, iteration))
}

func (s *Store) ListNodes(ctx context.Context, workflowID string) ([]NodeInstance, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT workflow_id, node_id, iteration, state, dependencies, input,
			accepted_result, current_attempt_number, retry_count, deadline_at, timer_fired, revision
		FROM engine.node_instances
		WHERE workflow_id = $1
		ORDER BY iteration, node_id`, workflowID)
	if err != nil {
		return nil, fmt.Errorf("list workflow nodes: %w", err)
	}
	defer rows.Close()
	var nodes []NodeInstance
	for rows.Next() {
		node, err := scanNodeRow(rows)
		if err != nil {
			return nil, fmt.Errorf("scan workflow node: %w", err)
		}
		nodes = append(nodes, node)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate workflow nodes: %w", err)
	}
	return nodes, nil
}

func (s *Store) PendingTimer(ctx context.Context, workflowID, nodeID string, iteration int) (time.Time, string, bool, error) {
	var dueAt time.Time
	var purpose string
	err := s.pool.QueryRow(ctx, `
		SELECT due_at, purpose
		FROM engine.timers
		WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3
			AND consumed_at IS NULL
		ORDER BY due_at ASC
		LIMIT 1`, workflowID, nodeID, iteration).Scan(&dueAt, &purpose)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, "", false, nil
	}
	if err != nil {
		return time.Time{}, "", false, fmt.Errorf("read pending timer: %w", err)
	}
	return dueAt, purpose, true, nil
}

func (s *Store) ScheduleTimer(ctx context.Context, input ScheduleTimerInput) (string, error) {
	if input.WorkflowID == "" || input.NodeID == "" || input.ActorID == "" || input.DueAt.IsZero() {
		return "", errors.New("timer identity, due time, and actor are required")
	}
	if input.Purpose == "" {
		input.Purpose = "WORKFLOW_TIMER"
	}
	if input.Purpose != "RETRY_BACKOFF" && input.Purpose != "WORKFLOW_TIMER" {
		return "", errors.New("invalid timer purpose")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := lockLease(ctx, tx, input.Lease); err != nil {
		return "", err
	}
	var workflowState WorkflowState
	var revision int64
	var partitionID int16
	if err := tx.QueryRow(ctx, `
		SELECT state, revision, partition_id
		FROM engine.workflow_executions
		WHERE workflow_id = $1 FOR UPDATE`, input.WorkflowID).Scan(&workflowState, &revision, &partitionID); err != nil {
		return "", fmt.Errorf("lock workflow for timer: %w", err)
	}
	if partitionID != input.Lease.PartitionID {
		return "", fmt.Errorf("workflow partition %d does not match lease %d", partitionID, input.Lease.PartitionID)
	}
	if revision != input.ExpectedRevision {
		return "", fmt.Errorf("%w: expected %d, got %d", ErrRevisionConflict, input.ExpectedRevision, revision)
	}
	if workflowState != StateRunnable {
		return "", fmt.Errorf("workflow state %s cannot schedule a timer", workflowState)
	}
	var nodeState WorkflowState
	if err := tx.QueryRow(ctx, `
		SELECT state
		FROM engine.node_instances
		WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3 FOR UPDATE`,
		input.WorkflowID, input.NodeID, input.Iteration).Scan(&nodeState); err != nil {
		return "", fmt.Errorf("lock node for timer: %w", err)
	}
	if nodeState != StateRunnable {
		return "", fmt.Errorf("node state %s cannot schedule a timer", nodeState)
	}
	timerID := NewID()
	if _, err := tx.Exec(ctx, `
		INSERT INTO engine.timers (timer_id, workflow_id, node_id, iteration, due_at, purpose, owning_revision)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		timerID, input.WorkflowID, input.NodeID, input.Iteration, input.DueAt, input.Purpose, revision+1); err != nil {
		return "", fmt.Errorf("insert timer: %w", err)
	}
	newRevision := revision + 1
	if _, err := tx.Exec(ctx, `
		UPDATE engine.node_instances
		SET state = 'WAITING_TIMER', deadline_at = $4,
			revision = revision + 1,
			updated_at = clock_timestamp()
		WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3`,
		input.WorkflowID, input.NodeID, input.Iteration, input.DueAt); err != nil {
		return "", fmt.Errorf("set timer node state: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE engine.workflow_executions
		SET state = 'WAITING_TIMER', revision = $2, updated_at = clock_timestamp()
		WHERE workflow_id = $1`, input.WorkflowID, newRevision); err != nil {
		return "", fmt.Errorf("set timer workflow state: %w", err)
	}
	if err := insertHistory(ctx, tx, input.WorkflowID, newRevision, "scheduler", input.ActorID,
		&input.Lease.Epoch, input.NodeID, &input.Iteration, nil, &workflowState,
		StateWaitingTimer, "TIMER_SCHEDULED"); err != nil {
		return "", err
	}
	payload := json.RawMessage(fmt.Sprintf(`{"workflow_id":%q,"node_id":%q,"iteration":%d,"timer_id":%q,"purpose":%q}`,
		input.WorkflowID, input.NodeID, input.Iteration, timerID, input.Purpose))
	if err := insertOutbox(ctx, tx, input.WorkflowID, newRevision, "timer.scheduled", payload); err != nil {
		return "", err
	}
	if err := tx.Commit(ctx); err != nil {
		return "", err
	}
	return timerID, nil
}

// CancelWorkflow settles every non-terminal node while holding the scheduler
// lease. Claimed effect attempts remain durable as OUTCOME_UNKNOWN so a late
// worker report can be retained as evidence without reopening the workflow.
func (s *Store) CancelWorkflow(ctx context.Context, input CancelWorkflowInput) (Workflow, error) {
	if input.WorkflowID == "" || input.ActorID == "" {
		return Workflow{}, errors.New("workflow and actor are required")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Workflow{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := lockLease(ctx, tx, input.Lease); err != nil {
		return Workflow{}, err
	}
	var workflow Workflow
	if err := tx.QueryRow(ctx, `
		SELECT workflow_id, namespace, submission_key, submission_payload_hash,
			definition_id, definition_version, partition_id, state, revision,
			created_at, updated_at
		FROM engine.workflow_executions
		WHERE workflow_id = $1 FOR UPDATE`, input.WorkflowID).Scan(
		&workflow.WorkflowID, &workflow.Namespace, &workflow.SubmissionKey,
		&workflow.PayloadHash, &workflow.DefinitionID, &workflow.DefinitionVersion,
		&workflow.PartitionID, &workflow.State, &workflow.Revision,
		&workflow.CreatedAt, &workflow.UpdatedAt); err != nil {
		return Workflow{}, fmt.Errorf("lock workflow for cancellation: %w", err)
	}
	if workflow.PartitionID != input.Lease.PartitionID {
		return Workflow{}, fmt.Errorf("workflow partition %d does not match lease %d", workflow.PartitionID, input.Lease.PartitionID)
	}
	if workflow.Revision != input.ExpectedRevision {
		return Workflow{}, fmt.Errorf("%w: expected %d, got %d", ErrRevisionConflict, input.ExpectedRevision, workflow.Revision)
	}
	if isTerminalWorkflowState(workflow.State) {
		return Workflow{}, ErrAttemptNotCurrent
	}
	rows, err := tx.Query(ctx, `
		SELECT node_id, iteration, state, current_attempt_number
		FROM engine.node_instances
		WHERE workflow_id = $1
		ORDER BY iteration, node_id
		FOR UPDATE`, input.WorkflowID)
	if err != nil {
		return Workflow{}, fmt.Errorf("lock workflow nodes for cancellation: %w", err)
	}
	type nodeRef struct {
		id      string
		iter    int
		state   WorkflowState
		attempt *int64
	}
	var nodes []nodeRef
	for rows.Next() {
		var node nodeRef
		if err := rows.Scan(&node.id, &node.iter, &node.state, &node.attempt); err != nil {
			rows.Close()
			return Workflow{}, fmt.Errorf("scan node for cancellation: %w", err)
		}
		nodes = append(nodes, node)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return Workflow{}, fmt.Errorf("iterate nodes for cancellation: %w", err)
	}
	rows.Close()
	for _, node := range nodes {
		if isTerminalWorkflowState(node.state) {
			continue
		}
		if node.attempt != nil {
			var ignored int
			if err := tx.QueryRow(ctx, `
				SELECT 1
				FROM engine.activity_attempts
				WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3
					AND attempt_number = $4
				FOR UPDATE`, input.WorkflowID, node.id, node.iter, *node.attempt).Scan(&ignored); err != nil {
				return Workflow{}, fmt.Errorf("lock active attempt for cancellation: %w", err)
			}
			if _, err := tx.Exec(ctx, `
				UPDATE engine.activity_attempts
				SET state = 'CANCELED', is_current = false,
					outcome_disposition = CASE
						WHEN state = 'CLAIMED' AND effect_class <> 'PURE_ACTIVITY' THEN 'OUTCOME_UNKNOWN'
						ELSE 'NONE'
					END,
					updated_at = clock_timestamp()
				WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3
					AND attempt_number = $4 AND is_current`,
				input.WorkflowID, node.id, node.iter, *node.attempt); err != nil {
				return Workflow{}, fmt.Errorf("settle attempt during cancellation: %w", err)
			}
		}
		if _, err := tx.Exec(ctx, `
			UPDATE engine.node_instances
			SET state = 'CANCELED', current_attempt_number = NULL, deadline_at = NULL,
				revision = revision + 1, updated_at = clock_timestamp()
			WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3`,
			input.WorkflowID, node.id, node.iter); err != nil {
			return Workflow{}, fmt.Errorf("settle node during cancellation: %w", err)
		}
	}
	newRevision := workflow.Revision + 1
	if _, err := tx.Exec(ctx, `
		UPDATE engine.workflow_executions
		SET state = 'CANCELED', revision = $2, updated_at = clock_timestamp()
		WHERE workflow_id = $1`, input.WorkflowID, newRevision); err != nil {
		return Workflow{}, fmt.Errorf("cancel workflow: %w", err)
	}
	if err := insertHistory(ctx, tx, input.WorkflowID, newRevision, "scheduler", input.ActorID,
		&input.Lease.Epoch, "", nil, nil, &workflow.State, StateCanceled, "CANCELED_ALL_ACTIVE_NODES"); err != nil {
		return Workflow{}, err
	}
	if err := insertOutbox(ctx, tx, input.WorkflowID, newRevision, "workflow.canceled",
		json.RawMessage(fmt.Sprintf(`{"workflow_id":%q}`, input.WorkflowID))); err != nil {
		return Workflow{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Workflow{}, err
	}
	workflow.State = StateCanceled
	workflow.Revision = newRevision
	workflow.UpdatedAt = time.Now().UTC()
	return workflow, nil
}

// AdvanceGraph is the scheduler-owned graph boundary. It marks the source
// node complete, creates any next nodes, and changes the workflow state in one
// lease-fenced transaction. A repeated call must use a new workflow revision,
// so it cannot duplicate downstream nodes after a response loss.
func (s *Store) AdvanceGraph(ctx context.Context, input AdvanceGraphInput) (AdvanceGraphResult, error) {
	if input.WorkflowID == "" || input.FromNodeID == "" || input.ActorID == "" {
		return AdvanceGraphResult{}, errors.New("graph advancement identity is required")
	}
	if len(input.Next) == 0 {
		if input.FinalWorkflowState == "" {
			input.FinalWorkflowState = StateSucceeded
		}
	} else if input.FinalWorkflowState == "" {
		input.FinalWorkflowState = StateRunnable
	}
	if input.FinalNodeState == "" {
		input.FinalNodeState = StateSucceeded
	}
	if input.FinalWorkflowState != StateRunnable && len(input.Next) > 0 {
		return AdvanceGraphResult{}, errors.New("non-terminal graph advancement must remain runnable")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return AdvanceGraphResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := lockLease(ctx, tx, input.Lease); err != nil {
		return AdvanceGraphResult{}, err
	}
	var workflow Workflow
	if err := tx.QueryRow(ctx, `
		SELECT workflow_id, namespace, submission_key, submission_payload_hash,
			definition_id, definition_version, partition_id, state, revision,
			created_at, updated_at
		FROM engine.workflow_executions
		WHERE workflow_id = $1 FOR UPDATE`, input.WorkflowID).Scan(
		&workflow.WorkflowID, &workflow.Namespace, &workflow.SubmissionKey,
		&workflow.PayloadHash, &workflow.DefinitionID, &workflow.DefinitionVersion,
		&workflow.PartitionID, &workflow.State, &workflow.Revision,
		&workflow.CreatedAt, &workflow.UpdatedAt); err != nil {
		return AdvanceGraphResult{}, fmt.Errorf("lock workflow for graph advancement: %w", err)
	}
	if workflow.PartitionID != input.Lease.PartitionID {
		return AdvanceGraphResult{}, fmt.Errorf("workflow partition %d does not match lease %d", workflow.PartitionID, input.Lease.PartitionID)
	}
	if workflow.Revision != input.ExpectedRevision {
		return AdvanceGraphResult{}, fmt.Errorf("%w: expected %d, got %d", ErrRevisionConflict, input.ExpectedRevision, workflow.Revision)
	}
	if isTerminalWorkflowState(workflow.State) {
		return AdvanceGraphResult{}, ErrAttemptNotCurrent
	}
	if !legalTransition(workflow.State, input.FinalWorkflowState) {
		return AdvanceGraphResult{}, fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, workflow.State, input.FinalWorkflowState)
	}
	var definitionRaw []byte
	if err := tx.QueryRow(ctx, `
		SELECT graph
		FROM engine.workflow_definitions
		WHERE definition_id = $1 AND version = $2`, workflow.DefinitionID, workflow.DefinitionVersion).Scan(&definitionRaw); err != nil {
		return AdvanceGraphResult{}, fmt.Errorf("read graph for advancement: %w", err)
	}
	definitionGraph, err := decodeDefinitionGraph(definitionRaw)
	if err != nil {
		return AdvanceGraphResult{}, fmt.Errorf("%w: %v", ErrGraphViolation, err)
	}
	sourceDefinition, err := definitionGraph.node(input.FromNodeID)
	if err != nil {
		return AdvanceGraphResult{}, err
	}
	var sourceState WorkflowState
	var sourceAttempt *int64
	var acceptedResult []byte
	if err := tx.QueryRow(ctx, `
		SELECT state, current_attempt_number, accepted_result
		FROM engine.node_instances
		WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3
		FOR UPDATE`, input.WorkflowID, input.FromNodeID, input.Iteration).Scan(&sourceState, &sourceAttempt, &acceptedResult); err != nil {
		return AdvanceGraphResult{}, fmt.Errorf("lock source node for graph advancement: %w", err)
	}
	if sourceAttempt != nil {
		return AdvanceGraphResult{}, ErrAttemptNotCurrent
	}
	if sourceDefinition.Kind == "activity" {
		if sourceState != StateSucceeded || len(acceptedResult) == 0 {
			return AdvanceGraphResult{}, ErrResultNotConsumable
		}
	} else if sourceState != StateRunnable {
		return AdvanceGraphResult{}, fmt.Errorf("%w: control node %q is not runnable", ErrGraphViolation, input.FromNodeID)
	}
	if err := validateGraphSuccessors(definitionGraph, sourceDefinition, input.Iteration, input.Next); err != nil {
		return AdvanceGraphResult{}, err
	}
	if isTerminalWorkflowState(input.FinalWorkflowState) {
		rows, err := tx.Query(ctx, `
			SELECT node_id, iteration, state, current_attempt_number
			FROM engine.node_instances
			WHERE workflow_id = $1
			ORDER BY iteration, node_id
			FOR UPDATE`, input.WorkflowID)
		if err != nil {
			return AdvanceGraphResult{}, fmt.Errorf("lock sibling nodes for terminal graph advancement: %w", err)
		}
		for rows.Next() {
			var nodeID string
			var iteration int
			var nodeState WorkflowState
			var currentAttempt *int64
			if err := rows.Scan(&nodeID, &iteration, &nodeState, &currentAttempt); err != nil {
				rows.Close()
				return AdvanceGraphResult{}, fmt.Errorf("scan sibling node: %w", err)
			}
			if nodeID == input.FromNodeID && iteration == input.Iteration {
				nodeState = input.FinalNodeState
				currentAttempt = nil
			}
			if !isTerminalWorkflowState(nodeState) || currentAttempt != nil {
				rows.Close()
				return AdvanceGraphResult{}, fmt.Errorf("%w: terminal workflow has live node %s/%d", ErrGraphViolation, nodeID, iteration)
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return AdvanceGraphResult{}, fmt.Errorf("iterate sibling nodes: %w", err)
		}
		rows.Close()
	}
	newRevision := workflow.Revision + 1
	if _, err := tx.Exec(ctx, `
		UPDATE engine.node_instances
		SET state = $4, revision = revision + 1, updated_at = clock_timestamp()
		WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3`,
		input.WorkflowID, input.FromNodeID, input.Iteration, input.FinalNodeState); err != nil {
		return AdvanceGraphResult{}, fmt.Errorf("complete source node: %w", err)
	}
	created := make([]NodeInstance, 0, len(input.Next))
	for _, next := range input.Next {
		if next.NodeID == "" || next.Iteration < 0 {
			return AdvanceGraphResult{}, errors.New("next graph node identity is required")
		}
		if len(next.Input) == 0 {
			next.Input = json.RawMessage(`{}`)
		}
		if len(next.Dependencies) == 0 {
			next.Dependencies = []string{}
		}
		dependencies, err := json.Marshal(next.Dependencies)
		if err != nil {
			return AdvanceGraphResult{}, fmt.Errorf("encode graph dependencies: %w", err)
		}
		commandTag, err := tx.Exec(ctx, `
			INSERT INTO engine.node_instances
				(workflow_id, node_id, iteration, state, dependencies, input)
			VALUES ($1, $2, $3, 'RUNNABLE', $4, $5)
			ON CONFLICT (workflow_id, node_id, iteration) DO NOTHING`,
			input.WorkflowID, next.NodeID, next.Iteration, dependencies, next.Input)
		if err != nil {
			return AdvanceGraphResult{}, fmt.Errorf("create next graph node: %w", err)
		}
		if commandTag.RowsAffected() == 1 {
			created = append(created, NodeInstance{WorkflowID: input.WorkflowID, NodeID: next.NodeID,
				Iteration: next.Iteration, State: StateRunnable, Dependencies: next.Dependencies, Input: next.Input})
		}
	}
	oldState := workflow.State
	workflow.State = input.FinalWorkflowState
	workflow.Revision = newRevision
	workflow.UpdatedAt = time.Now().UTC()
	if _, err := tx.Exec(ctx, `
		UPDATE engine.workflow_executions
		SET state = $2, revision = $3, updated_at = clock_timestamp()
		WHERE workflow_id = $1`, input.WorkflowID, workflow.State, newRevision); err != nil {
		return AdvanceGraphResult{}, fmt.Errorf("advance workflow graph: %w", err)
	}
	if err := insertHistory(ctx, tx, input.WorkflowID, newRevision, "scheduler", input.ActorID,
		&input.Lease.Epoch, input.FromNodeID, &input.Iteration, nil, &oldState, workflow.State, "GRAPH_ADVANCED"); err != nil {
		return AdvanceGraphResult{}, err
	}
	payload, err := json.Marshal(map[string]any{"workflow_id": input.WorkflowID, "from_node_id": input.FromNodeID, "next": input.Next})
	if err != nil {
		return AdvanceGraphResult{}, err
	}
	if err := insertOutbox(ctx, tx, input.WorkflowID, newRevision, "graph.advanced", payload); err != nil {
		return AdvanceGraphResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return AdvanceGraphResult{}, err
	}
	return AdvanceGraphResult{Workflow: workflow, Created: created}, nil
}

type nodeScanner interface {
	Scan(dest ...any) error
}

func scanNode(scanner nodeScanner) (NodeInstance, error) {
	var node NodeInstance
	var dependencies, input, accepted []byte
	if err := scanner.Scan(&node.WorkflowID, &node.NodeID, &node.Iteration, &node.State,
		&dependencies, &input, &accepted, &node.CurrentAttempt, &node.RetryCount,
		&node.DeadlineAt, &node.TimerFired, &node.Revision); err != nil {
		return NodeInstance{}, err
	}
	if err := json.Unmarshal(dependencies, &node.Dependencies); err != nil {
		return NodeInstance{}, fmt.Errorf("decode node dependencies: %w", err)
	}
	node.Input = json.RawMessage(input)
	if accepted != nil {
		node.AcceptedResult = json.RawMessage(accepted)
	}
	return node, nil
}

func scanNodeRow(row pgx.Row) (NodeInstance, error) {
	return scanNode(row)
}
