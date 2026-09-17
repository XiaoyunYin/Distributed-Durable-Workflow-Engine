// Package engine contains the test-only scheduler/interpreter used by M1.
// It deliberately delegates every durable mutation to state.Store; the
// activity driver is the only in-process extension point.
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"durable-agent-execution-engine/internal/state"
)

type Graph struct {
	Entry string
	Nodes map[string]Node
}

type Node struct {
	ID       string
	Kind     string
	Next     []string
	Branches []string
	Join     string
	Delay    time.Duration
}

type graphDocument struct {
	Entry string            `json:"entry"`
	Nodes []json.RawMessage `json:"nodes"`
}

type nodeDocument struct {
	ID       string          `json:"id"`
	Kind     string          `json:"kind"`
	Next     json.RawMessage `json:"next"`
	Branches []string        `json:"branches"`
	Join     string          `json:"join"`
	DelayMS  int             `json:"delay_ms"`
}

// ParseGraph accepts the documented object form and the original DUR-005
// shorthand {"nodes":["root"]}. The returned map is validated independently
// of the repository's transition rules.
func ParseGraph(raw json.RawMessage) (Graph, error) {
	var document graphDocument
	if err := json.Unmarshal(raw, &document); err != nil {
		return Graph{}, fmt.Errorf("decode workflow graph: %w", err)
	}
	if len(document.Nodes) == 0 {
		return Graph{}, errors.New("workflow graph must contain nodes")
	}
	graph := Graph{Entry: document.Entry, Nodes: make(map[string]Node, len(document.Nodes))}
	for _, rawNode := range document.Nodes {
		var shorthand string
		if err := json.Unmarshal(rawNode, &shorthand); err == nil {
			if shorthand == "" {
				return Graph{}, errors.New("workflow graph node ID cannot be empty")
			}
			if _, exists := graph.Nodes[shorthand]; exists {
				return Graph{}, fmt.Errorf("duplicate workflow graph node %q", shorthand)
			}
			graph.Nodes[shorthand] = Node{ID: shorthand, Kind: "activity"}
			continue
		}
		var node nodeDocument
		if err := json.Unmarshal(rawNode, &node); err != nil || node.ID == "" {
			return Graph{}, errors.New("workflow graph nodes must have non-empty IDs")
		}
		if node.Kind == "" {
			node.Kind = "activity"
		}
		if _, exists := graph.Nodes[node.ID]; exists {
			return Graph{}, fmt.Errorf("duplicate workflow graph node %q", node.ID)
		}
		next, err := decodeNext(node.Next)
		if err != nil {
			return Graph{}, fmt.Errorf("node %q: %w", node.ID, err)
		}
		graph.Nodes[node.ID] = Node{ID: node.ID, Kind: node.Kind, Next: next,
			Branches: append([]string(nil), node.Branches...), Join: node.Join,
			Delay: time.Duration(node.DelayMS) * time.Millisecond}
	}
	if graph.Entry == "" {
		if len(graph.Nodes) != 1 {
			return Graph{}, errors.New("graph entry is required when more than one node is declared")
		}
		for id := range graph.Nodes {
			graph.Entry = id
		}
	}
	if _, ok := graph.Nodes[graph.Entry]; !ok {
		return Graph{}, fmt.Errorf("graph entry %q is not declared", graph.Entry)
	}
	for _, node := range graph.Nodes {
		for _, ref := range append(append([]string{}, node.Next...), append(node.Branches, node.Join)...) {
			if ref != "" {
				if _, ok := graph.Nodes[ref]; !ok {
					return Graph{}, fmt.Errorf("node %q references undeclared node %q", node.ID, ref)
				}
			}
		}
	}
	return graph, nil
}

func decodeNext(raw json.RawMessage) ([]string, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var one string
	if err := json.Unmarshal(raw, &one); err == nil {
		if one == "" {
			return nil, errors.New("next node ID cannot be empty")
		}
		return []string{one}, nil
	}
	var many []string
	if err := json.Unmarshal(raw, &many); err != nil {
		return nil, errors.New("next must be a node ID or array of node IDs")
	}
	for _, id := range many {
		if id == "" {
			return nil, errors.New("next node ID cannot be empty")
		}
	}
	return many, nil
}

type Activity struct {
	WorkflowID       string
	NodeID           string
	Iteration        int
	AttemptNumber    int64
	EffectClass      state.EffectClass
	Input            json.RawMessage
	LogicalEffectKey string
	GrantScopeHash   string
}

type ActivityResult struct {
	AttemptState state.AttemptState
	Payload      json.RawMessage
	RetryAfter   time.Duration
}

type ActivityDriver interface {
	Run(context.Context, Activity) (ActivityResult, error)
}

type ActivityDriverFunc func(context.Context, Activity) (ActivityResult, error)

func (f ActivityDriverFunc) Run(ctx context.Context, activity Activity) (ActivityResult, error) {
	return f(ctx, activity)
}

type Engine struct {
	Store        *state.Store
	Driver       ActivityDriver
	OwnerID      string
	WorkerID     string
	ActorID      string
	LeaseTTL     time.Duration
	AttemptLease time.Duration
	RetryBackoff time.Duration
	MaxSteps     int
	// AfterBoundary is a test-only crash injector. Returning an error models a
	// process crash immediately after the named repository transaction commits.
	AfterBoundary func(string) error
}

type RunResult struct {
	Workflow state.Workflow
	Steps    int
	Blocked  bool
}

func New(store *state.Store, driver ActivityDriver) *Engine {
	return &Engine{Store: store, Driver: driver, LeaseTTL: time.Minute,
		AttemptLease: time.Minute, RetryBackoff: 25 * time.Millisecond, MaxSteps: 100}
}

func (e *Engine) Run(ctx context.Context, workflowID string) (RunResult, error) {
	if e == nil || e.Store == nil || e.Driver == nil {
		return RunResult{}, errors.New("engine requires a store and activity driver")
	}
	wf, err := e.Store.GetWorkflow(ctx, workflowID)
	if err != nil {
		return RunResult{}, err
	}
	definition, err := e.Store.GetDefinition(ctx, wf.DefinitionID, wf.DefinitionVersion)
	if err != nil {
		return RunResult{}, err
	}
	graph, err := ParseGraph(definition.Graph)
	if err != nil {
		return RunResult{}, err
	}
	ownerID := e.OwnerID
	if ownerID == "" {
		ownerID = state.NewID()
	}
	workerID := e.WorkerID
	if workerID == "" {
		workerID = "m1-worker"
	}
	actorID := e.ActorID
	if actorID == "" {
		actorID = "m1-scheduler"
	}
	ttl := e.LeaseTTL
	if ttl <= 0 {
		ttl = time.Minute
	}
	lease, acquired, err := e.Store.AcquireLease(ctx, wf.PartitionID, ownerID, ttl)
	if err != nil {
		return RunResult{}, err
	}
	if !acquired {
		return RunResult{Workflow: wf, Blocked: true}, state.ErrLeaseNotOwned
	}
	ownedLease := lease
	defer func() {
		_ = e.Store.ReleaseLease(context.Background(), state.LeaseRef{PartitionID: ownedLease.PartitionID, OwnerID: ownedLease.OwnerID, Epoch: ownedLease.Epoch})
	}()
	maxSteps := e.MaxSteps
	if maxSteps <= 0 {
		maxSteps = 100
	}
	for steps := 0; steps < maxSteps; steps++ {
		var renewedLease state.Lease
		renewedLease, err = e.Store.RenewLease(ctx, state.LeaseRef{
			PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch,
		}, ttl)
		if err != nil {
			return RunResult{}, err
		}
		lease = renewedLease
		ownedLease = renewedLease
		wf, err = e.Store.GetWorkflow(ctx, workflowID)
		if err != nil {
			return RunResult{}, err
		}
		if terminal(wf.State) {
			return RunResult{Workflow: wf, Steps: steps}, nil
		}
		nodes, err := e.Store.ListNodes(ctx, workflowID)
		if err != nil {
			return RunResult{}, err
		}
		if progressed, err := e.advanceDueTimer(ctx, wf, nodes, lease, actorID); err != nil {
			return RunResult{}, err
		} else if progressed {
			continue
		}
		progressed := false
		for _, node := range nodes {
			definitionNode, ok := graph.Nodes[node.NodeID]
			if !ok {
				return RunResult{}, fmt.Errorf("node %q is not in definition", node.NodeID)
			}
			var nodeProgress bool
			switch node.State {
			case state.StateRunnable:
				if definitionNode.Kind == "join" && !e.joinReady(ctx, workflowID, node) {
					continue
				}
				if err := e.runNode(ctx, wf, node, definitionNode, graph, lease, workerID, actorID); err != nil {
					return RunResult{}, err
				}
				nodeProgress = true
			case state.StateWaitingActivity:
				nodeProgress, err = e.recoverActivity(ctx, wf, node, definitionNode, graph, lease, workerID, actorID)
				if err != nil {
					return RunResult{}, err
				}
			case state.StateSucceeded:
				nodeProgress, err = e.recoverSucceeded(ctx, wf, node, definitionNode, graph, lease, actorID)
				if err != nil {
					return RunResult{}, err
				}
			}
			if !nodeProgress {
				continue
			}
			progressed = true
			break
		}
		if !progressed {
			return RunResult{Workflow: wf, Steps: steps + 1, Blocked: true}, nil
		}
		// Re-read state to distinguish a completed last step from a durable wait.
		wf, err = e.Store.GetWorkflow(ctx, workflowID)
		if err != nil {
			return RunResult{}, err
		}
		if terminal(wf.State) {
			return RunResult{Workflow: wf, Steps: steps + 1}, nil
		}
	}
	wf, err = e.Store.GetWorkflow(ctx, workflowID)
	return RunResult{Workflow: wf, Steps: maxSteps, Blocked: err == nil}, err
}

func (e *Engine) advanceDueTimer(ctx context.Context, wf state.Workflow, nodes []state.NodeInstance, lease state.Lease, actorID string) (bool, error) {
	for _, node := range nodes {
		if node.State != state.StateWaitingTimer {
			continue
		}
		due, _, found, err := e.Store.PendingTimer(ctx, wf.WorkflowID, node.NodeID, node.Iteration)
		if err != nil {
			return false, err
		}
		if found && !due.After(time.Now()) {
			newState := state.StateRunnable
			ref := state.LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch}
			err := e.Store.ApplyOwnerTransition(ctx, state.OwnerTransitionInput{Lease: ref,
				WorkflowID: wf.WorkflowID, ExpectedRevision: wf.Revision, NewState: newState,
				ActorID: actorID, Reason: "TIMER_DUE", NodeID: node.NodeID,
				Iteration: node.Iteration, NodeState: &newState})
			return true, err
		}
	}
	return false, nil
}

func (e *Engine) runNode(ctx context.Context, wf state.Workflow, node state.NodeInstance, definitionNode Node, graph Graph,
	lease state.Lease, workerID, actorID string) error {
	ref := state.LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch}
	switch definitionNode.Kind {
	case "activity":
		return e.runActivity(ctx, wf, node, definitionNode, graph, ref, workerID, actorID)
	case "fanout", "fan_out":
		next := make([]state.GraphNodeInput, 0, len(definitionNode.Branches)+1)
		for _, branch := range definitionNode.Branches {
			next = append(next, state.GraphNodeInput{NodeID: branch, Iteration: node.Iteration, Input: node.Input})
		}
		if definitionNode.Join != "" {
			next = append(next, state.GraphNodeInput{NodeID: definitionNode.Join, Iteration: node.Iteration,
				Dependencies: append([]string(nil), definitionNode.Branches...), Input: node.Input})
		}
		_, err := e.Store.AdvanceGraph(ctx, state.AdvanceGraphInput{Lease: ref, WorkflowID: wf.WorkflowID,
			FromNodeID: node.NodeID, Iteration: node.Iteration, ExpectedRevision: wf.Revision, Next: next, ActorID: actorID})
		return err
	case "join":
		return e.advanceFromControl(ctx, wf, node, definitionNode, graph, ref, actorID)
	case "timer":
		if !node.TimerFired {
			delay := definitionNode.Delay
			if delay <= 0 {
				delay = 25 * time.Millisecond
			}
			_, err := e.Store.ScheduleTimer(ctx, state.ScheduleTimerInput{Lease: ref, WorkflowID: wf.WorkflowID,
				NodeID: node.NodeID, Iteration: node.Iteration, ExpectedRevision: wf.Revision,
				DueAt: time.Now().Add(delay), Purpose: "WORKFLOW_TIMER", ActorID: actorID})
			return err
		}
		return e.advanceFromControl(ctx, wf, node, definitionNode, graph, ref, actorID)
	case "success", "succeed":
		_, err := e.Store.AdvanceGraph(ctx, state.AdvanceGraphInput{Lease: ref, WorkflowID: wf.WorkflowID,
			FromNodeID: node.NodeID, Iteration: node.Iteration, ExpectedRevision: wf.Revision,
			FinalWorkflowState: state.StateSucceeded, ActorID: actorID})
		return err
	case "failure", "fail":
		_, err := e.Store.AdvanceGraph(ctx, state.AdvanceGraphInput{Lease: ref, WorkflowID: wf.WorkflowID,
			FromNodeID: node.NodeID, Iteration: node.Iteration, ExpectedRevision: wf.Revision,
			FinalWorkflowState: state.StateFailed, FinalNodeState: state.StateFailed, ActorID: actorID})
		return err
	default:
		return fmt.Errorf("unsupported graph node kind %q", definitionNode.Kind)
	}
}

func (e *Engine) advanceFromControl(ctx context.Context, wf state.Workflow, node state.NodeInstance, definitionNode Node,
	graph Graph, lease state.LeaseRef, actorID string) error {
	next, err := nextInputs(node, definitionNode, graph)
	if err != nil {
		return err
	}
	ref := state.LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch}
	_, err = e.Store.AdvanceGraph(ctx, state.AdvanceGraphInput{Lease: ref, WorkflowID: wf.WorkflowID,
		FromNodeID: node.NodeID, Iteration: node.Iteration, ExpectedRevision: wf.Revision, Next: next, ActorID: actorID})
	return err
}

func (e *Engine) runActivity(ctx context.Context, wf state.Workflow, node state.NodeInstance, definitionNode Node,
	graph Graph, lease state.LeaseRef, workerID, actorID string) error {
	transitionNode := state.StateWaitingActivity
	if err := e.Store.ApplyOwnerTransition(ctx, state.OwnerTransitionInput{Lease: lease, WorkflowID: wf.WorkflowID,
		ExpectedRevision: wf.Revision, NewState: state.StateWaitingActivity, NodeID: node.NodeID,
		Iteration: node.Iteration, NodeState: &transitionNode, ActorID: actorID, Reason: "ACTIVITY_SCHEDULED"}); err != nil {
		return err
	}
	if err := e.boundary("activity_scheduled"); err != nil {
		return err
	}
	wf, err := e.Store.GetWorkflow(ctx, wf.WorkflowID)
	if err != nil {
		return err
	}
	return e.startActivity(ctx, wf, node, definitionNode, graph, lease, workerID, actorID)
}

func (e *Engine) startActivity(ctx context.Context, wf state.Workflow, node state.NodeInstance, definitionNode Node,
	graph Graph, lease state.LeaseRef, workerID, actorID string) error {
	effectClass, ok := definitionEffect(wf, node.NodeID, e.Store, ctx)
	if !ok {
		return fmt.Errorf("no effect class for activity %q", node.NodeID)
	}
	logicalKey := wf.WorkflowID + ":" + node.NodeID + ":" + fmt.Sprint(node.Iteration)
	attempt, err := e.Store.CreateAttempt(ctx, state.AttemptInput{Lease: lease, WorkflowID: wf.WorkflowID,
		NodeID: node.NodeID, Iteration: node.Iteration, ExpectedRevision: wf.Revision,
		EffectClass: effectClass, LogicalEffectKey: logicalKey, GrantScopeHash: node.NodeID,
		HeartbeatDeadline: time.Now().Add(e.AttemptLease), ActorID: actorID})
	if err != nil {
		return err
	}
	if err := e.boundary("attempt_created"); err != nil {
		return err
	}
	return e.executeAttempt(ctx, wf, node, definitionNode, graph, attempt, lease, workerID, actorID)
}

func (e *Engine) executeAttempt(ctx context.Context, wf state.Workflow, node state.NodeInstance, definitionNode Node,
	graph Graph, attempt state.Attempt, lease state.LeaseRef, workerID, actorID string) error {
	claim := state.ClaimResult{AttemptNumber: attempt.AttemptNumber, ClaimToken: attempt.ClaimToken,
		EffectClass: attempt.EffectClass}
	if attempt.State == state.AttemptDispatchable {
		var err error
		claim, err = e.Store.ClaimAttempt(ctx, state.ClaimInput{WorkflowID: wf.WorkflowID, NodeID: node.NodeID,
			Iteration: node.Iteration, WorkerID: workerID,
			RequestID:    "engine:" + wf.WorkflowID + ":" + node.NodeID + ":" + fmt.Sprint(node.Iteration) + ":" + fmt.Sprint(attempt.AttemptNumber),
			AttemptLease: e.AttemptLease})
		if err != nil {
			return err
		}
		if err := e.boundary("attempt_claimed"); err != nil {
			return err
		}
	}
	if claim.ClaimToken == "" {
		return state.ErrStaleClaim
	}
	result, runErr := e.Driver.Run(ctx, Activity{WorkflowID: wf.WorkflowID, NodeID: node.NodeID,
		Iteration: node.Iteration, AttemptNumber: claim.AttemptNumber, EffectClass: claim.EffectClass,
		Input: node.Input, LogicalEffectKey: attempt.LogicalEffectKey, GrantScopeHash: attempt.GrantScopeHash})
	if runErr != nil {
		result = ActivityResult{AttemptState: state.AttemptFailedRetryable,
			Payload: json.RawMessage(fmt.Sprintf(`{"error":%q}`, runErr.Error())), RetryAfter: e.RetryBackoff}
	}
	if result.AttemptState == "" {
		result.AttemptState = state.AttemptSucceeded
	}
	if result.Payload == nil {
		result.Payload = json.RawMessage(`{}`)
	}
	if result.RetryAfter <= 0 {
		result.RetryAfter = e.RetryBackoff
	}
	if result.AttemptState == state.AttemptSucceeded {
		if err := validateActivityResult(definitionNode, result.Payload); err != nil {
			result.AttemptState = state.AttemptFailedFinal
			result.Payload = json.RawMessage(fmt.Sprintf(`{"error":%q}`, err.Error()))
		}
	}
	if _, err := e.Store.RecordResultReceipt(ctx, state.ResultInput{WorkflowID: wf.WorkflowID, NodeID: node.NodeID,
		Iteration: node.Iteration, AttemptNumber: claim.AttemptNumber, ClaimToken: claim.ClaimToken,
		AttemptState: result.AttemptState, Payload: result.Payload, EventType: "activity.result"}); err != nil {
		return err
	}
	if err := e.boundary("result_recorded"); err != nil {
		return err
	}
	return e.consumeRecordedResult(ctx, wf.WorkflowID, node, claim.AttemptNumber, result.AttemptState,
		result.RetryAfter, lease, actorID)
}

func (e *Engine) consumeRecordedResult(ctx context.Context, workflowID string, node state.NodeInstance,
	attemptNumber int64, attemptState state.AttemptState, retryAfter time.Duration, lease state.LeaseRef, actorID string) error {
	wf, err := e.Store.GetWorkflow(ctx, workflowID)
	if err != nil {
		return err
	}
	consume := state.ConsumeResultInput{Lease: lease, WorkflowID: workflowID, NodeID: node.NodeID,
		Iteration: node.Iteration, AttemptNumber: attemptNumber, ExpectedRevision: wf.Revision,
		NewWorkflowState: state.StateRunnable, ActorID: actorID}
	if attemptState == state.AttemptFailedRetryable {
		consume.NewWorkflowState = state.StateWaitingTimer
		due := time.Now().Add(retryAfter)
		consume.RetryDueAt = &due
	} else if attemptState == state.AttemptFailedFinal {
		consume.NewWorkflowState = state.StateFailed
	}
	if _, err := e.Store.ConsumeResult(ctx, consume); err != nil {
		return err
	}
	return e.boundary("result_consumed")
}

func (e *Engine) recoverActivity(ctx context.Context, wf state.Workflow, node state.NodeInstance, definitionNode Node,
	graph Graph, lease state.Lease, workerID, actorID string) (bool, error) {
	ref := state.LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch}
	if node.CurrentAttempt == nil {
		if err := e.startActivity(ctx, wf, node, definitionNode, graph, ref, workerID, actorID); err != nil {
			return false, err
		}
		return true, nil
	}
	attempt, err := e.Store.GetAttempt(ctx, wf.WorkflowID, node.NodeID, node.Iteration, *node.CurrentAttempt)
	if err != nil {
		return false, err
	}
	if !attempt.IsCurrent && terminalAttempt(attempt.State) && len(attempt.Result) > 0 {
		if err := e.consumeRecordedResult(ctx, wf.WorkflowID, node, attempt.AttemptNumber, attempt.State,
			e.RetryBackoff, ref, actorID); err != nil {
			return false, err
		}
		return true, nil
	}
	if attempt.State == state.AttemptDispatchable {
		if err := e.executeAttempt(ctx, wf, node, definitionNode, graph, attempt, ref, workerID, actorID); err != nil {
			return false, err
		}
		return true, nil
	}
	if attempt.State == state.AttemptClaimed && attempt.HeartbeatDeadline != nil && !attempt.HeartbeatDeadline.After(time.Now()) {
		_, err := e.Store.TimeoutAttempt(ctx, state.TimeoutInput{Lease: ref, WorkflowID: wf.WorkflowID,
			NodeID: node.NodeID, Iteration: node.Iteration, AttemptNumber: attempt.AttemptNumber,
			ExpectedRevision: wf.Revision, ActorID: actorID, DispatchLease: e.AttemptLease})
		if err != nil {
			return false, err
		}
		return true, e.boundary("attempt_timed_out")
	}
	return false, nil
}

func (e *Engine) recoverSucceeded(ctx context.Context, wf state.Workflow, node state.NodeInstance, definitionNode Node,
	graph Graph, lease state.Lease, actorID string) (bool, error) {
	var next []state.GraphNodeInput
	var err error
	if definitionNode.Kind == "activity" {
		next, err = nextInputsForResult(node, definitionNode, graph, node.AcceptedResult)
	} else {
		next, err = nextInputs(node, definitionNode, graph)
	}
	if err != nil {
		return false, err
	}
	nodes, err := e.Store.ListNodes(ctx, wf.WorkflowID)
	if err != nil {
		return false, err
	}
	allExist := true
	for _, item := range next {
		found := false
		for _, existing := range nodes {
			if existing.NodeID == item.NodeID && existing.Iteration == item.Iteration {
				found = true
				break
			}
		}
		if !found {
			allExist = false
			break
		}
	}
	if allExist && len(next) > 0 {
		return false, nil
	}
	ref := state.LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch}
	_, err = e.Store.AdvanceGraph(ctx, state.AdvanceGraphInput{Lease: ref, WorkflowID: wf.WorkflowID,
		FromNodeID: node.NodeID, Iteration: node.Iteration, ExpectedRevision: wf.Revision,
		Next: next, ActorID: actorID})
	if err != nil {
		return false, err
	}
	return true, e.boundary("graph_advanced")
}

func (e *Engine) boundary(name string) error {
	if e.AfterBoundary != nil {
		return e.AfterBoundary(name)
	}
	return nil
}

func validateActivityResult(node Node, payload json.RawMessage) error {
	if len(node.Next) <= 1 {
		return nil
	}
	var choice struct {
		Next string `json:"next"`
	}
	if err := json.Unmarshal(payload, &choice); err != nil || choice.Next == "" || !contains(node.Next, choice.Next) {
		return fmt.Errorf("activity output selected an undeclared successor")
	}
	return nil
}

func contains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func definitionEffect(wf state.Workflow, nodeID string, store *state.Store, ctx context.Context) (state.EffectClass, bool) {
	definition, err := store.GetDefinition(ctx, wf.DefinitionID, wf.DefinitionVersion)
	if err != nil {
		return "", false
	}
	effect, ok := definition.EffectClasses[nodeID]
	return effect, ok
}

func nextInputs(node state.NodeInstance, definitionNode Node, graph Graph) ([]state.GraphNodeInput, error) {
	if len(definitionNode.Next) == 0 {
		return nil, nil
	}
	result := make([]state.GraphNodeInput, 0, len(definitionNode.Next))
	for _, id := range definitionNode.Next {
		if _, ok := graph.Nodes[id]; !ok {
			return nil, fmt.Errorf("next node %q is not declared", id)
		}
		result = append(result, state.GraphNodeInput{NodeID: id, Iteration: node.Iteration, Input: node.Input})
	}
	return result, nil
}

func nextInputsForResult(node state.NodeInstance, definitionNode Node, graph Graph, payload json.RawMessage) ([]state.GraphNodeInput, error) {
	next := definitionNode.Next
	if len(next) > 1 {
		var choice struct {
			Next string `json:"next"`
		}
		if json.Unmarshal(payload, &choice) == nil && choice.Next != "" {
			next = []string{choice.Next}
		} else {
			return nil, fmt.Errorf("activity %q returned no branch choice", node.NodeID)
		}
	}
	if len(next) == 0 {
		return nil, nil
	}
	result := make([]state.GraphNodeInput, 0, len(next))
	for _, id := range next {
		if _, ok := graph.Nodes[id]; !ok {
			return nil, fmt.Errorf("next node %q is not declared", id)
		}
		if len(definitionNode.Next) > 0 && !containsString(definitionNode.Next, id) {
			return nil, fmt.Errorf("activity %q selected an undeclared successor %q", node.NodeID, id)
		}
		result = append(result, state.GraphNodeInput{NodeID: id, Iteration: node.Iteration, Input: payload})
	}
	return result, nil
}

func (e *Engine) joinReady(ctx context.Context, workflowID string, node state.NodeInstance) bool {
	for _, dependency := range node.Dependencies {
		dependencyNode, err := e.Store.GetNode(ctx, workflowID, dependency, node.Iteration)
		if err != nil || dependencyNode.State != state.StateSucceeded {
			return false
		}
	}
	return len(node.Dependencies) > 0
}

func terminal(s state.WorkflowState) bool {
	return s == state.StateSucceeded || s == state.StateFailed || s == state.StateRejected ||
		s == state.StateCanceled || s == state.StateAbandoned
}

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func terminalAttempt(attempt state.AttemptState) bool {
	return attempt == state.AttemptSucceeded || attempt == state.AttemptFailedRetryable ||
		attempt == state.AttemptFailedFinal
}
