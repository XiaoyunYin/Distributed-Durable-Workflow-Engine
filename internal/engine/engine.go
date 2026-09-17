// Package engine contains the test-only scheduler/interpreter used by M1.
// It deliberately delegates every durable mutation to state.Store; the
// activity driver is the only in-process extension point.
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
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
		for id := range graph.Nodes {
			graph.Entry = id
			break
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
	lease, _, err := e.Store.AcquireLease(ctx, wf.PartitionID, ownerID, ttl)
	if err != nil {
		return RunResult{}, err
	}
	defer func() {
		_ = e.Store.ReleaseLease(context.Background(), state.LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch})
	}()
	maxSteps := e.MaxSteps
	if maxSteps <= 0 {
		maxSteps = 100
	}
	for steps := 0; steps < maxSteps; steps++ {
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
		for _, node := range nodes {
			if node.State != state.StateRunnable {
				continue
			}
			definitionNode, ok := graph.Nodes[node.NodeID]
			if !ok {
				return RunResult{}, fmt.Errorf("node %q is not in definition", node.NodeID)
			}
			if definitionNode.Kind == "join" && !e.joinReady(ctx, workflowID, node) {
				continue
			}
			if err := e.runNode(ctx, wf, node, definitionNode, graph, lease, workerID, actorID); err != nil {
				return RunResult{}, err
			}
			break
		}
		// Re-read state to distinguish a completed last step from a durable wait.
		wf, err = e.Store.GetWorkflow(ctx, workflowID)
		if err != nil {
			return RunResult{}, err
		}
		if terminal(wf.State) {
			return RunResult{Workflow: wf, Steps: steps + 1}, nil
		}
		if !hasRunnable(nodes) {
			return RunResult{Workflow: wf, Steps: steps + 1, Blocked: true}, nil
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
		if node.RetryCount == 0 {
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
	_, err = e.Store.AdvanceGraph(ctx, state.AdvanceGraphInput{Lease: lease, WorkflowID: wf.WorkflowID,
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
	wf, err := e.Store.GetWorkflow(ctx, wf.WorkflowID)
	if err != nil {
		return err
	}
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
	claim, err := e.Store.ClaimAttempt(ctx, state.ClaimInput{WorkflowID: wf.WorkflowID, NodeID: node.NodeID,
		Iteration: node.Iteration, WorkerID: workerID,
		RequestID:    "engine:" + wf.WorkflowID + ":" + node.NodeID + ":" + fmt.Sprint(node.Iteration) + ":" + fmt.Sprint(attempt.AttemptNumber),
		AttemptLease: e.AttemptLease})
	if err != nil {
		return err
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
	_, err = e.Store.RecordResultReceipt(ctx, state.ResultInput{WorkflowID: wf.WorkflowID, NodeID: node.NodeID,
		Iteration: node.Iteration, AttemptNumber: claim.AttemptNumber, ClaimToken: claim.ClaimToken,
		AttemptState: result.AttemptState, Payload: result.Payload, EventType: "activity.result"})
	if err != nil {
		return err
	}
	wf, err = e.Store.GetWorkflow(ctx, wf.WorkflowID)
	if err != nil {
		return err
	}
	consume := state.ConsumeResultInput{Lease: lease, WorkflowID: wf.WorkflowID, NodeID: node.NodeID,
		Iteration: node.Iteration, AttemptNumber: claim.AttemptNumber, ExpectedRevision: wf.Revision,
		NewWorkflowState: state.StateRunnable, ActorID: actorID}
	if result.AttemptState == state.AttemptFailedRetryable {
		consume.NewWorkflowState = state.StateWaitingTimer
		due := time.Now().Add(result.RetryAfter)
		consume.RetryDueAt = &due
	} else if result.AttemptState == state.AttemptFailedFinal {
		consume.NewWorkflowState = state.StateFailed
	}
	consumed, err := e.Store.ConsumeResult(ctx, consume)
	if err != nil {
		return err
	}
	if result.AttemptState != state.AttemptSucceeded {
		return nil
	}
	next, err := nextInputsForResult(node, definitionNode, graph, result.Payload)
	if err != nil {
		return err
	}
	_, err = e.Store.AdvanceGraph(ctx, state.AdvanceGraphInput{Lease: lease, WorkflowID: wf.WorkflowID,
		FromNodeID: node.NodeID, Iteration: node.Iteration, ExpectedRevision: consumed.Workflow.Revision,
		Next: next, ActorID: actorID})
	return err
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

func hasRunnable(nodes []state.NodeInstance) bool {
	for _, node := range nodes {
		if node.State == state.StateRunnable {
			return true
		}
	}
	return false
}

func sortNodes(nodes []state.NodeInstance) {
	sort.Slice(nodes, func(i, j int) bool {
		if nodes[i].Iteration != nodes[j].Iteration {
			return nodes[i].Iteration < nodes[j].Iteration
		}
		return nodes[i].NodeID < nodes[j].NodeID
	})
}
