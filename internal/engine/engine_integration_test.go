package engine

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/partition"
	"durable-agent-execution-engine/internal/state"
)

func TestM1InterpreterTimersAndRestart(t *testing.T) {
	ctx, store := openM1Database(t)
	defer store.Close()
	definitionID := "dur007-engine-" + state.NewID()
	workflowID, lease := createM1Workflow(t, ctx, store, definitionID, `{"entry":"root","nodes":[{"id":"root","kind":"activity","next":"wait"},{"id":"wait","kind":"timer","delay_ms":30,"next":"finish"},{"id":"finish","kind":"activity","next":"done"},{"id":"done","kind":"success"}]}`, `{"root":"PURE_ACTIVITY","finish":"PURE_ACTIVITY"}`)
	defer func() {
		_, _ = store.Pool().Exec(ctx, `DELETE FROM engine.workflow_executions WHERE workflow_id = $1`, workflowID)
		_, _ = store.Pool().Exec(ctx, `DELETE FROM engine.workflow_definitions WHERE definition_id = $1`, definitionID)
		_ = store.ReleaseLease(ctx, state.LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch})
	}()

	var mu sync.Mutex
	rootCalls := 0
	driver := ActivityDriverFunc(func(_ context.Context, activity Activity) (ActivityResult, error) {
		mu.Lock()
		defer mu.Unlock()
		if activity.NodeID == "root" {
			rootCalls++
			if rootCalls == 1 {
				return ActivityResult{}, errors.New("transient")
			}
		}
		return ActivityResult{Payload: []byte(`{"ok":true}`)}, nil
	})

	first := New(store, driver)
	first.OwnerID = lease.OwnerID
	first.RetryBackoff = 30 * time.Millisecond
	first.LeaseTTL = time.Minute
	first.MaxSteps = 1
	run, err := first.Run(ctx, workflowID)
	if err != nil || !run.Blocked || run.Workflow.State != state.StateWaitingTimer {
		t.Fatalf("first run = %+v, err=%v", run, err)
	}
	time.Sleep(60 * time.Millisecond)

	// A fresh Engine instance stands in for a scheduler restart.
	second := New(store, driver)
	second.OwnerID = lease.OwnerID
	second.RetryBackoff = 30 * time.Millisecond
	second.LeaseTTL = time.Minute
	second.MaxSteps = 3
	run, err = second.Run(ctx, workflowID)
	if err != nil || !run.Blocked || run.Workflow.State != state.StateWaitingTimer {
		t.Fatalf("second run = %+v, err=%v", run, err)
	}
	time.Sleep(60 * time.Millisecond)
	third := New(store, driver)
	third.OwnerID = lease.OwnerID
	third.LeaseTTL = time.Minute
	run, err = third.Run(ctx, workflowID)
	if err != nil || run.Blocked || run.Workflow.State != state.StateSucceeded {
		t.Fatalf("restart run = %+v, err=%v", run, err)
	}
	if rootCalls != 2 {
		t.Fatalf("root calls = %d, want one retry", rootCalls)
	}
	nodes, err := store.ListNodes(ctx, workflowID)
	if err != nil || len(nodes) != 4 {
		t.Fatalf("nodes = %+v, err=%v", nodes, err)
	}
	var unconsumed int
	if err := store.Pool().QueryRow(ctx, `SELECT count(*) FROM engine.timers WHERE workflow_id = $1 AND consumed_at IS NULL`, workflowID).Scan(&unconsumed); err != nil {
		t.Fatal(err)
	}
	if unconsumed != 0 {
		t.Fatalf("unconsumed timers = %d", unconsumed)
	}
}

func TestM1FanoutCancellationPreservesEffectEvidence(t *testing.T) {
	ctx, store := openM1Database(t)
	defer store.Close()
	definitionID := "dur007-cancel-" + state.NewID()
	workflowID, lease := createM1Workflow(t, ctx, store, definitionID, `{"entry":"root","nodes":[{"id":"root","kind":"fanout","branches":["left","right"],"join":"join"},{"id":"left","kind":"activity","next":"join"},{"id":"right","kind":"activity","next":"join"},{"id":"join","kind":"join","next":"done"},{"id":"done","kind":"success"}]}`, `{"left":"COOPERATING_EFFECT","right":"NON_COOPERATING_EFFECT"}`)
	defer func() {
		_, _ = store.Pool().Exec(ctx, `DELETE FROM engine.workflow_executions WHERE workflow_id = $1`, workflowID)
		_, _ = store.Pool().Exec(ctx, `DELETE FROM engine.workflow_definitions WHERE definition_id = $1`, definitionID)
		_ = store.ReleaseLease(ctx, state.LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch})
	}()
	ref := state.LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch}
	workflow, err := store.GetWorkflow(ctx, workflowID)
	if err != nil {
		t.Fatal(err)
	}
	advanced, err := store.AdvanceGraph(ctx, state.AdvanceGraphInput{Lease: ref, WorkflowID: workflowID,
		FromNodeID: "root", Iteration: 0, ExpectedRevision: workflow.Revision,
		Next: []state.GraphNodeInput{{NodeID: "left", Iteration: 0}, {NodeID: "right", Iteration: 0},
			{NodeID: "join", Iteration: 0, Dependencies: []string{"left", "right"}}}, ActorID: "test"})
	if err != nil {
		t.Fatal(err)
	}
	workflow = advanced.Workflow
	claimTokens := make(map[string]state.ClaimResult)
	for _, node := range []struct {
		id    string
		class state.EffectClass
	}{
		{id: "left", class: state.EffectCooperating}, {id: "right", class: state.EffectNonCooperating},
	} {
		nodeState := state.StateWaitingActivity
		if err := store.ApplyOwnerTransition(ctx, state.OwnerTransitionInput{Lease: ref, WorkflowID: workflowID,
			ExpectedRevision: workflow.Revision, NewState: state.StateWaitingActivity, NodeID: node.id,
			Iteration: 0, NodeState: &nodeState, ActorID: "test", Reason: "SCHEDULE_ACTIVITY"}); err != nil {
			t.Fatal(err)
		}
		workflow, err = store.GetWorkflow(ctx, workflowID)
		if err != nil {
			t.Fatal(err)
		}
		attempt, err := store.CreateAttempt(ctx, state.AttemptInput{Lease: ref, WorkflowID: workflowID,
			NodeID: node.id, Iteration: 0, ExpectedRevision: workflow.Revision, EffectClass: node.class,
			LogicalEffectKey: workflowID + ":" + node.id, GrantScopeHash: "grant", ActorID: "test"})
		if err != nil {
			t.Fatal(err)
		}
		claim, err := store.ClaimAttempt(ctx, state.ClaimInput{WorkflowID: workflowID, NodeID: node.id,
			Iteration: 0, WorkerID: "worker-" + node.id, RequestID: "request-" + node.id, AttemptLease: time.Minute})
		if err != nil {
			t.Fatal(err)
		}
		if claim.AttemptNumber != attempt.AttemptNumber {
			t.Fatalf("claim = %+v, attempt = %+v", claim, attempt)
		}
		claimTokens[node.id] = claim
		workflow, err = store.GetWorkflow(ctx, workflowID)
		if err != nil {
			t.Fatal(err)
		}
	}
	workflow, err = store.CancelWorkflow(ctx, state.CancelWorkflowInput{Lease: ref, WorkflowID: workflowID,
		ExpectedRevision: workflow.Revision, ActorID: "test"})
	if err != nil || workflow.State != state.StateCanceled {
		t.Fatalf("cancel = %+v, err=%v", workflow, err)
	}
	nodes, err := store.ListNodes(ctx, workflowID)
	if err != nil {
		t.Fatal(err)
	}
	for _, node := range nodes {
		if node.NodeID != "root" && node.State != state.StateCanceled {
			t.Fatalf("node %s was not canceled: %+v", node.NodeID, node)
		}
	}
	claim := claimTokens["right"]
	if _, err := store.RecordResultReceipt(ctx, state.ResultInput{WorkflowID: workflowID, NodeID: "right", Iteration: 0,
		AttemptNumber: claim.AttemptNumber, ClaimToken: claim.ClaimToken, AttemptState: state.AttemptSucceeded,
		Payload: []byte(`{"applied":true}`)}); err != nil {
		t.Fatal(err)
	}
	var evidence int
	if err := store.Pool().QueryRow(ctx, `SELECT count(*) FROM engine.attempt_result_evidence WHERE workflow_id = $1`, workflowID).Scan(&evidence); err != nil {
		t.Fatal(err)
	}
	if evidence != 1 {
		t.Fatalf("evidence rows = %d, want one", evidence)
	}
}

func TestM1InterpreterFanoutJoin(t *testing.T) {
	ctx, store := openM1Database(t)
	defer store.Close()
	definitionID := "dur007-fanout-" + state.NewID()
	workflowID, lease := createM1Workflow(t, ctx, store, definitionID, `{"entry":"root","nodes":[{"id":"root","kind":"fanout","branches":["left","right"],"join":"join"},{"id":"left","kind":"activity","next":"join"},{"id":"right","kind":"activity","next":"join"},{"id":"join","kind":"join","next":"done"},{"id":"done","kind":"success"}]}`, `{"left":"PURE_ACTIVITY","right":"PURE_ACTIVITY"}`)
	defer func() {
		_, _ = store.Pool().Exec(ctx, `DELETE FROM engine.workflow_executions WHERE workflow_id = $1`, workflowID)
		_, _ = store.Pool().Exec(ctx, `DELETE FROM engine.workflow_definitions WHERE definition_id = $1`, definitionID)
		_ = store.ReleaseLease(ctx, state.LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch})
	}()
	e := New(store, ActivityDriverFunc(func(_ context.Context, _ Activity) (ActivityResult, error) {
		return ActivityResult{Payload: []byte(`{"branch":"done"}`)}, nil
	}))
	e.OwnerID = lease.OwnerID
	result, err := e.Run(ctx, workflowID)
	if err != nil || result.Blocked || result.Workflow.State != state.StateSucceeded {
		t.Fatalf("fan-out run = %+v, err=%v", result, err)
	}
	nodes, err := store.ListNodes(ctx, workflowID)
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 5 {
		t.Fatalf("fan-out nodes = %d, want 5: %+v", len(nodes), nodes)
	}
	for _, node := range nodes {
		if node.State != state.StateSucceeded {
			t.Fatalf("node %s did not succeed: %+v", node.NodeID, node)
		}
	}
	var joins int
	if err := store.Pool().QueryRow(ctx, `SELECT count(*) FROM engine.node_instances WHERE workflow_id = $1 AND node_id = 'join'`, workflowID).Scan(&joins); err != nil {
		t.Fatal(err)
	}
	if joins != 1 {
		t.Fatalf("join instances = %d, want one", joins)
	}
}

func TestM1ConcurrentBranchCompletionCreatesJoinOnce(t *testing.T) {
	ctx, store := openM1Database(t)
	defer store.Close()
	definitionID := "dur007-race-" + state.NewID()
	workflowID, lease := createM1Workflow(t, ctx, store, definitionID, `{"entry":"root","nodes":[{"id":"root","kind":"fanout","branches":["left","right"],"join":"join"},{"id":"left","kind":"activity","next":"join"},{"id":"right","kind":"activity","next":"join"},{"id":"join","kind":"join","next":"done"},{"id":"done","kind":"success"}]}`, `{"left":"PURE_ACTIVITY","right":"PURE_ACTIVITY"}`)
	defer func() {
		_, _ = store.Pool().Exec(ctx, `DELETE FROM engine.workflow_executions WHERE workflow_id = $1`, workflowID)
		_, _ = store.Pool().Exec(ctx, `DELETE FROM engine.workflow_definitions WHERE definition_id = $1`, definitionID)
		_ = store.ReleaseLease(ctx, state.LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch})
	}()
	ref := state.LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch}
	wf, err := store.GetWorkflow(ctx, workflowID)
	if err != nil {
		t.Fatal(err)
	}
	advanced, err := store.AdvanceGraph(ctx, state.AdvanceGraphInput{Lease: ref, WorkflowID: workflowID,
		FromNodeID: "root", Iteration: 0, ExpectedRevision: wf.Revision,
		Next: []state.GraphNodeInput{{NodeID: "left", Iteration: 0}, {NodeID: "right", Iteration: 0},
			{NodeID: "join", Iteration: 0, Dependencies: []string{"left", "right"}}}, ActorID: "test"})
	if err != nil {
		t.Fatal(err)
	}
	wf = advanced.Workflow
	type outcome struct {
		node string
		err  error
	}
	results := make(chan outcome, 2)
	for _, nodeID := range []string{"left", "right"} {
		go func(id string) {
			_, advanceErr := store.AdvanceGraph(ctx, state.AdvanceGraphInput{Lease: ref, WorkflowID: workflowID,
				FromNodeID: id, Iteration: 0, ExpectedRevision: wf.Revision,
				Next: []state.GraphNodeInput{{NodeID: "join", Iteration: 0, Dependencies: []string{"left", "right"}}}, ActorID: "test"})
			results <- outcome{node: id, err: advanceErr}
		}(nodeID)
	}
	var winner, loser string
	for range 2 {
		result := <-results
		if result.err == nil {
			winner = result.node
		} else if errors.Is(result.err, state.ErrRevisionConflict) {
			loser = result.node
		} else {
			t.Fatalf("branch %s error = %v", result.node, result.err)
		}
	}
	if winner == "" || loser == "" {
		t.Fatalf("race outcomes winner=%q loser=%q", winner, loser)
	}
	wf, err = store.GetWorkflow(ctx, workflowID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.AdvanceGraph(ctx, state.AdvanceGraphInput{Lease: ref, WorkflowID: workflowID,
		FromNodeID: loser, Iteration: 0, ExpectedRevision: wf.Revision,
		Next: []state.GraphNodeInput{{NodeID: "join", Iteration: 0, Dependencies: []string{"left", "right"}}}, ActorID: "retry"}); err != nil {
		t.Fatal(err)
	}
	var joins, downstreamEvents int
	if err := store.Pool().QueryRow(ctx, `SELECT count(*) FROM engine.node_instances WHERE workflow_id = $1 AND node_id = 'join'`, workflowID).Scan(&joins); err != nil {
		t.Fatal(err)
	}
	if err := store.Pool().QueryRow(ctx, `SELECT count(*) FROM engine.outbox WHERE workflow_id = $1 AND event_type = 'graph.advanced'`, workflowID).Scan(&downstreamEvents); err != nil {
		t.Fatal(err)
	}
	if joins != 1 || downstreamEvents != 3 {
		t.Fatalf("join rows=%d graph events=%d, want 1 and 3", joins, downstreamEvents)
	}
}

func openM1Database(t *testing.T) (context.Context, *state.Store) {
	t.Helper()
	if os.Getenv("DURABLE_RUN_INTEGRATION") != "1" && os.Getenv("DURABLE_REQUIRE_DATABASE") != "1" {
		t.Skip("M1 PostgreSQL integration tests require DURABLE_RUN_INTEGRATION=1 or service CI mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	store, err := state.NewFromURL(ctx, m1DatabaseURL(t))
	if err != nil {
		if os.Getenv("DURABLE_REQUIRE_DATABASE") == "1" {
			t.Fatalf("PostgreSQL is required but unavailable: %v", err)
		}
		t.Skipf("PostgreSQL is not available: %v", err)
	}
	var version int
	if err := store.Pool().QueryRow(ctx, `SELECT max(version) FROM engine.schema_migrations`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version < 4 {
		t.Fatalf("DUR-007 migration is not applied: version = %d", version)
	}
	return ctx, store
}

func m1DatabaseURL(t *testing.T) string {
	if value := os.Getenv("DURABLE_DATABASE_URL"); value != "" {
		return value
	}
	workingDirectory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for directory := workingDirectory; ; directory = filepath.Dir(directory) {
		data, readErr := os.ReadFile(filepath.Join(directory, ".env"))
		if readErr == nil {
			values := map[string]string{}
			for _, line := range strings.Split(string(data), "\n") {
				parts := strings.SplitN(strings.TrimSpace(line), "=", 2)
				if len(parts) == 2 {
					values[parts[0]] = strings.Trim(parts[1], "\r\"")
				}
			}
			port := values["POSTGRES_PORT"]
			if port == "" {
				port = "5432"
			}
			if _, err := strconv.Atoi(port); err != nil {
				t.Fatalf("invalid POSTGRES_PORT: %v", err)
			}
			return (&url.URL{Scheme: "postgresql", User: url.UserPassword(values["POSTGRES_USER"], values["POSTGRES_PASSWORD"]),
				Host: "127.0.0.1:" + port, Path: "/" + values["POSTGRES_DB"], RawQuery: "sslmode=disable"}).String()
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			break
		}
	}
	t.Fatal("set DURABLE_DATABASE_URL or provide .env for PostgreSQL integration tests")
	return ""
}

func createM1Workflow(t *testing.T, ctx context.Context, store *state.Store, definitionID, graph, effects string) (string, state.Lease) {
	t.Helper()
	if err := store.CreateDefinition(ctx, state.DefinitionInput{DefinitionID: definitionID, Version: 1,
		DefinitionHash: "hash-" + definitionID, Graph: []byte(graph), ActivityVersions: []byte(`{"root":"1"}`), EffectClasses: []byte(effects)}); err != nil {
		t.Fatal(err)
	}
	ownerID := state.NewID()
	var lease state.Lease
	var acquired bool
	var err error
	for partitionID := int16(0); partitionID < 16; partitionID++ {
		lease, acquired, err = store.AcquireLease(ctx, partitionID, ownerID, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if acquired {
			break
		}
	}
	if !acquired {
		t.Fatal("could not acquire a test partition lease")
	}
	for attempt := 0; attempt < 100; attempt++ {
		workflowID := "dur007-" + state.NewID()
		mapped, _ := partition.ID(workflowID)
		if int16(mapped) != lease.PartitionID {
			continue
		}
		created, createErr := store.CreateWorkflow(ctx, state.CreateWorkflowInput{WorkflowID: workflowID,
			Namespace: "dur007", SubmissionKey: "key-" + workflowID, SubmissionPayloadHash: "sub-v1:" + workflowID,
			DefinitionID: definitionID, DefinitionVersion: 1, PartitionID: lease.PartitionID,
			InitialNodeID: "root", InitialInput: []byte(`{}`), ActorID: "test"})
		if createErr != nil {
			t.Fatal(createErr)
		}
		return created.Workflow.WorkflowID, lease
	}
	t.Fatal(fmt.Sprintf("could not generate workflow for partition %d", lease.PartitionID))
	return "", lease
}
