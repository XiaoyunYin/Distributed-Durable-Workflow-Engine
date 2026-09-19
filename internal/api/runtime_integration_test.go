package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/engine"
	"durable-agent-execution-engine/internal/invariants"
	"durable-agent-execution-engine/internal/partition"
	"durable-agent-execution-engine/internal/state"
)

func TestRuntimeDispatchRecoveryAndDeliveryFence(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	store, err := state.NewFromURL(ctx, apiIntegrationDatabaseURL(t))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	id := "runtime-test-" + state.NewID()
	t.Setenv("RUNTIME_NAMESPACE", id)
	if err := store.CreateDefinition(ctx, state.DefinitionInput{DefinitionID: id, Version: 1, DefinitionHash: id,
		Graph:            []byte(`{"entry":"pure.echo","nodes":[{"id":"pure.echo","kind":"activity","next":"done"},{"id":"done","kind":"success"}]}`),
		ActivityVersions: []byte(`{"pure.echo":"v1"}`), EffectClasses: []byte(`{"pure.echo":"PURE_ACTIVITY"}`)}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = store.Pool().Exec(context.Background(), `DELETE FROM engine.workflow_executions WHERE workflow_id=$1`, id)
		_, _ = store.Pool().Exec(context.Background(), `DELETE FROM engine.workflow_definitions WHERE definition_id=$1`, id)
		_, _ = store.Pool().Exec(context.Background(), `DELETE FROM engine.consumer_offsets WHERE consumer_id=$1`, id)
	}()
	part, _ := partition.ID(id)
	if _, err := store.CreateWorkflow(ctx, state.CreateWorkflowInput{WorkflowID: id, Namespace: id, SubmissionKey: id,
		SubmissionPayloadHash: "sub-v1:" + id, DefinitionID: id, DefinitionVersion: 1, PartitionID: int16(part), InitialNodeID: "pure.echo",
		InitialInput: []byte(`{"value":7}`), ActorID: "runtime-test"}); err != nil {
		t.Fatal(err)
	}
	runner := engine.New(store, nil)
	runner.ExternalActivities = true
	runner.AttemptLease = time.Minute
	if _, err := runner.Run(ctx, id); err != nil {
		t.Fatal(err)
	}
	attempt, err := store.GetAttempt(ctx, id, "pure.echo", 0, 1)
	if err != nil || attempt.State != state.AttemptDispatchable || attempt.ClaimToken != "" {
		t.Fatalf("scheduler ran activity: %+v %v", attempt, err)
	}
	// Lose every broker wake-up. A later scheduler scan must redispatch the
	// same unclaimed attempt, not manufacture a second attempt.
	if _, err := store.Pool().Exec(ctx, `UPDATE engine.activity_attempts SET heartbeat_deadline=clock_timestamp()-interval '1 second' WHERE workflow_id=$1`, id); err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Run(ctx, id); err != nil {
		t.Fatal(err)
	}
	events, err := store.ListOutbox(ctx, id, 100)
	if err != nil {
		t.Fatal(err)
	}
	var dispatch state.OutboxEvent
	for _, event := range events {
		if event.EventType == "attempt.redispatch" {
			dispatch = event
		}
	}
	if dispatch.EventID == "" {
		t.Fatal("missing redispatch")
	}
	server := NewServer(store)
	server.workerConsumerID = id
	handler := server.Handler()
	delivery := func(event state.OutboxEvent) *httptest.ResponseRecorder {
		body, _ := json.Marshal(map[string]any{"event_id": event.EventID, "event_type": event.EventType, "schema_version": 1,
			"topic": state.TaskTopic, "partition": int(part), "offset": 0, "payload": []byte(event.Payload)})
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest("POST", "/v1/worker-deliveries", bytes.NewReader(body)))
		return response
	}
	response := delivery(dispatch)
	if response.Code != 200 {
		t.Fatalf("delivery: %d %s", response.Code, response.Body.String())
	}
	var decoded struct {
		Task   *state.RuntimeActivity `json:"task"`
		Commit bool                   `json:"commit_offset"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	if !decoded.Commit || decoded.Task == nil || !bytes.Contains(decoded.Task.Input, []byte(`7`)) {
		t.Fatalf("delivery = %s", response.Body.String())
	}
	if duplicate := delivery(dispatch); duplicate.Code != 200 {
		t.Fatalf("retry: %s", duplicate.Body.String())
	}
	claim, err := store.ClaimAttempt(ctx, state.ClaimInput{WorkflowID: id, NodeID: "pure.echo", WorkerID: "python", RequestID: id,
		ExpectedAttempt: 1, AttemptLease: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Pool().Exec(ctx, `UPDATE engine.activity_attempts SET heartbeat_deadline=clock_timestamp()-interval '1 second' WHERE workflow_id=$1`, id); err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Run(ctx, id); err != nil {
		t.Fatal(err)
	}
	// R019 gap: retrying the replaced claim must never return the old grant.
	if _, err := store.ClaimAttempt(ctx, state.ClaimInput{WorkflowID: id, NodeID: "pure.echo", WorkerID: "python", RequestID: id}); !errors.Is(err, state.ErrStaleClaim) {
		t.Fatalf("old claim retry: %v", err)
	}
	if _, err := store.ClaimAttempt(ctx, state.ClaimInput{WorkflowID: id, NodeID: "pure.echo", WorkerID: "python", RequestID: id + "stale", ExpectedAttempt: 1}); !errors.Is(err, state.ErrAttemptNotCurrent) {
		t.Fatalf("stale delivery fence: %v", err)
	}
	if _, err := store.RecordResultReceipt(ctx, state.ResultInput{WorkflowID: id, NodeID: "pure.echo", AttemptNumber: 1, ClaimToken: claim.ClaimToken, AttemptState: state.AttemptSucceeded, Payload: []byte(`7`)}); !errors.Is(err, state.ErrAttemptNotCurrent) && !errors.Is(err, state.ErrStaleClaim) {
		t.Fatalf("stale result: %v", err)
	}
	claim, err = store.ClaimAttempt(ctx, state.ClaimInput{WorkflowID: id, NodeID: "pure.echo", WorkerID: "python", RequestID: id + "new", ExpectedAttempt: 2})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordResultReceipt(ctx, state.ResultInput{WorkflowID: id, NodeID: "pure.echo", AttemptNumber: 2, ClaimToken: claim.ClaimToken, AttemptState: state.AttemptSucceeded, Payload: []byte(`7`)}); err != nil {
		t.Fatal(err)
	}
	if result, err := runner.Run(ctx, id); err != nil || result.Workflow.State != state.StateSucceeded {
		t.Fatalf("resume: %+v %v", result, err)
	}
	trace, err := invariants.Load(ctx, store, []string{id})
	if err != nil {
		t.Fatal(err)
	}
	if verdict := invariants.Check(trace); !verdict.Valid {
		t.Fatalf("invariants: %+v", verdict)
	}
}
