package engine

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/partition"
	"durable-agent-execution-engine/internal/state"
)

const timeoutWorkerWorkflowEnv = "DURABLE_J19_WORKER_WORKFLOW"

func TestPostgresTimeoutRetryWorkerKill(t *testing.T) {
	if workflowID := os.Getenv(timeoutWorkerWorkflowEnv); workflowID != "" {
		ctx, store := openM1Database(t)
		defer store.Close()
		if _, err := store.ClaimAttempt(ctx, state.ClaimInput{
			WorkflowID: workflowID, NodeID: "pure.echo", ExpectedAttempt: 1,
			WorkerID: "j19-killed-worker", RequestID: state.NewID(), AttemptLease: time.Second,
		}); err != nil {
			t.Fatal(err)
		}
		fmt.Println("claim committed")
		<-ctx.Done()
		t.Fatal("worker was not killed")
	}

	t.Setenv("RUNTIME_TIMEOUT_RETRY_POLICY", "1")
	ctx, store := openM1Database(t)
	defer store.Close()
	definitionID := "j19-worker-kill-" + state.NewID()
	workflowID := "j19-worker-kill-" + state.NewID()
	if err := store.CreateDefinition(ctx, state.DefinitionInput{
		DefinitionID: definitionID, Version: 1, DefinitionHash: definitionID,
		Graph:            []byte(`{"entry":"pure.echo","nodes":[{"id":"pure.echo","kind":"activity","next":"done"},{"id":"done","kind":"success"}]}`),
		EffectClasses:    []byte(`{"pure.echo":"PURE_ACTIVITY"}`),
		ActivityVersions: []byte(`{"pure.echo":"v1"}`),
	}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for _, query := range []struct{ sql, value string }{
			{`DELETE FROM engine.workflow_executions WHERE workflow_id = $1`, workflowID},
			{`DELETE FROM engine.workflow_definitions WHERE definition_id = $1`, definitionID},
		} {
			if _, err := store.Pool().Exec(cleanupCtx, query.sql, query.value); err != nil {
				t.Errorf("clean up killed worker fixture: %v", err)
			}
		}
	}()
	partitionID, err := partition.ID(workflowID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateWorkflow(ctx, state.CreateWorkflowInput{
		WorkflowID: workflowID, Namespace: "j19", SubmissionKey: workflowID, SubmissionPayloadHash: workflowID,
		DefinitionID: definitionID, DefinitionVersion: 1, PartitionID: int16(partitionID),
		InitialNodeID: "pure.echo", ActorID: "j19-test",
	}); err != nil {
		t.Fatal(err)
	}
	runner := New(store, nil)
	runner.OwnerID = state.NewID()
	runner.ExternalActivities = true
	initial, err := runner.Run(ctx, workflowID)
	if err != nil || !initial.Blocked || initial.Workflow.State != state.StateWaitingActivity {
		t.Fatalf("initial dispatch = %+v, err=%v", initial, err)
	}
	killClaimedWorker(t, ctx, workflowID)
	lost, err := store.GetAttempt(ctx, workflowID, "pure.echo", 0, 1)
	if err != nil || lost.State != state.AttemptClaimed || lost.HeartbeatDeadline == nil {
		t.Fatalf("lost claim = %+v, err=%v", lost, err)
	}
	if _, err := store.Pool().Exec(ctx, `
		SELECT pg_sleep(GREATEST(EXTRACT(EPOCH FROM ($1::timestamptz - clock_timestamp())), 0)::double precision)`,
		*lost.HeartbeatDeadline); err != nil {
		t.Fatal(err)
	}
	recovered, err := runner.Run(ctx, workflowID)
	if err != nil || !recovered.Blocked || recovered.Workflow.State != state.StateWaitingActivity {
		t.Fatalf("replacement dispatch = %+v, err=%v", recovered, err)
	}
	claim, err := store.ClaimAttempt(ctx, state.ClaimInput{
		WorkflowID: workflowID, NodeID: "pure.echo", ExpectedAttempt: 2,
		WorkerID: "j19-replacement-worker", RequestID: state.NewID(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordResult(ctx, state.ResultInput{
		WorkflowID: workflowID, NodeID: "pure.echo", AttemptNumber: claim.AttemptNumber,
		ClaimToken: claim.ClaimToken, AttemptState: state.AttemptSucceeded, Payload: []byte(`{"ok":true}`),
	}); err != nil {
		t.Fatal(err)
	}
	completed, err := runner.Run(ctx, workflowID)
	if err != nil || completed.Blocked || completed.Workflow.State != state.StateSucceeded {
		t.Fatalf("completion after worker kill = %+v, err=%v", completed, err)
	}
	assertM1TraceValid(t, ctx, store, workflowID)
}

func killClaimedWorker(t *testing.T, ctx context.Context, workflowID string) {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	worker := exec.CommandContext(ctx, executable, "-test.run", "^TestPostgresTimeoutRetryWorkerKill$")
	worker.Env = append(os.Environ(), timeoutWorkerWorkflowEnv+"="+workflowID)
	worker.Stderr = os.Stderr
	stdout, err := worker.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = worker.Process.Kill()
		_ = worker.Wait()
	}()
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil || line != "claim committed\n" {
		t.Fatalf("worker readiness = %q, err=%v", line, err)
	}
	if err := worker.Process.Kill(); err != nil {
		t.Fatal(err)
	}
}
