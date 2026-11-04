package api

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/engine"
	"durable-agent-execution-engine/internal/partition"
	"durable-agent-execution-engine/internal/state"
)

// The old worker has exactly the pre-K2b default registry. The new worker retains
// v1 and adds a test-only v2. Both use the real runner and HTTP claim/result path.
// This is a native PostgreSQL mixed-registry test, not a Kafka/cluster rollout.
func TestK2MixedWorkerVersionsCompleteExistingWorkflow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := state.NewFromURL(ctx, apiIntegrationDatabaseURL(t))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	id := "k2-mixed-" + state.NewID()
	ns := id
	err = store.CreateDefinition(ctx, state.DefinitionInput{DefinitionID: id, Version: 1, DefinitionHash: id,
		Graph:            []byte(`{"entry":"pure.echo","nodes":[{"id":"pure.echo","kind":"activity","next":"pure.add"},{"id":"pure.add","kind":"activity","next":"done"},{"id":"done","kind":"success"}]}`),
		ActivityVersions: []byte(`{"pure.echo":"v1","pure.add":"v1"}`), EffectClasses: []byte(`{"pure.echo":"PURE_ACTIVITY","pure.add":"PURE_ACTIVITY"}`)})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := store.Pool().Exec(context.Background(), `DELETE FROM engine.workflow_executions WHERE definition_id=$1`, id); err != nil {
			t.Error(err)
		}
		if _, err := store.Pool().Exec(context.Background(), `DELETE FROM engine.workflow_definitions WHERE definition_id=$1`, id); err != nil {
			t.Error(err)
		}
	}()
	server := httptest.NewServer(NewServer(store).WithRoutes(RoutesWorkerControl).Handler())
	defer server.Close()
	// Swap which worker handles the first/second activity, proving both directions.
	for _, order := range [][]string{{"old", "new"}, {"new", "old"}} {
		wfID := id + "-" + order[0]
		mapped, _ := partition.ID(wfID)
		_, err := store.CreateWorkflow(ctx, state.CreateWorkflowInput{WorkflowID: wfID, PartitionID: int16(mapped), Namespace: ns, SubmissionKey: wfID, SubmissionPayloadHash: wfID, DefinitionID: id, DefinitionVersion: 1, InitialNodeID: "pure.echo", InitialInput: []byte(`[1,2,3]`)})
		if err != nil {
			t.Fatal(err)
		}
		if err := store.CheckActivityCompatibility(ctx, ns, state.RuntimeRegistry()); err != nil {
			t.Fatal(err)
		}
		runtime := engine.New(store, nil)
		runtime.ExternalActivities = true
		runtime.OwnerID = state.NewID()
		for index, nodeID := range []string{"pure.echo", "pure.add"} {
			result, err := runtime.Run(ctx, wfID)
			if err != nil || !result.Blocked || result.Workflow.State != state.StateWaitingActivity {
				t.Fatalf("mixed rollout dispatch: %+v %v", result, err)
			}
			events, err := store.ListOutbox(ctx, wfID, 100)
			if err != nil {
				t.Fatal(err)
			}
			var task *state.RuntimeActivity
			for _, event := range events {
				if event.EventType == "attempt.dispatch" {
					task, err = store.RuntimeActivity(ctx, ns, event.Payload)
					if err != nil {
						t.Fatal(err)
					}
					if task != nil && task.NodeID == nodeID {
						break
					}
				}
			}
			if task == nil || task.NodeID != nodeID {
				t.Fatal("missing authoritative mixed-version task")
			}
			raw, _ := json.Marshal(task)
			python := os.Getenv("DURABLE_WORKER_PYTHON")
			if python == "" {
				python = "uv"
			}
			script := `import json,sys
from workers.registry import default_registry
from workers.runner import ActivityTask,runner_for_url
registry=default_registry()
assert registry.manifest()==[{"name":"pure.add","version":"v1"},{"name":"pure.echo","version":"v1"}]
if sys.argv[3]=="new": registry.register("pure.echo","v2",lambda x:x)
t=json.loads(sys.argv[2])
task=ActivityTask(t["workflow_id"],t["node_id"],t["iteration"],"mixed-"+sys.argv[3],t["workflow_id"]+t["node_id"],t["activity_name"],t["activity_version"],t["input"])
print(json.dumps(runner_for_url(sys.argv[1],registry,1).run_task(task)))`
			args := []string{"-c", script, server.URL, string(raw), order[index]}
			if python == "uv" {
				args = append([]string{"run", "--no-sync", "python"}, args...)
			}
			command := exec.CommandContext(ctx, python, args...)
			command.Dir = filepath.Join("..", "..")
			command.Env = append(os.Environ(), "PYTHONPATH=python", "DUR048_ACTIVITY_DELAY_MS=0", "DUR050_ENABLE_SHA256_ACTIVITY=0", "WORKER_TOKEN_FILE=")
			output, err := command.CombinedOutput()
			if err != nil {
				t.Fatalf("mixed rollout worker %s: %v %s", order[index], err, output)
			}
		}
		result, err := runtime.Run(ctx, wfID)
		if err != nil || result.Workflow.State != state.StateSucceeded {
			t.Fatalf("mixed rollout did not complete: %+v %v", result, err)
		}
		var receipts int
		if err := store.Pool().QueryRow(ctx, `SELECT count(*) FROM engine.activity_attempts WHERE workflow_id=$1 AND state='SUCCEEDED' AND result IS NOT NULL`, wfID).Scan(&receipts); err != nil || receipts != 2 {
			t.Fatalf("mixed rollout receipt count=%d err=%v", receipts, err)
		}
	}
}
