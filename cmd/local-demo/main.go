// local-demo drives the deployed API/Kafka/Python path, not an in-process Engine.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"durable-agent-execution-engine/internal/invariants"
	"durable-agent-execution-engine/internal/state"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	api := flag.String("api", "http://127.0.0.1:8080", "deployed control API URL")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	store, err := state.NewFromURL(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		return err
	}
	defer store.Close()
	id := "local-demo-" + state.NewID()
	if err := store.CreateDefinition(ctx, state.DefinitionInput{DefinitionID: id, Version: 1, DefinitionHash: id,
		Graph:            []byte(`{"entry":"pure.add","nodes":[{"id":"pure.add","kind":"activity","next":"pure.echo"},{"id":"pure.echo","kind":"activity","next":"done"},{"id":"done","kind":"success"}]}`),
		ActivityVersions: []byte(`{"pure.add":"v1","pure.echo":"v1"}`), EffectClasses: []byte(`{"pure.add":"PURE_ACTIVITY","pure.echo":"PURE_ACTIVITY"}`)}); err != nil {
		return err
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_, _ = store.Pool().Exec(cleanup, `DELETE FROM engine.workflow_executions WHERE workflow_id=$1`, id)
		_, _ = store.Pool().Exec(cleanup, `DELETE FROM engine.workflow_definitions WHERE definition_id=$1`, id)
	}()
	body, _ := json.Marshal(map[string]any{"workflow_id": id, "namespace": "local-runtime", "submission_key": id,
		"definition_id": id, "definition_version": 1, "initial_node_id": "pure.add", "initial_input": []int{1, 2, 3}, "payload": map[string]any{}})
	request, _ := http.NewRequestWithContext(ctx, "POST", *api+"/v1/workflows", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	_ = response.Body.Close()
	if response.StatusCode != 201 {
		return fmt.Errorf("submission status: %d", response.StatusCode)
	}
	for {
		wf, err := store.GetWorkflow(ctx, id)
		if err != nil {
			return err
		}
		if wf.State == state.StateSucceeded {
			node, err := store.GetNode(ctx, id, "pure.echo", 0)
			if err != nil {
				return err
			}
			if string(node.AcceptedResult) != "6" {
				return fmt.Errorf("Python result = %s, want 6", node.AcceptedResult)
			}
			var claims, tasks, events int
			if err := store.Pool().QueryRow(ctx, `SELECT count(*) FROM engine.activity_attempts WHERE workflow_id=$1 AND worker_id LIKE 'worker-%'`, id).Scan(&claims); err != nil {
				return err
			}
			if err := store.Pool().QueryRow(ctx, `SELECT count(*) FROM engine.event_inbox i JOIN engine.outbox o USING(event_id) WHERE o.workflow_id=$1 AND i.consumer_id='runtime-workers-v1'`, id).Scan(&tasks); err != nil {
				return err
			}
			if err := store.Pool().QueryRow(ctx, `SELECT count(*) FROM engine.event_inbox i JOIN engine.outbox o USING(event_id) WHERE o.workflow_id=$1 AND i.consumer_id='runtime-schedulers-v1'`, id).Scan(&events); err != nil {
				return err
			}
			if claims != 2 || tasks < 2 {
				return fmt.Errorf("missing deployed work: claims=%d task-inbox=%d", claims, tasks)
			}
			// Wait for the event ingestor as well as the scheduler's fallback scan.
			if events == 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(100 * time.Millisecond):
				}
				continue
			}
			trace, err := invariants.Load(ctx, store, []string{id})
			if err != nil {
				return err
			}
			if verdict := invariants.Check(trace); !verdict.Valid {
				return fmt.Errorf("invariant violations: %+v", verdict.Violations)
			}
			fmt.Printf("workflow: %s\nstate: %s\nactivity result: 6\nPython worker claims: %d\ninvariant checker: PASS\n",
				id, wf.State, claims)
			return nil
		}
		if wf.State == state.StateFailed {
			return fmt.Errorf("workflow failed at revision %d", wf.Revision)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}
