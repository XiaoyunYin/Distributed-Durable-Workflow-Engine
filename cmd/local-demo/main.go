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
	"strings"
	"time"

	"durable-agent-execution-engine/internal/invariants"
	"durable-agent-execution-engine/internal/state"
	"github.com/segmentio/kafka-go"
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
	kafkaBroker := strings.TrimSpace(os.Getenv("KAFKA_BROKERS"))
	if kafkaBroker == "" {
		return fmt.Errorf("KAFKA_BROKERS is required to verify committed consumer offsets before cleanup")
	}
	id := "local-demo-" + state.NewID()
	if err := store.CreateDefinition(ctx, state.DefinitionInput{DefinitionID: id, Version: 1, DefinitionHash: id,
		Graph:            []byte(`{"entry":"pure.add","nodes":[{"id":"pure.add","kind":"activity","next":"pure.echo"},{"id":"pure.echo","kind":"activity","next":"done"},{"id":"done","kind":"success"}]}`),
		ActivityVersions: []byte(`{"pure.add":"v1","pure.echo":"v1"}`), EffectClasses: []byte(`{"pure.add":"PURE_ACTIVITY","pure.echo":"PURE_ACTIVITY"}`)}); err != nil {
		return err
	}
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
			var claims, tasks int
			if err := store.Pool().QueryRow(ctx, `SELECT count(*) FROM engine.activity_attempts WHERE workflow_id=$1 AND worker_id LIKE 'worker-%'`, id).Scan(&claims); err != nil {
				return err
			}
			if err := store.Pool().QueryRow(ctx, `SELECT count(*) FROM engine.event_inbox i JOIN engine.outbox o USING(event_id) WHERE o.workflow_id=$1 AND i.consumer_id='runtime-workers-v1'`, id).Scan(&tasks); err != nil {
				return err
			}
			if claims != 2 || tasks < 2 {
				return fmt.Errorf("missing deployed work: claims=%d task-inbox=%d", claims, tasks)
			}
			if err := waitForOutboxConsumption(ctx, store, id, kafkaBroker); err != nil {
				return err
			}
			trace, err := invariants.Load(ctx, store, []string{id})
			if err != nil {
				return err
			}
			if verdict := invariants.Check(trace); !verdict.Valid {
				return fmt.Errorf("invariant violations: %+v", verdict.Violations)
			}
			if err := cleanupCompletedDemo(ctx, store, id); err != nil {
				return fmt.Errorf("clean up completed demo after consumer offsets advanced: %w", err)
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

func waitForOutboxConsumption(ctx context.Context, store *state.Store, workflowID, broker string) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	kafkaClient := &kafka.Client{Addr: kafka.TCP(broker), Timeout: 3 * time.Second}
	for {
		events, err := store.ListOutbox(ctx, workflowID, 10000)
		if err != nil {
			return err
		}
		if len(events) == 0 {
			return fmt.Errorf("workflow %s has no outbox events to drain", workflowID)
		}
		allConsumed := true
		for _, event := range events {
			if event.PublishState == state.OutboxQuarantined {
				return fmt.Errorf("outbox event %s was quarantined: %s", event.EventID, event.LastError)
			}
			if event.PublishState != state.OutboxPublished {
				allConsumed = false
				continue
			}
			consumerID := ""
			switch event.Topic {
			case state.EventTopic:
				consumerID = "runtime-schedulers-v1"
			case state.TaskTopic:
				consumerID = "runtime-workers-v1"
			default:
				return fmt.Errorf("outbox event %s has unrecognized topic %q", event.EventID, event.Topic)
			}
			var quarantined bool
			var partition int
			var messageOffset int64
			if err := store.Pool().QueryRow(ctx, `
				SELECT
					EXISTS (SELECT 1 FROM engine.event_inbox
						WHERE event_id=$1 AND consumer_id=$2 AND disposition='QUARANTINED'),
					COALESCE((SELECT kafka_partition FROM engine.event_inbox
						WHERE event_id=$1 AND consumer_id=$2 AND topic=$3
						  AND disposition <> 'QUARANTINED' LIMIT 1), -1),
					COALESCE((SELECT kafka_offset FROM engine.event_inbox
						WHERE event_id=$1 AND consumer_id=$2 AND topic=$3
						  AND disposition <> 'QUARANTINED' LIMIT 1), -1)`, event.EventID, consumerID, event.Topic).Scan(&quarantined, &partition, &messageOffset); err != nil {
				return fmt.Errorf("check consumer acknowledgement for outbox event %s: %w", event.EventID, err)
			}
			if quarantined {
				return fmt.Errorf("outbox event %s was quarantined by %s", event.EventID, consumerID)
			}
			if partition < 0 || messageOffset < 0 {
				allConsumed = false
				continue
			}
			response, err := kafkaClient.OffsetFetch(ctx, &kafka.OffsetFetchRequest{
				GroupID: consumerID,
				Topics:  map[string][]int{event.Topic: {partition}},
			})
			if err != nil {
				return fmt.Errorf("read Kafka committed offset for %s/%d: %w", consumerID, partition, err)
			}
			if response.Error != nil {
				return fmt.Errorf("read Kafka committed offset for %s: %w", consumerID, response.Error)
			}
			committed := int64(-1)
			for _, offset := range response.Topics[event.Topic] {
				if offset.Error != nil {
					return fmt.Errorf("read Kafka committed offset for %s/%d: %w", consumerID, offset.Partition, offset.Error)
				}
				if offset.Partition == partition {
					committed = offset.CommittedOffset
					break
				}
			}
			if committed <= messageOffset {
				allConsumed = false
			}
		}
		if allConsumed {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for workflow %s outbox consumption: %w", workflowID, ctx.Err())
		case <-ticker.C:
		}
	}
}

func cleanupCompletedDemo(ctx context.Context, store *state.Store, workflowID string) error {
	tx, err := store.Pool().Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(ctx, `
		DELETE FROM engine.event_inbox i
		USING engine.outbox o
		WHERE i.event_id=o.event_id AND o.workflow_id=$1`, workflowID); err != nil {
		return fmt.Errorf("delete drained inbox records: %w", err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM engine.workflow_executions WHERE workflow_id=$1`, workflowID); err != nil {
		return fmt.Errorf("delete completed workflow: %w", err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM engine.workflow_definitions WHERE definition_id=$1`, workflowID); err != nil {
		return fmt.Errorf("delete demo definition: %w", err)
	}
	return tx.Commit(ctx)
}
