package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"slices"
	"strings"
	"time"

	"durable-agent-execution-engine/internal/state"
	"durable-agent-execution-engine/internal/transport"
)

type workerFixtureConfig struct {
	ControlOrigin, Broker, CA string
	Held                      []string
}
type fixtureClaim struct {
	Attempt  int64             `json:"attempt_number"`
	Token    string            `json:"claim_token"`
	Deadline time.Time         `json:"heartbeat_deadline"`
	Dispatch *state.K6Dispatch `json:"k6_dispatch"`
}

func attemptPath(w, n string, i int) string {
	return fmt.Sprintf("/v1/workflows/%s/nodes/%s/iterations/%d", w, n, i)
}
func claimFixture(ctx context.Context, client fixtureHTTP, w, n string, i int, a int64) (fixtureClaim, int, error) {
	var c fixtureClaim
	raw, _ := json.Marshal(map[string]any{"worker_id": "j7-worker", "request_id": state.NewID(), "expected_attempt": a, "attempt_lease_ms": 60000})
	status, response, err := client.post(ctx, attemptPath(w, n, i)+"/claim", "", raw)
	if err != nil {
		return c, status, err
	}
	if status != http.StatusOK || json.Unmarshal(response, &c) != nil || c.Attempt != a || c.Token == "" || c.Deadline.IsZero() {
		return c, status, errors.New("worker-control did not accept the exact ordinary claim")
	}
	return c, status, nil
}
func resultFixture(ctx context.Context, client fixtureHTTP, w, n string, i int, c fixtureClaim, payload json.RawMessage) error {
	raw, err := json.Marshal(map[string]any{"attempt_number": c.Attempt, "claim_token": c.Token, "attempt_state": "SUCCEEDED", "payload": payload})
	if err != nil {
		return err
	}
	status, _, err := client.post(ctx, attemptPath(w, n, i)+"/result", "", raw)
	if err != nil {
		return err
	}
	expected := http.StatusOK
	if n == state.K6Activity {
		expected = http.StatusAccepted
	}
	if status != expected {
		return fmt.Errorf("ordinary worker result returned %d", status)
	}
	return nil
}
func runFixtureWorker(ctx context.Context, path string) error {
	var cfg workerFixtureConfig
	if _, err := readFaultJSON(path, &cfg); err != nil {
		return err
	}
	g, err := transport.ConsumerGenerationFromEnvironment()
	if err != nil {
		return err
	}
	broker, err := transport.NewKafkaSource([]string{cfg.Broker}, state.TaskTopic, g.WorkerID())
	if err != nil {
		return err
	}
	defer broker.Close()
	tlsClient, err := fixtureClientTransport([]byte(cfg.CA))
	if err != nil {
		return err
	}
	for ctx.Err() == nil {
		message, err := broker.Receive(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			return err
		}
		token, err := os.ReadFile("/tokens/worker")
		if err != nil {
			return errors.New("worker fixture projected token is unavailable")
		}
		control := newFixtureHTTP(cfg.ControlOrigin, strings.TrimSpace(string(token)), tlsClient)
		if err := acceptFixtureTask(ctx, cfg, broker, control, message); err != nil {
			return err
		}
	}
	return ctx.Err()
}
func acceptFixtureTask(ctx context.Context, cfg workerFixtureConfig, broker transport.MessageSource,
	control fixtureHTTP, m transport.Message) error {
	raw, err := json.Marshal(map[string]any{"event_id": m.EventID, "topic": m.Topic, "partition": m.Partition, "offset": m.Offset,
		"event_type": m.EventType, "schema_version": m.SchemaVersion, "payload": []byte(m.Payload)})
	if err != nil {
		return err
	}
	status, reply, err := control.post(ctx, "/v1/worker-deliveries", "", raw)
	var delivery struct {
		Commit bool                   `json:"commit_offset"`
		Next   int64                  `json:"next_offset"`
		Task   *state.RuntimeActivity `json:"task"`
	}
	if err != nil || status != 200 || json.Unmarshal(reply, &delivery) != nil {
		return errors.New("ordinary worker inbox disposition was not recorded")
	}
	if delivery.Commit {
		if err := broker.Commit(ctx, m, delivery.Next); err != nil {
			return err
		}
	}
	task := delivery.Task
	if task == nil || slices.Contains(cfg.Held, task.WorkflowID) {
		return nil
	}
	if task.ActivityName != "pure.echo" || task.ActivityVersion != "v1" {
		return errors.New("J7 worker received an unplanned activity")
	}
	cl, status, err := claimFixture(ctx, control, task.WorkflowID, task.NodeID, task.Iteration, task.AttemptNumber)
	if status == http.StatusConflict {
		return nil
	} // the ordinary scheduler or another delivery superseded this notice
	if err != nil {
		return err
	}
	return resultFixture(ctx, control, task.WorkflowID, task.NodeID, task.Iteration, cl, task.Input)
}
