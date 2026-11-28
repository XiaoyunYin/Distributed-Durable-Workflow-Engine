package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/state"
	"durable-agent-execution-engine/internal/transport"
)

type deliveryTestRepository struct {
	fakeRepository
	inbox, quarantine state.InboxMessage
}

func (r *deliveryTestRepository) RecordInbox(_ context.Context, m state.InboxMessage) (state.InboxResult, error) {
	r.inbox = m
	return state.InboxResult{}, state.ErrInvalidEvent
}

func (r *deliveryTestRepository) QuarantineMessage(_ context.Context, m state.InboxMessage, _ string) (state.InboxResult, error) {
	r.quarantine = m
	return state.InboxResult{Disposition: state.InboxQuarantined, CommitOffset: true, NextOffset: 1}, nil
}

func (r *deliveryTestRepository) RuntimeActivity(context.Context, string, json.RawMessage) (*state.RuntimeActivity, error) {
	return nil, errors.New("quarantined delivery should never create a task")
}

func TestWorkerControlDeliveryUsesServerGeneration(t *testing.T) {
	repo := &deliveryTestRepository{}
	send := func(server *Server) *httptest.ResponseRecorder {
		r := httptest.NewRecorder()
		server.WithRoutes(RoutesWorkerControl).Handler().ServeHTTP(r, httptest.NewRequest(http.MethodPost,
			"/v1/worker-deliveries", strings.NewReader(`{"event_id":"bad","topic":"durable-agent.tasks.v1","partition":0,"offset":0}`)))
		return r
	}
	legacy := send(NewServer(repo))
	if legacy.Code != 200 || repo.inbox.ConsumerID != "runtime-workers-v1" || repo.quarantine.ConsumerID != "runtime-workers-v1" {
		t.Fatalf("legacy delivery: %d %s %+v", legacy.Code, legacy.Body, repo.inbox)
	}
	parsed, err := transport.ParseConsumerGeneration("")
	if err != nil {
		t.Fatal(err)
	}
	unset := send(NewServer(repo).WithConsumerGeneration(parsed))
	if unset.Code != legacy.Code || unset.Body.String() != legacy.Body.String() || repo.inbox.ConsumerID != "runtime-workers-v1" {
		t.Fatal("unset runtime changed the worker-control API or durable identity")
	}
	target, err := transport.ParseConsumerGeneration(transport.J7RestoreGeneration)
	if err != nil {
		t.Fatal(err)
	}
	restored := send(NewServer(repo).WithConsumerGeneration(target))
	if restored.Code != legacy.Code || restored.Body.String() != legacy.Body.String() ||
		repo.inbox.ConsumerID != "runtime-workers-v1-j7-restore" || repo.quarantine.ConsumerID != repo.inbox.ConsumerID ||
		repo.inbox.Topic != state.TaskTopic || repo.inbox.Offset != 0 {
		t.Fatal("worker control did not isolate the restored delivery and quarantine keys")
	}
}

func TestWorkerControlGenerationsReuseOffsetZeroInPostgres(t *testing.T) {
	const partition = 900701 // Standalone fixture partition, outside hosted brokers.
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	store, err := state.NewFromURL(ctx, apiIntegrationDatabaseURL(t))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	target, err := transport.ParseConsumerGeneration(transport.J7RestoreGeneration)
	if err != nil {
		t.Fatal(err)
	}
	ids := []string{(transport.ConsumerGeneration{}).WorkerID(), target.WorkerID()}
	var existing int
	if err := store.Pool().QueryRow(ctx, `SELECT count(*) FROM engine.consumer_offsets
		WHERE consumer_id=ANY($1) AND topic=$2 AND kafka_partition=$3`, ids, state.TaskTopic, partition).Scan(&existing); err != nil || existing != 0 {
		t.Fatalf("standalone fixture partition is already occupied: %d %v", existing, err)
	}
	defer func() {
		clean, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		for _, id := range ids {
			ref := fmt.Sprintf("transport/%s/%s/%d/0", id, state.TaskTopic, partition)
			for _, q := range []struct {
				sql  string
				args []any
			}{
				{`DELETE FROM engine.reconciliation_items WHERE kind='POISON_RECORD' AND reference=$1`, []any{ref}},
				{`DELETE FROM engine.transport_quarantine WHERE consumer_id=$1 AND topic=$2 AND kafka_partition=$3 AND kafka_offset=0`, []any{id, state.TaskTopic, partition}},
				{`DELETE FROM engine.consumer_offsets WHERE consumer_id=$1 AND topic=$2 AND kafka_partition=$3`, []any{id, state.TaskTopic, partition}},
			} {
				if _, err := store.Pool().Exec(clean, q.sql, q.args...); err != nil {
					t.Errorf("remove standalone generation fixture: %v", err)
				}
			}
		}
	}()
	send := func(s *Server, event string) {
		t.Helper()
		body := fmt.Sprintf(`{"event_id":%q,"topic":%q,"partition":%d,"offset":0,"event_type":"attempt.dispatch","schema_version":1,"payload":"e30="}`,
			event, state.TaskTopic, partition)
		response := httptest.NewRecorder()
		s.WithRoutes(RoutesWorkerControl).Handler().ServeHTTP(response, httptest.NewRequest(http.MethodPost,
			"/v1/worker-deliveries", strings.NewReader(body)))
		if response.Code != 200 || !strings.Contains(response.Body.String(), `"next_offset":1`) {
			t.Fatalf("offset zero for %s: %d %s", s.workerConsumerID, response.Code, response.Body)
		}
	}
	events := []string{state.NewID(), state.NewID()}
	send(NewServer(store), events[0])
	send(NewServer(store).WithConsumerGeneration(target), events[1])
	for i, id := range ids {
		var event string
		var next int64
		err := store.Pool().QueryRow(ctx, `SELECT q.event_id, o.next_offset
			FROM engine.transport_quarantine q JOIN engine.consumer_offsets o USING (consumer_id, topic, kafka_partition)
			WHERE q.consumer_id=$1 AND q.topic=$2 AND q.kafka_partition=$3 AND q.kafka_offset=0`,
			id, state.TaskTopic, partition).Scan(&event, &next)
		if err != nil || event != events[i] || next != 1 {
			t.Fatalf("offset namespaces interfered for %s: event %q next %d error %v", id, event, next, err)
		}
	}
}
