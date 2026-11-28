package main

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/invariants"
	"durable-agent-execution-engine/internal/restoreledger"
	"durable-agent-execution-engine/internal/state"
	"durable-agent-execution-engine/internal/transport"
	"github.com/segmentio/kafka-go"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func brokerCase(cluster string, before int64) kafkaSnapshot {
	s := kafkaSnapshot{Endpoint: cluster + ":29092", Cluster: cluster, BeforeNS: before, AfterNS: before + 1,
		Partitions: []kafkaPartition{}, Groups: []string{}}
	for _, name := range j7Topics {
		for i := range 8 {
			s.Partitions = append(s.Partitions, kafkaPartition{Topic: name, TopicID: cluster + "-" + name, Partition: i})
		}
	}
	return s
}

func TestFreshKafkaRejectsRetainedIdentityAndOffsets(t *testing.T) {
	old, fresh := brokerCase("source", 100), brokerCase("target", 200)
	if err := verifyFreshKafka(old, fresh); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*kafkaSnapshot)
	}{
		{"same-cluster", func(s *kafkaSnapshot) { s.Cluster = old.Cluster }},
		{"same-endpoint", func(s *kafkaSnapshot) { s.Endpoint = old.Endpoint }},
		{"old-topic", func(s *kafkaSnapshot) { s.Partitions[0].TopicID = old.Partitions[8].TopicID }},
		{"nonzero-end", func(s *kafkaSnapshot) { s.Partitions[4].End = 1 }},
		{"old-group", func(s *kafkaSnapshot) { s.Groups = []string{"runtime-workers-v1"} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			row := fresh
			row.Partitions = slices.Clone(fresh.Partitions)
			tc.mutate(&row)
			if err := verifyFreshKafka(old, row); err == nil {
				t.Fatal("accepted retained Kafka")
			}
		})
	}
}

func TestTargetGenerationAdmissionRejectsOldWorkerID(t *testing.T) {
	const endpoint = "j7-new:19092"
	var rows []renderedConsumer
	if err := json.Unmarshal([]byte(`[
		{"role":"scheduler","env":[{"name":"RUNTIME_KAFKA_GENERATION","value":"j7-restore"},{"name":"KAFKA_BOOTSTRAP_SERVERS","value":"j7-new:19092"}]},
		{"role":"worker","env":[{"name":"RUNTIME_KAFKA_GENERATION","value":"j7-restore"},{"name":"KAFKA_BOOTSTRAP_SERVERS","value":"j7-new:19092"}]},
		{"role":"worker-control","env":[{"name":"RUNTIME_KAFKA_GENERATION","value":"j7-restore"}]}
	]`), &rows); err != nil {
		t.Fatal(err)
	}
	if err := verifyGenerationRender(rows, endpoint); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func([]renderedConsumer){
		func(v []renderedConsumer) { legacy := ""; v[1].Env[0].Value = &legacy },
		func(v []renderedConsumer) { v[2].Env = nil },
		func(v []renderedConsumer) { v[0].EnvFrom = true },
		func(v []renderedConsumer) { v[0].Env = append(v[0].Env, v[0].Env[0]) },
		func(v []renderedConsumer) { old := "j7-old:19092"; v[0].Env[1].Value = &old },
	} {
		copy := slices.Clone(rows)
		for i := range copy {
			copy[i].Env = slices.Clone(rows[i].Env)
		}
		mutate(copy)
		if err := verifyGenerationRender(copy, endpoint); err == nil || !strings.Contains(err.Error(), "CONSUMER_GENERATION_MISMATCH") {
			t.Fatalf("bad generation admitted: %v", err)
		}
	}
}

func TestRedispatchJoinsOriginalAttemptAndNoOldNotice(t *testing.T) {
	dead := time.Date(2026, 11, 28, 12, 30, 0, 0, time.UTC)
	hold := dispatchAtCut{Attempt: attemptAtCut{WorkflowID: "d1", NodeID: "root", Attempt: 1, Deadline: dead},
		EventID: "old-notice", Topic: state.TaskTopic}
	msg := kafkaEvent{EventID: "new-notice", Kind: "attempt.redispatch", WorkflowID: "d1",
		Topic: state.TaskTopic, TopicID: "task-topic", PayloadSHA: testDigest, BrokerTime: dead,
		Attempt: &eventAttempt{WorkflowID: "d1", NodeID: "root", Attempt: 1}}
	broker := kafkaSnapshot{Events: []kafkaEvent{msg}}
	notice := redispatchNotice{ObservedNS: 400, Event: msg}
	before := int64(300)
	history := []dispatchHistory{
		{WorkflowID: "d1", Revision: 5, ActorKind: "scheduler", NodeID: "root", Attempt: 1, Reason: "REDISPATCH_UNCLAIMED", CreatedAt: dead},
		{WorkflowID: "d1", Revision: 6, ActorKind: "worker", NodeID: "root", Attempt: 1, Reason: "ATTEMPT_RESULT_RECORDED", CreatedAt: dead},
		{WorkflowID: "d1", Revision: 7, ActorKind: "scheduler", Epoch: 1, NodeID: "root", Attempt: 1, Reason: "RESULT_CONSUMED", NewState: "RUNNABLE", CreatedAt: dead},
		{WorkflowID: "d1", Revision: 8, ActorKind: "scheduler", Epoch: 1, Reason: "GRAPH_ADVANCED", NewState: "SUCCEEDED", CreatedAt: dead},
	}
	trace := invariants.Trace{
		Workflows: []invariants.WorkflowRecord{{WorkflowID: "d1", State: "SUCCEEDED", Revision: 8}},
		Results:   []invariants.AcceptedResult{{WorkflowID: "d1", NodeID: "root", Accepted: true}},
		Attempts: []invariants.AttemptRecord{{WorkflowID: "d1", NodeID: "root", AttemptNumber: 1, State: "SUCCEEDED",
			ClaimToken: "token", WorkerID: "worker", WorkerRequestID: "request", Outcome: "KNOWN_SUCCESS", ResultRecorded: true}},
		Outbox: []invariants.OutboxRecord{{WorkflowID: "d1", EventID: msg.EventID, AggregateRevision: 5, EventType: msg.Kind,
			Topic: msg.Topic, PublishState: "PUBLISHED", PayloadValid: true}},
		Inbox: []invariants.InboxRecord{{ConsumerID: "runtime-workers-v1-j7-restore", EventID: msg.EventID, Topic: msg.Topic,
			Disposition: "ACCEPTED"}},
		Snapshot: map[string]invariants.SnapshotTable{
			"engine.outbox": {Rows: []json.RawMessage{
				json.RawMessage(`{"event_id":"new-notice","payload":{"workflow_id":"d1","node_id":"root","iteration":0,"attempt_number":1}}`),
			}},
			"engine.outbox_publications": {Rows: []json.RawMessage{json.RawMessage(`{"event_id":"new-notice","broker_acknowledged":true}`)}},
		},
	}
	setHistory := func(rows []dispatchHistory) {
		table := invariants.SnapshotTable{}
		for _, row := range rows {
			raw, err := json.Marshal(row)
			if err != nil {
				t.Fatal(err)
			}
			table.Rows = append(table.Rows, raw)
		}
		trace.Snapshot["engine.transition_history"] = table
	}
	setHistory(history)
	got, err := verifyRedispatchedWorkflow(trace, hold, notice, broker, before)
	if err != nil || got.ClaimSHA != restoreledger.SHA256([]byte("token")) || got.ConsumedRevision != 7 {
		t.Fatalf("normal same-attempt redispatch: %+v %v", got, err)
	}
	setHistory(history[:3])
	if _, err := verifyRedispatchedWorkflow(trace, hold, notice, broker, before); err == nil {
		t.Fatal("consume receipt without terminal graph completion passed")
	}
	setHistory(history)
	retry := msg
	retry.Offset = msg.Offset + 1
	broker.Events = []kafkaEvent{msg, retry}
	if _, err := verifyRedispatchedWorkflow(trace, hold, notice, broker, before); err != nil {
		t.Fatalf("matching physical retry of the same redispatch: %v", err)
	}
	wrong := msg
	wrong.EventID, wrong.Kind = "old-notice", "attempt.dispatch"
	broker.Events = append(broker.Events, wrong)
	if _, err := verifyRedispatchedWorkflow(trace, hold, notice, broker, before); err == nil {
		t.Fatal("allowed old broker dispatch to reappear")
	}
	broker.Events = []kafkaEvent{msg}
	history[0].CreatedAt = dead.Add(-time.Nanosecond)
	setHistory(history)
	if _, err := verifyRedispatchedWorkflow(trace, hold, notice, broker, before); err == nil {
		t.Fatal("allowed redispatch before the archived deadline")
	}
	history[0].CreatedAt = dead
	setHistory(history)
	notice.ObservedNS = before + int64(redispatchWindow) + 1
	if _, err := verifyRedispatchedWorkflow(trace, hold, notice, broker, before); err == nil {
		t.Fatal("allowed notice after the 45-second deadline")
	}
}

func TestFreshWatermarksStartAtZeroWithoutChangingLegacyRows(t *testing.T) {
	target, _ := transport.ParseConsumerGeneration(transport.J7RestoreGeneration)
	oldOffset := json.RawMessage(`{"consumer_id":"runtime-workers-v1","next_offset":45}`)
	oldInbox := json.RawMessage(`{"consumer_id":"runtime-workers-v1","kafka_offset":44}`)
	before := invariants.Trace{Snapshot: map[string]invariants.SnapshotTable{
		"engine.event_inbox":      {Rows: []json.RawMessage{oldInbox}},
		"engine.consumer_offsets": {Rows: []json.RawMessage{oldOffset}},
	}}
	after := invariants.Trace{
		Snapshot: map[string]invariants.SnapshotTable{
			"engine.event_inbox":      before.Snapshot["engine.event_inbox"],
			"engine.consumer_offsets": before.Snapshot["engine.consumer_offsets"],
		},
		ConsumerOffsets: []invariants.ConsumerOffsetRecord{
			{ConsumerID: target.WorkerID(), Topic: state.TaskTopic, Partition: 0, NextOffset: 2},
		},
		Inbox: []invariants.InboxRecord{{ConsumerID: target.WorkerID(), Topic: state.TaskTopic, EventID: "d",
			Disposition: "ACCEPTED", Offset: 0}},
	}
	broker := brokerCase("fresh", 200)
	broker.Groups = []string{target.SchedulerID(), target.WorkerID()}
	broker.Partitions[8].End = 2
	broker.Events = []kafkaEvent{
		{Topic: state.TaskTopic, EventID: "d", Kind: "attempt.redispatch", PayloadSHA: testDigest},
		{Topic: state.TaskTopic, EventID: "d", Kind: "attempt.redispatch", Offset: 1, PayloadSHA: testDigest},
	}
	if err := checkFreshPositions(before, after, broker); err != nil {
		t.Fatal(err)
	}
	broker.Events[1].EventID = "unjoined"
	if err := checkFreshPositions(before, after, broker); err == nil {
		t.Fatal("watermark advanced past an unaccounted offset")
	}
	broker.Events[1].EventID = "d"
	after.Snapshot["engine.consumer_offsets"] = invariants.SnapshotTable{}
	if err := checkFreshPositions(before, after, broker); err == nil {
		t.Fatal("missing original watermark was not rejected")
	}
}

type offsetTestBroker struct {
	t    *testing.T
	mode string
}

func (b offsetTestBroker) Request(_ context.Context, request kmsg.Request) (kmsg.Response, error) {
	r, ok := request.(*kmsg.ListOffsetsRequest)
	if !ok || r.ReplicaID != -1 || len(r.Topics) != 2 {
		b.t.Fatalf("incorrect ListOffsets request: %+v", request)
	}
	out := &kmsg.ListOffsetsResponse{}
	for _, topic := range r.Topics {
		row := kmsg.ListOffsetsResponseTopic{Topic: topic.Topic}
		for i, p := range topic.Partitions {
			if int(p.Partition) != i || p.Timestamp != kafka.FirstOffset {
				b.t.Fatalf("unexpected partition or timestamp: %+v", p)
			}
			row.Partitions = append(row.Partitions, kmsg.ListOffsetsResponseTopicPartition{Partition: p.Partition})
		}
		out.Topics = append(out.Topics, row)
	}
	switch b.mode {
	case "missing":
		out.Topics[0].Partitions = out.Topics[0].Partitions[1:]
	case "duplicate":
		out.Topics[0].Partitions[7].Partition = 0
	case "error":
		out.Topics[0].Partitions[0].ErrorCode = 3
	}
	return out, nil
}

func TestKafkaOffsetEvidenceRequiresEveryBrokerResponse(t *testing.T) {
	offsets, err := readKafkaOffsets(t.Context(), offsetTestBroker{t: t}, kafka.FirstOffset)
	if err != nil || offsets[state.TaskTopic] != [8]int64{} || len(offsets) != 2 {
		t.Fatalf("valid zero inventory was rejected: %v %v", offsets, err)
	}
	for _, bad := range []string{"missing", "duplicate", "error"} {
		t.Run(bad, func(t *testing.T) {
			if _, err := readKafkaOffsets(t.Context(), offsetTestBroker{t, bad}, kafka.FirstOffset); err == nil {
				t.Fatal("unproved offset was treated as zero")
			}
		})
	}
}
