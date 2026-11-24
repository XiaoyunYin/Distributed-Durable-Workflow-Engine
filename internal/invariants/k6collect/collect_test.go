package k6collect

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/invariants/k6v1"
	"durable-agent-execution-engine/internal/k6ingress"
	"github.com/segmentio/kafka-go"
)

func TestK6RosterCollector(t *testing.T) {
	at := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	m := k6v1.Manifest{Namespace: "episode"}
	w := NewPodWatch(m, "observer", at)
	if err := w.Baseline([]byte(`{"metadata":{"resourceVersion":"10"},"items":[]}`), "worker", at); err != nil {
		t.Fatal(err)
	}
	event := `{"type":"ADDED","object":{"metadata":{"uid":"pod-uid","namespace":"episode","resourceVersion":"11"},"status":{"containerStatuses":[{"name":"worker","containerID":"containerd://container","restartCount":0,"state":{"running":{"startedAt":"2026-11-01T12:00:00Z"}}}]}}}`
	now := func() time.Time { return at.Add(time.Second) }
	if err := w.Read(strings.NewReader(event), "worker", now); err != nil {
		t.Fatal(err)
	}
	const id = "00000000-0000-0000-0000-000000000001"
	s := k6v1.WorkerSession{ID: id, PodUID: "pod-uid", ContainerID: "container", ContainerStartedUTC: k6v1.UTC(at), StartedUTC: k6v1.UTC(now()), BootID: "boot", Claimant: "worker"}
	reg := k6v1.Registration{PodUID: s.PodUID, Claimant: s.Claimant, ServiceAccountUID: "worker-sa", Audience: "durable-worker-control", Observer: "reviewer"}
	wrong := s
	wrong.ContainerID = "wrong"
	if err := w.Register(now(), wrong, reg); err == nil {
		t.Fatal("worker self-enumeration registered unseen container")
	}
	if err := w.Register(now(), s, reg); err != nil {
		t.Fatal(err)
	}
	termination := `{"type":"MODIFIED","object":{"metadata":{"uid":"pod-uid","namespace":"episode","resourceVersion":"12"},"status":{"containerStatuses":[{"name":"worker","containerID":"containerd://container","restartCount":0,"state":{"terminated":{"containerID":"containerd://container","startedAt":"2026-11-01T12:00:00Z","finishedAt":"2026-11-01T12:00:01Z","exitCode":143,"signal":15}}}]}}}`
	now = func() time.Time { return at.Add(2 * time.Second) }
	if err := w.Read(strings.NewReader(termination), "worker", now); err != nil {
		t.Fatal(err)
	}
	ledger := NewStream[k6v1.WorkerRow](m, "worker", "receiver", at)
	Append(&ledger, at, k6v1.WorkerRow{Kind: "session", Session: &s})
	start := k6v1.AttemptIdentity{Workflow: "wf", Node: "node", Attempt: 1, ClaimHash: k6v1.Hash([]byte("token")), RequestID: "request"}
	Append(&ledger, at.Add(time.Second), k6v1.WorkerRow{Kind: "start", Invocation: "invocation", Attempt: &start})
	if err := k6v1.SealStream(&ledger, "checkpoint", k6v1.UTC(now()), 2, 0); err != nil {
		t.Fatal(err)
	}
	w.BindShutdown(now(), s, ledger)
	bound := w.Stream.Rows[len(w.Stream.Rows)-1].Data.Shutdown
	if bound == nil || bound.Invocation != "invocation" || bound.Attempt != start || bound.StartSequence != 2 ||
		w.Stream.Rows[bound.ExitSequence-1].Data.Exit.Signal != 15 {
		t.Fatalf("unbound collector shutdown: %+v", bound)
	}
	if err := w.ConfirmDeath(now(), s, &ledger); err != nil {
		t.Fatal(err)
	}
	roster, err := w.Snapshot(now(), true)
	if err != nil || roster.Rows[len(roster.Rows)-1].Data.Death.FinalSHA256 != ledger.Seal.SHA256 || ledger.Seal.Mode != "receiver_final" {
		t.Fatalf("unbound persisted death: %v", err)
	}
	if err := w.Read(strings.NewReader(`{"type":"ERROR","object":{"code":410}}`), "worker", now); err == nil {
		t.Fatal("expired Kubernetes watch accepted")
	}
}

type brokerFixture struct{}

func (brokerFixture) Metadata(context.Context, *kafka.MetadataRequest) (*kafka.MetadataResponse, error) {
	return &kafka.MetadataResponse{ClusterID: "cluster", Topics: []kafka.Topic{{Name: "topic", Partitions: []kafka.Partition{{ID: 0}}}}}, nil
}
func (brokerFixture) ListOffsets(context.Context, *kafka.ListOffsetsRequest) (*kafka.ListOffsetsResponse, error) {
	return &kafka.ListOffsetsResponse{Topics: map[string][]kafka.PartitionOffsets{"topic": {{Partition: 0, FirstOffset: 1, LastOffset: 9}}}}, nil
}
func (brokerFixture) OffsetFetch(context.Context, *kafka.OffsetFetchRequest) (*kafka.OffsetFetchResponse, error) {
	return &kafka.OffsetFetchResponse{Topics: map[string][]kafka.OffsetFetchPartition{"topic": {{Partition: 0, CommittedOffset: 7}}}}, nil
}
func (brokerFixture) DescribeGroups(context.Context, *kafka.DescribeGroupsRequest) (*kafka.DescribeGroupsResponse, error) {
	return &kafka.DescribeGroupsResponse{Groups: []kafka.DescribeGroupsResponseGroup{{GroupID: "g", GroupState: "Stable",
		Members: []kafka.DescribeGroupsResponseMember{{MemberID: "m", ClientID: "client",
			MemberAssignments: kafka.DescribeGroupsResponseAssignments{Topics: []kafka.GroupMemberTopic{{Topic: "topic", Partitions: []int{0}}}}}}}}}, nil
}

func TestK6BrokerCollector(t *testing.T) {
	ctx := context.Background()
	ids := map[string]string{"topic": "topic-uuid"}
	row, err := BrokerSnapshot(ctx, brokerFixture{}, "g", ids, func(context.Context) (int64, map[string]int64, *k6v1.CoordinatorObservation, error) {
		return 4, map[string]int64{"m": 4}, nil, nil
	})
	if err != nil || row.Generation != 4 || row.Members[0].Generation != 4 || row.Members[0].Assignments[0].TopicID != "topic-uuid" ||
		row.Partitions[0].Low != 1 || row.Partitions[0].End != 9 || *row.Partitions[0].Committed != 7 {
		t.Fatalf("broker observations not joined: %+v, %v", row, err)
	}
	_, err = BrokerSnapshot(ctx, brokerFixture{}, "g", ids, func(context.Context) (int64, map[string]int64, *k6v1.CoordinatorObservation, error) {
		return 4, map[string]int64{"wrong-member": 4}, nil, nil
	})
	if err == nil {
		t.Fatal("unrelated generation witness accepted")
	}
}

func TestK6BrokerCollectorCachedAssignments(t *testing.T) {
	const id = "topic-uuid"
	observation := &k6v1.CoordinatorObservation{Generation: 4, Members: []k6v1.BrokerMember{
		{ID: "m", ClientID: "client", Generation: 4, Assignments: []k6v1.PartitionIdentity{{TopicID: id, Partition: 9}}},
	}}
	observe := func(context.Context) (int64, map[string]int64, *k6v1.CoordinatorObservation, error) {
		return observation.Generation, map[string]int64{"m": observation.Generation}, observation, nil
	}
	_, err := BrokerSnapshot(context.Background(), brokerFixture{}, "g", map[string]string{"topic": id}, observe)
	if err == nil || err.Error() != "K6_BROKER_ASSIGNMENTS_UNCORROBORATED" {
		t.Fatalf("cached assignments were treated as corroborated: %v", err)
	}
	observation.Members[0].Assignments[0].Partition = 0
	got, err := BrokerSnapshot(context.Background(), brokerFixture{}, "g", map[string]string{"topic": id}, observe)
	if err != nil || !got.CoordinatorMatches() {
		t.Fatalf("matching refreshed group lost corroboration: %+v, %v", got, err)
	}
}

func TestK6BrokerPositionsTopicReplacement(t *testing.T) {
	row, err := BrokerPositions(context.Background(), brokerFixture{}, "g", map[string]string{"topic": "new-id"})
	if err != nil || row.Group != "g" || row.State != "membership_unavailable" || len(row.Members) != 0 ||
		row.Partitions[0].TopicID != "new-id" || *row.Partitions[0].Committed != 7 {
		t.Fatalf("changed topic offsets lost when membership was unavailable: %+v, %v", row, err)
	}
}

func TestK6IngressCollector(t *testing.T) {
	now := time.Now()
	h := k6v1.Hash([]byte("request"))
	reply := k6v1.Hash([]byte("reply"))
	const relay, sink = "spiffe://k6.test/relay", "spiffe://k6.test/sink"
	const intent = "00000000-0000-0000-0000-000000000001"
	open := k6ingress.Record{Session: strings.Repeat("a", 32), Time: now, Sequence: 1, Event: "inbound", Ordinal: 1,
		Method: "POST", Operation: "apply", InvocationHash: h, KeyHash: h, RequestIDHash: h, IntentHash: k6v1.Hash([]byte(intent)),
		IntentID: intent, ResourceHash: h, Length: 7, BodyHash: h, WireLength: 22, WireHash: h, WorkerBinding: h, ConnectionHash: h,
		GrantHash: h, Attempt: 1, RelayPeer: relay, SinkPeer: sink}
	sent, response, worker, closed := open, open, open, open
	sent.Sequence, sent.Event = 2, "sink_send"
	response.Sequence, response.Event, response.Status, response.Length, response.BodyHash, response.WireHash = 3, "sink_response", 200, 5, reply, reply
	worker = response
	worker.Sequence, worker.Event = 4, "worker_delivery"
	closed.Sequence, closed.Event, closed.Length, closed.BodyHash, closed.WireLength, closed.WireHash = 5, "close", 0, "", 0, ""
	seal := k6ingress.Record{Session: open.Session, Time: now, Sequence: 6, Event: "seal", HighWater: 5, RelayPeer: relay, SinkPeer: sink}
	encode := func(rows ...k6ingress.Record) *bytes.Buffer {
		t.Helper()
		var out bytes.Buffer
		for _, row := range rows {
			if err := json.NewEncoder(&out).Encode(row); err != nil {
				t.Fatal(err)
			}
		}
		return &out
	}
	stream := NewStream[k6v1.IngressRow](k6v1.Manifest{}, "ingress", "receiver", now)
	err := IngressSnapshot(encode(open, sent, response, worker, closed, seal), &stream, relay, sink, now, nil, true)
	if err != nil || stream.Seal.Mode != "terminal" || len(stream.Rows) != 1 || stream.Rows[0].Data.Response.SHA256 != reply {
		t.Fatalf("collector lost durable ingress: %v", err)
	}
	if err := IngressSnapshot(encode(open, sent, worker, closed, seal), &stream, relay, sink, now, nil, true); err == nil {
		t.Fatal("collector accepted missing durable row")
	}
}

func TestK6TopologyList(t *testing.T) {
	raw := []byte(`{"metadata":{"resourceVersion":"1"},"items":[{"metadata":{"name":"cooperating-sink","labels":{"dur053.io/component":"api"}}}]}`)
	_, rows, err := topologyList(raw, "service")
	if err != nil || len(rows) != 1 || rows[0].Role != "sink" {
		t.Fatalf("collector missed the sink Service: %v %+v", err, rows)
	}
}
