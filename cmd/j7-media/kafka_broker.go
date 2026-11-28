package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"strconv"
	"time"

	"durable-agent-execution-engine/internal/invariants/k6collect"
	"durable-agent-execution-engine/internal/restoreledger"
	"durable-agent-execution-engine/internal/state"
	"github.com/segmentio/kafka-go"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

var j7Topics = []string{state.EventTopic, state.TaskTopic}

type kafkaPartition struct {
	Topic     string `json:"topic"`
	TopicID   string `json:"topic_id"`
	Partition int    `json:"partition"`
	First     int64  `json:"first_offset"`
	End       int64  `json:"end_offset"`
}

type eventAttempt struct {
	WorkflowID string `json:"workflow_id"`
	NodeID     string `json:"node_id"`
	Iteration  int    `json:"iteration"`
	Attempt    int64  `json:"attempt_number"`
}

type kafkaEvent struct {
	Partition  int           `json:"partition"`
	Offset     int64         `json:"offset"`
	Topic      string        `json:"topic"`
	TopicID    string        `json:"topic_id"`
	EventID    string        `json:"event_id"`
	Kind       string        `json:"event_type"`
	WorkflowID string        `json:"workflow_id"`
	BrokerTime time.Time     `json:"broker_timestamp"`
	PayloadSHA string        `json:"payload_sha256"`
	Attempt    *eventAttempt `json:"attempt,omitempty"`
}

type kafkaSnapshot struct {
	Endpoint       string           `json:"endpoint"`
	Cluster        string           `json:"cluster_id"`
	BeforeNS       int64            `json:"before_ns"`
	AfterNS        int64            `json:"after_ns"`
	Partitions     []kafkaPartition `json:"partitions"`
	Groups         []string         `json:"groups"`
	Events         []kafkaEvent     `json:"events,omitempty"`
	CopiesComplete bool             `json:"copies_complete"`
}

type j7BrokerClient struct {
	endpoint string
	api      *kafka.Client
	ids      *kgo.Client
}

func openJ7Broker(endpoint string) (*j7BrokerClient, error) {
	ids, err := kgo.NewClient(kgo.SeedBrokers(endpoint), kgo.DialTimeout(2*time.Second))
	if err != nil {
		return nil, err
	}
	return &j7BrokerClient{endpoint: endpoint, ids: ids,
		api: &kafka.Client{Addr: kafka.TCP(endpoint), Timeout: 3 * time.Second}}, nil
}

func (c *j7BrokerClient) snapshot(ctx context.Context, now restoreledger.Clock, copies bool) (kafkaSnapshot, error) {
	s := kafkaSnapshot{Endpoint: c.endpoint}
	var err error
	s.BeforeNS, err = now()
	if err != nil {
		return s, err
	}
	meta, err := c.api.Metadata(ctx, &kafka.MetadataRequest{})
	if err != nil || meta.ClusterID == "" || len(meta.Brokers) != 1 ||
		net.JoinHostPort(meta.Brokers[0].Host, strconv.Itoa(meta.Brokers[0].Port)) != c.endpoint {
		return s, errors.New("Kafka broker metadata does not identify the sealed host listener")
	}
	s.Cluster = meta.ClusterID
	ids, err := k6collect.BrokerTopicIDs(ctx, c.ids, j7Topics)
	if err != nil || ids[state.TaskTopic] == ids[state.EventTopic] {
		return s, errors.New("Kafka did not identify both independent engine topics")
	}
	seenTopics := map[string]bool{}
	brokerID := meta.Brokers[0].ID
	for _, topic := range meta.Topics {
		if !slices.Contains(j7Topics, topic.Name) {
			if topic.Internal {
				continue
			}
			return s, fmt.Errorf("unexpected engine broker topic %s", topic.Name)
		}
		if topic.Error != nil || topic.Internal || len(topic.Partitions) != 8 || seenTopics[topic.Name] {
			return s, errors.New("J7 Kafka requires exactly eight partitions per engine topic")
		}
		seenTopics[topic.Name] = true
		seen := [8]bool{}
		for _, part := range topic.Partitions {
			if part.Error != nil || part.ID < 0 || part.ID >= len(seen) || seen[part.ID] ||
				len(part.Replicas) != 1 || len(part.Isr) != 1 || part.Leader.ID != brokerID ||
				part.Replicas[0].ID != brokerID || part.Isr[0].ID != brokerID {
				return s, errors.New("J7 Kafka partition leadership or replication factor differs")
			}
			seen[part.ID] = true
		}
	}
	if len(seenTopics) != 2 {
		return s, errors.New("J7 Kafka is missing an engine topic")
	}
	firstOffsets, err := readKafkaOffsets(ctx, c.ids, kafka.FirstOffset)
	if err != nil {
		return s, err
	}
	endOffsets, err := readKafkaOffsets(ctx, c.ids, kafka.LastOffset)
	if err != nil {
		return s, err
	}
	for _, name := range j7Topics {
		for p := range 8 {
			first, end := firstOffsets[name][p], endOffsets[name][p]
			if first != 0 || end < first {
				return s, errors.New("Kafka lost an offset in the retained witness range")
			}
			s.Partitions = append(s.Partitions, kafkaPartition{name, ids[name], p, first, end})
		}
	}
	groups, err := c.api.ListGroups(ctx, &kafka.ListGroupsRequest{})
	if err != nil || groups.Error != nil {
		return s, errors.New("cannot enumerate Kafka groups on the witnessed broker")
	}
	s.Groups = make([]string, 0, len(groups.Groups))
	for _, g := range groups.Groups {
		if g.GroupID == "" || slices.Contains(s.Groups, g.GroupID) {
			return s, errors.New("invalid Kafka group inventory")
		}
		s.Groups = append(s.Groups, g.GroupID)
	}
	slices.Sort(s.Groups)
	if copies {
		for _, p := range s.Partitions {
			if err := c.readCopies(ctx, &s, p); err != nil {
				return s, err
			}
		}
		s.CopiesComplete = true
	}
	s.AfterNS, err = now()
	if err == nil && (s.BeforeNS <= 0 || s.AfterNS < s.BeforeNS) {
		err = errors.New("invalid Kafka observation clock bracket")
	}
	return s, err
}

// Keep the actual per-partition responses. A missing response must not be
// interpreted as offset zero by a convenience client.
func readKafkaOffsets(ctx context.Context, client kmsg.Requestor, timestamp int64) (map[string][8]int64, error) {
	req := kmsg.NewPtrListOffsetsRequest()
	for _, name := range j7Topics {
		topic := kmsg.ListOffsetsRequestTopic{Topic: name}
		for partition := range 8 {
			topic.Partitions = append(topic.Partitions, kmsg.ListOffsetsRequestTopicPartition{
				Partition: int32(partition), CurrentLeaderEpoch: -1, Timestamp: timestamp,
			})
		}
		req.Topics = append(req.Topics, topic)
	}
	raw, err := req.RequestWith(ctx, client)
	if err != nil || raw == nil || len(raw.Topics) != 2 {
		return nil, errors.New("broker did not return a complete ListOffsets response")
	}
	result := map[string][8]int64{}
	for _, topic := range raw.Topics {
		if _, duplicate := result[topic.Topic]; duplicate || !slices.Contains(j7Topics, topic.Topic) || len(topic.Partitions) != 8 {
			return nil, errors.New("Kafka offset inventory has a missing or duplicated topic or partition")
		}
		var seen [8]bool
		var offsets [8]int64
		for _, p := range topic.Partitions {
			if p.ErrorCode != 0 || p.Partition < 0 || int(p.Partition) >= len(seen) || seen[p.Partition] || p.Offset < 0 {
				return nil, errors.New("Kafka offset response did not prove every partition")
			}
			seen[p.Partition], offsets[p.Partition] = true, p.Offset
		}
		result[topic.Topic] = offsets
	}
	return result, nil
}

func (c *j7BrokerClient) readCopies(ctx context.Context, s *kafkaSnapshot, p kafkaPartition) error {
	for next := p.First; next < p.End; {
		last := next
		resp, err := c.api.Fetch(ctx, &kafka.FetchRequest{Topic: p.Topic, Partition: p.Partition, Offset: next,
			MinBytes: 1, MaxBytes: 2 << 20, MaxWait: 250 * time.Millisecond, IsolationLevel: kafka.ReadUncommitted})
		if err != nil || resp.Error != nil || resp.Records == nil || resp.Topic != p.Topic || resp.Partition != p.Partition ||
			resp.LogStartOffset != 0 || resp.HighWatermark < p.End {
			return fmt.Errorf("fetch %s/%d at %d could not prove the sealed range", p.Topic, p.Partition, next)
		}
		next, err = inspectCopyBatch(resp.Records, s, p, next)
		if err != nil || next == last {
			return fmt.Errorf("Kafka copy is incomplete at %s/%d/%d: %v", p.Topic, p.Partition, last, err)
		}
	}
	return nil
}

func inspectCopyBatch(records kafka.RecordReader, s *kafkaSnapshot, p kafkaPartition, next int64) (int64, error) {
	if closer, ok := records.(io.Closer); ok {
		defer closer.Close()
	}
	for next < p.End {
		record, err := records.ReadRecord()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil || record.Offset > next {
			return next, errors.New("Kafka record stream has a gap or malformed batch")
		}
		e, err := inspectKafkaRecord(*record, p)
		if record.Offset < next {
			continue
		}
		if err != nil {
			return next, err
		}
		s.Events = append(s.Events, e)
		next++
	}
	return next, nil
}

func inspectKafkaRecord(record kafka.Record, p kafkaPartition) (kafkaEvent, error) {
	e := kafkaEvent{Partition: p.Partition, Offset: record.Offset, Topic: p.Topic, TopicID: p.TopicID, BrokerTime: record.Time}
	headers := make(map[string]string)
	for _, header := range record.Headers {
		if _, exists := headers[header.Key]; exists {
			return e, errors.New("duplicate Kafka record header")
		}
		headers[header.Key] = string(header.Value)
	}
	if record.Key == nil || record.Value == nil {
		return e, errors.New("Kafka record lacks its durable key or payload")
	}
	key, keyErr := io.ReadAll(record.Key)
	body, bodyErr := io.ReadAll(record.Value)
	e.EventID, e.Kind, e.WorkflowID = headers["event-id"], headers["event-type"], headers["workflow-id"]
	if keyErr != nil || bodyErr != nil || string(key) != e.EventID || e.EventID == "" || e.Kind == "" ||
		e.WorkflowID == "" || headers["schema-version"] != "1" || !json.Valid(body) || e.BrokerTime.IsZero() {
		return e, errors.New("Kafka record lacks a valid engine publication identity")
	}
	e.PayloadSHA = restoreledger.SHA256(body)
	if p.Topic == state.TaskTopic {
		var a eventAttempt
		if json.Unmarshal(body, &a) != nil || a.WorkflowID != e.WorkflowID || a.NodeID == "" || a.Iteration < 0 || a.Attempt < 1 {
			return e, errors.New("Kafka task has an invalid workflow and attempt identity")
		}
		e.Attempt = &a
	}
	return e, nil
}

func verifyFreshKafka(source, target kafkaSnapshot) error {
	if source.Cluster == "" || target.Cluster == "" || source.Cluster == target.Cluster ||
		source.Endpoint == target.Endpoint || target.Endpoint == "" ||
		target.BeforeNS <= source.AfterNS || target.AfterNS < target.BeforeNS ||
		len(source.Partitions) != 16 || len(target.Partitions) != 16 || len(target.Groups) != 0 || len(target.Events) != 0 {
		return errors.New("fresh Kafka cluster, endpoint, topic, or empty group evidence is invalid")
	}
	oldTopics := map[string]string{source.Partitions[0].TopicID: source.Partitions[0].Topic, source.Partitions[8].TopicID: source.Partitions[8].Topic}
	for i, p := range target.Partitions {
		o := source.Partitions[i]
		if o.Topic != j7Topics[i/8] || p.Topic != o.Topic || p.Partition != i%8 || o.Partition != p.Partition ||
			p.TopicID == "" || oldTopics[p.TopicID] != "" || p.TopicID != target.Partitions[i/8*8].TopicID ||
			p.First != 0 || p.End != 0 {
			return errors.New("fresh Kafka reused topic identity or copied an old offset")
		}
	}
	if target.Partitions[0].TopicID == target.Partitions[8].TopicID {
		return errors.New("fresh Kafka topic identities are duplicated")
	}
	return nil
}
