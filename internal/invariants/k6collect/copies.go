package k6collect

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"slices"
	"time"

	"durable-agent-execution-engine/internal/invariants/k6v1"
	"github.com/segmentio/kafka-go"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// The older metadata client decodes names and partitions but lacks topic IDs.
func BrokerTopicIDs(ctx context.Context, client *kgo.Client, names []string) (map[string]string, error) {
	req := kmsg.NewPtrMetadataRequest()
	for _, name := range names {
		req.Topics = append(req.Topics, kmsg.MetadataRequestTopic{Topic: &name})
	}
	res, err := req.RequestWith(ctx, client)
	if err != nil || len(res.Topics) != len(names) {
		return nil, errors.New("K6_BROKER_TOPIC_IDS")
	}
	ids := map[string]string{}
	for _, topic := range res.Topics {
		if topic.ErrorCode != 0 || topic.Topic == nil || !slices.Contains(names, *topic.Topic) || topic.TopicID == [16]byte{} || ids[*topic.Topic] != "" {
			return nil, errors.New("K6_BROKER_TOPIC_IDS")
		}
		ids[*topic.Topic] = base64.RawURLEncoding.EncodeToString(topic.TopicID[:])
	}
	return ids, nil
}

// Each scan reads exactly [0, end) from a fresh, uncompacted engine topic.
// Read failures and vanished prefixes produce an explicit incomplete range.
func ReadBrokerCopies(ctx context.Context, client *kafka.Client, stream *k6v1.Stream[k6v1.BrokerCopyRow], partitions []k6v1.BrokerPartition) {
	for _, part := range partitions {
		if !k6v1.EngineTopic(part.Topic) {
			continue
		}
		key := k6v1.PartitionIdentity{TopicID: part.TopicID, Partition: part.Partition}
		part.Committed = nil
		next := part.Low
		readable := true
		for part.Low == 0 && next < part.End {
			advanced, ok := readCopyBatch(ctx, client, stream, key, part, next)
			if !ok || advanced == next {
				readable = false
				break
			}
			next = advanced
		}
		Append(stream, time.Now(), k6v1.BrokerCopyRow{PartitionIdentity: key, Range: &part, Complete: readable && part.Low == 0 && next == part.End})
	}
}

func readCopyBatch(ctx context.Context, client *kafka.Client, stream *k6v1.Stream[k6v1.BrokerCopyRow],
	key k6v1.PartitionIdentity, part k6v1.BrokerPartition, next int64) (int64, bool) {
	res, err := client.Fetch(ctx, &kafka.FetchRequest{Topic: part.Topic, Partition: int(part.Partition), Offset: next,
		MinBytes: 1, MaxBytes: 4 << 20, MaxWait: 250 * time.Millisecond, IsolationLevel: kafka.ReadUncommitted})
	if err != nil {
		return next, false
	}
	if closer, ok := res.Records.(io.Closer); ok {
		defer closer.Close()
	}
	if res.Error != nil || res.Topic != part.Topic || int64(res.Partition) != part.Partition || res.LogStartOffset != 0 || res.HighWatermark < part.End {
		return next, false
	}
	for next < part.End {
		r, err := res.Records.ReadRecord()
		if errors.Is(err, io.EOF) {
			return next, true
		}
		if err != nil || r.Offset > next {
			return next, false
		}
		if r.Offset < next {
			continue // A fetch may include earlier members of the first batch.
		}
		var ids []string
		for _, header := range r.Headers {
			if header.Key == "event-id" {
				ids = append(ids, string(header.Value))
			}
		}
		var actualKey, payload []byte
		if r.Key != nil {
			actualKey, err = io.ReadAll(r.Key)
			if err != nil {
				return next, false
			}
		}
		if r.Value != nil {
			payload, err = io.ReadAll(r.Value)
			if err != nil {
				return next, false
			}
		}
		copy := k6v1.BrokerCopy{Offset: next, EventID: k6v1.BrokerEventID(ids), Key: actualKey, PayloadSHA256: k6v1.Hash(payload)}
		Append(stream, time.Now(), k6v1.BrokerCopyRow{PartitionIdentity: key, Copy: &copy})
		next++
	}
	return next, true
}
