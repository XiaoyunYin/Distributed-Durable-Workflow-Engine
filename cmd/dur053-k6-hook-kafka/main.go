// dur053-k6-hook-kafka reads broker evidence and probes an isolated F2 topic.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

const probePrefix = "dur053-k6-f2-probe-"

type probeResult struct {
	Topic     string `json:"topic"`
	Nonce     string `json:"nonce"`
	Metadata  bool   `json:"metadata"`
	Produced  bool   `json:"produced"`
	ReadBack  bool   `json:"read_back"`
	Partition int    `json:"partition"`
	Offset    *int64 `json:"offset"`
}

func bridgeDial(address string) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, _ string) (net.Conn, error) {
		var dialer net.Dialer
		return dialer.DialContext(ctx, network, address)
	}
}

// The dedicated, single-partition topic has only this serial probe writer.
func probe(ctx context.Context, address, topic, nonce string) (probeResult, error) {
	result := probeResult{Topic: topic, Nonce: nonce}
	dialer := kafka.Dialer{DialFunc: bridgeDial(address)}
	metadata, err := dialer.LookupPartitions(ctx, "tcp", address, topic)
	if err != nil {
		return result, err
	}
	if len(metadata) != 1 || metadata[0].Topic != topic || metadata[0].ID != 0 {
		return result, errors.New("unexpected probe topic metadata")
	}
	result.Metadata = true
	conn, err := dialer.DialLeader(ctx, "tcp", address, topic, 0)
	if err != nil {
		return result, err
	}
	defer conn.Close()
	deadline, _ := ctx.Deadline()
	if err := conn.SetDeadline(deadline); err != nil {
		return result, err
	}
	_, next, err := conn.ReadOffsets()
	if err != nil {
		return result, err
	}
	if err := conn.SetRequiredAcks(-1); err != nil {
		return result, err
	}
	if _, err := conn.WriteMessages(kafka.Message{Key: []byte(nonce), Value: []byte(nonce)}); err != nil {
		return result, err
	}
	result.Produced = true
	if _, err := conn.Seek(next, kafka.SeekAbsolute); err != nil {
		return result, err
	}
	for {
		message, err := conn.ReadMessage(1 << 20)
		if err != nil {
			return result, err
		}
		if string(message.Key) == nonce && string(message.Value) == nonce && message.Offset >= next {
			result.ReadBack = true
			result.Offset = &message.Offset
			return result, nil
		}
	}
}

type record struct {
	Offset      int64  `json:"offset"`
	Key         string `json:"key"`
	Event       string `json:"event"`
	Workflow    string `json:"workflow"`
	Traceparent string `json:"traceparent"`
}

type logRange struct {
	Partition int      `json:"partition"`
	First     int64    `json:"first"`
	Next      int64    `json:"next"`
	Records   []record `json:"records"`
}

func readRange(ctx context.Context, dialer *kafka.Dialer, address, topic string, partition int) (logRange, error) {
	result := logRange{Partition: partition, Records: []record{}}
	conn, err := dialer.DialLeader(ctx, "tcp", address, topic, partition)
	if err != nil {
		return result, err
	}
	defer conn.Close()
	deadline, _ := ctx.Deadline()
	if err := conn.SetDeadline(deadline); err != nil {
		return result, err
	}
	result.First, result.Next, err = conn.ReadOffsets()
	if err != nil {
		return result, err
	}
	if result.Next-result.First > 1024 {
		return result, errors.New("broker evidence range exceeds engineering smoke bound")
	}
	if _, err := conn.Seek(result.First, kafka.SeekAbsolute); err != nil {
		return result, err
	}
	for offset := result.First; offset < result.Next; offset++ {
		message, err := conn.ReadMessage(1 << 20)
		if err != nil {
			return result, err
		}
		if message.Offset != offset {
			return result, fmt.Errorf("read broker offset %d, expected %d", message.Offset, offset)
		}
		row := record{Offset: offset, Key: string(message.Key)}
		for _, header := range message.Headers {
			switch header.Key {
			case "event-id":
				row.Event = string(header.Value)
			case "workflow-id":
				row.Workflow = string(header.Value)
			case "traceparent":
				row.Traceparent = string(header.Value)
			}
		}
		result.Records = append(result.Records, row)
	}
	return result, nil
}

func sample(ctx context.Context, address, topic, group string, selected int) (any, error) {
	// kind uses one locked broker. Its advertised Docker DNS name is
	// available inside kind, while the runner observes its bridge address.
	dial := bridgeDial(address)
	if group != "" {
		transport := &kafka.Transport{Dial: dial}
		defer transport.CloseIdleConnections()
		client := kafka.Client{Transport: transport}
		reply, err := client.OffsetFetch(ctx, &kafka.OffsetFetchRequest{
			Addr: kafka.TCP(address), GroupID: group, Topics: map[string][]int{topic: {selected}},
		})
		if err != nil {
			return nil, err
		}
		if reply.Error != nil {
			return nil, reply.Error
		}
		rows := reply.Topics[topic]
		if len(rows) != 1 || rows[0].Partition != selected {
			return nil, errors.New("broker offset sample omitted requested partition")
		}
		return map[string]any{"group": group, "topic": topic, "partition": selected,
			"next": rows[0].CommittedOffset}, rows[0].Error
	}

	dialer := kafka.Dialer{DialFunc: dial}
	partitions := []int{selected}
	if selected == -1 {
		conn, err := dialer.DialContext(ctx, "tcp", address)
		if err != nil {
			return nil, err
		}
		deadline, _ := ctx.Deadline()
		if err := conn.SetDeadline(deadline); err != nil {
			_ = conn.Close()
			return nil, err
		}
		metadata, err := conn.ReadPartitions(topic)
		_ = conn.Close()
		if err != nil {
			return nil, err
		}
		partitions = partitions[:0]
		for _, partition := range metadata {
			partitions = append(partitions, partition.ID)
		}
	}
	ranges := make([]logRange, 0, len(partitions))
	for _, partition := range partitions {
		value, err := readRange(ctx, &dialer, address, topic, partition)
		if err != nil {
			return nil, err
		}
		ranges = append(ranges, value)
	}
	return ranges, nil
}

func main() {
	address := flag.String("broker", "", "kind broker bridge address with port")
	topic := flag.String("topic", "", "topic to inspect")
	group := flag.String("group", "", "sample the group's committed offset")
	partition := flag.Int("partition", -1, "partition, or -1 for all log ranges")
	nonce := flag.String("probe", "", "produce and read back a nonce on an isolated F2 probe topic")
	timeout := flag.Duration("timeout", 5*time.Second, "deadline for the entire sample")
	flag.Parse()
	host, port, err := net.SplitHostPort(*address)
	_, portErr := strconv.ParseUint(port, 10, 16)
	if err != nil || net.ParseIP(host) == nil || portErr != nil || *topic == "" ||
		*partition < -1 || (*group != "" && *partition < 0) || *timeout <= 0 || *timeout > 5*time.Second {
		fmt.Fprintln(os.Stderr, "K6E1_KAFKA_ARGUMENTS")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	if *nonce != "" {
		if !strings.HasPrefix(*topic, probePrefix) || *group != "" || *partition != -1 {
			fmt.Fprintln(os.Stderr, "K6F2_KAFKA_PROBE_TOPIC_REQUIRED")
			os.Exit(2)
		}
		value, err := probe(ctx, *address, *topic, *nonce)
		_ = json.NewEncoder(os.Stdout).Encode(value)
		if err != nil {
			fmt.Fprintf(os.Stderr, "K6F2_KAFKA_PROBE: %v\n", err)
			os.Exit(1)
		}
		return
	}
	value, err := sample(ctx, *address, *topic, *group, *partition)
	if err != nil {
		fmt.Fprintf(os.Stderr, "K6E1_KAFKA_READ: %v\n", err)
		os.Exit(1)
	}
	_ = json.NewEncoder(os.Stdout).Encode(value)
}
