package transport

import (
	"testing"

	"durable-agent-execution-engine/internal/state"
	"github.com/segmentio/kafka-go"
)

func TestSchedulerGenerationKeepsUnsetReaderAndManualCommit(t *testing.T) {
	for _, tc := range []struct {
		generation, group string
	}{
		{"", "runtime-schedulers-v1"},
		{J7RestoreGeneration, "runtime-schedulers-v1-j7-restore"},
	} {
		t.Run(tc.group, func(t *testing.T) {
			t.Setenv("RUNTIME_KAFKA_GENERATION", tc.generation)
			generation, err := ConsumerGenerationFromEnvironment()
			if err != nil {
				t.Fatal(err)
			}
			source, err := NewKafkaSource([]string{"127.0.0.1:1"}, state.EventTopic, generation.SchedulerID())
			if err != nil {
				t.Fatal(err)
			}
			defer source.Close()
			config := source.reader.Config()
			if config.GroupID != tc.group || config.Topic != state.EventTopic ||
				config.StartOffset != kafka.FirstOffset || config.CommitInterval != 0 ||
				NewConsumer(nil, source, ConsumerConfig{ConsumerID: generation.SchedulerID()}).Config.ConsumerID != tc.group {
				t.Fatalf("Kafka and durable scheduler identities or position policy differ: %+v", config)
			}
		})
	}
	for _, invalid := range []string{"0", "1", "-j7-restore", " j7-restore", "J7-RESTORE"} {
		if _, err := ParseConsumerGeneration(invalid); err == nil {
			t.Fatalf("accepted unregistered consumer generation %q", invalid)
		}
	}
}
