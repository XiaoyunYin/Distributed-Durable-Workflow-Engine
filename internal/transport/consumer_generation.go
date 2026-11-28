package transport

import (
	"errors"
	"os"
)

const (
	consumerGenerationEnv = "RUNTIME_KAFKA_GENERATION"
	J7RestoreGeneration   = "j7-restore"
)

// ConsumerGeneration selects a matched Kafka group and durable inbox identity.
// The zero value preserves the registered source identities.
type ConsumerGeneration struct{ suffix string }

func ParseConsumerGeneration(value string) (ConsumerGeneration, error) {
	switch value {
	case "":
		return ConsumerGeneration{}, nil
	case J7RestoreGeneration:
		return ConsumerGeneration{suffix: "-" + value}, nil
	default:
		return ConsumerGeneration{}, errors.New(consumerGenerationEnv + " must be unset or " + J7RestoreGeneration)
	}
}

func ConsumerGenerationFromEnvironment() (ConsumerGeneration, error) {
	return ParseConsumerGeneration(os.Getenv(consumerGenerationEnv))
}

func (g ConsumerGeneration) SchedulerID() string { return "runtime-schedulers-v1" + g.suffix }
func (g ConsumerGeneration) WorkerID() string    { return "runtime-workers-v1" + g.suffix }
