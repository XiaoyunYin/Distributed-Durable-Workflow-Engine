//go:build dur053_k6

package main

import (
	"crypto/sha256"
	"fmt"
	"os"
	"strconv"
	"strings"

	"durable-agent-execution-engine/internal/engine"
	"durable-agent-execution-engine/internal/faults"
	"durable-agent-execution-engine/internal/k6hook"
	"durable-agent-execution-engine/internal/state"
)

func configureK6SchedulerHook(options *engine.ServeOptions) error {
	if os.Getenv("DUR053_K6_LEASE_TAKEOVER_ROLE") != "" {
		return configureK6LeaseTakeover(options)
	}
	client, err := k6hook.FromEnvironment("scheduler", strings.ReplaceAll(state.K4Session, "-", ""))
	if err != nil || client == nil {
		return err
	}
	options.BeforeResultConsumption = func(input state.ConsumeResultInput, attempt state.Attempt) error {
		return faults.HoldK6(client, "result_recorded", k6hook.Fields{Key: input.WorkflowID,
			Objects: map[string]string{
				"cell": "hook-4", "node": input.NodeID, "iteration": strconv.Itoa(input.Iteration),
				"attempt":   strconv.FormatInt(input.AttemptNumber, 10),
				"revision":  strconv.FormatInt(input.ExpectedRevision, 10),
				"partition": strconv.Itoa(int(input.Lease.PartitionID)),
				"owner":     input.Lease.OwnerID, "epoch": strconv.FormatInt(input.Lease.Epoch, 10),
				"token_hash":  fmt.Sprintf("%x", sha256.Sum256([]byte(attempt.ClaimToken))),
				"result_hash": fmt.Sprintf("%x", sha256.Sum256(attempt.Result)),
			}})
	}
	return nil
}

func k6RelayHook(owner string) (func(string, state.OutboxEvent) error, error) {
	client, err := k6hook.FromEnvironment("relay", strings.ReplaceAll(owner, "-", ""))
	if err != nil || client == nil {
		return nil, err
	}
	return func(boundary string, event state.OutboxEvent) error {
		cell := "hook-0"
		if boundary == "after_broker_ack" {
			cell = "hook-1"
		}
		return faults.HoldK6(client, boundary, k6hook.Fields{Key: event.WorkflowID,
			Objects: map[string]string{
				"cell": cell, "event": event.EventID, "type": event.EventType, "topic": event.Topic,
				"owner": event.ClaimOwner, "attempt": strconv.Itoa(event.RelayAttempts),
			}})
	}, nil
}
