//go:build !dur053_k6

package main

import (
	"durable-agent-execution-engine/internal/engine"
	"durable-agent-execution-engine/internal/state"
)

func configureK6SchedulerHook(*engine.ServeOptions) error { return nil }

func k6RelayHook(string) (func(string, state.OutboxEvent) error, error) { return nil, nil }
