package main

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"durable-agent-execution-engine/internal/engine"
	"durable-agent-execution-engine/internal/state"
	"durable-agent-execution-engine/internal/transport"
)

// Events accelerate the durable repair scan; they are not the source of work.
func runScheduler(ctx context.Context, store *state.Store, brokers []string, namespace string) {
	wake := make(chan struct{}, 1)
	go func() {
		if err := engine.Serve(ctx, store, namespace, wake); err != nil && !errors.Is(err, context.Canceled) {
			slog.Error("scheduler stopped", "error", err)
		}
	}()
	for ctx.Err() == nil {
		source, err := transport.NewKafkaSource(brokers, state.EventTopic, "runtime-schedulers-v1")
		if err == nil {
			consumer := transport.NewConsumer(store, source, transport.ConsumerConfig{ConsumerID: "runtime-schedulers-v1"})
			for ctx.Err() == nil {
				message, receiveErr := source.Receive(ctx)
				if receiveErr != nil {
					err = receiveErr
					break
				}
				// Never fetch a later message while this delivery lacks a durable
				// disposition. A restart resumes at committed group offsets.
				for ctx.Err() == nil {
					if _, processErr := consumer.Process(ctx, message); processErr != nil {
						slog.Warn("event ingestion failed", "error", processErr)
						if !schedulerWait(ctx) {
							break
						}
						continue
					}
					select {
					case wake <- struct{}{}:
					default:
					}
					break
				}
			}
			_ = source.Close()
		}
		if ctx.Err() == nil {
			slog.Warn("event consumer restarting", "error", err)
		}
		if !schedulerWait(ctx) {
			return
		}
	}
}

func schedulerWait(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(time.Second):
		return true
	}
}
