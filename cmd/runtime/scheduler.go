package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"strconv"
	"time"

	"durable-agent-execution-engine/internal/engine"
	"durable-agent-execution-engine/internal/state"
	"durable-agent-execution-engine/internal/telemetry"
	"durable-agent-execution-engine/internal/transport"
)

// Events accelerate the durable repair scan; they are not the source of work.
func schedulerOptions(tracing *telemetry.Tracing) engine.ServeOptions {
	serveOptions := engine.ServeOptions{Tracing: tracing}
	if milliseconds, err := strconv.Atoi(os.Getenv("RUNTIME_SCHEDULER_HOLD_AFTER_ACQUIRE_MS")); err == nil && milliseconds > 0 {
		hold := time.Duration(milliseconds) * time.Millisecond
		serveOptions.AfterLeaseAcquired = func(hookContext context.Context, lease state.Lease) error {
			campaign := os.Getenv("DUR053_K4_CAMPAIGN") == "1"
			if campaign {
				acquisition := state.NewID()
				emitSchedulerAcknowledgment(lease, acquisition, "after_lease_acquired_hold_enter")
				defer emitSchedulerAcknowledgment(lease, acquisition, "after_lease_acquired_hold_exit")
			}
			timer := time.NewTimer(hold)
			defer timer.Stop()
			select {
			case <-hookContext.Done():
				return hookContext.Err()
			case <-timer.C:
				return nil
			}
		}
		slog.Warn("scheduler lease observation hold enabled", "duration", hold)
	}
	return serveOptions
}

var schedulerClockOrigin = time.Now()

func emitSchedulerAcknowledgment(lease state.Lease, acquisition, boundary string) {
	data, _ := json.Marshal(map[string]any{
		"event": "scheduler_acquisition", "boundary": boundary,
		"acquisition": acquisition,
		"owner":       lease.OwnerID, "epoch": lease.Epoch, "partition": lease.PartitionID,
		"session": state.K4Session, "pod_uid": os.Getenv("POD_UID"),
		"container_id": os.Getenv("CONTAINER_ID"), "pid": os.Getpid(),
		"timestamp": time.Now().UTC(), "monotonic_seconds": time.Since(schedulerClockOrigin).Seconds(),
	})
	slog.Info("k4 scheduler acknowledgment", "data", string(data))
}

func runIngestor(ctx context.Context, drain <-chan struct{}, store *state.Store, brokers []string, wake chan<- struct{}) error {
	for ctx.Err() == nil {
		select {
		case <-drain:
			return nil
		default:
		}
		source, err := transport.NewKafkaSource(brokers, state.EventTopic, "runtime-schedulers-v1")
		if err == nil {
			consumer := transport.NewConsumer(store, source, transport.ConsumerConfig{ConsumerID: "runtime-schedulers-v1"})
			for ctx.Err() == nil {
				select {
				case <-drain:
					return source.Close()
				default:
				}
				receiveCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
				message, receiveErr := source.Receive(receiveCtx)
				cancel()
				if errors.Is(receiveErr, context.DeadlineExceeded) && ctx.Err() == nil {
					continue
				}
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
			return ctx.Err()
		}
	}
	return ctx.Err()
}

func schedulerWait(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(time.Second):
		return true
	}
}
