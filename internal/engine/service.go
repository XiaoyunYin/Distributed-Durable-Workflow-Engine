package engine

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"durable-agent-execution-engine/internal/state"
	"durable-agent-execution-engine/internal/telemetry"
)

const schedulerIterationTimeout = 5 * time.Second

type ServeOptions struct {
	AfterLeaseAcquired func(context.Context, state.Lease) error
	Tracing            *telemetry.Tracing
}

// Serve scans one explicit namespace. It never executes activities in the
// scheduler process and never borrows an unacquired partition lease.
func Serve(ctx context.Context, store *state.Store, namespace string, wake <-chan struct{}) error {
	return ServeWithOptions(ctx, store, namespace, wake, ServeOptions{})
}

func ServeWithOptions(ctx context.Context, store *state.Store, namespace string, wake <-chan struct{}, options ServeOptions) error {
	if namespace == "" {
		return errors.New("scheduler namespace is required")
	}
	runner := New(store, nil)
	runner.ExternalActivities = true
	runner.AfterLeaseAcquired = options.AfterLeaseAcquired
	runner.Tracing = options.Tracing
	runner.OwnerID, runner.ActorID = state.NewID(), "runtime-scheduler"
	runner.LeaseTTL, runner.AttemptLease = 15*time.Second, 30*time.Second
	cursor := ""
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		scanCtx, scanCancel := context.WithTimeout(ctx, schedulerIterationTimeout)
		ids, err := store.RuntimeWork(scanCtx, namespace, cursor, 100)
		scanCancel()
		if err != nil {
			slog.Warn("scheduler repair scan failed", "error", err)
		} else {
			for _, id := range ids {
				cursor = id
				iterationCtx, iterationCancel := context.WithTimeout(ctx, schedulerIterationTimeout)
				_, runErr := runner.Run(iterationCtx, id)
				iterationCancel()
				if runErr != nil && !errors.Is(runErr, state.ErrLeaseNotOwned) &&
					!errors.Is(runErr, state.ErrLeaseAcquisitionTimeout) &&
					!errors.Is(runErr, state.ErrRevisionConflict) && !errors.Is(runErr, state.ErrAttemptNotCurrent) {
					slog.Warn("scheduler pass failed", "workflow_id", id, "error", runErr)
				}
			}
			if len(ids) < 100 {
				cursor = ""
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		case <-wake:
		}
	}
}
