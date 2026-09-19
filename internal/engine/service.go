package engine

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"durable-agent-execution-engine/internal/state"
)

// Serve scans one explicit namespace. It never executes activities in the
// scheduler process and never borrows an unacquired partition lease.
func Serve(ctx context.Context, store *state.Store, namespace string, wake <-chan struct{}) error {
	if namespace == "" {
		return errors.New("scheduler namespace is required")
	}
	runner := New(store, nil)
	runner.ExternalActivities = true
	runner.OwnerID, runner.ActorID = state.NewID(), "runtime-scheduler"
	runner.LeaseTTL, runner.AttemptLease = 15*time.Second, 30*time.Second
	cursor := ""
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		ids, err := store.RuntimeWork(ctx, namespace, cursor, 100)
		if err != nil {
			slog.Warn("scheduler repair scan failed", "error", err)
		} else {
			for _, id := range ids {
				cursor = id
				if _, err := runner.Run(ctx, id); err != nil && !errors.Is(err, state.ErrLeaseNotOwned) &&
					!errors.Is(err, state.ErrRevisionConflict) && !errors.Is(err, state.ErrAttemptNotCurrent) {
					slog.Warn("scheduler pass failed", "workflow_id", id, "error", err)
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
