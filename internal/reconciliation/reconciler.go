// Package reconciliation finds unfinished transport and scheduler work from
// PostgreSQL. It does not make workflow decisions without the partition lease.
package reconciliation

import (
	"context"
	"errors"
	"fmt"
	"time"

	"durable-agent-execution-engine/internal/state"
)

type Config struct {
	BatchSize     int
	ClaimLease    time.Duration
	DispatchLease time.Duration
	Backpressure  state.BackpressureLimits
}

type Reconciler struct {
	Store  *state.Store
	Config Config
}

type Report struct {
	ExpiredAttempts int
	PendingOutbox   int
	PendingWakeups  int
	PoisonRecords   int
	DueTimers       int
	OpenItems       int
	TimedOut        int
	Backlog         state.Backlog
}

func New(store *state.Store, config Config) *Reconciler {
	if config.BatchSize <= 0 {
		config.BatchSize = 100
	}
	if config.ClaimLease <= 0 {
		config.ClaimLease = time.Minute
	}
	if config.DispatchLease <= 0 {
		config.DispatchLease = time.Minute
	}
	return &Reconciler{Store: store, Config: config}
}

// RunOnce performs only lease-authorized timeout transitions. Pending outbox,
// wake-up and timer counts are returned as durable obligations for the relay
// and scheduler; merely observing a row never marks it resolved.
func (r *Reconciler) RunOnce(ctx context.Context, lease state.LeaseRef, actorID string) (Report, error) {
	if r == nil || r.Store == nil || actorID == "" {
		return Report{}, errors.New("reconciler requires a store and actor")
	}
	backlog, err := r.Store.CheckBackpressure(ctx, lease.PartitionID, r.Config.Backpressure)
	if err != nil {
		return Report{}, err
	}
	report := Report{Backlog: backlog, PendingOutbox: backlog.PendingOutbox,
		PendingWakeups: backlog.PendingWakeups, OpenItems: backlog.OpenItems}
	dueAttempts, err := r.Store.ListDueAttempts(ctx, lease.PartitionID, r.Config.BatchSize)
	if err != nil {
		return report, err
	}
	report.ExpiredAttempts = len(dueAttempts)
	for _, due := range dueAttempts {
		item := state.ReconciliationItem{WorkflowID: due.WorkflowID, PartitionID: due.PartitionID,
			NodeID: due.NodeID, Iteration: intPtr(due.Iteration), AttemptNumber: int64Ptr(due.AttemptNumber),
			Kind:      state.ReconcileExpiredAttempt,
			Reference: fmt.Sprintf("%s/%s/%d/%d", due.WorkflowID, due.NodeID, due.Iteration, due.AttemptNumber),
			Detail:    []byte(fmt.Sprintf(`{"deadline":"%s"}`, due.Deadline.UTC().Format(time.RFC3339Nano)))}
		if _, upsertErr := r.Store.UpsertReconciliationItem(ctx, item); upsertErr != nil {
			return report, upsertErr
		}
		timeout, timeoutErr := r.Store.TimeoutAttempt(ctx, state.TimeoutInput{Lease: lease,
			WorkflowID: due.WorkflowID, NodeID: due.NodeID, Iteration: due.Iteration,
			AttemptNumber: due.AttemptNumber, ExpectedRevision: due.Revision,
			ActorID: actorID, DispatchLease: r.Config.DispatchLease})
		if timeoutErr != nil {
			if errors.Is(timeoutErr, state.ErrRevisionConflict) || errors.Is(timeoutErr, state.ErrAttemptNotCurrent) {
				continue
			}
			return report, timeoutErr
		}
		if _, resolveErr := r.Store.UpsertReconciliationItem(ctx, state.ReconciliationItem{
			WorkflowID: due.WorkflowID, PartitionID: due.PartitionID, NodeID: due.NodeID,
			Iteration: intPtr(due.Iteration), AttemptNumber: int64Ptr(due.AttemptNumber),
			Kind:      state.ReconcileExpiredAttempt,
			Reference: fmt.Sprintf("%s/%s/%d/%d", due.WorkflowID, due.NodeID, due.Iteration, due.AttemptNumber),
			Detail:    []byte(`{"outcome":"timeout-applied"}`),
		}); resolveErr != nil {
			return report, resolveErr
		}
		if !timeout.Reconciliation {
			items, listErr := r.Store.ListReconciliationItems(ctx, lease.PartitionID, r.Config.BatchSize)
			if listErr != nil {
				return report, listErr
			}
			for _, item := range items {
				if item.Kind == state.ReconcileExpiredAttempt && item.WorkflowID == due.WorkflowID &&
					item.NodeID == due.NodeID && item.AttemptNumber != nil && *item.AttemptNumber == due.AttemptNumber {
					if resolveErr := r.Store.ResolveReconciliationItem(ctx, item.ItemID, "RESOLVED"); resolveErr != nil {
						return report, resolveErr
					}
				}
			}
		}
		if timeout.Reconciliation || timeout.Redispatched || timeout.ReplacementNumber != nil {
			report.TimedOut++
		}
	}
	dueTimers, err := r.Store.ListDueTimers(ctx, lease.PartitionID, r.Config.BatchSize)
	if err != nil {
		return report, err
	}
	report.DueTimers = len(dueTimers)
	for _, due := range dueTimers {
		if _, itemErr := r.Store.UpsertReconciliationItem(ctx, state.ReconciliationItem{
			WorkflowID: due.WorkflowID, PartitionID: due.PartitionID, NodeID: due.NodeID,
			Iteration: intPtr(due.Iteration), Kind: state.ReconcileDueTimer,
			Reference: "timer/" + due.TimerID,
			Detail:    []byte(fmt.Sprintf(`{"purpose":%q,"due_at":%q}`, due.Purpose, due.DueAt.UTC().Format(time.RFC3339Nano))),
		}); itemErr != nil {
			return report, itemErr
		}
	}
	reportPending, err := r.Store.ListPendingOutbox(ctx, lease.PartitionID, r.Config.BatchSize)
	if err != nil {
		return report, err
	}
	for _, event := range reportPending {
		if _, itemErr := r.Store.UpsertReconciliationItem(ctx, state.ReconciliationItem{
			WorkflowID: event.WorkflowID, PartitionID: lease.PartitionID, Kind: state.ReconcilePendingOutbox,
			Reference: "outbox/" + event.EventID,
			Detail:    []byte(fmt.Sprintf(`{"event_type":%q,"publish_state":%q}`, event.EventType, event.PublishState)),
		}); itemErr != nil {
			return report, itemErr
		}
	}
	pendingWakeups, err := r.Store.ListPendingWakeups(ctx, lease.PartitionID, r.Config.BatchSize)
	if err != nil {
		return report, err
	}
	for _, wakeup := range pendingWakeups {
		if _, itemErr := r.Store.UpsertReconciliationItem(ctx, state.ReconciliationItem{
			WorkflowID: wakeup.WorkflowID, PartitionID: lease.PartitionID, Kind: state.ReconcileLostWakeup,
			Reference: "wakeup/" + wakeup.WakeupID,
			Detail:    []byte(fmt.Sprintf(`{"reason":%q}`, wakeup.Reason)),
		}); itemErr != nil {
			return report, itemErr
		}
	}
	poisonRecords, err := r.Store.ListTransportQuarantine(ctx, r.Config.BatchSize)
	if err != nil {
		return report, err
	}
	report.PoisonRecords = len(poisonRecords)
	for _, poison := range poisonRecords {
		if poison.WorkflowID == "" || poison.PartitionID == nil || *poison.PartitionID != lease.PartitionID {
			// An event ID that cannot be linked to a workflow is global poison
			// evidence. It is intentionally not assigned to this partition.
			continue
		}
		reference := fmt.Sprintf("transport/%s/%s/%d/%d", poison.ConsumerID, poison.Topic,
			poison.Partition, poison.Offset)
		if _, itemErr := r.Store.UpsertReconciliationItem(ctx, state.ReconciliationItem{
			WorkflowID: poison.WorkflowID, PartitionID: *poison.PartitionID,
			Kind: state.ReconcilePoisonRecord, Reference: reference,
			Detail: []byte(fmt.Sprintf(`{"event_id":%q,"event_type":%q,"reason":%q}`,
				poison.EventID, poison.EventType, poison.Reason)),
		}); itemErr != nil {
			return report, itemErr
		}
	}
	if items, listErr := r.Store.ListReconciliationItems(ctx, lease.PartitionID, r.Config.BatchSize); listErr == nil {
		report.OpenItems = len(items)
	} else {
		return report, listErr
	}
	return report, nil
}

func intPtr(value int) *int { return &value }

func int64Ptr(value int64) *int64 { return &value }
