package state

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// TestSafeguardProfile is intentionally carried in a context. It is a
// measurement-only seam for DUR-034; no production caller should enable a
// weakened profile. The normal Store methods remain the default when the
// context has no profile.
type TestSafeguardProfile struct {
	Name                  string
	DisableHistory        bool
	UnsafeLeaseValidation bool
	DisableOutbox         bool
	CheckToCommit         func(context.Context) error
}

type safeguardProfileKey struct{}

// WithTestSafeguardProfile enables one named test-only profile for the
// operations derived from ctx. The function is deliberately explicit at the
// call site so profile use cannot leak into normal runtime construction.
func WithTestSafeguardProfile(ctx context.Context, profile TestSafeguardProfile) context.Context {
	return context.WithValue(ctx, safeguardProfileKey{}, profile)
}

func testSafeguardProfile(ctx context.Context) TestSafeguardProfile {
	if profile, ok := ctx.Value(safeguardProfileKey{}).(TestSafeguardProfile); ok {
		return profile
	}
	return TestSafeguardProfile{Name: "full"}
}

func (s *Store) applyUnsafeOwnerTransition(ctx context.Context, input OwnerTransitionInput) error {
	if input.WorkflowID == "" || input.ActorID == "" || input.Reason == "" {
		return errors.New("workflow, actor, and reason are required")
	}
	var owner *string
	var epoch int64
	var expiry *time.Time
	var databaseNow time.Time
	if err := s.pool.QueryRow(ctx, `
		SELECT owner_id::text, epoch, lease_expires_at, clock_timestamp()
		FROM engine.partition_leases
		WHERE partition_id = $1`, input.Lease.PartitionID).Scan(&owner, &epoch, &expiry, &databaseNow); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrLeaseNotOwned
		}
		return fmt.Errorf("read partition lease for unsafe profile: %w", err)
	}
	if owner == nil || expiry == nil || *owner != input.Lease.OwnerID || epoch != input.Lease.Epoch || !expiry.After(databaseNow) {
		return ErrLeaseNotOwned
	}
	profile := testSafeguardProfile(ctx)
	if profile.CheckToCommit != nil {
		if err := profile.CheckToCommit(ctx); err != nil {
			return err
		}
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var oldState WorkflowState
	var revision int64
	var partitionID int16
	if err := tx.QueryRow(ctx, `
		SELECT state, revision, partition_id
		FROM engine.workflow_executions
		WHERE workflow_id = $1 FOR UPDATE`, input.WorkflowID).Scan(&oldState, &revision, &partitionID); err != nil {
		return fmt.Errorf("lock workflow for unsafe transition: %w", err)
	}
	if partitionID != input.Lease.PartitionID {
		return fmt.Errorf("workflow partition %d does not match lease %d", partitionID, input.Lease.PartitionID)
	}
	if revision != input.ExpectedRevision {
		return fmt.Errorf("%w: expected %d, got %d", ErrRevisionConflict, input.ExpectedRevision, revision)
	}
	if !legalTransition(oldState, input.NewState) {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, oldState, input.NewState)
	}
	if input.NewState == StateCanceled {
		return errors.New("unsafe ablation does not model cancellation")
	}
	if input.NodeID != "" {
		var ignored *int64
		if err := tx.QueryRow(ctx, `
			SELECT current_attempt_number
			FROM engine.node_instances
			WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3 FOR UPDATE`,
			input.WorkflowID, input.NodeID, input.Iteration).Scan(&ignored); err != nil {
			return fmt.Errorf("lock node for unsafe transition: %w", err)
		}
	}
	newRevision := revision + 1
	if _, err := tx.Exec(ctx, `
		UPDATE engine.workflow_executions
		SET state = $2, revision = $3, updated_at = clock_timestamp()
		WHERE workflow_id = $1`, input.WorkflowID, input.NewState, newRevision); err != nil {
		return fmt.Errorf("update workflow for unsafe transition: %w", err)
	}
	if input.NodeID != "" && input.NodeState != nil {
		if _, err := tx.Exec(ctx, `
			UPDATE engine.node_instances
			SET state = $4, deadline_at = CASE WHEN $4 = 'RUNNABLE' THEN NULL ELSE deadline_at END,
				revision = revision + 1, updated_at = clock_timestamp()
			WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3`,
			input.WorkflowID, input.NodeID, input.Iteration, *input.NodeState); err != nil {
			return fmt.Errorf("update node for unsafe transition: %w", err)
		}
	}
	if err := insertHistory(ctx, tx, input.WorkflowID, newRevision, "scheduler", input.ActorID,
		&input.Lease.Epoch, input.NodeID, nullableIteration(input.NodeID, input.Iteration), input.AttemptNumber,
		&oldState, input.NewState, input.Reason); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
