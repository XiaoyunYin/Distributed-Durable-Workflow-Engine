package state

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	timeoutRetryPolicyEnv = "RUNTIME_TIMEOUT_RETRY_POLICY"
	timeoutBudgetReason   = "TIMEOUT_REPLACEMENT_BUDGET_EXHAUSTED"
	timeoutDeadlineReason = "TIMEOUT_REPLACEMENT_DEADLINE_EXCEEDED"
)

func timeoutRetryPolicyFromEnvironment() (bool, error) {
	switch value := os.Getenv(timeoutRetryPolicyEnv); value {
	case "", "0":
		return false, nil
	case "1":
		return true, nil
	default:
		return false, fmt.Errorf("%s must be 0 or 1, got %q", timeoutRetryPolicyEnv, value)
	}
}

func timeoutReplacementFailureTx(ctx context.Context, tx pgx.Tx, input TimeoutInput) (string, error) {
	policy, err := retryPolicyTx(ctx, tx, input.WorkflowID, input.NodeID, input.Iteration)
	if err != nil {
		return "", fmt.Errorf("read retry policy for expired attempt: %w", err)
	}
	// A previous eligible TIMED_OUT row had to commit a replacement to reach
	// this current attempt. Under the workflow lock those rows form the expiry
	// counter, including legacy replacements. Count only up to the policy limit.
	var expiries int
	if err := tx.QueryRow(ctx, `
		SELECT count(*) FROM (
			SELECT 1 FROM engine.activity_attempts
			WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3 AND attempt_number < $4
				AND state = 'TIMED_OUT' AND effect_class IN ('PURE_ACTIVITY', 'COOPERATING_EFFECT')
			LIMIT $5
		) AS charged`, input.WorkflowID, input.NodeID, input.Iteration, input.AttemptNumber,
		policy.MaxRetries).Scan(&expiries); err != nil {
		return "", fmt.Errorf("count claimed timeout replacements: %w", err)
	}
	var databaseNow time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&databaseNow); err != nil {
		return "", fmt.Errorf("read clock for timeout retry policy: %w", err)
	}
	return timeoutReplacementFailure(policy, expiries, databaseNow), nil
}

func timeoutReplacementFailure(policy RetryPolicy, expiries int, now time.Time) string {
	if policy.TotalDeadlineAt != nil && !policy.TotalDeadlineAt.After(now) {
		return timeoutDeadlineReason
	}
	if expiries >= policy.MaxRetries {
		return timeoutBudgetReason
	}
	return ""
}

func (s *Store) failExpiredAttemptTx(ctx context.Context, tx pgx.Tx, input TimeoutInput,
	oldState WorkflowState, revision int64, reason string) error {
	// Fence any live siblings before closing the workflow. Preserve results
	// and attempts that are already noncurrent, including reconciliation evidence.
	if _, err := tx.Exec(ctx, `
		UPDATE engine.activity_attempts
		SET state = 'CANCELED', is_current = false,
			outcome_disposition = CASE
				WHEN state = 'CLAIMED' AND effect_class <> 'PURE_ACTIVITY' THEN 'OUTCOME_UNKNOWN'
				ELSE 'NONE'
			END,
			updated_at = clock_timestamp()
		WHERE workflow_id = $1 AND is_current`, input.WorkflowID); err != nil {
		return fmt.Errorf("retire sibling attempts after timeout exhaustion: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE engine.node_instances
		SET state = CASE WHEN node_id = $2 AND iteration = $3 THEN 'FAILED' ELSE 'CANCELED' END,
			current_attempt_number = NULL, deadline_at = NULL,
			revision = revision + 1, updated_at = clock_timestamp()
		WHERE workflow_id = $1 AND (state NOT IN ('SUCCEEDED', 'FAILED', 'REJECTED', 'CANCELED', 'ABANDONED')
			OR (node_id = $2 AND iteration = $3))`, input.WorkflowID, input.NodeID, input.Iteration); err != nil {
		return fmt.Errorf("settle nodes after timeout exhaustion: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE engine.timers SET consumed_at = clock_timestamp()
		WHERE workflow_id = $1 AND consumed_at IS NULL`, input.WorkflowID); err != nil {
		return fmt.Errorf("retire timers after timeout exhaustion: %w", err)
	}
	var namespace string
	if err := tx.QueryRow(ctx, `
		UPDATE engine.workflow_executions
		SET state = 'FAILED', revision = $2, updated_at = clock_timestamp()
		WHERE workflow_id = $1 RETURNING namespace`, input.WorkflowID, revision).Scan(&namespace); err != nil {
		return fmt.Errorf("fail workflow after timeout exhaustion: %w", err)
	}
	if err := s.releaseAdmissionSlotTx(ctx, tx, namespace, input.WorkflowID, oldState, StateFailed); err != nil {
		return err
	}
	return insertHistory(ctx, tx, input.WorkflowID, revision, "scheduler", input.ActorID,
		&input.Lease.Epoch, input.NodeID, &input.Iteration, &input.AttemptNumber,
		&oldState, StateFailed, reason)
}
