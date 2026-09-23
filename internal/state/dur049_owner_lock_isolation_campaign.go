//go:build dur049_owner_lock_isolation

package state

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

var dur049OwnerLockIsolationUsed atomic.Bool

const dur049OwnerLockSessionIdleTimeout = "2min"

// dur049OwnerLockIsolationAfterLeaseLock is compiled only into the explicit
// DUR-049 campaign runtime. It deliberately leaves the real ConsumeResult
// transaction idle while holding the partition lease row lock. After the
// controller isolates and later reconnects this same runtime process, the
// retained lease identity attempts a fresh ReleaseLease; a takeover must make
// that write fail with ErrLeaseNotOwned.
func dur049OwnerLockIsolationAfterLeaseLock(ctx context.Context, store *Store, tx pgx.Tx, input ConsumeResultInput) error {
	workflowID := os.Getenv("DUR049_OWNER_LOCK_ISOLATION_WORKFLOW_ID")
	if workflowID == "" || workflowID != input.WorkflowID {
		return nil
	}
	if !dur049OwnerLockIsolationUsed.CompareAndSwap(false, true) {
		return nil
	}
	holdText := os.Getenv("DUR049_OWNER_LOCK_ISOLATION_HOLD_MS")
	holdMS, err := strconv.Atoi(holdText)
	if err != nil || holdMS < 30000 || holdMS > 180000 {
		return fmt.Errorf("DUR049_OWNER_LOCK_ISOLATION_HOLD_MS must be 30000..180000, got %q", holdText)
	}
	// The normal server default is 10s. The campaign controller needs several
	// SSM observations to confirm isolation across three hosts, so extend only
	// this tagged fixture transaction. This is transaction-local, not a change
	// to the deployed/global PostgreSQL setting, and is recorded in the artifact.
	if _, err := tx.Exec(ctx, `SET LOCAL idle_in_transaction_session_timeout = '2min'`); err != nil {
		return fmt.Errorf("set campaign-local idle transaction timeout: %w", err)
	}
	var backendPID int
	var idleTimeout string
	if err := tx.QueryRow(ctx, `SELECT pg_backend_pid(), current_setting('idle_in_transaction_session_timeout')`).Scan(&backendPID, &idleTimeout); err != nil {
		return fmt.Errorf("observe lock-owning PostgreSQL backend: %w", err)
	}
	if idleTimeout != dur049OwnerLockSessionIdleTimeout {
		return fmt.Errorf("campaign-local idle transaction timeout = %q, want %q", idleTimeout, dur049OwnerLockSessionIdleTimeout)
	}
	startedAt := time.Now().UTC()
	_, _ = fmt.Fprintf(os.Stderr,
		"DUR049_OWNER_LOCK_HELD workflow_id=%s partition_id=%d owner_id=%s epoch=%d backend_pid=%d idle_timeout=%s idle_timeout_scope=transaction_local hold_ms=%d started_at_utc=%s\n",
		input.WorkflowID, input.Lease.PartitionID, input.Lease.OwnerID, input.Lease.Epoch,
		backendPID, idleTimeout, holdMS, startedAt.Format(time.RFC3339Nano))

	// Intentionally ignore ctx cancellation: the server-side idle-transaction
	// timeout, not a client-side rollback or context deadline, must release the
	// lock while this original runtime process remains alive.
	time.Sleep(time.Duration(holdMS) * time.Millisecond)

	var lastErr error
	for attempt := 1; attempt <= 20; attempt++ {
		probeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := store.ReleaseLease(probeCtx, input.Lease)
		cancel()
		if errors.Is(err, ErrLeaseNotOwned) {
			_, _ = fmt.Fprintf(os.Stderr,
				"DUR049_OWNER_LOCK_STALE_WRITE workflow_id=%s partition_id=%d owner_id=%s epoch=%d operation=ReleaseLease stale_write_rejected=true attempt=%d observed_at_utc=%s error=%q\n",
				input.WorkflowID, input.Lease.PartitionID, input.Lease.OwnerID, input.Lease.Epoch,
				attempt, time.Now().UTC().Format(time.RFC3339Nano), err.Error())
			return nil
		}
		if err == nil {
			_, _ = fmt.Fprintf(os.Stderr,
				"DUR049_OWNER_LOCK_STALE_WRITE workflow_id=%s partition_id=%d owner_id=%s epoch=%d operation=ReleaseLease stale_write_rejected=false attempt=%d observed_at_utc=%s\n",
				input.WorkflowID, input.Lease.PartitionID, input.Lease.OwnerID, input.Lease.Epoch,
				attempt, time.Now().UTC().Format(time.RFC3339Nano))
			return errors.New("stale owner ReleaseLease unexpectedly succeeded")
		}
		lastErr = err
		time.Sleep(time.Second)
	}
	_, _ = fmt.Fprintf(os.Stderr,
		"DUR049_OWNER_LOCK_STALE_WRITE workflow_id=%s partition_id=%d owner_id=%s epoch=%d operation=ReleaseLease stale_write_rejected=false retries=20 error=%q\n",
		input.WorkflowID, input.Lease.PartitionID, input.Lease.OwnerID, input.Lease.Epoch, fmt.Sprint(lastErr))
	return fmt.Errorf("could not observe post-reconnect stale ReleaseLease rejection: %w", lastErr)
}
