//go:build dur053_k6

package state

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type K6RenewalPoint struct {
	Backend LeaseTakeoverBackend
	Lease   Lease
	DBTime  time.Time
	XID     string
}

type renewalHoldKey struct{}

type renewalHold struct {
	timeout time.Duration
	wait    func(K6RenewalPoint) (context.Context, error)
}

var ErrK6RenewalSessionLost = errors.New("F1B_RENEWAL_SESSION_LOST")

// WithK6RenewalHold scopes the instrumentation to one normal RenewLease call.
func WithK6RenewalHold(ctx context.Context, timeout time.Duration,
	wait func(K6RenewalPoint) (context.Context, error)) context.Context {
	return context.WithValue(ctx, renewalHoldKey{}, renewalHold{timeout, wait})
}

func k6BeforeRenewLease(ctx context.Context, tx pgx.Tx, lease Lease) (context.Context, error) {
	hold, armed := ctx.Value(renewalHoldKey{}).(renewalHold)
	if !armed {
		return ctx, nil
	}
	ms := hold.timeout.Milliseconds()
	if ms < 1 || ms > 60_000 {
		return ctx, errors.New("F1B_RENEWAL_TIMEOUT_INVALID")
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL idle_in_transaction_session_timeout = %d", ms)); err != nil {
		return ctx, fmt.Errorf("F1B_RENEWAL_SET_LOCAL: %w", err)
	}
	point := K6RenewalPoint{Lease: lease}
	if err := tx.QueryRow(ctx, `SELECT pg_backend_pid(),
		(extract(epoch FROM backend_start) * 1000000)::bigint,
		clock_timestamp(), pg_current_xact_id()::text
		FROM pg_stat_activity WHERE pid = pg_backend_pid()`).Scan(
		&point.Backend.PID, &point.Backend.StartUS, &point.DBTime, &point.XID); err != nil {
		return ctx, fmt.Errorf("F1B_RENEWAL_BACKEND: %w", err)
	}
	resumed, err := hold.wait(point)
	if err != nil {
		return ctx, err
	}
	// The held operation resumes on its original transaction. Fail closed even
	// if a misconfigured experiment releases a session that is still alive.
	if _, err := tx.Exec(resumed, "SELECT 1"); err != nil {
		var pgErr *pgconn.PgError
		if (errors.As(err, &pgErr) && pgErr.Code == "25P03") || tx.Conn().IsClosed() {
			return resumed, fmt.Errorf("%w: %w", ErrK6RenewalSessionLost, err)
		}
		return resumed, fmt.Errorf("F1B_RENEWAL_RESUME: %w", err)
	}
	return resumed, errors.New("F1B_RENEWAL_BACKEND_STILL_ALIVE")
}
