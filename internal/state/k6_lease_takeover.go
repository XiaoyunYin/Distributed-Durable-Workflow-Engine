//go:build dur053_k6

package state

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

// LeaseTakeoverBackend identifies one PostgreSQL backend lifetime.
type LeaseTakeoverBackend struct {
	PID     int
	StartUS int64
}

func leaseTakeoverBackend(ctx context.Context, conn *pgx.Conn) (LeaseTakeoverBackend, error) {
	var backend LeaseTakeoverBackend
	var application string
	err := conn.QueryRow(ctx, `
		SELECT pid, (extract(epoch FROM backend_start) * 1000000)::bigint,
		       set_config('application_name', 'dur053-e2-' || current_setting('durable.k4_session'), false)
		FROM pg_stat_activity WHERE pid = pg_backend_pid()
	`).Scan(&backend.PID, &backend.StartUS, &application)
	return backend, err
}

// AcquireLeaseForTakeover runs the normal acquisition and captures the backend
// and transaction of the winning UPDATE before that same transaction commits.
func (s *Store) AcquireLeaseForTakeover(ctx context.Context, partitionID int16, ownerID string,
	ttl time.Duration) (Lease, bool, LeaseTakeoverBackend, string, error) {
	var backend LeaseTakeoverBackend
	var xid string
	lease, acquired, err := s.acquireLease(ctx, partitionID, ownerID, ttl, func(tx pgx.Tx) error {
		var err error
		backend, err = leaseTakeoverBackend(ctx, tx.Conn())
		if err != nil {
			return err
		}
		return tx.QueryRow(ctx, "SELECT pg_current_xact_id()::text").Scan(&xid)
	})
	return lease, acquired, backend, xid, err
}

// CreateAttemptForTakeover pins a connection across the hold and the normal
// CreateAttempt operation. No transaction exists until prepare has returned.
func (s *Store) CreateAttemptForTakeover(ctx context.Context, input AttemptInput,
	prepare func(LeaseTakeoverBackend) (context.Context, error)) (Attempt, error) {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return Attempt{}, err
	}
	defer conn.Release()
	backend, err := leaseTakeoverBackend(ctx, conn.Conn())
	if err != nil {
		return Attempt{}, err
	}
	ctx, err = prepare(backend)
	if err != nil {
		return Attempt{}, err
	}
	return s.createAttempt(ctx, input, conn.Begin)
}
