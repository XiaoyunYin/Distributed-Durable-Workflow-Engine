//go:build dur053_k6

package state

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestK6RenewalIdleTimeoutTerminatesRealTransaction(t *testing.T) {
	store := k4Store(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	lease, ok, err := store.AcquireLease(ctx, 0, NewID(), 2*time.Second)
	if err != nil || !ok {
		t.Fatalf("acquire: %v %v", ok, err)
	}
	ref := LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch}
	monitor, err := store.Pool().Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer monitor.Release()
	var baselineGUC string
	if err := monitor.QueryRow(ctx, "SHOW idle_in_transaction_session_timeout").Scan(&baselineGUC); err != nil {
		t.Fatal(err)
	}
	probe := func() (int64, error) {
		tx, err := monitor.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var epoch int64
		err = tx.QueryRow(ctx, `SELECT epoch FROM engine.partition_leases WHERE partition_id = $1 FOR UPDATE NOWAIT`, ref.PartitionID).Scan(&epoch)
		if rb := tx.Rollback(ctx); rb != nil {
			t.Fatal(rb)
		}
		return epoch, err
	}
	var point K6RenewalPoint
	holdCtx := WithK6RenewalHold(ctx, 700*time.Millisecond, func(p K6RenewalPoint) (context.Context, error) {
		point = p
		if p.Lease.OwnerID != ref.OwnerID || p.Lease.Epoch != ref.Epoch || p.Lease.PartitionID != ref.PartitionID ||
			!p.Lease.LeaseExpiresAt.Equal(lease.LeaseExpiresAt) || p.Backend.PID <= 0 || p.Backend.StartUS <= 0 ||
			p.XID == "" || !p.DBTime.Before(lease.LeaseExpiresAt) {
			t.Fatalf("renewal point: %+v; acquired lease: %+v", p, lease)
		}
		var status, xid string
		if err := monitor.QueryRow(ctx, "SELECT state, backend_xid::text FROM pg_stat_activity WHERE pid = $1", p.Backend.PID).Scan(&status, &xid); err != nil || status != "idle in transaction" || xid != p.XID {
			t.Fatalf("held backend: state=%q xid=%q err=%v", status, xid, err)
		}
		var blocked *pgconn.PgError
		if _, err := probe(); !errors.As(err, &blocked) || blocked.Code != "55P03" {
			t.Fatalf("real renewal row was not locked: %v", err)
		}
		var exists bool
		for {
			if err := monitor.QueryRow(ctx, "SELECT EXISTS (SELECT FROM pg_stat_activity WHERE pid = $1)", p.Backend.PID).Scan(&exists); err != nil {
				t.Fatal(err)
			}
			if !exists {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if epoch, err := probe(); err != nil || epoch != ref.Epoch {
			t.Fatalf("row did not clear without an epoch change: epoch=%d err=%v", epoch, err)
		}
		return ctx, nil
	})
	if _, err := store.RenewLease(holdCtx, ref, time.Minute); !errors.Is(err, ErrK6RenewalSessionLost) {
		t.Fatalf("F1B_REAL_RENEWAL_HOLD_REQUIRED: renewal did not fail on its timed-out session: %v", err)
	}
	var expiry time.Time
	var guc string
	if err := monitor.QueryRow(ctx, `SELECT lease_expires_at, current_setting('idle_in_transaction_session_timeout')
		FROM engine.partition_leases WHERE partition_id = $1`, ref.PartitionID).Scan(&expiry, &guc); err != nil || !expiry.Equal(lease.LeaseExpiresAt) || guc != baselineGUC {
		t.Fatalf("held renewal changed expiry or leaked session setting: expiry=%v setting=%q err=%v", expiry, guc, err)
	}
	if point.Backend.PID == 0 {
		t.Fatal("F1B_REAL_RENEWAL_HOLD_REQUIRED")
	}
}
