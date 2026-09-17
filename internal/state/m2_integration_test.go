package state

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

func TestM2LeaseRenewalAndTakeoverFence(t *testing.T) {
	databaseURL := integrationDatabaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	store, err := NewFromURL(ctx, databaseURL)
	if err != nil {
		if os.Getenv("DURABLE_REQUIRE_DATABASE") == "1" {
			t.Fatalf("PostgreSQL is required but unavailable: %v", err)
		}
		t.Skipf("PostgreSQL is not available: %v", err)
	}
	defer store.Close()
	ownerA := NewID()
	ownerB := NewID()
	lease, acquired, err := acquireTestLease(ctx, store, ownerA)
	if err != nil || !acquired {
		t.Fatalf("owner A acquire = %+v acquired=%v err=%v", lease, acquired, err)
	}
	partitionID := lease.PartitionID
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = store.Pool().Exec(cleanupCtx, `UPDATE engine.partition_leases
			SET owner_id = NULL, lease_expires_at = NULL, updated_at = clock_timestamp()
			WHERE partition_id = $1 AND owner_id::text = $2`, partitionID, ownerB)
		_ = store.ReleaseLease(cleanupCtx, LeaseRef{PartitionID: partitionID, OwnerID: ownerA, Epoch: lease.Epoch})
	}()

	renewed, err := store.RenewLease(ctx, LeaseRef{PartitionID: partitionID, OwnerID: ownerA, Epoch: lease.Epoch}, 2*time.Minute)
	if err != nil || renewed.Epoch != lease.Epoch || !renewed.LeaseExpiresAt.After(lease.LeaseExpiresAt) {
		t.Fatalf("owner A renewal = %+v err=%v", renewed, err)
	}
	if observed, got, err := store.AcquireLease(ctx, partitionID, ownerB, time.Minute); err != nil || got || observed.OwnerID != ownerA {
		t.Fatalf("owner B acquired a live lease = %+v acquired=%v err=%v", observed, got, err)
	}
	if _, err := store.Pool().Exec(ctx, `UPDATE engine.partition_leases
		SET lease_expires_at = clock_timestamp() - interval '1 second'
		WHERE partition_id = $1`, partitionID); err != nil {
		t.Fatal(err)
	}
	taken, acquired, err := store.AcquireLease(ctx, partitionID, ownerB, time.Minute)
	if err != nil || !acquired || taken.Epoch <= lease.Epoch || taken.OwnerID != ownerB {
		t.Fatalf("owner B takeover = %+v acquired=%v err=%v", taken, acquired, err)
	}
	if _, err := store.RenewLease(ctx, LeaseRef{PartitionID: partitionID, OwnerID: ownerA, Epoch: lease.Epoch}, time.Minute); !errors.Is(err, ErrLeaseNotOwned) {
		t.Fatalf("stale owner renewal = %v, want ErrLeaseNotOwned", err)
	}
	if err := store.ReleaseLease(ctx, LeaseRef{PartitionID: partitionID, OwnerID: ownerA, Epoch: lease.Epoch}); !errors.Is(err, ErrLeaseNotOwned) {
		t.Fatalf("stale owner release = %v, want ErrLeaseNotOwned", err)
	}
	current, held, err := store.AcquireLease(ctx, partitionID, ownerB, time.Minute)
	if err != nil || !held || current.Epoch != taken.Epoch {
		t.Fatalf("owner B renewal = %+v acquired=%v err=%v", current, held, err)
	}
}
