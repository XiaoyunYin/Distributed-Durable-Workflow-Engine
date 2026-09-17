package state

import (
	"context"
	"errors"
	"os"
	"sync"
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

func TestM2ConcurrentExpiredLeaseHasOneWinner(t *testing.T) {
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
	initial, acquired, err := acquireTestLease(ctx, store, NewID())
	if err != nil || !acquired {
		t.Fatalf("initial lease = %+v acquired=%v err=%v", initial, acquired, err)
	}
	partitionID := initial.PartitionID
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = store.Pool().Exec(cleanupCtx, `UPDATE engine.partition_leases
			SET owner_id = NULL, lease_expires_at = NULL, updated_at = clock_timestamp()
			WHERE partition_id = $1`, partitionID)
	}()
	if _, err := store.Pool().Exec(ctx, `UPDATE engine.partition_leases
		SET lease_expires_at = clock_timestamp() - interval '1 second'
		WHERE partition_id = $1`, partitionID); err != nil {
		t.Fatal(err)
	}
	const contenders = 12
	start := make(chan struct{})
	results := make(chan struct {
		lease    Lease
		acquired bool
		err      error
	}, contenders)
	var group sync.WaitGroup
	for i := 0; i < contenders; i++ {
		ownerID := NewID()
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			lease, won, acquireErr := store.AcquireLease(ctx, partitionID, ownerID, time.Minute)
			results <- struct {
				lease    Lease
				acquired bool
				err      error
			}{lease, won, acquireErr}
		}()
	}
	close(start)
	group.Wait()
	close(results)
	winners := 0
	var winner Lease
	for result := range results {
		if result.err != nil {
			t.Fatal(result.err)
		}
		if result.acquired {
			winners++
			winner = result.lease
		}
	}
	if winners != 1 {
		t.Fatalf("expired lease winners = %d, want one", winners)
	}
	if winner.Epoch != initial.Epoch+1 {
		t.Fatalf("winner epoch = %d, want %d", winner.Epoch, initial.Epoch+1)
	}
}

func TestM2TakeoverWaitsForLockedLeaseTransaction(t *testing.T) {
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
	initial, acquired, err := acquireTestLease(ctx, store, ownerA)
	if err != nil || !acquired {
		t.Fatalf("initial lease = %+v acquired=%v err=%v", initial, acquired, err)
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = store.Pool().Exec(cleanupCtx, `UPDATE engine.partition_leases
			SET owner_id = NULL, lease_expires_at = NULL, updated_at = clock_timestamp()
			WHERE partition_id = $1`, initial.PartitionID)
	}()
	transaction, err := store.Pool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = transaction.Rollback(ctx) }()
	var heldEpoch int64
	if err := transaction.QueryRow(ctx, `SELECT epoch FROM engine.partition_leases
		WHERE partition_id = $1 FOR UPDATE`, initial.PartitionID).Scan(&heldEpoch); err != nil {
		t.Fatal(err)
	}
	type acquireResult struct {
		lease    Lease
		acquired bool
		err      error
	}
	result := make(chan acquireResult, 1)
	go func() {
		lease, won, acquireErr := store.AcquireLease(ctx, initial.PartitionID, ownerB, time.Minute)
		result <- acquireResult{lease: lease, acquired: won, err: acquireErr}
	}()
	select {
	case early := <-result:
		t.Fatalf("takeover bypassed the held lease row: %+v", early)
	case <-time.After(50 * time.Millisecond):
	}
	if _, err := transaction.Exec(ctx, `UPDATE engine.partition_leases
		SET lease_expires_at = clock_timestamp() - interval '1 second'
		WHERE partition_id = $1`, initial.PartitionID); err != nil {
		t.Fatal(err)
	}
	if err := transaction.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	taken := <-result
	if taken.err != nil || !taken.acquired || taken.lease.OwnerID != ownerB || taken.lease.Epoch != heldEpoch+1 {
		t.Fatalf("takeover after lock release = %+v", taken)
	}
	if err := store.ReleaseLease(ctx, LeaseRef{PartitionID: taken.lease.PartitionID, OwnerID: ownerB, Epoch: taken.lease.Epoch}); err != nil {
		t.Fatal(err)
	}
}
