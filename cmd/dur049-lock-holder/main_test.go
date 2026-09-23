package main

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestHoldTransactionRefreshesUntilDuration(t *testing.T) {
	var refreshes atomic.Int32
	started := time.Now()
	err := holdTransaction(context.Background(), 35*time.Millisecond, 5*time.Millisecond, func(context.Context) error {
		refreshes.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("holdTransaction() error = %v", err)
	}
	if elapsed := time.Since(started); elapsed < 30*time.Millisecond {
		t.Fatalf("holdTransaction returned too early: %s", elapsed)
	}
	if got := refreshes.Load(); got < 3 {
		t.Fatalf("keepalive refreshes = %d, want at least 3", got)
	}
}

func TestHoldTransactionPropagatesKeepaliveFailure(t *testing.T) {
	want := errors.New("database backend ended transaction")
	err := holdTransaction(context.Background(), time.Second, time.Millisecond, func(context.Context) error {
		return want
	})
	if !errors.Is(err, want) || !strings.Contains(err.Error(), "refresh lock-holder transaction") {
		t.Fatalf("holdTransaction() error = %v, want wrapped keepalive error", err)
	}
}

func TestHoldTransactionHonorsContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err := holdTransaction(ctx, time.Second, 5*time.Millisecond, func(context.Context) error { return nil })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("holdTransaction() error = %v, want context deadline", err)
	}
}
