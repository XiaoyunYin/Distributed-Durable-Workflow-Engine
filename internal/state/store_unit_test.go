package state

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestLeaseAcquisitionTimeoutClassification(t *testing.T) {
	if !isLeaseAcquisitionTimeout(&pgconn.PgError{Code: "55P03"}) {
		t.Fatal("PostgreSQL lock timeout was not classified as retryable")
	}
	if !isLeaseAcquisitionTimeout(context.DeadlineExceeded) {
		t.Fatal("context deadline was not classified as retryable")
	}
	if isLeaseAcquisitionTimeout(&pgconn.PgError{Code: "23505"}) {
		t.Fatal("unique violation was classified as a lease timeout")
	}
	if isLeaseAcquisitionTimeout(pgx.ErrNoRows) || errors.Is(pgx.ErrNoRows, context.DeadlineExceeded) {
		t.Fatal("missing lease row was classified as a timeout")
	}
}
