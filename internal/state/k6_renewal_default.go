//go:build !dur053_k6

package state

import (
	"context"

	"github.com/jackc/pgx/v5"
)

func k6BeforeRenewLease(ctx context.Context, _ pgx.Tx, _ Lease) (context.Context, error) {
	return ctx, nil
}
