//go:build !dur049_owner_lock_isolation

package state

import (
	"context"

	"github.com/jackc/pgx/v5"
)

func dur049OwnerLockIsolationAfterLeaseLock(context.Context, *Store, pgx.Tx, ConsumeResultInput) error {
	return nil
}
