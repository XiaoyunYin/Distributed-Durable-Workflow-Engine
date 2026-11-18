package k6auth

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Deprovision removes the current installation after its clients have closed.
// Database ACL isolation remains in force for subsequent installations.
func Deprovision(ctx context.Context, admin *pgx.Conn, namespace string) error {
	tx, err := admin.Begin(ctx)
	if err != nil {
		return ErrDenied
	}
	defer tx.Rollback(ctx)
	if err := resetEpisode(ctx, tx, namespace); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return ErrDenied
	}
	return nil
}

type resetTransaction interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

func resetEpisode(ctx context.Context, tx resetTransaction, namespace string) error {
	var database string
	if namespace == "" || tx.QueryRow(ctx, `SELECT pg_catalog.current_database() FROM k6_auth.episode
		WHERE namespace = $1 FOR UPDATE`, namespace).Scan(&database) != nil {
		return ErrDenied
	}
	for _, role := range []string{ReaderRole, DecisionRole} {
		// Revoke the installation's grants. DROP ROLE refuses unexpected
		// dependencies instead of deleting objects outside this installation.
		if _, err := tx.Exec(ctx, `REVOKE CONNECT ON DATABASE `+pgx.Identifier{database}.Sanitize()+` FROM `+role); err != nil {
			return ErrDenied
		}
		if _, err := tx.Exec(ctx, `REVOKE USAGE ON SCHEMA k6_auth FROM `+role); err != nil {
			return ErrDenied
		}
		if _, err := tx.Exec(ctx, `REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA k6_auth FROM `+role); err != nil {
			return ErrDenied
		}
		if _, err := tx.Exec(ctx, `DROP ROLE `+role); err != nil {
			return ErrDenied
		}
	}
	if tag, err := tx.Exec(ctx, `DELETE FROM k6_auth.episode WHERE namespace = $1`, namespace); err != nil || tag.RowsAffected() != 1 {
		return ErrDenied
	}
	return verifyReset(ctx, tx)
}

func verifyReset(ctx context.Context, db interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}) error {
	var absent bool
	if err := db.QueryRow(ctx, `SELECT NOT EXISTS (SELECT FROM k6_auth.episode)
		AND NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname IN ($1, $2))`,
		ReaderRole, DecisionRole).Scan(&absent); err != nil || !absent {
		return ErrDenied
	}
	return nil
}
