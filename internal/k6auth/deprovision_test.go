package k6auth

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type resetFixture struct {
	namespace string
	logins    map[string]bool
	verified  bool
}

type resetRow func(...any) error

func (r resetRow) Scan(args ...any) error { return r(args...) }

func (f *resetFixture) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	switch {
	case strings.HasPrefix(sql, "DROP ROLE "):
		delete(f.logins, strings.TrimPrefix(sql, "DROP ROLE "))
	case strings.HasPrefix(sql, "DELETE FROM k6_auth.episode"):
		if args[0] == f.namespace {
			f.namespace = ""
			return pgconn.NewCommandTag("DELETE 1"), nil
		}
		return pgconn.NewCommandTag("DELETE 0"), nil
	}
	return pgconn.CommandTag{}, nil
}

func (f *resetFixture) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	return resetRow(func(dest ...any) error {
		switch {
		case strings.Contains(sql, "FOR UPDATE") && f.namespace == args[0]:
			*dest[0].(*string) = "engine_database"
			return nil
		case strings.Contains(sql, "pg_catalog.pg_roles") && len(args) == 2:
			f.verified = true
			*dest[0].(*bool) = f.namespace == "" && !f.logins[args[0].(string)] && !f.logins[args[1].(string)]
			return nil
		}
		return pgx.ErrNoRows
	})
}

func TestK6DeprovisionRemovesBothLogins(t *testing.T) {
	f := &resetFixture{namespace: "episode", logins: map[string]bool{ReaderRole: true, DecisionRole: true}}
	if err := resetEpisode(context.Background(), f, "foreign"); err == nil || len(f.logins) != 2 {
		t.Fatal("reset accepted the wrong episode")
	}
	if err := resetEpisode(context.Background(), f, "episode"); err != nil || len(f.logins) != 0 ||
		f.namespace != "" || !f.verified {
		t.Fatal("reset must remove and verify both logins and the episode row")
	}
}
