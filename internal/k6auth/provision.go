package k6auth

import (
	"context"
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

type Credentials struct {
	ReaderPassword, DecisionPassword string
}

// Provision installs one episode on a dedicated DB cluster. PostgreSQL has no
// per-role DENY ACL, so PUBLIC CONNECT must be removed from every other
// connectable database in this cluster. The installation refuses role reuse.
// engineRoles identifies deployed roles whose preexisting CONNECT access must
// become explicit before PUBLIC is revoked. Setup retains credentials by role.
func Provision(ctx context.Context, admin *pgx.Conn, namespace string, cutoff time.Time, uncertainty time.Duration, engineRoles ...string) (Credentials, error) {
	if namespace == "" || cutoff.IsZero() || uncertainty < 0 || uncertainty%time.Microsecond != 0 || cutoff.Nanosecond()%1000 != 0 {
		return Credentials{}, ErrDenied
	}
	tx, err := admin.Begin(ctx)
	if err != nil {
		return Credentials{}, ErrDenied
	}
	defer tx.Rollback(ctx)
	var database string
	if err := tx.QueryRow(ctx, `SELECT pg_catalog.current_database()`).Scan(&database); err != nil {
		return Credentials{}, ErrDenied
	}
	rows, err := tx.Query(ctx, `SELECT datname FROM pg_catalog.pg_database WHERE datallowconn AND datname <> $1 ORDER BY datname`, database)
	if err != nil {
		return Credentials{}, ErrDenied
	}
	others, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return Credentials{}, ErrDenied
	}
	for _, role := range engineRoles {
		if role == "" || role == ReaderRole || role == DecisionRole || role == "public" {
			return Credentials{}, ErrDenied
		}
		// Preserve existing effective access, including access through PUBLIC.
		// Do not give an engine role access to a database it could not use.
		for _, name := range append(others, database) {
			var allowed bool
			if err := tx.QueryRow(ctx, `SELECT pg_catalog.has_database_privilege($1::name, $2::text, 'CONNECT')`, role, name).Scan(&allowed); err != nil {
				return Credentials{}, ErrDenied
			}
			if !allowed {
				if name == database {
					return Credentials{}, ErrDenied
				}
				continue
			}
			if _, err := tx.Exec(ctx, `GRANT CONNECT ON DATABASE `+pgx.Identifier{name}.Sanitize()+` TO `+pgx.Identifier{role}.Sanitize()); err != nil {
				return Credentials{}, ErrDenied
			}
		}
	}
	for _, other := range others {
		if _, err := tx.Exec(ctx, `REVOKE CONNECT ON DATABASE `+pgx.Identifier{other}.Sanitize()+` FROM PUBLIC`); err != nil {
			return Credentials{}, ErrDenied
		}
	}
	database = pgx.Identifier{database}.Sanitize()
	if _, err := tx.Exec(ctx, `REVOKE ALL ON DATABASE `+database+` FROM PUBLIC`); err != nil {
		return Credentials{}, ErrDenied
	}
	if _, err := tx.Exec(ctx, `REVOKE ALL ON SCHEMA public FROM PUBLIC`); err != nil {
		return Credentials{}, ErrDenied
	}
	if _, err := tx.Exec(ctx, `INSERT INTO k6_auth.episode (namespace, approver_subject, valid_until, uncertainty_us)
		VALUES ($1, $2, $3, $4)`, namespace, (Episode{Namespace: namespace}).subject(), cutoff, uncertainty.Microseconds()); err != nil {
		return Credentials{}, ErrDenied
	}
	var result Credentials
	roles := []struct {
		role, function string
		password       *string
	}{
		{ReaderRole, "k6_auth.read_exact_grant(uuid, text, text, text, integer, text, text, text)", &result.ReaderPassword},
		{DecisionRole, "k6_auth.approval_decision(uuid, text, text, integer, text, text, text)", &result.DecisionPassword},
	}
	for _, role := range roles {
		*role.password = rand.Text()
		verifier, err := scramVerifier(*role.password)
		if err != nil {
			return Credentials{}, ErrDenied
		}
		// Only the SCRAM verifier enters DDL. Its fixed alphabet cannot close the literal.
		if _, err := tx.Exec(ctx, `CREATE ROLE `+role.role+` LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '`+verifier+`'`); err != nil {
			return Credentials{}, ErrDenied
		}
		for _, statement := range []string{
			`ALTER ROLE ` + role.role + ` SET log_parameter_max_length = 0`,
			`ALTER ROLE ` + role.role + ` SET log_parameter_max_length_on_error = 0`,
			`GRANT CONNECT ON DATABASE ` + database + ` TO ` + role.role,
			`GRANT USAGE ON SCHEMA k6_auth TO ` + role.role,
			`GRANT EXECUTE ON FUNCTION ` + role.function + ` TO ` + role.role,
		} {
			if _, err := tx.Exec(ctx, statement); err != nil {
				return Credentials{}, ErrDenied
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return Credentials{}, ErrDenied
	}
	return result, nil
}

func scramVerifier(password string) (string, error) {
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}
	const iterations = 4096
	salted, err := pbkdf2.Key(sha256.New, password, salt, iterations, sha256.Size)
	if err != nil {
		return "", err
	}
	client := hmac.New(sha256.New, salted)
	client.Write([]byte("Client Key"))
	stored := sha256.Sum256(client.Sum(nil))
	server := hmac.New(sha256.New, salted)
	server.Write([]byte("Server Key"))
	encode := base64.StdEncoding.EncodeToString
	return fmt.Sprintf("SCRAM-SHA-256$%d:%s$%s:%s", iterations, encode(salt), encode(stored[:]), encode(server.Sum(nil))), nil
}
