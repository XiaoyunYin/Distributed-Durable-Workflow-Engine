// dur053-migrate is the fail-closed, two-connection pre-deployment Job.
package main

import (
	"context"
	"durable-agent-execution-engine/migrations"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	cfg, err := pgxpool.ParseConfig(os.Getenv("DATABASE_URL"))
	if err != nil {
		return err
	}
	cfg.MaxConns = 2
	cfg.ConnConfig.RuntimeParams = map[string]string{"application_name": "dur053-migrate", "statement_timeout": "120s", "lock_timeout": "2s"}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return err
	}
	defer pool.Close()
	entries, err := migrations.SQL.ReadDir(".")
	if err != nil {
		return err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	for _, entry := range entries {
		name := entry.Name()
		version, err := strconv.Atoi(strings.SplitN(name, "_", 2)[0])
		if err != nil {
			return err
		}
		var present bool
		if err = pool.QueryRow(ctx, `SELECT to_regclass('engine.schema_migrations') IS NOT NULL`).Scan(&present); err != nil {
			return err
		}
		applied := false
		if present {
			if err = pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM engine.schema_migrations WHERE version=$1)`, version).Scan(&applied); err != nil {
				return err
			}
		}
		if applied {
			continue
		}
		sql, err := migrations.SQL.ReadFile(name)
		if err != nil {
			return err
		}
		conn, err := pool.Acquire(ctx)
		if err != nil {
			return err
		}
		_, err = conn.Conn().PgConn().Exec(ctx, string(sql)).ReadAll()
		conn.Release()
		if err != nil {
			return fmt.Errorf("migration %s: %w", name, err)
		}
		if err = pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM engine.schema_migrations WHERE version=$1)`, version).Scan(&applied); err != nil || !applied {
			return fmt.Errorf("migration %s ledger missing: %v", name, err)
		}
		fmt.Println("applied", name)
	}
	return nil
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
