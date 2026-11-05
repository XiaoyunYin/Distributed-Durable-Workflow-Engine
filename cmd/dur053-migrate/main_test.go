package main

import (
	"context"
	"durable-agent-execution-engine/migrations"
	"fmt"
	"github.com/jackc/pgx/v5"
	"net/url"
	"os"
	"testing"
	"time"
)

func TestK3MigrateFreshAndIdempotent(t *testing.T) {
	if os.Getenv("DURABLE_RUN_INTEGRATION") != "1" && os.Getenv("DURABLE_REQUIRE_DATABASE") != "1" {
		t.Skip("requires PostgreSQL")
	}
	cfg, err := pgx.ParseConfig(os.Getenv("DURABLE_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Database = "postgres"
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	admin, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(context.Background())
	name := fmt.Sprintf("dur053_k3a_migrate_%d", time.Now().UnixNano())
	if _, err = admin.Exec(ctx, `CREATE DATABASE `+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		c, done := context.WithTimeout(context.Background(), 10*time.Second)
		defer done()
		if _, err := admin.Exec(c, `DROP DATABASE `+pgx.Identifier{name}.Sanitize()+` WITH (FORCE)`); err != nil {
			t.Error(err)
		}
		var n int
		if err := admin.QueryRow(c, `SELECT count(*) FROM pg_database WHERE datname=$1`, name).Scan(&n); err != nil || n != 0 {
			t.Error("migration database cleanup failed", err)
		}
	}()
	cfg.Database = name
	parsed, err := url.Parse(os.Getenv("DURABLE_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	parsed.Path = "/" + name
	t.Setenv("DATABASE_URL", parsed.String())
	if err := run(); err != nil {
		t.Fatal("fresh migration failed", err)
	}
	if err := run(); err != nil {
		t.Fatal("idempotent migration failed", err)
	}
	entries, err := migrations.SQL.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	var count int
	if err = conn.QueryRow(ctx, `SELECT count(*) FROM engine.schema_migrations`).Scan(&count); err != nil || count != len(entries) {
		t.Fatal("migration ledger incomplete", count, err)
	}
}
