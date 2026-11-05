package main

import (
	"context"
	"durable-agent-execution-engine/migrations"
	"fmt"
	"github.com/jackc/pgx/v5"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestK3MigrateFreshAndIdempotent(t *testing.T) {
	databaseURL := integrationDatabaseURL(t)
	cfg, err := pgx.ParseConfig(databaseURL)
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
	parsed, err := url.Parse(databaseURL)
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

func TestK3IntegrationDatabaseURL(t *testing.T) {
	t.Setenv("DURABLE_RUN_INTEGRATION", "1")
	t.Setenv("DURABLE_DATABASE_URL", "postgresql://explicit:secret@127.0.0.1:9876/test")
	if got := integrationDatabaseURL(t); got != os.Getenv("DURABLE_DATABASE_URL") {
		t.Fatal("explicit integration URL ignored")
	}
	t.Setenv("DURABLE_DATABASE_URL", "")
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, ".env"), []byte("POSTGRES_USER=\"a b\"\nPOSTGRES_PASSWORD=\"p@ss/word\"\nPOSTGRES_DB=fresh\nPOSTGRES_PORT=6543\n"), 0600); err != nil {
		t.Fatal(err)
	}
	nested := filepath.Join(root, "cmd", "migrate")
	if err := os.MkdirAll(nested, 0700); err != nil {
		t.Fatal(err)
	}
	t.Chdir(nested)
	u, err := url.Parse(integrationDatabaseURL(t))
	if err != nil {
		t.Fatal(err)
	}
	password, _ := u.User.Password()
	if u.Host != "127.0.0.1:6543" || u.Path != "/fresh" || u.User.Username() != "a b" || password != "p@ss/word" {
		t.Fatal("repository .env URL resolution changed", u.Redacted())
	}
}

// Keep URL resolution identical to internal/state integration tests.
func integrationDatabaseURL(t *testing.T) string {
	t.Helper()
	if os.Getenv("DURABLE_RUN_INTEGRATION") != "1" && os.Getenv("DURABLE_REQUIRE_DATABASE") != "1" {
		t.Skip("PostgreSQL integration tests require DURABLE_RUN_INTEGRATION=1 or service CI mode")
	}
	if value := os.Getenv("DURABLE_DATABASE_URL"); value != "" {
		return value
	}
	workingDirectory, err := os.Getwd()
	if err != nil {
		integrationUnavailable(t, "cannot locate repository root: "+err.Error())
	}
	envPath := ""
	for directory := workingDirectory; ; directory = filepath.Dir(directory) {
		candidate := filepath.Join(directory, ".env")
		if _, statErr := os.Stat(candidate); statErr == nil {
			envPath = candidate
			break
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			break
		}
	}
	data, err := os.ReadFile(envPath)
	if err != nil {
		integrationUnavailable(t, "set DURABLE_DATABASE_URL or provide .env for PostgreSQL integration tests")
	}
	values := map[string]string{}
	for _, line := range strings.Split(string(data), "\n") {
		parts := strings.SplitN(strings.TrimSpace(line), "=", 2)
		if len(parts) == 2 {
			values[parts[0]] = strings.Trim(parts[1], "\r\"")
		}
	}
	user := values["POSTGRES_USER"]
	database := values["POSTGRES_DB"]
	password := values["POSTGRES_PASSWORD"]
	if user == "" || database == "" || password == "" {
		integrationUnavailable(t, ".env lacks PostgreSQL credentials for integration tests")
	}
	port := values["POSTGRES_PORT"]
	if port == "" {
		port = "5432"
	}
	if _, err := strconv.Atoi(port); err != nil {
		t.Fatalf("invalid POSTGRES_PORT: %v", err)
	}
	return (&url.URL{
		Scheme:   "postgresql",
		User:     url.UserPassword(user, password),
		Host:     "127.0.0.1:" + port,
		Path:     "/" + database,
		RawQuery: "sslmode=disable",
	}).String()
}

func integrationUnavailable(t *testing.T, message string) {
	t.Helper()
	if os.Getenv("DURABLE_REQUIRE_DATABASE") == "1" {
		t.Fatalf("PostgreSQL integration is required: %s", message)
	}
	t.Skip(message)
}
