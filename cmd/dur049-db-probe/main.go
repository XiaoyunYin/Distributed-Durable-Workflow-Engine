// dur049-db-probe is a campaign-only PostgreSQL connectivity check. It runs
// inside the runtime container so a network-isolation arm can verify the
// runtime's actual route to PostgreSQL, rather than inferring it from a peer
// worker container.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"os"
	"time"

	"durable-agent-execution-engine/internal/state"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type result struct {
	Status     string `json:"status"`
	ObservedAt string `json:"observed_at_utc"`
	SQLState   string `json:"sqlstate,omitempty"`
}

func main() {
	timeout := flag.Duration("timeout", 3*time.Second, "maximum PostgreSQL connectivity probe duration")
	partitionID := flag.Int("partition-id", -1, "optionally probe whether a partition lease row is locked")
	flag.Parse()
	if *timeout <= 0 || *partitionID < -1 || *partitionID >= 16 {
		write(result{Status: "invalid_arguments", ObservedAt: time.Now().UTC().Format(time.RFC3339Nano)})
		os.Exit(2)
	}
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		write(result{Status: "database_url_missing", ObservedAt: time.Now().UTC().Format(time.RFC3339Nano)})
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	store, err := state.NewFromURL(ctx, databaseURL)
	if err != nil {
		write(result{Status: "unreachable", ObservedAt: time.Now().UTC().Format(time.RFC3339Nano)})
		os.Exit(1)
	}
	defer store.Close()
	if *partitionID >= 0 {
		tx, err := store.Pool().Begin(ctx)
		if err != nil {
			write(result{Status: "probe_error", ObservedAt: time.Now().UTC().Format(time.RFC3339Nano), SQLState: sqlState(err)})
			os.Exit(1)
		}
		var epoch int64
		err = tx.QueryRow(ctx, `SELECT epoch FROM engine.partition_leases WHERE partition_id = $1 FOR UPDATE NOWAIT`, *partitionID).Scan(&epoch)
		_ = tx.Rollback(context.Background())
		status, stateCode := classifyLockProbe(err)
		write(result{Status: status, ObservedAt: time.Now().UTC().Format(time.RFC3339Nano), SQLState: stateCode})
		if status != "row_lock_held" && status != "row_lock_available" {
			os.Exit(1)
		}
		return
	}
	write(result{Status: "reachable", ObservedAt: time.Now().UTC().Format(time.RFC3339Nano)})
}

func classifyLockProbe(err error) (status, stateCode string) {
	if err == nil {
		return "row_lock_available", ""
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if pgErr.Code == "55P03" {
			return "row_lock_held", pgErr.Code
		}
		return "probe_error", pgErr.Code
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return "partition_row_missing", ""
	}
	return "probe_error", ""
}

func sqlState(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

func write(value result) {
	_ = json.NewEncoder(os.Stdout).Encode(value)
}
