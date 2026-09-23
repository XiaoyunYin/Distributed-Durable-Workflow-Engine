// dur049-lock-holder deliberately holds a PostgreSQL lease-row lock so the
// local lock-held recovery arm can observe bounded takeover without weakening
// the production lease fence. It is campaign-only and never an entrypoint.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"durable-agent-execution-engine/internal/state"
)

func main() {
	partition := flag.Int("partition-id", -1, "partition whose lease row should be locked")
	duration := flag.Duration("duration", 45*time.Second, "how long to hold the row lock")
	marker := flag.String("marker", "/tmp/dur049-lock-holder.ready", "file written after the row lock is acquired")
	flag.Parse()
	if *partition < 0 || *partition >= 16 || *duration <= 0 {
		fatal("partition-id must be 0..15 and duration must be positive")
	}
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		fatal("DATABASE_URL is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	store, err := state.NewFromURL(ctx, databaseURL)
	if err != nil {
		fatal(err.Error())
	}
	defer store.Close()
	tx, err := store.Pool().Begin(ctx)
	if err != nil {
		fatal(err.Error())
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	var epoch int64
	if err := tx.QueryRow(ctx, `SELECT epoch FROM engine.partition_leases WHERE partition_id = $1 FOR UPDATE`, *partition).Scan(&epoch); err != nil {
		fatal(err.Error())
	}
	if err := os.WriteFile(*marker, []byte(fmt.Sprintf("partition=%d epoch=%d locked_at=%s\n", *partition, epoch, time.Now().UTC().Format(time.RFC3339Nano))), 0600); err != nil {
		fatal(err.Error())
	}
	timer := time.NewTimer(*duration)
	<-timer.C
	if err := tx.Rollback(context.Background()); err != nil {
		fatal(err.Error())
	}
}

func fatal(message string) {
	fmt.Fprintln(os.Stderr, message)
	os.Exit(1)
}
