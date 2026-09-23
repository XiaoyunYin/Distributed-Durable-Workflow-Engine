// dur049-db-probe is a campaign-only PostgreSQL connectivity check. It runs
// inside the runtime container so a network-isolation arm can verify the
// runtime's actual route to PostgreSQL, rather than inferring it from a peer
// worker container.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"os"
	"time"

	"durable-agent-execution-engine/internal/state"
)

type result struct {
	Status     string `json:"status"`
	ObservedAt string `json:"observed_at_utc"`
}

func main() {
	timeout := flag.Duration("timeout", 3*time.Second, "maximum PostgreSQL connectivity probe duration")
	flag.Parse()
	if *timeout <= 0 {
		write(result{Status: "invalid_timeout", ObservedAt: time.Now().UTC().Format(time.RFC3339Nano)})
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
	write(result{Status: "reachable", ObservedAt: time.Now().UTC().Format(time.RFC3339Nano)})
}

func write(value result) {
	_ = json.NewEncoder(os.Stdout).Encode(value)
}
