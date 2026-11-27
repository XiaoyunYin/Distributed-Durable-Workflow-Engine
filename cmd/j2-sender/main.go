// Command j2-sender submits the J2 short workload across an adjacent rate step.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"durable-agent-execution-engine/internal/dur053"
)

func main() {
	var periods [2]dur053.J2Segment
	c := dur053.Config{Arm: "short", Concurrency: 16, Timeout: 10 * time.Second, CPUUnits: 10000}
	flag.StringVar(&c.URL, "url", "http://127.0.0.1:8080", "submission API")
	flag.StringVar(&c.Namespace, "namespace", "", "fresh J2 namespace")
	flag.StringVar(&c.RunID, "run-id", "", "immutable episode ID")
	flag.Int64Var(&c.Seed, "seed", 0, "registered episode seed")
	flag.StringVar(&c.BearerTokenFile, "token-file", os.Getenv("DUR053_CLIENT_TOKEN_FILE"), "rotating pod token")
	flag.Float64Var(&periods[0].Rate, "hold-rate", 0, "offered arrivals/s during the hold")
	flag.DurationVar(&periods[0].Duration, "hold-duration", 0, "hold duration including warm-up")
	flag.Float64Var(&periods[1].Rate, "step-rate", 0, "offered arrivals/s during the step")
	flag.DurationVar(&periods[1].Duration, "step-duration", 0, "step duration")
	start := flag.String("start", "", "absolute RFC3339Nano arrival zero")
	output := flag.String("output", "", "new fsynced arrival journal path")
	db := flag.String("database-url", os.Getenv("DATABASE_URL"), "fixture definition database URL")
	flag.Parse()

	if *output == "" || flag.NArg() != 0 {
		fatal(fmt.Errorf("output required and positional arguments are unsupported"))
	}
	result, err := dur053.RunJ2Continuous(context.Background(), c, periods, *db, *output, *start)
	if err != nil {
		fatal(err)
	}
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		fatal(err)
	}
	if result.Status != "PASS" {
		os.Exit(1)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(2)
}
