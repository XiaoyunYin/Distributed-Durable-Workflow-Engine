package main

import (
	"context"
	"durable-agent-execution-engine/internal/dur053"
	"durable-agent-execution-engine/internal/state"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"
)

func main() {
	var c dur053.Config
	flag.StringVar(&c.BearerTokenFile, "token-file", os.Getenv("DUR053_CLIENT_TOKEN_FILE"), "rotating pod-bound client credential")
	flag.StringVar(&c.URL, "url", "http://127.0.0.1:8080", "submission URL")
	flag.StringVar(&c.Namespace, "namespace", "", "fresh campaign namespace")
	flag.StringVar(&c.RunID, "run-id", "", "immutable run ID")
	flag.StringVar(&c.Arm, "arm", "short", "short, medium, long or cpu")
	flag.Int64Var(&c.Seed, "seed", 530001, "registered episode seed")
	flag.Float64Var(&c.Rate, "rate", 1, "absolute scheduled arrivals per second")
	flag.IntVar(&c.Count, "count", 10, "offered arrivals (includes all rejected/uncertain)")
	flag.IntVar(&c.Concurrency, "concurrency", 16, "bounded in-flight HTTP requests, no queue")
	flag.IntVar(&c.CPUUnits, "cpu-units", 10000, "bounded deterministic CPU work units")
	flag.DurationVar(&c.Timeout, "request-timeout", 10*time.Second, "HTTP response deadline")
	output := flag.String("output", "", "new arrival manifest path")
	db := flag.String("database-url", os.Getenv("DATABASE_URL"), "fixture definition database URL")
	flag.Parse()
	if *output == "" {
		fatal(fmt.Errorf("output required"))
	}
	plan, err := dur053.Plan(c)
	if err != nil {
		fatal(err)
	}
	f, err := os.OpenFile(*output, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		fatal(err)
	}
	encode := func(value any) {
		b, e := json.MarshalIndent(value, "", "  ")
		if e != nil {
			fatal(e)
		}
		if e = f.Truncate(0); e != nil {
			fatal(e)
		}
		if _, e = f.Seek(0, 0); e != nil {
			fatal(e)
		}
		if _, e = f.Write(append(b, '\n')); e != nil {
			fatal(e)
		}
		if e = f.Sync(); e != nil {
			fatal(e)
		}
	}
	encode(plan)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(float64(c.Count)/c.Rate*float64(time.Second))+2*c.Timeout+30*time.Second)
	defer cancel()
	store, err := state.NewCampaignFromURL(ctx, *db)
	if err != nil {
		fatal(err)
	}
	defer store.Close()
	if err = dur053.Prepare(ctx, store, c); err != nil {
		fatal(err)
	}
	result := dur053.Run(ctx, c, plan)
	encode(result)
	if err = f.Close(); err != nil {
		fatal(err)
	}
	if result.Status != "PASS" {
		os.Exit(1)
	}
}
func fatal(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(2) }
