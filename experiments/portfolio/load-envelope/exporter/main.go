package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

const interval = time.Second
const statementTimeout = "2000"

var metrics = [...]string{
	"outstanding", "unpublished", "oldest_claimable_seconds", "admissions", "terminals",
	"db_wait_lock", "db_wait_lwlock", "db_wait_io", "db_wait_ipc", "db_wait_client",
	"db_wait_timeout", "db_wait_other",
}

type sample struct {
	at     time.Time
	values [len(metrics)]float64
}

type sampler struct {
	latest atomic.Pointer[sample]
}

func (s *sampler) collect(ctx context.Context, read func(context.Context) (*sample, error)) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	value, err := read(ctx)
	if err != nil {
		log.Print("monitor query failed")
		s.latest.Store(nil)
		return
	}
	s.latest.Store(value)
}

func (s *sampler) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	value := s.latest.Load()
	if value == nil || time.Since(value.at) > 5*interval {
		fmt.Fprintln(w, "j2_query_ok 0")
		return
	}
	fmt.Fprintln(w, "j2_query_ok 1")
	fmt.Fprintf(w, "j2_sample_timestamp_seconds %.6f\n", float64(value.at.UnixMicro())/1e6)
	for i, name := range metrics {
		fmt.Fprintf(w, "j2_%s %g\n", name, value.values[i])
	}
}

func readSample(ctx context.Context, db *sql.DB) (*sample, error) {
	const query = `SELECT sampled_at, outstanding, unpublished, oldest_claimable_seconds,
admissions, terminals, wait_lock, wait_lwlock, wait_io, wait_ipc, wait_client, wait_timeout, wait_other
FROM j2_monitor.sample()`
	value := new(sample)
	fields := []any{&value.at}
	for i := range value.values {
		fields = append(fields, &value.values[i])
	}
	return value, db.QueryRowContext(ctx, query).Scan(fields...)
}

func database(dsn string) (*sql.DB, error) {
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	config.RuntimeParams["application_name"] = "j2-db-exporter"
	config.RuntimeParams["statement_timeout"] = statementTimeout
	config.RuntimeParams["default_transaction_read_only"] = "on"
	db := stdlib.OpenDB(*config)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return db, nil
}

func main() {
	db, err := database(os.Getenv("J2_MONITOR_DSN"))
	if err != nil {
		log.Fatal("invalid monitor DSN")
	}
	defer db.Close()
	s := new(sampler)
	go func() {
		tick := time.NewTicker(interval)
		defer tick.Stop()
		for range tick.C {
			// The ticker coalesces ticks while a query runs. Scrapes only read the cache.
			s.collect(context.Background(), func(ctx context.Context) (*sample, error) { return readSample(ctx, db) })
		}
	}()
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", s)
	server := http.Server{
		Addr: ":9187", Handler: mux, ReadHeaderTimeout: 2 * time.Second,
		WriteTimeout: 2 * time.Second, IdleTimeout: 5 * time.Second,
	}
	log.Fatal(server.ListenAndServe())
}
