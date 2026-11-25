package main

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMetricsAreMissingOnQueryError(t *testing.T) {
	s := new(sampler)
	read := func() string {
		record := httptest.NewRecorder()
		s.ServeHTTP(record, nil)
		return record.Body.String()
	}
	if got := read(); got != "j2_query_ok 0\n" {
		t.Fatalf("unexpected bytes before first sample: %s", got)
	}
	s.collect(context.Background(), func(context.Context) (*sample, error) { return &sample{at: time.Now()}, nil })
	if got := read(); !strings.Contains(got, "j2_query_ok 1\n") || !strings.Contains(got, "j2_unpublished 0\n") {
		t.Fatalf("missing successful sample: %s", got)
	}
	s.collect(context.Background(), func(context.Context) (*sample, error) { return nil, errors.New("timeout") })
	if got := read(); got != "j2_query_ok 0\n" {
		t.Fatalf("stale successful sample after error: %s", got)
	}
}

func TestStaleSampleAndPoolLimit(t *testing.T) {
	s := new(sampler)
	s.latest.Store(&sample{at: time.Now().Add(-6 * interval)})
	record := httptest.NewRecorder()
	s.ServeHTTP(record, nil)
	if got := record.Body.String(); got != "j2_query_ok 0\n" {
		t.Fatalf("emitted an expired sample: %s", got)
	}
	db, err := database("postgresql://test@127.0.0.1/test?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if n := db.Stats().MaxOpenConnections; n != 1 {
		t.Fatalf("monitor pool limit %d exceeds its one-slot budget", n)
	}
}
