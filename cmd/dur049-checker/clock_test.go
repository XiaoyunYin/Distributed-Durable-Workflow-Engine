package main

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestObserverClockBracket(t *testing.T) {
	probe, err := bracketClock(func() (time.Time, error) { return time.Now().UTC(), nil })
	if err != nil || probe.Database.Before(probe.ObserverBefore) || probe.Database.After(probe.ObserverAfter) {
		t.Fatalf("observer bracket must enclose database query: %+v %v", probe, err)
	}
	if probe.ObserverBefore.Nanosecond()%1000 != 0 || probe.ObserverAfter.Nanosecond()%1000 != 0 {
		t.Fatal("observer bounds must preserve width at offline microsecond precision")
	}
	want := errors.New("database unavailable")
	if _, err := bracketClock(func() (time.Time, error) { return time.Time{}, want }); !errors.Is(err, want) {
		t.Fatalf("clock query failure must propagate: %v", err)
	}
}

func TestDatabaseClockProbe(t *testing.T) {
	if os.Getenv("DURABLE_RUN_INTEGRATION") != "1" {
		t.Skip("set DURABLE_RUN_INTEGRATION=1 for PostgreSQL probe")
	}
	url := os.Getenv("DURABLE_DATABASE_URL")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	var databaseBefore, databaseAfter time.Time
	if err := conn.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&databaseBefore); err != nil {
		t.Fatal(err)
	}
	probe, err := databaseClockProbe(url)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&databaseAfter); err != nil {
		t.Fatal(err)
	}
	// Native Windows and Docker's Linux VM do not share a kernel clock. Prove
	// the query's DB-clock source independently, without inventing an offset or
	// relaxing the pilot's strict shared-kernel bracket check in summarize().
	if probe.Database.Before(databaseBefore) || probe.Database.After(databaseAfter) || probe.ObserverAfter.Before(probe.ObserverBefore) {
		t.Fatalf("clock query source/order: %+v DB fences %s..%s", probe, databaseBefore, databaseAfter)
	}
	if _, err := databaseClockProbe("postgresql://invalid@127.0.0.1:1/invalid?sslmode=disable"); err == nil {
		t.Fatal("unavailable database clock must fail")
	}
}

func TestClockCapturePreservesForeignClock(t *testing.T) {
	foreign := time.Unix(1, 0).UTC()
	probe, err := bracketClock(func() (time.Time, error) { return foreign, nil })
	if err != nil || !probe.Database.Equal(foreign) {
		t.Fatal("clock capture must preserve DB evidence, never clamp it to observer time")
	}
}
