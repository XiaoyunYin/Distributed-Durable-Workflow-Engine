package main

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

type clockBracket struct {
	ObserverBefore time.Time `json:"observer_before"`
	Database       time.Time `json:"database"`
	ObserverAfter  time.Time `json:"observer_after"`
}

func bracketClock(query func() (time.Time, error)) (clockBracket, error) {
	// Round outward to microseconds: PostgreSQL timestamps and the offline
	// datetime reader use microseconds, so truncation must not narrow a bracket.
	before := time.Now().UTC().Truncate(time.Microsecond)
	database, err := query()
	after := time.Now().UTC().Add(time.Microsecond - time.Nanosecond).Truncate(time.Microsecond)
	return clockBracket{before, database.UTC(), after}, err
}

// Runs inside the observer pod. Connection setup and kubectl transport are
// outside the bracket; its width bounds the actual SELECT, not exec RTT.
func databaseClockProbe(url string) (clockBracket, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		return clockBracket{}, err
	}
	defer conn.Close(ctx)
	return bracketClock(func() (time.Time, error) {
		var value time.Time
		err := conn.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&value)
		return value, err
	})
}
