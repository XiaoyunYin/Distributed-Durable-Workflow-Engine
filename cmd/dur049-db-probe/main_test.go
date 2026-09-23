package main

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestClassifyLockProbe(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		want      string
		wantState string
	}{
		{name: "available", want: "row_lock_available"},
		{name: "held", err: &pgconn.PgError{Code: "55P03"}, want: "row_lock_held", wantState: "55P03"},
		{name: "other postgres error", err: &pgconn.PgError{Code: "42P01"}, want: "probe_error", wantState: "42P01"},
		{name: "non postgres error", err: errors.New("connection lost"), want: "probe_error"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, state := classifyLockProbe(tt.err)
			if got != tt.want || state != tt.wantState {
				t.Fatalf("classifyLockProbe() = (%q, %q), want (%q, %q)", got, state, tt.want, tt.wantState)
			}
		})
	}
}
