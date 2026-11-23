//go:build dur053_k6

package state

import (
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestK6CaptureGate(t *testing.T) {
	for _, tc := range []struct {
		name, k4, capture string
		want              bool
	}{
		{"OutsideK4Campaign", "0", "1", false},
		{"K4Only", "1", "", false},
		{"K6Episode", "1", "1", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("DUR053_K4_CAMPAIGN", tc.k4)
			t.Setenv("DUR053_K6_CAPTURE", tc.capture)
			t.Setenv("POD_UID", "pod")
			t.Setenv("CONTAINER_ID", "container")
			t.Setenv("DUR053_K4_COMMIT_TIMING", "0")
			cfg, err := pgxpool.ParseConfig("postgres://user@127.0.0.1/database?sslmode=disable")
			if err != nil {
				t.Fatal(err)
			}
			if err := configureK4Audit(cfg); err != nil {
				t.Fatal(err)
			}
			if got := captureEnabled(cfg.ConnConfig); got != tc.want {
				t.Fatalf("capture enabled %t, want %t", got, tc.want)
			}
		})
	}
}
