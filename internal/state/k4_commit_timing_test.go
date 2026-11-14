package state

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestK4CommitTimingBothModes(t *testing.T) {
	for _, mode := range []string{"0", "1"} {
		t.Setenv("DUR053_K4_CAMPAIGN", mode)
		t.Setenv("DUR053_K4_COMMIT_TIMING", "1")
		t.Setenv("POD_UID", "pod")
		t.Setenv("CONTAINER_ID", "cid")
		cfg, _ := pgxpool.ParseConfig("postgres://unused@localhost/unused")
		if err := configureK4Audit(cfg); err != nil {
			t.Fatal(err)
		}
		if cfg.ConnConfig.Tracer == nil {
			t.Fatal("K4 commit timing absent from overhead mode", mode)
		}
		var output bytes.Buffer
		previous := slog.Default()
		slog.SetDefault(slog.New(slog.NewJSONHandler(&output, nil)))
		tracer := cfg.ConnConfig.Tracer
		ctx := tracer.TraceQueryStart(context.Background(), nil, pgx.TraceQueryStartData{SQL: "commit"})
		tracer.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{})
		slog.SetDefault(previous)
		var log struct {
			Data string `json:"data"`
		}
		var row struct {
			Event   string  `json:"event"`
			Seconds float64 `json:"seconds"`
			Success bool    `json:"success"`
		}
		if json.Unmarshal(output.Bytes(), &log) != nil || json.Unmarshal([]byte(log.Data), &row) != nil ||
			row.Event != "k4_commit_timing" || row.Seconds < 0 || !row.Success {
			t.Fatal("K4 commit round trip evidence missing", output.String())
		}
	}
}
