package state

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type k4CommitTracer struct{}
type k4CommitStart struct{}

// Both overhead modes enable this passive tracer. It times the actual pgx
// COMMIT round trip, including deferred audit finalization and lock waiting.
func (k4CommitTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if strings.EqualFold(strings.TrimSpace(data.SQL), "commit") {
		return context.WithValue(ctx, k4CommitStart{}, time.Now())
	}
	return ctx
}
func (k4CommitTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	start, ok := ctx.Value(k4CommitStart{}).(time.Time)
	if !ok {
		return
	}
	row, _ := json.Marshal(map[string]any{"event": "k4_commit_timing", "timestamp": time.Now().UTC(),
		"seconds": time.Since(start).Seconds(), "success": data.Err == nil,
		"session": K4Session, "pod_uid": os.Getenv("POD_UID")})
	slog.Info("k4 commit timing", "data", string(row))
}
