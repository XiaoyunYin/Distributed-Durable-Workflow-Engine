// Package k4observe supplies campaign-only acknowledged observation boundaries.
// It changes no lifecycle, claim, lease, or fencing predicate.
package k4observe

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"time"
)

// Begin records the target-local entry and exit. Optional shared-volume holds
// are bounded to sixty seconds and used identically in both lifecycle arms.
func Begin(ctx context.Context, role, identity, session string, workflows []string) func() {
	if os.Getenv("DUR053_K4_CAMPAIGN") != "1" {
		return func() {}
	}
	emit := func(boundary string) {
		data, _ := json.Marshal(map[string]any{"event": "k4_boundary", "boundary": boundary, "role": role,
			"identity": identity, "session": session, "pod_uid": os.Getenv("POD_UID"),
			"container_id": os.Getenv("CONTAINER_ID"), "timestamp": time.Now().UTC(), "workflows": workflows})
		slog.Info("k4 boundary acknowledgment", "data", string(data))
	}
	emit("enter")
	Wait(ctx)
	return func() { emit("exit") }
}

// Wait extends an existing acknowledged boundary while its hold file exists.
func Wait(ctx context.Context) {
	if os.Getenv("DUR053_K4_CAMPAIGN") != "1" {
		return
	}
	hold := os.Getenv("DUR053_K4_HOLD_FILE")
	deadline := time.Now().Add(60 * time.Second)
	for hold != "" && time.Now().Before(deadline) {
		if _, err := os.Stat(hold); os.IsNotExist(err) {
			break
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(10 * time.Millisecond):
		}
	}
}
