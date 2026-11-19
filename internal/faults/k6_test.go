//go:build dur053_k6

package faults

import (
	"errors"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/k6hook"
)

type deployedHit func(string, k6hook.Fields) (k6hook.Hold, error)

func (hit deployedHit) Hit(boundary string, fields k6hook.Fields) (k6hook.Hold, error) {
	return hit(boundary, fields)
}

type deployedHold struct {
	action   string
	finished bool
}

func (h *deployedHold) Action() string      { return h.action }
func (h *deployedHold) Deadline() time.Time { return time.Time{} }
func (h *deployedHold) Finish() error       { h.finished = true; return nil }

func TestDeployedHookAcceptsOnlyConfirmedRelease(t *testing.T) {
	fields := k6hook.Fields{Key: "workflow"}
	t.Run("release", func(t *testing.T) {
		hold := &deployedHold{action: "release"}
		hit := deployedHit(func(boundary string, got k6hook.Fields) (k6hook.Hold, error) {
			if boundary != "before_publish" || got.Key != fields.Key {
				t.Fatalf("boundary %s fields %+v", boundary, got)
			}
			return hold, nil
		})
		if err := HoldK6(hit, "before_publish", fields); err != nil || !hold.finished {
			t.Fatalf("confirmed release should finish: %v", err)
		}
	})
	t.Run("wrong command", func(t *testing.T) {
		hold := &deployedHold{action: "duplicate"}
		hit := deployedHit(func(string, k6hook.Fields) (k6hook.Hold, error) { return hold, nil })
		if err := HoldK6(hit, "before_publish", fields); err == nil || hold.finished {
			t.Fatalf("wrong command passed or issued a release: %v", err)
		}
	})
	t.Run("ack failure", func(t *testing.T) {
		refused := errors.New("mismatched acknowledgment")
		hit := deployedHit(func(string, k6hook.Fields) (k6hook.Hold, error) { return nil, refused })
		if err := HoldK6(hit, "before_publish", fields); !errors.Is(err, refused) {
			t.Fatalf("ACK failure was lost: %v", err)
		}
	})
}
