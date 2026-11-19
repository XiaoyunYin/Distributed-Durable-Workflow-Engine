//go:build dur053_k6

package faults

import (
	"errors"

	"durable-agent-execution-engine/internal/k6hook"
)

// HoldK6 waits at a deployed boundary. A kill needs no target-side release.
func HoldK6(client k6hook.Client, boundary string, fields k6hook.Fields) error {
	hold, err := client.Hit(boundary, fields)
	if err != nil || hold == nil {
		return err
	}
	if hold.Action() != "release" {
		return errors.New("K6_DEPLOYED_HOOK_COMMAND")
	}
	return hold.Finish()
}
