//go:build dur053_k6

package k6sink

import (
	"log"
	"net/http"
	"time"

	"durable-agent-execution-engine/internal/k6hook"
)

func (h *handler) afterCommit(r *http.Request, request Request, receipt Receipt, binding, reply string) {
	if h.cfg.Hook == nil {
		return
	}
	// execute has returned after committing state, receipt, and the call row.
	// Keep the response buffered in this goroutine. The DB timeout is unchanged.
	hold, err := h.cfg.Hook.Hit("sink-post-commit", k6hook.Fields{
		Key: request.LogicalKey, Invocation: r.Header.Get("X-Dur053-Invocation"), Request: request.RequestID,
		Intent: request.IntentID, Resource: request.ResourceID, Scope: receipt.ScopeHash,
		Binding: binding, Reply: reply, App: receipt.ApplicationID,
	})
	if err != nil || hold != nil && (hold.Action() != "release" || hold.Finish() != nil) {
		log.Print("K6_SINK_HOOK_FAILED")
	}
}

func ResponseTimeout(hook k6hook.Client) time.Duration {
	if hook != nil {
		return 65 * time.Second
	}
	return 12 * time.Second
}
