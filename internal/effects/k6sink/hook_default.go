//go:build !dur053_k6

package k6sink

import (
	"net/http"
	"time"

	"durable-agent-execution-engine/internal/k6hook"
)

func (h *handler) afterCommit(*http.Request, Request, Receipt, string, string) {}

func ResponseTimeout(k6hook.Client) time.Duration { return 12 * time.Second }
