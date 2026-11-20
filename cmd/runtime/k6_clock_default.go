//go:build !dur053_k6

package main

import (
	"context"
	"net/http"

	"durable-agent-execution-engine/internal/engine"
)

func registerK6Clock(*http.ServeMux, string) {}

func configureK6ClockLease(context.Context, *engine.ServeOptions) {}
