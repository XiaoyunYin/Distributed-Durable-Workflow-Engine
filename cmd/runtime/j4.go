package main

import (
	"errors"

	"durable-agent-execution-engine/internal/api"
	"durable-agent-execution-engine/internal/state"
)

func validateJ4Listener(routes api.RouteSet, workerAddr string, fixedDelay bool) error {
	if fixedDelay && !state.ProviderResilienceEnabled() {
		return errors.New("J4 B5 control requires provider resilience")
	}
	if workerAddr == "" {
		if state.ProviderResilienceEnabled() {
			return errors.New("J4 provider resilience requires a private worker listener")
		}
		return nil
	}
	if !state.RecordedLLMEnabled() || state.K6Enabled() || routes != api.RoutesPublic {
		return errors.New("J4 worker listener requires the J4 activity and a separate public API")
	}
	return nil
}
