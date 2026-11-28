package main

import (
	"errors"
	"os"

	"durable-agent-execution-engine/internal/api"
)

func publicAPIAuthFromEnvironment() (bool, error) {
	value, present := os.LookupEnv("RUNTIME_PUBLIC_API_AUTH")
	if !present || value == "0" {
		return false, nil
	}
	if value == "1" {
		return true, nil
	}
	return false, errors.New("RUNTIME_PUBLIC_API_AUTH must be unset, 0, or 1")
}

func validateJ12Credentials(routes api.RouteSet, reviewer *api.TokenReviewer, demand *api.DemandBearerAuthenticator) error {
	switch routes {
	case api.RoutesCombined:
		return errors.New("RUNTIME_PUBLIC_API_AUTH=1 rejects combined HTTP roles. Use split listeners")
	case api.RoutesOperator, api.RoutesWorkerControl:
		if reviewer == nil {
			return errors.New("J12 operator and worker listeners require Kubernetes TokenReview")
		}
	case api.RoutesDemand:
		if demand == nil {
			return errors.New("J12 demand listener requires its dedicated bearer credential")
		}
		return demand.ValidateJ12Secret()
	}
	return nil
}
