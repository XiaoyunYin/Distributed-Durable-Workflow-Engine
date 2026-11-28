package main

import (
	"fmt"
	"os"
)

const (
	relayFullBatchLoopEnv = "RUNTIME_RELAY_FULL_BATCH_LOOP"
	j9aStageTimingsEnv    = "RUNTIME_J9A_STAGE_TIMINGS"
)

type runtimeRelayConfig struct {
	fullBatchLoop bool
	stageTimings  bool
}

func runtimeRelayConfigFromEnvironment() (runtimeRelayConfig, error) {
	readSwitch := func(name, value string, present bool) (bool, error) {
		switch {
		case !present || value == "0":
			return false, nil
		case value == "1":
			return true, nil
		default:
			return false, fmt.Errorf("%s must be 0 or 1 when set, got %q", name, value)
		}
	}
	relayValue, relayPresent := os.LookupEnv(relayFullBatchLoopEnv)
	fullBatchLoop, err := readSwitch(relayFullBatchLoopEnv, relayValue, relayPresent)
	if err != nil {
		return runtimeRelayConfig{}, err
	}
	timingValue, timingPresent := os.LookupEnv(j9aStageTimingsEnv)
	stageTimings, err := readSwitch(j9aStageTimingsEnv, timingValue, timingPresent)
	if err != nil {
		return runtimeRelayConfig{}, err
	}
	return runtimeRelayConfig{fullBatchLoop: fullBatchLoop, stageTimings: stageTimings}, nil
}
