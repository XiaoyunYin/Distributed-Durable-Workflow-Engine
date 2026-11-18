package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"durable-agent-execution-engine/internal/state"
)

// Both commands are available in the candidate runtime image (including scratch).
func registryCommand(args []string, output io.Writer) error {
	if len(args) == 0 {
		return errors.New("registry command required")
	}
	if args[0] == "activity-registry" {
		return json.NewEncoder(output).Encode(state.RuntimeRegistry())
	}
	flags := flag.NewFlagSet("compatibility-check", flag.ContinueOnError)
	workerPath := flags.String("worker-registry", "", "JSON manifest exported by the candidate worker image")
	freshWorkers := flags.Bool("no-surviving-workers", false, "rollout has no surviving consumers")
	var survivors []string
	flags.Func("surviving-worker-registry", "exact image export for each surviving worker lineage (repeatable)", func(path string) error {
		survivors = append(survivors, path)
		return nil
	})
	namespace := flags.String("namespace", os.Getenv("RUNTIME_NAMESPACE"), "engine namespace being rolled out")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if *workerPath == "" || *namespace == "" || os.Getenv("DATABASE_URL") == "" {
		return errors.New("worker-registry, namespace and DATABASE_URL required")
	}
	runtime := state.RuntimeRegistry()
	worker, err := readWorkerRegistry(*workerPath)
	if err != nil {
		return err
	}
	if err := state.CheckRegistryParity(runtime, worker); err != nil {
		return err
	}
	if (*freshWorkers && len(survivors) > 0) || (state.K6Enabled() && !*freshWorkers && len(survivors) == 0) {
		return errors.New("K6 compatibility requires a surviving-worker inventory or a fresh consumer group")
	}
	for _, path := range survivors {
		lineage, err := readWorkerRegistry(path)
		if err != nil {
			return err
		}
		if err := state.CheckRegistryParity(runtime, lineage); err != nil {
			return fmt.Errorf("surviving consumer registry: %w", err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store, err := state.NewDemandFromURL(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		return err
	}
	defer store.Close()
	if err := store.CheckActivityCompatibility(ctx, *namespace, runtime); err != nil {
		return err
	}
	_, err = fmt.Fprintln(output, "activity compatibility PASS")
	return err
}

func readWorkerRegistry(path string) ([]state.ActivityVersion, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	const limit = 1 << 20
	body, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil || len(body) > limit {
		return nil, errors.New("could not read worker registry")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var worker []state.ActivityVersion
	if err := decoder.Decode(&worker); err != nil {
		return nil, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, errors.New("trailing registry data")
	}
	return worker, nil
}
