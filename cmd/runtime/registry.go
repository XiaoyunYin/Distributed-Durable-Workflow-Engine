package main

import (
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
	namespace := flags.String("namespace", os.Getenv("RUNTIME_NAMESPACE"), "engine namespace being rolled out")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if *workerPath == "" || *namespace == "" || os.Getenv("DATABASE_URL") == "" {
		return errors.New("worker-registry, namespace and DATABASE_URL required")
	}
	file, err := os.Open(*workerPath)
	if err != nil {
		return err
	}
	defer file.Close()
	decoder := json.NewDecoder(io.LimitReader(file, 1<<20))
	decoder.DisallowUnknownFields()
	var worker []state.ActivityVersion
	if err := decoder.Decode(&worker); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return errors.New("trailing registry data")
	}
	runtime := state.RuntimeRegistry()
	if err := state.CheckRegistryParity(runtime, worker); err != nil {
		return err
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
