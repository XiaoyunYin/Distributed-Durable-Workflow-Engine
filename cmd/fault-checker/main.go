// Command fault-checker validates a fault-trace.v1 file using the independent
// checker and, when a local database is available, loads the durable rows
// referenced by explicit durable_* trace fields before checking the join.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"durable-agent-execution-engine/internal/invariants"
	"durable-agent-execution-engine/internal/state"
)

func main() {
	path := flag.String("trace", "", "fault-trace.v1 JSONL path")
	databaseURL := flag.String("database-url", "", "PostgreSQL URL used to load durable rows referenced by the trace")
	durableTracePath := flag.String("durable-trace", "", "JSON snapshot path for the durable rows joined to the fault trace")
	offline := flag.Bool("offline", false, "check only the durable-trace snapshot; do not connect to PostgreSQL")
	flag.Parse()
	if *path == "" {
		fmt.Fprintln(os.Stderr, "-trace is required")
		os.Exit(2)
	}
	evidence, err := invariants.LoadFaultTrace(*path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load fault trace: %v\n", err)
		os.Exit(1)
	}
	trace := invariants.Trace{}
	if *offline {
		if *durableTracePath == "" {
			fmt.Fprintln(os.Stderr, "-durable-trace is required with -offline")
			os.Exit(2)
		}
		data, readErr := os.ReadFile(*durableTracePath)
		if readErr != nil {
			fmt.Fprintf(os.Stderr, "load durable trace snapshot: %v\n", readErr)
			os.Exit(1)
		}
		if err := json.Unmarshal(data, &trace); err != nil {
			fmt.Fprintf(os.Stderr, "decode durable trace snapshot: %v\n", err)
			os.Exit(1)
		}
	} else {
		if *databaseURL == "" {
			*databaseURL = durableDatabaseURL()
		}
		if *databaseURL != "" {
			store, storeErr := state.NewFromURL(context.Background(), *databaseURL)
			if storeErr != nil {
				fmt.Fprintf(os.Stderr, "open durable state: %v\n", storeErr)
				os.Exit(1)
			}
			workflowIDs := make([]string, 0, len(evidence))
			seen := map[string]bool{}
			for _, item := range evidence {
				if item.WorkflowID != "" && !seen[item.WorkflowID] {
					seen[item.WorkflowID] = true
					workflowIDs = append(workflowIDs, item.WorkflowID)
				}
			}
			if len(workflowIDs) > 0 {
				trace, err = invariants.Load(context.Background(), store, workflowIDs)
				if err != nil {
					store.Close()
					fmt.Fprintf(os.Stderr, "load durable trace: %v\n", err)
					os.Exit(1)
				}
			}
			store.Close()
		}
	}
	if *durableTracePath != "" && !*offline {
		data, marshalErr := json.MarshalIndent(trace, "", "  ")
		if marshalErr != nil {
			fmt.Fprintf(os.Stderr, "encode durable trace snapshot: %v\n", marshalErr)
			os.Exit(1)
		}
		if mkdirErr := os.MkdirAll(filepath.Dir(*durableTracePath), 0o755); mkdirErr != nil {
			fmt.Fprintf(os.Stderr, "create durable trace directory: %v\n", mkdirErr)
			os.Exit(1)
		}
		if writeErr := os.WriteFile(*durableTracePath, data, 0o644); writeErr != nil {
			fmt.Fprintf(os.Stderr, "write durable trace snapshot: %v\n", writeErr)
			os.Exit(1)
		}
	}
	verdict := invariants.CheckWithFaults(trace, evidence)
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{
		"valid":      verdict.Valid,
		"runs":       len(evidence),
		"violations": verdict.Violations,
	})
	if !verdict.Valid {
		os.Exit(1)
	}
}

func durableDatabaseURL() string {
	for _, name := range []string{"DURABLE_DATABASE_URL", "DATABASE_URL"} {
		if value := os.Getenv(name); value != "" {
			return value
		}
	}
	working, err := os.Getwd()
	if err != nil {
		return ""
	}
	for directory := working; ; directory = filepath.Dir(directory) {
		data, readErr := os.ReadFile(filepath.Join(directory, ".env"))
		if readErr == nil {
			values := map[string]string{}
			for _, line := range strings.Split(string(data), "\n") {
				parts := strings.SplitN(strings.TrimSpace(line), "=", 2)
				if len(parts) == 2 {
					values[parts[0]] = strings.Trim(parts[1], "\r\"")
				}
			}
			port := values["POSTGRES_PORT"]
			if port == "" {
				port = "5432"
			}
			return (&url.URL{Scheme: "postgresql", User: url.UserPassword(values["POSTGRES_USER"], values["POSTGRES_PASSWORD"]),
				Host: "127.0.0.1:" + port, Path: "/" + values["POSTGRES_DB"], RawQuery: "sslmode=disable"}).String()
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			break
		}
	}
	return ""
}
