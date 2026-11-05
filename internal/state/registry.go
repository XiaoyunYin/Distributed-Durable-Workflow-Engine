package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"time"
)

type ActivityVersion struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// RuntimeRegistry is the effective allowlist used by dispatch and image export.
func RuntimeRegistry() []ActivityVersion {
	entries := []ActivityVersion{{"pure.add", "v1"}, {"pure.echo", "v1"}}
	if os.Getenv("DUR048_ALLOW_FIXTURE_ACTIVITY") == "1" {
		entries = append(entries, ActivityVersion{"dur048.sleep", "v1"})
	}
	if os.Getenv("DUR053_ALLOW_FIXTURE_ACTIVITY") == "1" {
		entries = append(entries, ActivityVersion{"dur053.cpu", "v1"})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	return entries
}

func registrySet(entries []ActivityVersion) (map[ActivityVersion]bool, error) {
	if len(entries) == 0 {
		return nil, errors.New("empty activity registry")
	}
	result := map[ActivityVersion]bool{}
	for _, entry := range entries {
		if entry.Name == "" || entry.Version == "" || result[entry] {
			return nil, errors.New("invalid or duplicate activity registry entry")
		}
		result[entry] = true
	}
	return result, nil
}

func CheckRegistryParity(runtime, worker []ActivityVersion) error {
	left, err := registrySet(runtime)
	if err != nil {
		return err
	}
	right, err := registrySet(worker)
	if err != nil {
		return err
	}
	if len(left) != len(right) {
		return errors.New("runtime/worker registry mismatch")
	}
	for entry := range left {
		if !right[entry] {
			return fmt.Errorf("runtime/worker registry mismatch: %s/%s", entry.Name, entry.Version)
		}
	}
	return nil
}

// CheckActivityCompatibility checks the entire immutable definition referenced by
// every non-terminal workflow, including future nodes and paused work. A missing
// version or database/decoding error rejects the candidate. No state is changed.
// The deployment controller must freeze submissions across this check and rollout.
func (s *Store) CheckActivityCompatibility(ctx context.Context, namespace string, candidate []ActivityVersion) error {
	supported, err := registrySet(candidate)
	if err != nil {
		return err
	}
	if namespace == "" {
		return errors.New("compatibility namespace required")
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	rows, err := s.pool.Query(ctx, `SELECT d.activity_versions
        FROM engine.workflow_definitions d
        WHERE EXISTS (SELECT 1 FROM engine.workflow_executions w
          WHERE w.namespace=$1 AND w.definition_id=d.definition_id AND w.definition_version=d.version
          AND w.state NOT IN ('SUCCEEDED','FAILED','REJECTED','CANCELED','ABANDONED'))`, namespace)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return err
		}
		var versions map[string]string
		if err := json.Unmarshal(raw, &versions); err != nil {
			return fmt.Errorf("invalid referenced activity versions: %w", err)
		}
		if versions == nil {
			return errors.New("invalid referenced activity versions")
		}
		for name, version := range versions {
			if !supported[ActivityVersion{name, version}] {
				return fmt.Errorf("referenced activity unsupported: %s/%s", name, version)
			}
		}
	}
	return rows.Err()
}
