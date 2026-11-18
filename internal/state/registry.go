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
	Name    string      `json:"name"`
	Version string      `json:"version"`
	Class   EffectClass `json:"effect_class,omitempty"`
}

const K6Activity = "dur053.cooperating"

func K6Enabled() bool { return os.Getenv("DUR053_K6_ACTIVITY") == "1" }

// RuntimeRegistry is the effective allowlist used by dispatch and image export.
func RuntimeRegistry() []ActivityVersion {
	entries := []ActivityVersion{{"pure.add", "v1", ""}, {"pure.echo", "v1", ""}}
	if os.Getenv("DUR048_ALLOW_FIXTURE_ACTIVITY") == "1" {
		entries = append(entries, ActivityVersion{"dur048.sleep", "v1", ""})
	}
	if os.Getenv("DUR053_ALLOW_FIXTURE_ACTIVITY") == "1" {
		entries = append(entries, ActivityVersion{"dur053.cpu", "v1", ""})
	}
	if K6Enabled() {
		entries = append(entries, ActivityVersion{K6Activity, "v1", EffectCooperating})
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
		if entry.Class == EffectPure {
			entry.Class = ""
		}
		if entry.Name == "" || entry.Version == "" || result[entry] {
			return nil, errors.New("invalid or duplicate activity registry entry")
		}
		if (entry.Name == K6Activity && (entry.Version != "v1" || entry.Class != EffectCooperating)) ||
			(entry.Name != K6Activity && entry.Class != "") {
			return nil, errors.New("invalid activity effect class")
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
	rows, err := s.pool.Query(ctx, `SELECT d.activity_versions, d.effect_classes
        FROM engine.workflow_definitions d
        WHERE EXISTS (SELECT 1 FROM engine.workflow_executions w
          WHERE w.namespace=$1 AND w.definition_id=d.definition_id AND w.definition_version=d.version
          AND w.state NOT IN ('SUCCEEDED','FAILED','REJECTED','CANCELED','ABANDONED'))`, namespace)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var raw, rawClasses []byte
		if err := rows.Scan(&raw, &rawClasses); err != nil {
			return err
		}
		var versions map[string]string
		if err := json.Unmarshal(raw, &versions); err != nil {
			return fmt.Errorf("invalid referenced activity versions: %w", err)
		}
		if versions == nil {
			return errors.New("invalid referenced activity versions")
		}
		var classes map[string]EffectClass
		if err := json.Unmarshal(rawClasses, &classes); err != nil {
			return errors.New("invalid referenced activity effect classes")
		}
		for name, version := range versions {
			class := classes[name]
			if name == K6Activity {
				if class != EffectCooperating || !supported[ActivityVersion{name, version, class}] {
					return fmt.Errorf("referenced activity unsupported: %s/%s", name, version)
				}
				continue
			}
			if class != "" && class != EffectPure {
				return fmt.Errorf("referenced activity class unsupported: %s/%s", name, version)
			}
			if !supported[ActivityVersion{Name: name, Version: version}] {
				return fmt.Errorf("referenced activity unsupported: %s/%s", name, version)
			}
		}
	}
	return rows.Err()
}
