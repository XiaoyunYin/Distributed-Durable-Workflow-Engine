package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

func (h fixtureHost) ownedResources(ctx context.Context, kind, drill string) ([]string, error) {
	args := []string{kind, "ls", "-q"}
	switch kind {
	case "container":
		args = append(args, "--all", "--no-trunc")
	case "volume":
	default:
		return nil, errors.New("unsupported J7 cleanup resource")
	}
	args = append(args, "--filter", "label=j7.restore.run="+h.runID,
		"--filter", "label=j7.restore.attempt=1", "--filter", "label=j7.restore.campaign="+h.campaign)
	if drill != "" {
		args = append(args, "--filter", "label=j7.restore.drill="+drill)
	}
	raw, err := h.docker(ctx, args...)
	if err != nil {
		return nil, err
	}
	list := strings.Fields(string(raw))
	for _, id := range list {
		valid := dockerID
		if kind == "volume" {
			valid = volumeName
		}
		if !valid.MatchString(id) {
			return nil, fmt.Errorf("unexpected ID returned by Docker %s listing", kind)
		}
	}
	return list, nil
}

func (h fixtureHost) observeTeardown(ctx context.Context, drill string) (hostTeardown, error) {
	w := hostTeardown{Residuals: []string{}}
	var failures []error
	attempt := func(err error) {
		if err != nil {
			failures = append(failures, err)
		}
	}
	if drill == "" {
		nodes, err := h.docker(ctx, "container", "ls", "-aq", "--filter", "label=io.x-k8s.kind.cluster="+h.cluster)
		attempt(err)
		if err == nil && len(strings.Fields(string(nodes))) > 0 {
			_, err := h.kind(ctx, "delete", "cluster", "--kubeconfig", h.kubeconfig)
			attempt(err)
		}
	}
	// Dependent containers go first. Attempt remaining removals even if one
	// daemon call fails, then query the complete owned inventory again.
	for _, kind := range []string{"container", "volume"} {
		ids, err := h.ownedResources(ctx, kind, drill)
		attempt(err)
		for _, id := range ids {
			if kind == "container" {
				running, err := h.docker(ctx, "container", "inspect", "--format", "{{.State.Running}}", id)
				attempt(err)
				switch strings.TrimSpace(string(running)) {
				case "true":
					_, err = h.docker(ctx, "container", "stop", "--timeout=5", id)
					attempt(err)
				case "false":
				default:
					attempt(errors.New("could not prove owned container state"))
				}
			}
			_, err := h.docker(ctx, kind, "rm", id)
			attempt(err)
		}
	}
	for _, kind := range []string{"container", "volume"} {
		ids, err := h.ownedResources(ctx, kind, drill)
		attempt(err)
		if err != nil {
			w.Residuals = append(w.Residuals, kind+"-inventory-unavailable")
		}
		for _, id := range ids {
			w.Residuals = append(w.Residuals, kind+":"+id)
		}
	}
	if drill == "" {
		if err := h.checkClusterAbsent(ctx); err != nil {
			w.Residuals = append(w.Residuals, "kind:"+h.cluster)
			attempt(err)
		}
	}
	slices.Sort(w.Residuals)
	w.Verified = len(w.Residuals) == 0
	if !w.Verified {
		attempt(errors.New("J7 cleanup left resources or could not verify absence"))
	}
	return w, errors.Join(failures...)
}

func (h fixtureHost) teardown(ctx context.Context, drill string) error {
	w, teardownErr := h.observeTeardown(ctx, drill)
	name := "cleanup.json"
	if drill != "" {
		name = drill + "-cleanup.json"
	}
	// Bootstrap recovery can write an earlier teardown receipt. Leave each
	// attempt intact and atomically publish the last observed inventory.
	tmp, err := os.CreateTemp(h.private, ".cleanup-*.json")
	if err != nil {
		return errors.Join(teardownErr, err)
	}
	src := tmp.Name()
	if err := tmp.Close(); err != nil {
		return errors.Join(teardownErr, err)
	}
	defer os.Remove(src)
	if err := os.Remove(src); err != nil {
		return errors.Join(teardownErr, err)
	}
	if err := writeControllerJSON(src, w); err != nil {
		return errors.Join(teardownErr, err)
	}
	if err := os.Rename(src, filepath.Join(h.private, name)); err != nil {
		return errors.Join(teardownErr, err)
	}
	return errors.Join(teardownErr, syncDirectory(h.private))
}
