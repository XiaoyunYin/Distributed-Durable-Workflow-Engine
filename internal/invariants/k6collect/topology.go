package k6collect

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"time"

	"durable-agent-execution-engine/internal/invariants/k6v1"
)

var topologyScopes = []string{"database", "deployment", "journal", "relay", "service"}

// PlatformInventory supplies the independently enumerated DB instances,
// relay processes and journal mounts for the reset. It includes a stable
// inventory revision and explicit successful scans when those sets are empty.
type PlatformInventory struct {
	Revision string           `json:"revision"`
	Scanned  []string         `json:"scanned"`
	Objects  []k6v1.Component `json:"objects"`
}

func (k Kubernetes) Topology(ctx context.Context, namespace string, platform PlatformInventory) (k6v1.TopologyRow, error) {
	failure := errors.New("K6_TOPOLOGY_INVENTORY")
	if !objectName.MatchString(namespace) || platform.Revision == "" || platform.Objects == nil ||
		!slices.Equal(platform.Scanned, []string{"database", "journal", "relay"}) {
		return k6v1.TopologyRow{}, failure
	}
	paths := []struct{ kind, path string }{
		{"deployment", "/apis/apps/v1/namespaces/" + namespace + "/deployments"},
		{"service", "/api/v1/namespaces/" + namespace + "/services"},
	}
	revisions := []string{platform.Revision}
	objects := slices.Clone(platform.Objects)
	for _, scope := range paths {
		raw, err := k.raw(ctx, scope.path).Output()
		if err != nil {
			return k6v1.TopologyRow{}, failure
		}
		revision, components, err := topologyList(raw, scope.kind)
		if err != nil {
			return k6v1.TopologyRow{}, err
		}
		revisions = append(revisions, revision)
		objects = append(objects, components...)
	}
	sortComponents(objects)
	return k6v1.TopologyRow{Kind: "observed", Observer: "kubernetes-platform", Revision: k6v1.Hash([]byte(strings.Join(revisions, "\n"))),
		Components: objects, Scanned: slices.Clone(topologyScopes)}, nil
}

func topologyList(raw []byte, kind string) (string, []k6v1.Component, error) {
	var list struct {
		Metadata struct {
			ResourceVersion string `json:"resourceVersion"`
			Continue        string `json:"continue"`
		} `json:"metadata"`
		Items []struct {
			Metadata struct {
				Name   string            `json:"name"`
				Labels map[string]string `json:"labels"`
			} `json:"metadata"`
		} `json:"items"`
	}
	failure := errors.New("K6_TOPOLOGY_LIST")
	if json.Unmarshal(raw, &list) != nil || list.Metadata.ResourceVersion == "" || list.Metadata.Continue != "" || list.Items == nil {
		return "", nil, failure
	}
	components := make([]k6v1.Component, 0, len(list.Items))
	for _, item := range list.Items {
		meta := item.Metadata
		if !objectName.MatchString(meta.Name) {
			return "", nil, failure
		}
		role := meta.Labels["dur053.io/component"]
		if strings.Contains(meta.Name, "sink") {
			role = "sink"
		}
		if strings.Contains(meta.Name, "relay") {
			role = "relay"
		}
		if !objectName.MatchString(role) {
			role = "unclassified"
		}
		components = append(components, k6v1.Component{Kind: kind, Name: meta.Name, Role: role})
	}
	return list.Metadata.ResourceVersion, components, nil
}

func sortComponents(components []k6v1.Component) {
	slices.SortFunc(components, func(a, b k6v1.Component) int { return strings.Compare(a.Kind+"/"+a.Name, b.Kind+"/"+b.Name) })
}

func RenderedTopology(revision string, components []k6v1.Component) (k6v1.TopologyRow, string, error) {
	components = slices.Clone(components)
	sortComponents(components)
	raw, err := k6v1.Canonical(components)
	return k6v1.TopologyRow{Kind: "rendered", Observer: "arm-renderer", Revision: revision,
		Components: components, Scanned: slices.Clone(topologyScopes)}, k6v1.Hash(raw), err
}

func ObserveTopology(stream *k6v1.Stream[k6v1.TopologyRow], observation k6v1.TopologyRow, at time.Time, terminal bool) error {
	// Resource versions can advance on unrelated updates. A recorded
	// component change itself always advances the activity position.
	if len(stream.Rows) < 2 || !slices.Equal(stream.Rows[len(stream.Rows)-1].Data.Components, observation.Components) {
		Append(stream, at, observation)
	}
	mode := "checkpoint"
	if terminal {
		mode = "terminal"
	}
	return k6v1.SealStream(stream, mode, k6v1.UTC(at), int64(len(stream.Rows)), 0)
}
