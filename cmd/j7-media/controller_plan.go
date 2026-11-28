package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"path/filepath"
	"slices"
	"strings"

	"durable-agent-execution-engine/internal/restoreledger"
)

const (
	repairedRelaySwitch = "RUNTIME_RELAY_FULL_BATCH_LOOP"
	relayReaderPath     = "cmd/runtime/relay_mode.go"
)

type controllerOverlay struct {
	Schema           string            `json:"schema"`
	Profile          string            `json:"profile"`
	Environment      map[string]string `json:"environment"`
	EngineMigrations migrationRange    `json:"engine_migrations"`
	SinkMigrations   migrationRange    `json:"sink_migrations"`
}

type migrationRange struct {
	First int `json:"first"`
	Last  int `json:"last"`
}

type controllerTopology struct {
	Nodes        int    `json:"nodes"`
	Workers      int    `json:"workers"`
	Network      string `json:"network"`
	CNI          string `json:"cni"`
	BrokerPolicy string `json:"broker_policy"`
}

type controllerWorkload struct {
	Schema                  string             `json:"schema"`
	Topology                controllerTopology `json:"topology"`
	Roles                   map[string]quota   `json:"roles"`
	EngineConnections       int                `json:"engine_connections"`
	SinkConnections         int                `json:"sink_connections"`
	MarkerInterval          int                `json:"marker_interval_seconds"`
	ArchivedMarkers         int                `json:"archived_markers"`
	BaselineTerminalMarkers int                `json:"baseline_terminal_markers"`
	UnarchivedMarkers       int                `json:"unarchived_markers"`
	SourceCohorts           []string           `json:"source_cohorts"`
	TargetCohorts           []string           `json:"target_cohorts"`
	Gates                   map[string]string  `json:"gates"`
}

type controllerTool struct {
	URL    string `json:"url"`
	Digest string `json:"sha256"`
}

type controllerLock struct {
	Runner   string                    `json:"runner"`
	Node     string                    `json:"node"`
	Postgres string                    `json:"postgres"`
	Kafka    string                    `json:"kafka"`
	Tools    map[string]controllerTool `json:"tools"`
	Addons   map[string]controllerTool `json:"addons"`
	Images   map[string]string         `json:"images"`
}

type controllerPlan struct {
	Source controllerOverlay  `json:"source"`
	Target controllerOverlay  `json:"target"`
	Work   controllerWorkload `json:"workload"`
	Lock   controllerLock     `json:"tools"`
}

func readControllerPlan(root string) (controllerPlan, error) {
	var p controllerPlan
	for _, input := range []struct {
		name  string
		value any
	}{
		{"source-overlay.json", &p.Source}, {"target-overlay.json", &p.Target}, {"workload.json", &p.Work},
	} {
		raw, err := evidenceFile(root, filepath.Join("deploy/j7", input.name))
		if err != nil {
			return p, err
		}
		if err := evidenceJSON(raw, input.value); err != nil {
			return p, err
		}
	}
	rawKind, err := evidenceFile(root, "deploy/j7/kind.json")
	if err != nil {
		return p, err
	}
	var kind struct {
		Kind    string `json:"kind"`
		Version string `json:"apiVersion"`
		Network struct {
			Listen     string `json:"apiServerAddress"`
			PodSubnet  string `json:"podSubnet"`
			DisableCNI bool   `json:"disableDefaultCNI"`
		} `json:"networking"`
		Nodes []struct {
			Role string `json:"role"`
		} `json:"nodes"`
	}
	if err := evidenceJSON(rawKind, &kind); err != nil {
		return p, err
	}
	if kind.Kind != "Cluster" || kind.Version != "kind.x-k8s.io/v1alpha4" || len(kind.Nodes) != 1 ||
		kind.Nodes[0].Role != "control-plane" || kind.Network.Listen != "127.0.0.1" ||
		!kind.Network.DisableCNI || kind.Network.PodSubnet != "192.168.0.0/16" {
		return p, errors.New("J7_CANDIDATE_ADMISSION: kind must use the sealed control plane and Calico subnet")
	}
	raw, err := evidenceFile(root, "deploy/k8s/tools.lock.json")
	if err != nil {
		return p, err
	}
	if err := json.Unmarshal(raw, &p.Lock); err != nil {
		return p, err
	}
	if err := validateControllerPlan(p); err != nil {
		return p, err
	}
	return p, nil
}

func validateControllerPlan(p controllerPlan) error {
	for _, layer := range []struct {
		config  controllerOverlay
		profile string
		env     map[string]string
	}{
		{p.Source, "J7_source", map[string]string{repairedRelaySwitch: "1"}},
		{p.Target, "J7_target", map[string]string{repairedRelaySwitch: "1", "RUNTIME_KAFKA_GENERATION": "j7-restore"}},
	} {
		v := layer.config
		if v.Schema != "j7.controller-overlay.v1" || v.Profile != layer.profile ||
			!maps.Equal(v.Environment, layer.env) || v.EngineMigrations != (migrationRange{1, 23}) ||
			v.SinkMigrations != (migrationRange{1, 4}) {
			return fmt.Errorf("J7_CANDIDATE_ADMISSION: invalid %s overlay", layer.profile)
		}
	}
	w := p.Work
	if w.Schema != "j7.controller-workload.v1" || w.Topology != (controllerTopology{1, 2, "kind", "calico", "fresh"}) ||
		!maps.Equal(w.Roles, j7RoleLimits) || w.EngineConnections != 100 || w.SinkConnections != 16 ||
		w.MarkerInterval != 5 || w.ArchivedMarkers != 12 || w.UnarchivedMarkers != 3 || w.BaselineTerminalMarkers != 8 ||
		!slices.Equal(w.SourceCohorts, []string{"d1", "d2", "f1", "l1", "e1"}) ||
		!slices.Equal(w.TargetCohorts, []string{"n1", "n2", "n3", "n4"}) ||
		!maps.Equal(w.Gates, map[string]string{"d1": "unclaimed-published", "d2": "unclaimed-published",
			"f1": "claimed-held-result", "l1": "archived-lease", "e1": "authorized-held-apply"}) {
		return errors.New("J7_CANDIDATE_ADMISSION: workload differs from the frozen protocol")
	}
	if p.Lock.Runner != "ubuntu-24.04" {
		return errors.New("J7_CANDIDATE_ADMISSION: unsupported runner")
	}
	for path, image := range map[string]string{
		"kindest/node:v1.32.2": p.Lock.Node, "postgres:18.6-bookworm": p.Lock.Postgres, "apache/kafka:4.3.1": p.Lock.Kafka,
	} {
		digest, ok := strings.CutPrefix(image, path+"@sha256:")
		if !ok || !restoreledger.IsSHA256(digest) {
			return fmt.Errorf("J7_CANDIDATE_ADMISSION: unpinned image %s", path)
		}
	}
	for _, d := range []controllerTool{p.Lock.Tools["kind"], p.Lock.Tools["kubectl"], p.Lock.Addons["calico"]} {
		if !strings.HasPrefix(d.URL, "https://") || !restoreledger.IsSHA256(d.Digest) {
			return errors.New("J7_CANDIDATE_ADMISSION: unpinned topology tooling")
		}
	}
	return nil
}

type controllerDryRun struct {
	Plan         controllerPlan         `json:"fixture"`
	Dependencies []controllerDependency `json:"dependencies"`
	Commands     [][]string             `json:"dependency_create_commands"`
	Stages       []controllerStage      `json:"media_stage_commands"`
}

func controllerOverlayJSON(root string, w io.Writer) error {
	v, err := readControllerPlan(root)
	if err != nil {
		return err
	}
	host := fixtureHost{root: root, private: filepath.Join(root, ".j7-dry-run"), cluster: "j7-1-1", runID: "1", campaign: "dry-run", plan: v}
	d := controllerDryRun{Plan: v, Dependencies: host.dependencies("q1")}
	for _, item := range d.Dependencies {
		var command []string
		switch item.Role {
		case sourceRole, "sink-db":
			command, err = host.postgresCommand("q1", item)
		case sourceKafkaRole:
			command, err = host.sourceKafkaCommand("q1", item, "synthetic-cluster-id")
		default:
			continue
		}
		if err != nil {
			return err
		}
		d.Commands = append(d.Commands, command)
	}
	bound := stageFiles{Root: filepath.Join(host.private, "q1"), Drill: "q1", PGBin: filepath.Join(host.private, "pg-bin"), OpenNS: 1}
	bound.Source = restoreledger.SHA256([]byte("dry-run source broker"))
	bound.Restore = restoreledger.SHA256([]byte("dry-run restore"))
	bound.Fresh = restoreledger.SHA256([]byte("dry-run fresh broker"))
	bound.Watch = restoreledger.SHA256([]byte("dry-run redispatch watch"))
	bound.Engine = restoreledger.SHA256([]byte("dry-run final engine snapshot"))
	for _, name := range []string{"prepare", "source-preflight", "kafka-source", "orphan-source", "cut", "source-loss", "restore",
		"fresh-kafka", "orphan-rebind", "watch-redispatch", "quiet-start", "verify-orphan", "quiet-end", "verify-redispatch"} {
		stage, err := bound.mediaStage(name)
		if err != nil {
			return err
		}
		d.Stages = append(d.Stages, stage)
	}
	return json.NewEncoder(w).Encode(d)
}
