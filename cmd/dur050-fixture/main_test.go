package main

import (
	"encoding/json"
	"testing"

	"durable-agent-execution-engine/internal/engine"
)

func TestBuildFamiliesCreatesValidatedEightNodeGraphs(t *testing.T) {
	families := buildFamilies()
	if len(families) != 2 {
		t.Fatalf("family count=%d; want 2", len(families))
	}
	for _, family := range families {
		graph, err := engine.ParseGraph(family.Definition.Graph)
		if err != nil {
			t.Fatalf("parse %s graph: %v", family.Name, err)
		}
		versions := map[string]string{}
		if err := json.Unmarshal(family.Definition.ActivityVersions, &versions); err != nil {
			t.Fatalf("decode %s activity versions: %v", family.Name, err)
		}
		effects := map[string]string{}
		if err := json.Unmarshal(family.Definition.EffectClasses, &effects); err != nil {
			t.Fatalf("decode %s effect classes: %v", family.Name, err)
		}
		if len(versions) != 8 || len(effects) != 8 {
			t.Fatalf("%s activity metadata: versions=%d effects=%d; want 8 each", family.Name, len(versions), len(effects))
		}
		for nodeID := range versions {
			if _, ok := graph.Nodes[nodeID]; !ok {
				t.Errorf("%s activity version refers to undeclared node %q", family.Name, nodeID)
			}
			if versions[nodeID] != activityVersion || effects[nodeID] != "PURE_ACTIVITY" {
				t.Errorf("%s node %q metadata: version=%q effect=%q", family.Name, nodeID, versions[nodeID], effects[nodeID])
			}
		}
		if family.InitialNodeID != graph.Entry {
			t.Errorf("%s start node=%q; graph entry=%q", family.Name, family.InitialNodeID, graph.Entry)
		}
	}
}

func TestDur050ActivityAliasesMatchGraphNodes(t *testing.T) {
	families := buildFamilies()
	for _, family := range families {
		graph, err := engine.ParseGraph(family.Definition.Graph)
		if err != nil {
			t.Fatal(err)
		}
		for nodeID, node := range graph.Nodes {
			if node.Kind != "activity" {
				continue
			}
			if len(nodeID) < len("dur050.sha256.") || nodeID[:len("dur050.sha256.")] != "dur050.sha256." {
				t.Errorf("activity node %q has no matching campaign registry alias", nodeID)
			}
		}
	}
}
