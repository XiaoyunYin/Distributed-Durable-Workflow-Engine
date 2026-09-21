package engine

import (
	"testing"

	"encoding/json"
)

func TestParseGraphAndRejectsBrokenReferences(t *testing.T) {
	graph, err := ParseGraph(json.RawMessage(`{"entry":"start","nodes":[{"id":"start","kind":"fanout","branches":["left","right"],"join":"join"},{"id":"left","kind":"activity","next":"join"},{"id":"right","kind":"activity","next":"join"},{"id":"join","kind":"join","next":"done"},{"id":"done","kind":"success"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if graph.Entry != "start" || graph.Nodes["start"].Join != "join" || len(graph.Nodes) != 5 {
		t.Fatalf("parsed graph = %+v", graph)
	}
	if _, err := ParseGraph(json.RawMessage(`{"entry":"start","nodes":[{"id":"start","next":"missing"}]}`)); err == nil {
		t.Fatal("graph with undeclared reference was accepted")
	}
}

func TestParseGraphRequiresEntryForMultipleNodes(t *testing.T) {
	if _, err := ParseGraph(json.RawMessage(`{"nodes":[{"id":"one"},{"id":"two"}]}`)); err == nil {
		t.Fatal("graph with multiple nodes and no entry was accepted")
	}
}
