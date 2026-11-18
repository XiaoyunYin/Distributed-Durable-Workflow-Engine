package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"durable-agent-execution-engine/internal/k6ingress"
	"durable-agent-execution-engine/internal/k6receipt"
	"durable-agent-execution-engine/internal/state"
)

func TestK6AllocatorObservationJoinsSinkTLSWithoutGrant(t *testing.T) {
	type observation struct{ binding, bodyHash string }
	fromSink := make(chan observation, 2)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		binding, err := r.TLS.ExportKeyingMaterial(k6ingress.BindingLabel, nil, 32)
		body, readErr := io.ReadAll(r.Body)
		if err != nil || readErr != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		fromSink <- observation{k6ingress.Hash(binding), k6ingress.Hash(body)}
		w.Header().Set(k6receipt.ProtocolHeader, "1")
		_, _ = io.WriteString(w, `{"fence":27}`)
	}))
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS13}
	server.StartTLS()
	defer server.Close()
	client := server.Client()
	client.Transport.(*http.Transport).DisableKeepAlives = true
	const peer = "spiffe://k6-episode/worker-control"
	allocator := &k6Allocator{url: server.URL, client: client, observerPeer: peer}
	dispatch := state.K6Dispatch{
		WorkflowID: "workflow", NodeID: state.K6Activity, IntentID: state.NewID(), Attempt: 4,
		GrantToken: "not-for-observation", Arguments: json.RawMessage(`{}`),
		ExpectedRevision: "0", ResourceID: "resource", ScopeHash: "scope", LogicalKey: "logical",
	}

	var output bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&output)
	t.Cleanup(func() { log.SetOutput(previous) })
	for range 2 {
		if fence, err := allocator.Allocate(context.Background(), dispatch); err != nil || fence != 27 {
			t.Fatalf("observed allocation failed: %v", err)
		}
	}
	first, second := <-fromSink, <-fromSink
	if first.binding == second.binding ||
		strings.Contains(output.String(), dispatch.GrantToken) {
		t.Fatal("allocation observations reused a TLS binding or disclosed the grant")
	}
	for _, observed := range []observation{first, second} {
		if !strings.Contains(output.String(), observed.binding) ||
			!strings.Contains(output.String(), observed.bodyHash) {
			t.Fatal("allocation observation did not match the sink TLS exporter and body")
		}
	}
}
