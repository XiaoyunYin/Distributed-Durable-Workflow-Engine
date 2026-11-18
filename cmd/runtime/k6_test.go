package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/effects/k6sink"
	"durable-agent-execution-engine/internal/k6ingress"
	"durable-agent-execution-engine/internal/k6receipt"
	"durable-agent-execution-engine/internal/state"
)

func TestK6AllocatorRequiresExactMutualTLSPeer(t *testing.T) {
	now := time.Now()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Minute)}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, public, private)
	if err != nil {
		t.Fatal(err)
	}
	newLeaf := func(serial int64, peer string, usage x509.ExtKeyUsage) k6ingress.TLSFiles {
		t.Helper()
		key, secret, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		uri, _ := url.Parse(peer)
		leaf := &x509.Certificate{SerialNumber: big.NewInt(serial), NotBefore: ca.NotBefore, NotAfter: ca.NotAfter,
			URIs: []*url.URL{uri}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
			KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage}}
		cert, err := x509.CreateCertificate(rand.Reader, leaf, ca, key, private)
		if err != nil {
			t.Fatal(err)
		}
		pkcs, err := x509.MarshalPKCS8PrivateKey(secret)
		if err != nil {
			t.Fatal(err)
		}
		return k6ingress.TLSFiles{Certificate: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert}),
			Key: pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: pkcs}),
			CA:  pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})}
	}
	const sinkPeer = "spiffe://k6-episode/sink"
	const workerControlPeer = "spiffe://k6-episode/worker-control"
	sink := newLeaf(2, sinkPeer, x509.ExtKeyUsageServerAuth)
	engine := newLeaf(3, workerControlPeer, x509.ExtKeyUsageClientAuth)
	serverTLS, err := sink.Server(true)
	if err != nil {
		t.Fatal(err)
	}
	var mutex sync.Mutex
	var requests []k6sink.Request
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request k6sink.Request
		if r.URL.Path != "/v1/allocate" || r.Method != http.MethodPost || r.Header.Get("Authorization") != "" ||
			!k6ingress.ValidPeer(*r.TLS, workerControlPeer) ||
			json.NewDecoder(r.Body).Decode(&request) != nil {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		mutex.Lock()
		requests = append(requests, request)
		mutex.Unlock()
		w.Header().Set(k6receipt.ProtocolHeader, "1")
		_, _ = io.WriteString(w, `{"fence":27}`)
	}))
	server.TLS = serverTLS
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.StartTLS()
	defer server.Close()
	allocator, err := newK6Allocator(server.URL, sinkPeer, engine)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(allocator.client.CloseIdleConnections)
	dispatch := state.K6Dispatch{WorkflowID: "workflow", NodeID: state.K6Activity, IntentID: state.NewID(), Attempt: 4,
		GrantToken: "test-only-grant", Arguments: json.RawMessage(`{}`), ExpectedRevision: "0",
		ResourceID: "resource", ScopeHash: "scope", LogicalKey: "logical"}
	for range 2 {
		if fence, err := allocator.Allocate(context.Background(), dispatch); err != nil || fence != 27 {
			t.Fatalf("exact engine peer could not allocate: %v", err)
		}
	}
	wrong, err := newK6Allocator(server.URL, workerControlPeer, engine)
	if err != nil {
		t.Fatal(err)
	}
	defer wrong.client.CloseIdleConnections()
	if _, err := wrong.Allocate(context.Background(), dispatch); err == nil {
		t.Fatal("allocator accepted a server with the wrong SPIFFE identity")
	}
	mutex.Lock()
	defer mutex.Unlock()
	if len(requests) != 2 || requests[0].RequestID == "" || requests[0].RequestID != requests[1].RequestID ||
		requests[0].IntentID != dispatch.IntentID || requests[0].GrantToken != dispatch.GrantToken ||
		requests[0].Attempt != dispatch.Attempt || requests[0].Fence != 0 {
		t.Fatal("allocation retries did not bind the persisted grant and attempt")
	}
}
