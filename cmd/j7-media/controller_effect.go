package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"durable-agent-execution-engine/internal/effects/k6sink"
	"durable-agent-execution-engine/internal/k6ingress"
	"durable-agent-execution-engine/internal/k6receipt"
	"durable-agent-execution-engine/internal/state"
)

type liveEffect struct {
	client   *http.Client
	receiver *k6ingress.Receiver
	witness  *k6ingress.Client
	servers  []*heldProcess
	journal  string
}

func (e *liveEffect) close() {
	for i := len(e.servers) - 1; i >= 0; i-- {
		e.servers[i].stop()
	}
	if e.receiver != nil {
		_ = e.receiver.Close()
	}
}
func serveControllerTLS(ctx context.Context, addr string, cfg *tls.Config, handle func(*tls.Conn)) (*heldProcess, error) {
	run, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- k6ingress.Serve(run, addr, cfg, handle); close(done) }()
	p := &heldProcess{cancel, done}
	deadline, stop := context.WithTimeout(ctx, 3*time.Second)
	defer stop()
	for {
		raw, err := (&net.Dialer{Timeout: 50 * time.Millisecond}).DialContext(deadline, "tcp", addr)
		if err == nil {
			raw.Close()
			return p, nil
		}
		select {
		case err := <-done:
			cancel()
			return nil, err
		case <-deadline.Done():
			p.stop()
			return nil, deadline.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}
func openLiveEffect(ctx context.Context, ca controllerCA, path string) (e liveEffect, err error) {
	e.journal = path
	defer func() {
		if err != nil {
			e.close()
		}
	}()
	relayTLS, err := ca.certificate("j7-effect-relay", relayURI)
	if err != nil {
		return e, err
	}
	witnessTLS, err := ca.certificate("j7-witness", "spiffe://j7.local/witness")
	if err != nil {
		return e, err
	}
	workerTLS, err := relayTLS.Server(false)
	if err != nil {
		return e, err
	}
	toSink, err := relayTLS.Client("j7-sink")
	if err != nil {
		return e, err
	}
	toWitness, err := relayTLS.Client("j7-witness")
	if err != nil {
		return e, err
	}
	receiveTLS, err := witnessTLS.Server(true)
	if err != nil {
		return e, err
	}
	e.receiver, err = k6ingress.OpenReceiver(path, relayURI, sinkURI)
	if err != nil {
		return e, err
	}
	p, err := serveControllerTLS(ctx, "127.0.0.1:19445", receiveTLS, e.receiver.Handle)
	if err != nil {
		return e, err
	}
	e.servers = append(e.servers, p)
	session := strings.ReplaceAll(state.NewID(), "-", "")
	if _, err := hex.DecodeString(session); err != nil || len(session) != 32 {
		return e, errors.New("invalid K6 fixture relay session")
	}
	e.witness = &k6ingress.Client{Address: "127.0.0.1:19445", TLS: toWitness, Session: session, RelayPeer: relayURI, SinkPeer: sinkURI}
	relay := &k6ingress.Relay{SinkAddress: "127.0.0.1:18444", SinkPeer: sinkURI, SinkTLS: toSink, Witness: e.witness}
	p, err = serveControllerTLS(ctx, "127.0.0.1:19444", workerTLS, relay.Handle)
	if err != nil {
		return e, err
	}
	e.servers = append(e.servers, p)
	transport, err := fixtureClientTransport(ca.pem)
	if err != nil {
		return e, err
	}
	transport.DisableKeepAlives = true
	e.client = &http.Client{Transport: transport, Timeout: 8 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return e, nil
}

func (e *liveEffect) perform(ctx context.Context, key ed25519.PublicKey, token, operation string, cl fixtureClaim) (k6receipt.Proof, error) {
	proof := k6receipt.Proof{Operation: operation}
	d := cl.Dispatch
	if d == nil || d.Attempt != cl.Attempt || d.NodeID != state.K6Activity || d.Iteration != 0 || d.Fence <= 0 ||
		d.LookupFirst != (d.Attempt > 1) || (operation == "apply" && d.Attempt != 1) || (operation == "lookup" && d.Attempt != 2) {
		return proof, errors.New("E1 held control claim does not authorize this effect phase")
	}
	i := int32(d.Iteration)
	invocation := state.NewID()
	payload, err := json.Marshal(k6sink.Request{IntentID: d.IntentID, GrantToken: d.GrantToken, WorkflowID: d.WorkflowID, NodeID: d.NodeID,
		Iteration: &i, LogicalKey: d.LogicalKey, ScopeHash: d.ScopeHash, ResourceID: d.ResourceID, ExpectedRevision: d.ExpectedRevision,
		Arguments: d.Arguments, Attempt: d.Attempt, Fence: d.Fence, MessageID: invocation, RequestID: state.NewID()})
	if err != nil {
		return proof, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://127.0.0.1:19444/v1/"+operation, bytes.NewReader(payload))
	if err != nil {
		return proof, err
	}
	req.Close = true
	req.GetBody = nil
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Dur053-Invocation", invocation)
	resp, err := e.client.Do(req)
	if err != nil {
		return proof, errors.New("E1 ingress did not complete")
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 8193))
	if err != nil || len(raw) > 8192 || resp.StatusCode != 200 || resp.Header.Get(k6receipt.ProtocolHeader) != "1" ||
		json.Unmarshal(raw, &proof.Receipt) != nil {
		return proof, errors.New("E1 ingress did not return a known signed outcome")
	}
	proof.Signature = resp.Header.Get(k6receipt.SignatureHeader)
	if !k6receipt.Verify(key, k6receipt.Claim{NodeID: d.NodeID, Iteration: d.Iteration, Attempt: d.Attempt, Fence: d.Fence}, proof) {
		return proof, errors.New("E1 receipt signature does not match the active claim")
	}
	return proof, nil
}
