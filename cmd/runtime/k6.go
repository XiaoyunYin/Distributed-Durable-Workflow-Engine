package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"

	"durable-agent-execution-engine/internal/api"
	"durable-agent-execution-engine/internal/effects/k6sink"
	"durable-agent-execution-engine/internal/engine"
	"durable-agent-execution-engine/internal/k6ingress"
	"durable-agent-execution-engine/internal/k6receipt"
	"durable-agent-execution-engine/internal/state"
)

type k6Allocator struct {
	url    string
	client *http.Client
}

func newK6Allocator(endpoint, peer string, credentials k6ingress.TLSFiles) (*k6Allocator, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.Path != "" ||
		u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || peer == "" {
		return nil, state.ErrK6Unavailable
	}
	cfg, err := credentials.Client(u.Hostname())
	if err != nil {
		return nil, state.ErrK6Unavailable
	}
	cfg.VerifyConnection = func(connection tls.ConnectionState) error {
		if !k6ingress.ValidPeer(connection, peer) {
			return state.ErrK6Unavailable
		}
		return nil
	}
	return &k6Allocator{url: u.String() + "/v1/allocate", client: &http.Client{
		Transport:     &http.Transport{TLSClientConfig: cfg, IdleConnTimeout: 30 * time.Second},
		Timeout:       4 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}, nil
}

func (a *k6Allocator) Allocate(ctx context.Context, dispatch state.K6Dispatch) (int64, error) {
	if dispatch.Iteration < 0 || dispatch.Iteration > 1<<31-1 {
		return 0, state.ErrK6Unavailable
	}
	iteration := int32(dispatch.Iteration)
	identity := fmt.Sprintf("k6-allocate/%s/%d", dispatch.IntentID, dispatch.Attempt)
	body, err := json.Marshal(k6sink.Request{IntentID: dispatch.IntentID, GrantToken: dispatch.GrantToken,
		WorkflowID: dispatch.WorkflowID, NodeID: dispatch.NodeID, Iteration: &iteration, LogicalKey: dispatch.LogicalKey,
		ScopeHash: dispatch.ScopeHash, ResourceID: dispatch.ResourceID, ExpectedRevision: dispatch.ExpectedRevision,
		Arguments: dispatch.Arguments, Attempt: dispatch.Attempt, MessageID: identity, RequestID: identity})
	if err != nil {
		return 0, state.ErrK6Unavailable
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.url, bytes.NewReader(body))
	if err != nil {
		return 0, state.ErrK6Unavailable
	}
	req.Header.Set("Content-Type", "application/json")
	response, err := a.client.Do(req)
	if err != nil {
		return 0, state.ErrK6Unavailable
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || response.Header.Get(k6receipt.ProtocolHeader) != "1" {
		return 0, state.ErrK6Unavailable
	}
	var out struct {
		Fence int64 `json:"fence"`
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, 1024))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&out); err != nil || decoder.Decode(new(any)) != io.EOF || out.Fence <= 0 {
		return 0, state.ErrK6Unavailable
	}
	return out.Fence, nil
}

type k6RuntimeConfig struct {
	scheduler *engine.K6Options
	worker    *api.K6WorkerControl
	tls       *tls.Config
}

func loadK6RuntimeConfig(value string, roles map[string]bool, reviewer *api.TokenReviewer) (k6RuntimeConfig, error) {
	var out k6RuntimeConfig
	if !state.K6Enabled() {
		return out, nil
	}
	namespace := os.Getenv("RUNTIME_NAMESPACE")
	if value == "combined" || roles["operator"] || namespace == "" {
		return out, errors.New("K6 requires split roles without operator routes and an explicit namespace")
	}
	if roles["worker-control"] && len(roles) != 1 {
		return out, errors.New("K6 allocator credentials require a dedicated worker-control process")
	}
	if !roles["scheduler"] && !roles["worker-control"] {
		return out, nil
	}
	if os.Getenv("DATABASE_URL") == "" {
		return out, errors.New("K6 scheduler and worker-control require an engine database")
	}
	key, err := base64.StdEncoding.Strict().DecodeString(os.Getenv("DUR053_K6_RECEIPT_PUBLIC_KEY"))
	if err != nil || len(key) != ed25519.PublicKeySize {
		return out, errors.New("K6 sink receipt public key is required")
	}
	if roles["scheduler"] {
		f, err := os.Open(os.Getenv("DUR053_K6_PLAN_FILE"))
		if err != nil {
			return out, errors.New("K6 sealed plan is unavailable")
		}
		defer f.Close()
		const limit = 16 << 20
		body, err := io.ReadAll(io.LimitReader(f, limit+1))
		if err != nil || len(body) > limit {
			return out, errors.New("K6 sealed plan could not be read")
		}
		plan, err := engine.ParseK6Plan(body, os.Getenv("DUR053_K6_PLAN_SHA256"), namespace)
		if err != nil {
			return out, err
		}
		out.scheduler = &engine.K6Options{Plan: plan, ReceiptKey: key}
	}
	if roles["worker-control"] {
		if reviewer == nil {
			return out, errors.New("K6 worker-control requires projected-token authentication")
		}
		var config struct {
			SinkURL, SinkPeer              string
			AllocatorTLS, WorkerControlTLS k6ingress.TLSFiles
		}
		if err := k6ingress.ReadConfig(os.Getenv("DUR053_K6_WORKER_CONTROL_FILE"), &config); err != nil {
			return out, err
		}
		allocator, err := newK6Allocator(config.SinkURL, config.SinkPeer, config.AllocatorTLS)
		if err != nil {
			return out, err
		}
		out.tls, err = config.WorkerControlTLS.Server(false)
		if err != nil {
			return out, err
		}
		out.worker = &api.K6WorkerControl{Allocator: allocator, ReceiptKey: key}
	}
	return out, nil
}
