package k6sink

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	"durable-agent-execution-engine/internal/k6auth"
	"durable-agent-execution-engine/internal/k6hook"
	"durable-agent-execution-engine/internal/k6receipt"
	"durable-agent-execution-engine/internal/state"
)

const maxBody = 64 << 10

type HTTPConfig struct {
	Worker                        WorkerIdentity
	EnginePeerURI, IngressPeerURI string
	ClockUncertainty              time.Duration
	ReceiptKey                    ed25519.PrivateKey
	ObserveTimings                bool
	Hook                          k6hook.Client
}

type handler struct {
	cfg      HTTPConfig
	reviewer k6auth.Reviewer
	reader   GrantReader
	store    backend
	now      func() time.Time
}

func NewHandler(cfg HTTPConfig, reviewer k6auth.Reviewer, reader GrantReader, store *Store) (http.Handler, error) {
	if store == nil {
		return nil, errDenied
	}
	return newHandler(cfg, reviewer, reader, store)
}

func newHandler(cfg HTTPConfig, reviewer k6auth.Reviewer, reader GrantReader, store backend) (*handler, error) {
	isPeer := func(identity string) bool {
		u, err := url.Parse(identity)
		return err == nil && u.Scheme == "spiffe" && u.Host != "" && u.Path != "" &&
			u.RawQuery == "" && u.Fragment == "" && u.User == nil
	}
	if !identifier(cfg.Worker.Namespace) || !identifier(cfg.Worker.ServiceAccount) ||
		!identifier(cfg.Worker.ServiceAccountUID) || !isPeer(cfg.EnginePeerURI) || !isPeer(cfg.IngressPeerURI) ||
		cfg.EnginePeerURI == cfg.IngressPeerURI || cfg.ClockUncertainty <= 0 ||
		(len(cfg.ReceiptKey) != 0 && len(cfg.ReceiptKey) != ed25519.PrivateKeySize) ||
		reviewer == nil || reader == nil || store == nil {
		return nil, errDenied
	}
	return &handler{cfg, reviewer, reader, store, time.Now}, nil
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || r.URL.RawQuery != "" {
		http.NotFound(w, r)
		return
	}
	var operation, peer string
	switch r.URL.Path {
	case "/v1/apply":
		operation, peer = "apply", h.cfg.IngressPeerURI
	case "/v1/lookup":
		operation, peer = "lookup", h.cfg.IngressPeerURI
	case "/v1/allocate":
		operation, peer = "allocate", h.cfg.EnginePeerURI
	default:
		http.NotFound(w, r)
		return
	}
	if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 || len(r.TLS.VerifiedChains[0]) == 0 {
		http.Error(w, errDenied.Error(), http.StatusForbidden)
		return
	}
	uris := r.TLS.VerifiedChains[0][0].URIs
	if len(uris) != 1 || uris[0].String() != peer {
		http.Error(w, errDenied.Error(), http.StatusForbidden)
		return
	}
	binding, err := r.TLS.ExportKeyingMaterial("EXPORTER-dur053-k6-sink", nil, 32)
	if err != nil {
		http.Error(w, errDenied.Error(), http.StatusForbidden)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 7*time.Second)
	defer cancel()
	if operation == "allocate" {
		if len(r.Header.Values("Authorization")) != 0 {
			http.Error(w, errDenied.Error(), http.StatusForbidden)
			return
		}
	} else {
		headers := r.Header.Values("Authorization")
		if len(headers) != 1 || !strings.HasPrefix(headers[0], "Bearer ") {
			http.Error(w, errDenied.Error(), http.StatusForbidden)
			return
		}
		expiry, err := h.cfg.Worker.authenticate(ctx, h.reviewer, strings.TrimPrefix(headers[0], "Bearer "), h.now)
		if err != nil {
			http.Error(w, errDenied.Error(), http.StatusForbidden)
			return
		}
		// Preserve monotonic bounds even if the process wall clock changes.
		var cancelWorkerTimeout context.CancelFunc
		ctx, cancelWorkerTimeout = context.WithTimeout(ctx, expiry.Sub(h.now()))
		defer cancelWorkerTimeout()
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
	if err != nil {
		var limit *http.MaxBytesError
		if errors.As(err, &limit) {
			http.Error(w, "K6_SINK_BODY_LIMIT", http.StatusRequestEntityTooLarge)
		} else {
			http.Error(w, "K6_SINK_BODY", http.StatusBadRequest)
		}
		return
	}
	var request Request
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if !utf8.Valid(body) || state.ValidateK6Arguments(body) != nil || decoder.Decode(&request) != nil ||
		(operation == "allocate" && request.Fence != 0) || (operation != "allocate" && request.Fence <= 0) {
		http.Error(w, "K6_SINK_BODY", http.StatusBadRequest)
		return
	}
	bodyHash := hash(body)
	observe := func(stage string, started time.Time) {
		if h.cfg.ObserveTimings {
			log.Printf("k6_grant %s %s %s %d", bodyHash, operation, stage, time.Since(started).Nanoseconds())
		}
	}
	started := time.Now()
	readCtx, cancelRead := context.WithTimeout(ctx, 2*time.Second)
	auth, err := authorize(readCtx, h.reader, request, h.cfg.ClockUncertainty)
	cancelRead()
	observe("read", started)
	if err != nil {
		http.Error(w, errDenied.Error(), http.StatusForbidden)
		return
	}
	ctx, cancelGrant := context.WithDeadline(ctx, auth.deadline)
	defer cancelGrant()
	cooperating := request.NodeID == state.K6Activity
	if cooperating && len(h.cfg.ReceiptKey) == 0 {
		http.Error(w, errDenied.Error(), http.StatusForbidden)
		return
	}
	d := delivery{operation, bodyHash, hash(binding), request, auth, cooperating}
	out, err := h.store.execute(ctx, d, func(ctx context.Context) (authorization, error) {
		started := time.Now()
		readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		checked, err := recheck(readCtx, h.reader, request, auth, h.cfg.ClockUncertainty)
		observe("recheck", started)
		return checked, err
	})
	switch {
	case errors.Is(err, errDenied):
		http.Error(w, errDenied.Error(), http.StatusForbidden)
	case err != nil:
		http.Error(w, errUnavailable.Error(), http.StatusServiceUnavailable)
	default:
		switch out.outcome {
		case "APPLIED", "DUPLICATE", "FOUND", "ALLOCATED":
			if cooperating {
				w.Header().Set(k6receipt.ProtocolHeader, "1")
				if operation != "allocate" {
					var receipt Receipt
					if json.Unmarshal(out.body, &receipt) != nil {
						http.Error(w, errUnavailable.Error(), http.StatusServiceUnavailable)
						return
					}
					claim := k6receipt.Claim{NodeID: request.NodeID, Iteration: int(*request.Iteration), Attempt: request.Attempt, Fence: request.Fence}
					w.Header().Set(k6receipt.SignatureHeader, k6receipt.Sign(h.cfg.ReceiptKey, claim, operation, receipt))
					if out.outcome == "APPLIED" {
						h.afterCommit(r, request, receipt, hash(binding), hash(out.body))
					}
				}
			}
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Cache-Control", "no-store")
			_, _ = w.Write(out.body)
		case "MISSING":
			http.Error(w, "K6_SINK_MISSING", http.StatusNotFound)
		default:
			http.Error(w, "K6_SINK_"+out.outcome, http.StatusConflict)
		}
	}
}
