//go:build dur053_k6

package k6hook

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestK6HookRejectsMismatchedACK(t *testing.T) {
	var polled atomic.Bool
	deadline := time.Now().Add(4 * time.Second).UnixMilli()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request message
		_ = json.NewDecoder(r.Body).Decode(&request)
		reply := response{Command: "ack", Arm: "arm", Sequence: 1, Hash: "no-durable-ack", Journal: strings.Repeat("1", 64), Until: deadline}
		if request.Event == "held" {
			polled.Store(true)
			reply.Command = "drop"
		}
		_ = json.NewEncoder(w).Encode(reply)
	}))
	defer server.Close()
	c := &client{address: server.URL, http: server.Client()}
	if hit, err := c.Hit("relay-post-commit", Fields{App: "application"}); hit != nil || err == nil || polled.Load() {
		t.Fatal("drop without a matching durable ACK was accepted")
	}
}

func TestK6HookBindsDecisionAndRelease(t *testing.T) {
	until := time.Now().Add(4 * time.Second).UnixMilli()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var request message
		_ = json.Unmarshal(raw, &request)
		reply := response{Arm: "arm", Sequence: 4, Journal: strings.Repeat("2", 64), Until: until}
		switch request.Event {
		case "boundary_reached":
			sum := sha256.Sum256(raw)
			reply.Hash, reply.Command = hex.EncodeToString(sum[:]), "ack"
		case "held":
			reply.Hash, reply.Command = request.Hash, "duplicate"
		case "released":
			reply.Command = "done"
		}
		if request.HookID == request.Nonce || len(request.Nonce) != 32 || request.Fields.Invocation != "invocation" {
			t.Error("missing independent hook and caller identities")
		}
		_ = json.NewEncoder(w).Encode(reply)
	}))
	defer server.Close()
	c := &client{address: server.URL, http: server.Client()}
	hit, err := c.Hit("relay-pre-forward", Fields{Invocation: "invocation"})
	if err != nil || hit == nil || hit.Action() != "duplicate" || hit.Deadline().UnixMilli() != until || hit.Finish() != nil {
		t.Fatalf("authenticated decision and release failed: %v %v", hit, err)
	}
}
