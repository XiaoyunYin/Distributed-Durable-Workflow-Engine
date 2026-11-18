package k6sink

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"net/http"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/k6hook"
	"durable-agent-execution-engine/internal/state"
)

type testHook func(string, k6hook.Fields) (k6hook.Hold, error)

func (f testHook) Hit(boundary string, fields k6hook.Fields) (k6hook.Hold, error) {
	return f(boundary, fields)
}

func TestK6SinkCommitBoundary(t *testing.T) {
	_, signer, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	grants := testGrants{}
	req := testRequest()
	req.NodeID, req.Fence = state.K6Activity, 1
	req = grants.add(t, req, time.Now().Add(time.Minute))
	receipt := Receipt{WorkflowID: req.WorkflowID, ResourceID: req.ResourceID,
		IntentID: req.IntentID, ScopeHash: req.ScopeHash, LogicalKey: req.LogicalKey,
		AppliedRevision: 1, ApplicationID: "application"}
	responseBody := asJSON(t, receipt)
	executed, reached, released := make(chan struct{}), make(chan struct{}), make(chan struct{})
	cfg := testHTTP
	cfg.ReceiptKey = signer
	cfg.Hook = testHook(func(boundary string, fields k6hook.Fields) (k6hook.Hold, error) {
		select {
		case <-executed:
		default:
			t.Error("sink reached post-commit before backend completion")
		}
		if boundary != "sink-post-commit" || fields.App != "application" || fields.Invocation != "caller" ||
			fields.Scope != req.ScopeHash || fields.Reply != hash(responseBody) || len(fields.Binding) != 64 {
			t.Error("hold lacks durable commit and caller identities")
		}
		close(reached)
		<-released
		return nil, nil
	})
	handler, err := newHandler(cfg, testReviewer(), grants, backendFunc(func(_ context.Context, _ delivery, _ grantRecheck) (result, error) {
		close(executed)
		return result{outcome: "APPLIED", body: responseBody}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	clients := newTestServer(t, handler)
	call, err := http.NewRequest(http.MethodPost, clients.url+"/v1/apply", bytes.NewReader(asJSON(t, req)))
	if err != nil {
		t.Fatal(err)
	}
	call.Header.Set("Authorization", "Bearer "+workerToken(t, nil))
	call.Header.Set("X-Dur053-Invocation", "caller")
	completed := make(chan int, 1)
	go func() {
		response, err := clients.ingress.Do(call)
		if err != nil {
			completed <- 0
			return
		}
		defer response.Body.Close()
		completed <- response.StatusCode
	}()
	defer close(released)
	if ResponseTimeout(cfg.Hook) == 12*time.Second {
		select {
		case status := <-completed:
			if status != 200 {
				t.Fatalf("ordinary sink status %d", status)
			}
		case <-reached:
			t.Fatal("ordinary sink exposed a hold")
		case <-time.After(3 * time.Second):
			t.Fatal("ordinary sink did not respond")
		}
		return
	}
	select {
	case <-reached:
	case <-completed:
		t.Fatal("campaign sink wrote response without entering hold")
	case <-time.After(3 * time.Second):
		t.Fatal("campaign sink did not reach its hold")
	}
	select {
	case <-completed:
		t.Fatal("campaign sink sent response before release")
	case <-time.After(30 * time.Millisecond):
	}
}
