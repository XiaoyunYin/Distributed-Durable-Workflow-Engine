package k6auth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

const exactRequest = `{"intent_id":"intent","workflow_id":"workflow","node_id":"root","iteration":0,"proposal_hash":"hash","decision":"APPROVED"}`

type reviewFunc func(context.Context, string) (Review, error)

func (f reviewFunc) Review(ctx context.Context, token string) (Review, error) { return f(ctx, token) }

type writeFunc func(context.Context, DecisionRequest, string) (DecisionIdentity, error)

func (f writeFunc) Decide(ctx context.Context, r DecisionRequest, subject string) (DecisionIdentity, error) {
	return f(ctx, r, subject)
}

func identityFixture() (Episode, time.Time, tokenClaims, Review) {
	warmup := time.Unix(1_800_000_000, 0)
	episode := Episode{Namespace: "episode-1", ServiceAccountUID: "sa-uid", SecretName: "binding", SecretUID: "secret-uid",
		WarmupStart: warmup, LastPlannedArrival: warmup.Add(4 * time.Minute), TwoL: 100 * time.Second}
	claims := tokenClaims{Issuer: Issuer, Subject: episode.subject(), Audiences: []string{Audience},
		IssuedAt: warmup.Unix(), ExpiresAt: warmup.Add(10 * time.Minute).Unix()}
	claims.Binding.Namespace = episode.Namespace
	claims.Binding.ServiceAccount.Name = approver
	claims.Binding.ServiceAccount.UID = episode.ServiceAccountUID
	claims.Binding.Secret.Name = episode.SecretName
	claims.Binding.Secret.UID = episode.SecretUID
	review := Review{Authenticated: true, Audiences: []string{Audience}}
	review.User.Username, review.User.UID = episode.subject(), episode.ServiceAccountUID
	return episode, warmup.Add(time.Minute), claims, review
}

func jwt(t *testing.T, claims tokenClaims) string {
	t.Helper()
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	return "eyJhbGciOiJSUzI1NiJ9." + base64.RawURLEncoding.EncodeToString(payload) + ".synthetic-signature"
}

func sendDecision(a *Adapter, token, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, operation, strings.NewReader(body))
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response := httptest.NewRecorder()
	a.ServeHTTP(response, request)
	return response
}

func TestK6DecisionAuthentication(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*tokenClaims, *Review)
	}{
		{"wrong namespace", func(c *tokenClaims, r *Review) { c.Binding.Namespace = "episode-2" }},
		{"wrong service account", func(c *tokenClaims, r *Review) { c.Binding.ServiceAccount.Name = "worker" }},
		{"wrong service account UID", func(c *tokenClaims, r *Review) { r.User.UID = "recreated-account" }},
		{"worker credential", func(c *tokenClaims, r *Review) { r.User.Username = "system:serviceaccount:episode-1:worker" }},
		{"wrong issuer", func(c *tokenClaims, r *Review) { c.Issuer = Issuer + "/" }},
		{"wrong audience", func(c *tokenClaims, r *Review) { c.Audiences = []string{"worker"} }},
		{"additional audience", func(c *tokenClaims, r *Review) { c.Audiences = []string{Audience, "worker"} }},
		{"review omitted audience", func(c *tokenClaims, r *Review) { r.Audiences = nil }},
		{"expired", func(c *tokenClaims, r *Review) { c.ExpiresAt = c.IssuedAt + 60 }},
		{"wrong bound name", func(c *tokenClaims, r *Review) { c.Binding.Secret.Name = "different" }},
		{"wrong bound UID", func(c *tokenClaims, r *Review) { c.Binding.Secret.UID = "replacement" }},
		{"revoked binding", func(c *tokenClaims, r *Review) { r.Authenticated = false }},
		{"omitted TokenReview", func(c *tokenClaims, r *Review) { *r = Review{} }},
		{"early token", func(c *tokenClaims, r *Review) { c.IssuedAt-- }},
		{"insufficient token lifetime", func(c *tokenClaims, r *Review) { c.ExpiresAt = c.IssuedAt + 339 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			episode, now, claims, review := identityFixture()
			test.change(&claims, &review)
			reviews, writes := 0, 0
			token := jwt(t, claims)
			a, err := NewAdapter(episode, reviewFunc(func(_ context.Context, actual string) (Review, error) {
				reviews++
				if actual != token {
					t.Fatal("review did not receive the submitted assertion")
				}
				return review, nil
			}), writeFunc(func(context.Context, DecisionRequest, string) (DecisionIdentity, error) {
				writes++
				return DecisionIdentity{}, nil
			}))
			if err != nil {
				t.Fatal(err)
			}
			a.now = func() time.Time { return now }
			if response := sendDecision(a, token, exactRequest); response.Code != http.StatusForbidden || reviews != 1 || writes != 0 {
				t.Fatalf("invalid identity admitted: %s (status=%d reviews=%d writes=%d)", test.name, response.Code, reviews, writes)
			}
		})
	}
}

func TestK6AdapterDecisionIdentity(t *testing.T) {
	episode, now, claims, review := identityFixture()
	var reviews, writes int
	a, err := NewAdapter(episode, reviewFunc(func(context.Context, string) (Review, error) {
		reviews++
		return review, nil
	}), writeFunc(func(_ context.Context, request DecisionRequest, subject string) (DecisionIdentity, error) {
		writes++
		if request.Decision == nil && request.IntentID == "" {
			return DecisionIdentity{IntentID: "intent", ProposalHash: request.ProposalHash, Decision: "PENDING"}, nil
		}
		return DecisionIdentity{IntentID: request.IntentID, ProposalHash: request.ProposalHash,
			Decision: *request.Decision, Subject: &subject}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	a.now = func() time.Time { return now }
	token := jwt(t, claims)
	for _, body := range []string{
		strings.TrimSuffix(exactRequest, "}") + `,"actor":"forged-operator"}`,
		strings.TrimSuffix(exactRequest, "}") + `,"lease":{"epoch":10}}`,
	} {
		if response := sendDecision(a, token, body); response.Code != http.StatusForbidden || writes != 0 {
			t.Fatal("client-supplied actor or lease was accepted")
		}
	}
	if response := sendDecision(a, "", exactRequest); response.Code != http.StatusForbidden || writes != 0 || reviews != 2 {
		t.Fatal("missing token reached the review or decision operation")
	}
	response := sendDecision(a, token, exactRequest)
	var identity DecisionIdentity
	if response.Code != http.StatusOK || writes != 1 || reviews != 3 || json.Unmarshal(response.Body.Bytes(), &identity) != nil ||
		identity.IntentID != "intent" || identity.Subject == nil || *identity.Subject != episode.subject() ||
		strings.Contains(response.Body.String(), token) || strings.Contains(response.Body.String(), "grant_token") {
		t.Fatal("successful response did not contain only the authenticated decision identity")
	}
	lookup := `{"workflow_id":"workflow","node_id":"root","iteration":0,"proposal_hash":"hash"}`
	response = sendDecision(a, token, lookup)
	if response.Code != http.StatusOK || writes != 2 || reviews != 4 || !strings.Contains(response.Body.String(), `"decision":"PENDING"`) {
		t.Fatal("approver could not look up a pending identity using the sealed plan tuple and hash")
	}
	response = sendDecision(a, token, strings.TrimSuffix(lookup, "}")+`,"decision":"APPROVED"}`)
	if response.Code != http.StatusForbidden || writes != 2 || reviews != 5 {
		t.Fatal("decision without the exact intent identity reached the writer")
	}
}

func TestK6TokenWindow(t *testing.T) {
	episode, now, _, _ := identityFixture()
	deadline := episode.LastPlannedArrival.Add(episode.TwoL)
	for _, expiry := range []time.Time{deadline.Add(-time.Second), deadline} {
		if ValidateTokenWindow(now, episode.WarmupStart, expiry, episode.WarmupStart, episode.LastPlannedArrival, episode.TwoL) == nil {
			t.Fatal("insufficient token lifetime was accepted")
		}
	}
	if ValidateTokenWindow(now, episode.WarmupStart.Add(-time.Second), deadline.Add(time.Second), episode.WarmupStart,
		episode.LastPlannedArrival, episode.TwoL) == nil {
		t.Fatal("early token was accepted")
	}
	if err := ValidateTokenWindow(now, episode.WarmupStart, deadline.Add(time.Second), episode.WarmupStart,
		episode.LastPlannedArrival, episode.TwoL); err != nil {
		t.Fatal(err)
	}
}

func TestK6TokenWindowFractionalWarmupStart(t *testing.T) {
	episode, now, _, _ := identityFixture()
	warmupStart := episode.WarmupStart.Add(500 * time.Millisecond)
	deadline := episode.LastPlannedArrival.Add(episode.TwoL)

	// A token issued 0.2 seconds after warmup has this whole-second JWT iat.
	if err := ValidateTokenWindow(now, time.Unix(warmupStart.Unix(), 0), deadline.Add(time.Second),
		warmupStart, episode.LastPlannedArrival, episode.TwoL); err != nil {
		t.Fatalf("token issued 0.2 seconds after fractional warmup start was refused: %v", err)
	}
	if err := ValidateTokenWindow(now, episode.WarmupStart.Add(-time.Second), deadline.Add(time.Second),
		warmupStart, episode.LastPlannedArrival, episode.TwoL); err == nil {
		t.Fatal("token issued one second before the truncated warmup second was accepted")
	}
}

func TestK6AdapterRedactsCredentials(t *testing.T) {
	episode, now, claims, review := identityFixture()
	token := jwt(t, claims)
	var captured strings.Builder
	previous := log.Writer()
	log.SetOutput(&captured)
	t.Cleanup(func() { log.SetOutput(previous) })
	for _, failReview := range []bool{true, false} {
		writes := 0
		a, err := NewAdapter(episode, reviewFunc(func(context.Context, string) (Review, error) {
			if failReview {
				return Review{}, errors.New("lost reviewer access: " + token)
			}
			return review, nil
		}), writeFunc(func(context.Context, DecisionRequest, string) (DecisionIdentity, error) {
			writes++
			return DecisionIdentity{}, errors.New("driver error: " + token)
		}))
		if err != nil {
			t.Fatal(err)
		}
		a.now = func() time.Time { return now }
		response := sendDecision(a, token, exactRequest)
		if response.Code != http.StatusForbidden || response.Body.String() != ErrDenied.Error()+"\n" ||
			strings.Contains(captured.String(), token) || (failReview && writes != 0) || (!failReview && writes != 1) {
			t.Fatal("denial leaked a credential or lost reviewer access reached the decision operation")
		}
	}
}

func TestK6PrivateUnixSocket(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix permission bits are required for the runner socket")
	}
	episode, _, _, _ := identityFixture()
	a, err := NewAdapter(episode, reviewFunc(func(context.Context, string) (Review, error) {
		return Review{}, ErrDenied
	}), writeFunc(func(context.Context, DecisionRequest, string) (DecisionIdentity, error) {
		return DecisionIdentity{}, ErrDenied
	}))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	dir := filepath.Join(t.TempDir(), "private")
	path := filepath.Join(dir, "decision.sock")
	done := make(chan error, 1)
	go func() { done <- a.Serve(ctx, dir) }()
	client := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", path)
	}}}
	defer client.CloseIdleConnections()
	for {
		r, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://local"+operation, strings.NewReader(exactRequest))
		if err != nil {
			t.Fatal(err)
		}
		response, err := client.Do(r)
		if err == nil {
			response.Body.Close()
			if response.StatusCode != http.StatusForbidden {
				t.Fatal("unauthenticated socket traffic was accepted")
			}
			break
		}
		select {
		case serveErr := <-done:
			t.Fatalf("socket exited: %v", serveErr)
		case <-ctx.Done():
			t.Fatal("socket did not become ready")
		case <-time.After(time.Millisecond):
		}
	}
	for name, mode := range map[string]os.FileMode{dir: 0700, path: 0600} {
		info, err := os.Stat(name)
		if err != nil || info.Mode().Perm() != mode {
			t.Fatal("socket path lacks private permissions")
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal("private socket path remained after shutdown")
	}
}
