package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestDemandRequiresFreshScopedAuthenticatedRead(t *testing.T) {
	for _, tc := range []struct {
		name, scope, token string
		age                time.Duration
		fields             string
		ok                 bool
	}{
		{"fresh idle", "target", "good", time.Second, `"claimable_attempts":0,"active_claims":0,"oldest_claimable_age_seconds":0`, true},
		{"other scope", "foreign", "good", time.Second, `"claimable_attempts":0,"active_claims":0,"oldest_claimable_age_seconds":0`, false},
		{"wrong token", "target", "bad", time.Second, `"claimable_attempts":0,"active_claims":0,"oldest_claimable_age_seconds":0`, false},
		{"expired", "target", "good", 6 * time.Second, `"claimable_attempts":0,"active_claims":0,"oldest_claimable_age_seconds":0`, false},
		{"missing field", "target", "good", time.Second, `"claimable_attempts":0,"oldest_claimable_age_seconds":0`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "GET" || r.URL.Path != "/v1/demand" || r.Header.Get("Authorization") != "Bearer good" {
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				fmt.Fprintf(w, `{"namespace":%q,"sampled_at":%q,%s}`,
					tc.scope, time.Now().UTC().Add(-tc.age).Format(time.RFC3339Nano), tc.fields)
			}))
			defer server.Close()
			d, err := readDemand(context.Background(), server.Client(), server.URL+"/v1/demand", tc.token, "target")
			if (err == nil && d != nil) != tc.ok {
				t.Fatalf("demand=%+v, err=%v, valid=%v", d, err, tc.ok)
			}
		})
	}
}

func TestVisibilityDelta(t *testing.T) {
	d := delta(map[string]int64{"gone": 1, "kept": 1, "new-revision": 2},
		map[string]int64{"kept": 1, "new-revision": 3, "added": 1})
	if len(d.Set) != 2 || d.Set["new-revision"] != 3 || d.Set["added"] != 1 ||
		len(d.Delete) != 1 || d.Delete[0] != "gone" {
		t.Fatalf("delta=%+v", d)
	}
}

func TestFailedCycleRetainsSafeTiming(t *testing.T) {
	var logs bytes.Buffer
	output := log.Writer()
	t.Cleanup(func() { log.SetOutput(output) })
	log.SetOutput(&logs)
	s := sampler{}
	if err := s.collect(context.Background(), func(context.Context) (*projection, error) {
		return nil, &pgconn.PgError{Code: "57014", Message: "secret details"}
	}, func(context.Context) (*demand, error) {
		return nil, context.DeadlineExceeded
	}); err != nil {
		t.Fatal(err)
	}
	text := logs.String()
	_, payload, found := strings.Cut(text, " J3_CYCLE ")
	var timing cycleTiming
	if !found || strings.Contains(text, "secret details") || json.Unmarshal([]byte(payload), &timing) != nil {
		t.Fatalf("unexpected cycle diagnostics: %s", text)
	}
	if timing.StartedNS == 0 || timing.SQLFailure != "postgres_57014" || timing.DemandFailure != "deadline" ||
		timing.CycleNS < timing.SQLNS+timing.DemandNS {
		t.Fatalf("unexpected timing: %+v", timing)
	}
}
