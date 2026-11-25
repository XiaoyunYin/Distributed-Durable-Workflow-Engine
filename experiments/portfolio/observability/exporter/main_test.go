package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
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
