package dur053

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestK3CampaignToken(t *testing.T) {
	token := filepath.Join(t.TempDir(), "token")
	want := "first"
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.Header.Get("Authorization") != "Bearer "+want {
			t.Error("campaign token not sent or rotated")
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()
	send := HTTPWithToken(server.Client(), server.URL, token)
	if _, _, err := send(context.Background(), []byte(`{}`)); err == nil || requests != 0 {
		t.Fatal("campaign unreadable token guard bypassed")
	}
	for _, value := range []string{"first", "rotated"} {
		want = value
		if err := os.WriteFile(token, []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
		if status, _, err := send(context.Background(), []byte(`{}`)); err != nil || status != 200 {
			t.Fatal(err, status)
		}
	}
	if err := os.WriteFile(token, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := send(context.Background(), []byte(`{}`)); err == nil || requests != 2 {
		t.Fatal("campaign empty token guard bypassed")
	}
}
