package main

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestK4APIRequestBoundary(t *testing.T) {
	t.Setenv("DUR053_K4_CAMPAIGN", "1")
	t.Setenv("DUR053_K4_HOLD_FILE", "")
	t.Setenv("RUNTIME_ROLE", "api")
	old := slog.Default()
	defer slog.SetDefault(old)
	var logs bytes.Buffer
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	called := false
	h := k4Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true; w.WriteHeader(202) }))
	response := httptest.NewRecorder()
	h.ServeHTTP(response, httptest.NewRequest("POST", "/v1/workflows", nil))
	if !called || response.Code != 202 || strings.Count(logs.String(), "acknowledgment") != 2 {
		t.Fatal("K4 API boundary changed response or omitted acknowledgment")
	}
	logs.Reset()
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/healthz", nil))
	if logs.Len() != 0 {
		t.Fatal("K4 health request was mistaken for workload")
	}
	t.Setenv("DUR053_K4_CAMPAIGN", "0")
	h = k4Handler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", "/v1/workflows", nil))
	if logs.Len() != 0 {
		t.Fatal("K4 API trace emitted with flag off")
	}
}
