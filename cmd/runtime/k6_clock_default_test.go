//go:build !dur053_k6

package main

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestK6ClockAbsentFromDefaultBuild(t *testing.T) {
	t.Setenv("DUR053_K6_CLOCK", "1")
	handler := lifecycleHandler("scheduler", nil, nil, nil, &atomic.Bool{})
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/internal/k6/clock", nil))
	if response.Code != http.StatusNotFound {
		t.Fatalf("default runtime exposes campaign clock: %d", response.Code)
	}
}
