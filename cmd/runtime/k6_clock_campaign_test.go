//go:build dur053_k6

package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

func TestK6ClockOnSchedulerListener(t *testing.T) {
	t.Setenv("DUR053_K6_CLOCK", "1")
	t.Setenv("POD_UID", "test-uid")
	for _, role := range []string{"scheduler", "api"} {
		handler := lifecycleHandler(role, nil, nil, nil, &atomic.Bool{})
		response := httptest.NewRecorder()
		before := time.Now().UnixNano()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/internal/k6/clock", nil))
		if role == "api" {
			if response.Code != http.StatusNotFound {
				t.Fatalf("tagged API exposes campaign clock: %d", response.Code)
			}
			continue
		}
		if runtime.GOOS != "linux" {
			if response.Code != http.StatusServiceUnavailable {
				t.Fatalf("non-Linux clock handler must fail process identity: %d", response.Code)
			}
			continue
		}
		var sample struct {
			Wall  int64  `json:"wall_ns"`
			PID   int    `json:"pid"`
			Start string `json:"process_start_id"`
			Boot  string `json:"boot_id"`
			UID   string `json:"pod_uid"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &sample); err != nil {
			t.Fatal(err)
		}
		if response.Code != http.StatusOK || sample.Wall < before || sample.Wall > time.Now().UnixNano() ||
			sample.PID != os.Getpid() || sample.UID != "test-uid" || sample.Start == "" || sample.Boot == "" {
			t.Fatalf("invalid in-process sample: %+v", sample)
		}
	}
}
