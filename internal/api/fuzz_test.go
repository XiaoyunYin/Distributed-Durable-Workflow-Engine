package api

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
)

func FuzzWorkflowJSONDoesNotPanic(f *testing.F) {
	f.Add([]byte(`{"submission_key":"k","payload":{},"definition_id":"d","definition_version":1,"initial_node_id":"root"}`))
	f.Add([]byte(`{"submission_key":"k","payload":{"a":1,"a":2}}`))
	f.Add([]byte{0, 1, 2, 255})
	f.Fuzz(func(t *testing.T, body []byte) {
		if len(body) > 1<<16 {
			return
		}
		handler := NewServer(&fakeRepository{}).Handler()
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/v1/workflows", bytes.NewReader(body))
		handler.ServeHTTP(recorder, request)
		if recorder.Code < 200 || recorder.Code >= 600 {
			t.Fatalf("invalid HTTP status %d", recorder.Code)
		}
	})
}
