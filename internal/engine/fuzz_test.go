package engine

import (
	"encoding/json"
	"testing"
)

func FuzzParseWorkflowGraphDoesNotPanic(f *testing.F) {
	f.Add([]byte(`{"entry":"root","nodes":[{"id":"root","kind":"success"}]}`))
	f.Add([]byte(`{"entry":"root","nodes":[{"id":"root","next":"missing"}]}`))
	f.Add([]byte{0, 1, 2, 255})
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 1<<16 {
			return
		}
		_, _ = ParseGraph(json.RawMessage(raw))
	})
}
