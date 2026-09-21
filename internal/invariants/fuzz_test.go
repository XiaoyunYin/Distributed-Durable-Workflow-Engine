package invariants

import (
	"bytes"
	"testing"
)

func FuzzParseFaultTraceDoesNotPanic(f *testing.F) {
	f.Add([]byte(`{"schema_version":"fault-trace.v1","run_id":"r","sequence":1,"event":"process_started","command":["fixture"]}`))
	f.Add([]byte(`not-json`))
	f.Add([]byte{0, 1, 2, 255})
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 1<<16 {
			return
		}
		_, _ = ParseFaultTrace(bytes.NewReader(raw))
	})
}
