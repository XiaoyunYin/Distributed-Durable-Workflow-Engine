package invariants

import (
	"crypto/sha256"
	"encoding/json"
	"testing"
)

func TestSnapshotHashMatchesLegacyMarshal(t *testing.T) {
	rows := []json.RawMessage{
		json.RawMessage(` { "<>&": "\\\"/<tag>&\n\t\b\u003c", "nested": [null, {"b":true, "a":[1,2]}] } `),
		json.RawMessage("\"\u2028\u2029\""),
		json.RawMessage(`-123456789012345678901234567890.123456789e+12345`),
		json.RawMessage(` 42 `),
		nil,
	}
	digest := newSnapshotHash()
	for _, tc := range []struct {
		name string
		rows []json.RawMessage
	}{
		{"escaped-nested-and-large", rows},
		{"empty", []json.RawMessage{}},
		{"null", nil},
		{"reuse-after-empty", rows},
	} {
		t.Run(tc.name, func(t *testing.T) {
			legacy, err := json.Marshal(tc.rows)
			if err != nil {
				t.Fatal(err)
			}
			digest.start(tc.rows == nil)
			for _, row := range tc.rows {
				if err := digest.add(row); err != nil {
					t.Fatal(err)
				}
			}
			if got, want := digest.finish(tc.rows == nil), sha256.Sum256(legacy); got != want {
				t.Fatalf("digest %x, want %x for %s", got, want, legacy)
			}
		})
	}
}

func TestSnapshotHashRejectsInvalidRawRows(t *testing.T) {
	for _, row := range []json.RawMessage{json.RawMessage{}, json.RawMessage(`{} []`), json.RawMessage(`{"key":`)} {
		_, wantErr := json.Marshal([]json.RawMessage{row})
		digest := newSnapshotHash()
		digest.start(false)
		if gotErr := digest.add(row); wantErr == nil || gotErr == nil || gotErr.Error() != wantErr.Error() {
			t.Fatalf("row %q returned %v, want %v", row, gotErr, wantErr)
		}
	}
}
