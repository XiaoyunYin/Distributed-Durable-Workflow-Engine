package invariants

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"hash"
	"reflect"
)

// snapshotHash hashes the same compact, HTML-escaped JSON array as
// json.Marshal([]json.RawMessage), using scratch space for a single row.
type snapshotHash struct {
	sum     hash.Hash
	compact bytes.Buffer
	escaped bytes.Buffer
	first   bool
}

func newSnapshotHash() *snapshotHash {
	return &snapshotHash{sum: sha256.New()}
}

func (h *snapshotHash) start(null bool) {
	h.sum.Reset()
	h.first = true
	if null {
		h.sum.Write([]byte("null"))
	} else {
		h.sum.Write([]byte{'['})
	}
}

func (h *snapshotHash) add(row json.RawMessage) error {
	if !h.first {
		h.sum.Write([]byte{','})
	}
	h.first = false
	if row == nil {
		h.sum.Write([]byte("null"))
		return nil
	}
	h.compact.Reset()
	if err := json.Compact(&h.compact, row); err != nil {
		return &json.MarshalerError{Type: reflect.TypeFor[*json.RawMessage](), Err: err}
	}
	h.escaped.Reset()
	json.HTMLEscape(&h.escaped, h.compact.Bytes())
	h.sum.Write(h.escaped.Bytes())
	return nil
}

func (h *snapshotHash) finish(null bool) [sha256.Size]byte {
	if !null {
		h.sum.Write([]byte{']'})
	}
	var sum [sha256.Size]byte
	h.sum.Sum(sum[:0])
	return sum
}
