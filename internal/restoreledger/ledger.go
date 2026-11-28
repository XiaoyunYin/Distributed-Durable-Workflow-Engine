package restoreledger

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"time"
)

const maxRowBytes = 256 << 10

type row struct {
	Number    uint64     `json:"number"`
	Previous  string     `json:"previous_sha256"`
	Monotonic int64      `json:"monotonic_ns"`
	UTC       time.Time  `json:"utc"`
	Header    *Header    `json:"header,omitempty"`
	Send      *Operation `json:"send,omitempty"`
	Outcome   *Outcome   `json:"outcome,omitempty"`
	Synced    uint64     `json:"synced_row,omitempty"`
	Witness   *Witness   `json:"witness,omitempty"`
	Seal      *seal      `json:"seal,omitempty"`
}

type seal struct {
	Rows          uint64 `json:"rows"`
	ContentSHA256 string `json:"content_sha256"`
}

type Exchange struct {
	Operation   Operation
	SendSynced  int64
	Outcome     *Outcome
	OutcomeSync int64
	sendRow     uint64
	outcomeRow  uint64
}

// Acked requires both a successful observed response and durable fsync proof.
func (x Exchange) Acked() bool {
	return x.Disposition() == Acknowledged
}

func (x Exchange) Disposition() string {
	if x.Outcome == nil || x.OutcomeSync == 0 {
		return InFlight
	}
	return x.Outcome.Disposition
}

type RecordedWitness struct {
	Witness
	RecordedNS int64
}

// Report presents sends in journal order, including incomplete and ambiguous
// ones. Seal and Digest describe the evidence, not drill acceptance.
type Report struct {
	Header    Header
	Exchanges []Exchange
	Witnesses []RecordedWitness
	Sealed    bool
	Digest    string
}

type journal struct {
	report   Report
	hash     hash.Hash
	previous string
	rows     uint64
	lastNS   int64
	requests map[string]int
}

func newJournal() *journal {
	return &journal{hash: sha256.New(), requests: make(map[string]int)}
}

func (j *journal) digest() string { return hex.EncodeToString(j.hash.Sum(nil)) }

func (j *journal) accept(r row, raw []byte) error {
	if j.report.Sealed || r.Number != j.rows+1 || r.Previous != j.previous ||
		r.Monotonic <= 0 || r.Monotonic < j.lastNS || r.UTC.IsZero() || r.UTC.Location() != time.UTC {
		return errors.New("invalid ledger chain, clock or record after seal")
	}
	count := 0
	for _, present := range []bool{r.Header != nil, r.Send != nil, r.Outcome != nil,
		r.Synced != 0, r.Witness != nil, r.Seal != nil} {
		if present {
			count++
		}
	}
	if count != 1 || (r.Header != nil) != (r.Number == 1) {
		return errors.New("invalid ledger row payload")
	}
	switch {
	case r.Header != nil:
		if !r.Header.valid() {
			return errors.New("invalid ledger header")
		}
		j.report.Header = *r.Header
	case r.Send != nil:
		if _, exists := j.requests[r.Send.RequestID]; exists || !r.Send.valid(j.report.Header) {
			return errors.New("duplicate request ID or invalid send intent")
		}
		j.requests[r.Send.RequestID] = len(j.report.Exchanges)
		j.report.Exchanges = append(j.report.Exchanges, Exchange{Operation: *r.Send, sendRow: r.Number})
	case r.Outcome != nil:
		x := j.exchange(r.Outcome.RequestID)
		if x == nil || x.Outcome != nil || !r.Outcome.valid(x.Operation, x.SendSynced, r.Monotonic) {
			return errors.New("unmatched, duplicate or invalid outcome")
		}
		x.Outcome, x.outcomeRow = r.Outcome, r.Number
	case r.Synced != 0:
		// Sync witnesses immediately follow their associated send/outcome.
		if r.Synced != r.Number-1 {
			return errors.New("fsync witness must follow its own row")
		}
		if err := j.recordSync(r); err != nil {
			return err
		}
	case r.Witness != nil:
		if !r.Witness.valid(j.report.Header, r.Monotonic) ||
			(r.Witness.RequestID != "" && j.exchange(r.Witness.RequestID) == nil) {
			return errors.New("invalid or unmatched independent witness")
		}
		j.report.Witnesses = append(j.report.Witnesses, RecordedWitness{*r.Witness, r.Monotonic})
	case r.Seal != nil:
		if r.Seal.Rows != j.rows || r.Seal.ContentSHA256 != j.digest() {
			return errors.New("ledger seal does not match the prefix")
		}
		j.report.Sealed = true
	}
	j.rows, j.lastNS, j.previous = r.Number, r.Monotonic, SHA256(raw)
	_, _ = j.hash.Write(raw)
	return nil
}

func (j *journal) exchange(requestID string) *Exchange {
	index, ok := j.requests[requestID]
	if !ok {
		return nil
	}
	return &j.report.Exchanges[index]
}

func (j *journal) recordSync(r row) error {
	for i := len(j.report.Exchanges) - 1; i >= 0; i-- {
		x := &j.report.Exchanges[i]
		switch r.Synced {
		case x.sendRow:
			x.SendSynced = r.Monotonic
			return nil
		case x.outcomeRow:
			x.OutcomeSync = r.Monotonic
			return nil
		}
	}
	return errors.New("fsync witness has no matching send or outcome")
}

// Writer exclusively creates one journal. Call it from the host controller's
// serialized event loop. Close preserves an incomplete journal for diagnosis.
type Writer struct {
	file    *os.File
	journal *journal
	clock   Clock
	broken  bool
}

// Create binds the journal to the current Linux host boot. The path must live
// on controller storage independent of the source, target and archive.
func Create(path string, header Header) (*Writer, error) {
	clock, boot, err := HostClock()
	if err != nil {
		return nil, err
	}
	if header.BootID != boot {
		return nil, errors.New("ledger header does not match the host boot")
	}
	return create(path, header, clock, syncDirectory)
}

func create(path string, header Header, clock Clock, syncDir func(string) error) (*Writer, error) {
	if !filepath.IsAbs(path) || !header.valid() {
		return nil, errors.New("ledger requires an absolute path and a complete header")
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	w := &Writer{file: file, journal: newJournal(), clock: clock}
	if _, err := w.append(row{Header: &header}); err != nil {
		return nil, errors.Join(err, file.Close())
	}
	if err := syncDir(filepath.Dir(path)); err != nil {
		return nil, errors.Join(fmt.Errorf("sync ledger directory: %w", err), file.Close())
	}
	return w, nil
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}

func (w *Writer) Header() Header        { return w.journal.report.Header }
func (w *Writer) NowNS() (int64, error) { return w.clock() }
func (w *Writer) Close() error          { return w.file.Close() }

func (w *Writer) RecordSend(op Operation) (int64, error) {
	op.Lease, op.Claim = snapshot(op.Lease), snapshot(op.Claim)
	return w.appendWithSync(row{Send: &op})
}

func (w *Writer) RecordOutcome(outcome Outcome) (int64, error) {
	outcome.Lease, outcome.Claim = snapshot(outcome.Lease), snapshot(outcome.Claim)
	return w.appendWithSync(row{Outcome: &outcome})
}

func (w *Writer) RecordWitness(witness Witness) error {
	witness.Lease, witness.Claim = snapshot(witness.Lease), snapshot(witness.Claim)
	_, err := w.append(row{Witness: &witness})
	return err
}

func snapshot[T any](value *T) *T {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func (w *Writer) Seal() (string, error) {
	if _, err := w.append(row{Seal: &seal{Rows: w.journal.rows, ContentSHA256: w.journal.digest()}}); err != nil {
		return "", err
	}
	return w.journal.digest(), nil
}

func (w *Writer) appendWithSync(r row) (int64, error) {
	if _, err := w.append(r); err != nil {
		return 0, err
	}
	synced, err := w.append(row{Synced: w.journal.rows})
	if err != nil {
		w.broken = true
	}
	return synced, err
}

func (w *Writer) append(r row) (int64, error) {
	if w.broken {
		return 0, errors.New("ledger writer stopped after an uncertain write")
	}
	mono, err := w.clock()
	if err != nil {
		return 0, err
	}
	r.Number, r.Previous, r.Monotonic = w.journal.rows+1, w.journal.previous, mono
	r.UTC = time.Now().UTC()
	raw, err := encode(r)
	if err != nil {
		return 0, err
	}
	// Validate before touching the file. If a write or sync fails, this
	// writer must stop even if some of the attempted row remains visible.
	if err := w.journal.accept(r, raw); err != nil {
		return 0, err
	}
	if n, err := w.file.Write(raw); err != nil || n != len(raw) {
		w.broken = true
		return 0, errors.Join(err, io.ErrShortWrite)
	}
	if err := w.file.Sync(); err != nil {
		w.broken = true
		return 0, fmt.Errorf("sync ledger row %d: %w", r.Number, err)
	}
	return mono, nil
}

func encode(r row) ([]byte, error) {
	raw, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	if len(raw)+1 > maxRowBytes {
		return nil, errors.New("ledger row exceeds 256 KiB")
	}
	return append(raw, '\n'), nil
}
