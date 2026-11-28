package restoreledger

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// Read verifies the canonical UTF-8 encoding, row chain, operation joins and
// seal before exposing evidence. Supply the independently retained digest for
// binding a complete artifact. An unsealed prefix is only for failure triage.
func Read(input io.Reader, expectedDigest string, allowUnsealed bool) (Report, error) {
	reader := bufio.NewReaderSize(input, maxRowBytes)
	journal := newJournal()
	for {
		raw, err := reader.ReadSlice('\n')
		if errors.Is(err, io.EOF) && len(raw) == 0 {
			break
		}
		if err != nil {
			return Report{}, fmt.Errorf("read ledger row %d: %w", journal.rows+1, err)
		}
		var record row
		if err := json.Unmarshal(raw, &record); err != nil {
			return Report{}, fmt.Errorf("decode ledger row %d: %w", journal.rows+1, err)
		}
		canonical, err := encode(record)
		if err != nil || !bytes.Equal(canonical, raw) {
			return Report{}, fmt.Errorf("noncanonical ledger row %d", journal.rows+1)
		}
		if err := journal.accept(record, raw); err != nil {
			return Report{}, fmt.Errorf("ledger row %d: %w", record.Number, err)
		}
	}
	if journal.rows == 0 || (!allowUnsealed && !journal.report.Sealed) {
		return Report{}, errors.New("ledger header or required final seal missing")
	}
	journal.report.Digest = journal.digest()
	if expectedDigest != "" && expectedDigest != journal.report.Digest {
		return Report{}, errors.New("ledger digest differs from the retained handoff")
	}
	return journal.report, nil
}
