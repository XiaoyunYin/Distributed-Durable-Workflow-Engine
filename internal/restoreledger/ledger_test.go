package restoreledger

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func testHeader() Header {
	digest := SHA256([]byte("fixture"))
	return Header{
		Schema: Schema, Campaign: "campaign", Drill: "R1", RunID: 19, RunAttempt: 1, BootID: "boot",
		Source: "primary", Target: "standby", Revision: strings.Repeat("a", 40),
		WorkloadSHA256: digest, TopologySHA256: digest, PolicySHA256: digest,
	}
}

func newTestWriter(t *testing.T) (*Writer, string) {
	t.Helper()
	base := t.TempDir()
	path := filepath.Join(base, "journal.jsonl")
	var ns int64 = 1000
	clock := func() (int64, error) { ns++; return ns, nil }
	w, err := create(path, testHeader(), clock, func(dir string) error {
		if dir != base {
			t.Fatalf("sync wrong directory %q", dir)
		}
		if raw, err := os.ReadFile(path); err != nil || !bytes.Contains(raw, []byte(Schema)) {
			t.Fatalf("header not written before directory sync: %s, %v", raw, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return w, path
}

func TestLedgerRetainsAcknowledgmentsAndUncertainOperations(t *testing.T) {
	w, path := newTestWriter(t)
	if _, err := create(path, testHeader(), w.clock, func(string) error { return nil }); err == nil {
		t.Fatal("second writer reopened the journal")
	}
	request := Operation{
		RequestID: "offer-1", Kind: Submit, Phase: "markers", Authority: "primary",
		WorkflowID: "workflow", Namespace: "fixture", SubmissionKey: "key", RequestSHA256: SHA256([]byte("request")),
	}
	send, err := w.RecordSend(request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.RecordSend(request); err == nil {
		t.Fatal("duplicate request was recorded")
	}
	ack := Outcome{
		RequestID: request.RequestID, Disposition: Acknowledged, SentNS: send, CompletedNS: send,
		HTTPStatus: 201, ResponseSHA256: SHA256([]byte("reply")), DurableID: request.WorkflowID,
	}
	synced, err := w.RecordOutcome(ack)
	if err != nil {
		t.Fatal(err)
	}
	request.RequestID, request.WorkflowID = "offer-2", "other-workflow"
	secondSend, err := w.RecordSend(request)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.RecordWitness(Witness{
		Kind: StateProbe, Phase: "markers", Authority: "primary", RequestID: request.RequestID,
		StartedNS: secondSend, CompletedNS: secondSend, EvidenceSHA: SHA256([]byte("workflow exists")),
	}); err != nil {
		t.Fatal(err)
	}
	digest, err := w.Seal()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.RecordSend(Operation{}); err == nil {
		t.Fatal("appended after sealing")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	report, err := Read(bytes.NewReader(raw), digest, false)
	if err != nil || !report.Sealed || report.Digest != SHA256(raw) || report.Header != testHeader() ||
		len(report.Exchanges) != 2 || len(report.Witnesses) != 1 {
		t.Fatalf("replayed ledger = %+v, %v", report, err)
	}
	if !report.Exchanges[0].Acked() || report.Exchanges[0].OutcomeSync != synced ||
		report.Exchanges[1].Acked() || report.Exchanges[1].Disposition() != InFlight {
		t.Fatalf("unexpected receipt disposition: %+v", report.Exchanges)
	}
}

func TestLedgerRejectsAlteredOrIncompleteEvidence(t *testing.T) {
	w, path := newTestWriter(t)
	op := Operation{RequestID: "takeover", Kind: AcquireLease, Phase: "promotion", Authority: "standby",
		RequestSHA256: SHA256([]byte("acquire"))}
	sent, err := w.RecordSend(op)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.RecordOutcome(Outcome{
		RequestID: op.RequestID, SentNS: sent, CompletedNS: sent, Disposition: Acknowledged,
		DurableID: "owner", Lease: &LeaseRef{PartitionID: 9, OwnerID: "same-owner", Epoch: 21},
		ResponseSHA256: SHA256([]byte("lease response")),
	}); err != nil {
		t.Fatal(err)
	}
	digest, err := w.Seal()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := bytes.SplitAfter(raw, []byte("\n"))
	for _, tc := range []struct {
		name   string
		body   []byte
		digest string
	}{
		{"changed identity", bytes.Replace(raw, []byte("same-owner"), []byte("new-owner"), 1), ""},
		{"removed send", bytes.Join(append(append([][]byte{}, lines[:1]...), lines[2:]...), nil), ""},
		{"partial final record", raw[:len(raw)-1], ""},
		{"missing seal", bytes.Join(lines[:len(lines)-2], nil), ""},
		{"wrong retained digest", raw, strings.Repeat("1", 64)},
		{"trailing duplicate", append(bytes.Clone(raw), lines[0]...), ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Read(bytes.NewReader(tc.body), tc.digest, false); err == nil {
				t.Fatalf("accepted %s", tc.name)
			}
		})
	}
	// A crash after recording a response but before recording its fsync
	// witness retains the observed response without counting the ack.
	prefix, err := Read(bytes.NewReader(bytes.Join(lines[:4], nil)), "", true)
	if err != nil || prefix.Sealed || len(prefix.Exchanges) != 1 || prefix.Exchanges[0].Outcome == nil ||
		prefix.Exchanges[0].Acked() || prefix.Exchanges[0].Disposition() != InFlight {
		t.Fatalf("interrupted response = %+v, %v", prefix, err)
	}
	report, err := Read(bytes.NewReader(raw), digest, false)
	if err != nil || report.Exchanges[0].Outcome.Lease.OwnerID != "same-owner" || !report.Exchanges[0].Acked() {
		t.Fatalf("J10 takeover receipt = %+v, %v", report, err)
	}
}

func TestLedgerObservesWALOnSeparateAuthorities(t *testing.T) {
	w, _ := newTestWriter(t)
	now, err := w.NowNS()
	if err != nil {
		t.Fatal(err)
	}
	for _, position := range []string{"primary_flush", "standby_receive", "standby_flush", "standby_replay"} {
		authority := "standby"
		if position == "primary_flush" {
			authority = "primary"
		}
		err := w.RecordWitness(Witness{
			Kind: WALObservation, Phase: "promotion", Authority: authority, StartedNS: now,
			CompletedNS: now, EvidenceSHA: SHA256([]byte(position)), Timeline: 1,
			LSN: "0/12AB", Position: position,
		})
		if err != nil {
			t.Fatalf("record %s: %v", position, err)
		}
	}
	cut := Witness{
		Kind: ArchiveCut, Phase: "markers", Authority: "primary", StartedNS: now, CompletedNS: now,
		EvidenceSHA: SHA256([]byte("media")), Timeline: 1, LSN: "0/1000028",
		Segment: "000000010000000000000001", EndLSN: "0/2000000",
	}
	if err := w.RecordWitness(cut); err != nil {
		t.Fatal(err)
	}
	cut.EndLSN = "0/3000000"
	if err := w.RecordWitness(cut); err == nil {
		t.Fatal("accepted a mismatched exclusive segment end")
	}
}
