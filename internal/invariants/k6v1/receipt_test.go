package k6v1

import (
	"encoding/json"
	"os"
	"slices"
	"testing"
)

type receiptFixture struct {
	Key, WrongKey, Apply, Lookup, NonCanonical string
	Claim                                      receiptClaim
	Receipt                                    signedReceiptBody
}

func readReceiptFixture(t *testing.T) receiptFixture {
	t.Helper()
	var fixture receiptFixture
	raw, err := os.ReadFile("testdata/receipt-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := DecodeStrict(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	return fixture
}

func TestK6ReceiptSignatureVectors(t *testing.T) {
	fixture := readReceiptFixture(t)
	changed := fixture.Receipt
	changed.ResourceID = "resourcf"
	for _, tc := range []struct {
		name, publicKey, operation, signature string
		body                                  signedReceiptBody
		valid                                 bool
	}{
		{"Apply", fixture.Key, "apply", fixture.Apply, fixture.Receipt, true},
		{"Lookup", fixture.Key, "lookup", fixture.Lookup, fixture.Receipt, true},
		{"WrongKey", fixture.WrongKey, "apply", fixture.Apply, fixture.Receipt, false},
		{"ChangedByte", fixture.Key, "apply", fixture.Apply, changed, false},
		{"DifferentOperation", fixture.Key, "lookup", fixture.Apply, fixture.Receipt, false},
		{"NonCanonical", fixture.Key, "apply", fixture.NonCanonical, fixture.Receipt, false},
		{"MissingSignature", fixture.Key, "apply", "", fixture.Receipt, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key := decodeReceiptKey(tc.publicKey)
			if got := verifyReceiptSignature(key, fixture.Claim, tc.operation, tc.body, tc.signature); got != tc.valid {
				t.Fatalf("receipt signature verdict = %t, want %t", got, tc.valid)
			}
		})
	}
}

func TestK6PredicateReceiptSignature(t *testing.T) {
	fixture := readReceiptFixture(t)
	for _, tc := range []struct {
		name, reason string
		change       func(*Manifest, Row, map[string]any)
		kind         FindingKind
	}{
		{"Valid", "", func(*Manifest, Row, map[string]any) {}, ""},
		{"SignatureRemoved", "required_proof_missing:ReceiptSignature", func(_ *Manifest, _ Row, proof map[string]any) { delete(proof, "signature") }, MissingProof},
		{"ProofRemoved", "required_proof_missing:ReceiptSignature", func(_ *Manifest, row Row, _ map[string]any) { delete(row, "k6_proof") }, MissingProof},
		{"FenceRemoved", "required_proof_missing:ReceiptSignature", func(_ *Manifest, row Row, _ map[string]any) { delete(row, "k6_fence") }, MissingProof},
		{"KeyRemoved", "required_proof_missing:receipt_public_key", func(m *Manifest, _ Row, _ map[string]any) { m.ReceiptPublicKey = "" }, MissingProof},
		{"NonCanonical", "RECEIPT_SIGNATURE_INVALID", func(_ *Manifest, _ Row, proof map[string]any) { proof["signature"] = fixture.NonCanonical }, Violation},
		{"WrongAttempt", "RECEIPT_SIGNATURE_WRONG_ATTEMPT", func(_ *Manifest, row Row, _ map[string]any) { row["workflow_id"] = "other" }, Violation},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := Manifest{Cell: "medium-12", Workload: "COOPERATING_EFFECT", Sink: &DatabaseIdentity{}, ReceiptPublicKey: fixture.Key}
			proof := map[string]any{"operation": "apply", "receipt": fixture.Receipt, "signature": fixture.Apply}
			row := Row{"state": "SUCCEEDED", "effect_class": "COOPERATING_EFFECT", "workflow_id": fixture.Receipt.WorkflowID,
				"logical_effect_key": fixture.Receipt.LogicalKey, "node_id": fixture.Claim.NodeID,
				"iteration": json.Number("2"), "attempt_number": json.Number("3"), "k6_fence": json.Number("4"), "k6_proof": proof}
			tc.change(&m, row, proof)
			r := evaluate(m, Snapshot{}, map[string][]Row{activityAttempts: {row}}, Plan{}, Arrivals{}, false, false, "", nil)
			if tc.reason == "" {
				if r.ExitCode() != Incomplete || len(r.Findings) != 0 {
					t.Fatalf("receipt finding: %+v", r.Findings)
				}
			} else if r.ExitCode() != Fail || !slices.ContainsFunc(r.Findings, func(f Finding) bool {
				return f.Predicate == "ReceiptSignature" && f.Code == tc.reason && f.Kind == tc.kind && f.Witness != nil
			}) {
				t.Fatalf("receipt finding: %+v", r.Findings)
			}
		})
	}
}
