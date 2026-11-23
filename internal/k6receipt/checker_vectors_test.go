package k6receipt

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"os"
	"testing"
)

func TestCheckerReceiptVectors(t *testing.T) {
	const path = "../invariants/k6v1/testdata/receipt-v1.json"
	key := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{1}, ed25519.SeedSize))
	wrongKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{2}, ed25519.SeedSize))
	encode := base64.RawURLEncoding.EncodeToString
	claim := Claim{NodeID: "dur053.cooperating/v1", Iteration: 2, Attempt: 3, Fence: 4}
	body := Receipt{
		WorkflowID: "workflow", LogicalKey: "k6<&>", ResourceID: "resource",
		CanonicalHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		ScopeHash:     "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		IntentID:      "00000000-0000-0000-0000-000000000011", ExpectedRevision: 7, AppliedRevision: 8,
		ApplicationID: "00000000-0000-0000-0000-000000000012",
	}
	vectors := struct {
		Key          string
		WrongKey     string
		Claim        Claim
		Receipt      Receipt
		Apply        string
		Lookup       string
		NonCanonical string
	}{
		Key: base64.StdEncoding.EncodeToString(key.Public().(ed25519.PublicKey)), WrongKey: base64.StdEncoding.EncodeToString(wrongKey.Public().(ed25519.PublicKey)),
		Claim: claim, Receipt: body, Apply: Sign(key, claim, "apply", body), Lookup: Sign(key, claim, "lookup", body),
		// A trailing newline changes the bytes while preserving the JSON value.
		NonCanonical: encode(ed25519.Sign(key, append(message(claim, "apply", body), '\n'))),
	}
	got, err := json.MarshalIndent(vectors, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	got = append(got, '\n')
	if os.Getenv("DURABLE_UPDATE_RECEIPT_VECTORS") == "1" {
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("k6receipt checker vectors changed: %v", err)
	}
}
