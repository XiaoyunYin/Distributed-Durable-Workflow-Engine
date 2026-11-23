package k6v1

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
)

const activityAttempts = "engine.activity_attempts"

// This wire format is separate from the public bundle's sorted JSON encoding.
// Field order and Go's default JSON string escaping are part of receipt v1.
type receiptClaim struct {
	NodeID    string
	Iteration int64
	Attempt   int64
	Fence     int64
}

type signedReceiptBody struct {
	WorkflowID       string `json:"workflow_id"`
	LogicalKey       string `json:"logical_key"`
	ResourceID       string `json:"resource_id"`
	CanonicalHash    string `json:"canonical_hash"`
	ScopeHash        string `json:"scope_hash"`
	IntentID         string `json:"intent_id"`
	ExpectedRevision int64  `json:"expected_revision"`
	AppliedRevision  int64  `json:"applied_revision"`
	ApplicationID    string `json:"application_id"`
}

func decodeReceiptKey(encoded string) ed25519.PublicKey {
	key, err := base64.StdEncoding.Strict().DecodeString(encoded)
	if err != nil || len(key) != ed25519.PublicKeySize || base64.StdEncoding.EncodeToString(key) != encoded {
		return nil
	}
	return ed25519.PublicKey(key)
}

func verifyReceiptSignature(key ed25519.PublicKey, claim receiptClaim, operation string, receipt signedReceiptBody, encoded string) bool {
	if len(key) != ed25519.PublicKeySize || (operation != "apply" && operation != "lookup") ||
		len(encoded) != base64.RawURLEncoding.EncodedLen(ed25519.SignatureSize) {
		return false
	}
	signature, err := base64.RawURLEncoding.Strict().DecodeString(encoded)
	if err != nil {
		return false
	}
	body, _ := json.Marshal(struct {
		Claim     receiptClaim
		Operation string
		Receipt   signedReceiptBody
	}{claim, operation, receipt})
	return ed25519.Verify(key, append([]byte("dur053-k6-sink-receipt/v1\x00"), body...), signature)
}

func (r *Result) checkReceiptSignatures(m Manifest, attempts []Row, witness func(string, Row) Witness) {
	const predicate = "ReceiptSignature"
	key := decodeReceiptKey(m.ReceiptPublicKey)
	for _, row := range attempts {
		proof, ok := row["k6_proof"].(map[string]any)
		if !ok {
			if row["effect_class"] == "COOPERATING_EFFECT" && row["state"] == "SUCCEEDED" {
				ref := witness(activityAttempts, row)
				r.record(MissingProof, predicate, "required_proof_missing:ReceiptSignature", ref.Path, &ref)
			}
			continue
		}
		ref := witness(activityAttempts, row)
		sig, _ := proof["signature"].(string)
		switch {
		case sig == "" || row["k6_fence"] == nil:
			r.record(MissingProof, predicate, "required_proof_missing:ReceiptSignature", ref.Path, &ref)
		case key == nil:
			r.record(MissingProof, predicate, "required_proof_missing:receipt_public_key", ManifestPath, &ref)
		default:
			var body signedReceiptBody
			raw, _ := json.Marshal(proof["receipt"])
			// Row projection has already required every receipt field and type.
			claim := receiptClaim{row["node_id"].(string), number(row["iteration"]), number(row["attempt_number"]), number(row["k6_fence"])}
			if json.Unmarshal(raw, &body) != nil || !verifyReceiptSignature(key, claim, proof["operation"].(string), body, sig) {
				r.record(Violation, predicate, "RECEIPT_SIGNATURE_INVALID", ref.Path, &ref)
			} else if body.WorkflowID != row["workflow_id"] || body.LogicalKey != row["logical_effect_key"] {
				r.record(Violation, predicate, "RECEIPT_SIGNATURE_WRONG_ATTEMPT", ref.Path, &ref)
			}
		}
	}
}
