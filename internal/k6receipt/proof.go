// Package k6receipt defines the sink's signed attestation of a committed receipt.
package k6receipt

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
)

const SignatureHeader = "X-Dur053-K6-Receipt-Signature"
const ProtocolHeader = "X-Dur053-K6-Receipt-Version"

type Receipt struct {
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

type Proof struct {
	Operation string  `json:"operation"`
	Receipt   Receipt `json:"receipt"`
	Signature string  `json:"signature"`
}

type Claim struct {
	NodeID    string
	Iteration int
	Attempt   int64
	Fence     int64
}

func message(claim Claim, operation string, receipt Receipt) []byte {
	body, _ := json.Marshal(struct {
		Claim     Claim
		Operation string
		Receipt   Receipt
	}{claim, operation, receipt})
	return append([]byte("dur053-k6-sink-receipt/v1\x00"), body...)
}

func Sign(key ed25519.PrivateKey, claim Claim, operation string, receipt Receipt) string {
	return base64.RawURLEncoding.EncodeToString(ed25519.Sign(key, message(claim, operation, receipt)))
}

func Verify(key ed25519.PublicKey, claim Claim, proof Proof) bool {
	if len(key) != ed25519.PublicKeySize || (proof.Operation != "apply" && proof.Operation != "lookup") ||
		len(proof.Signature) != base64.RawURLEncoding.EncodedLen(ed25519.SignatureSize) {
		return false
	}
	signature, err := base64.RawURLEncoding.Strict().DecodeString(proof.Signature)
	return err == nil && ed25519.Verify(key, message(claim, proof.Operation, proof.Receipt), signature)
}
