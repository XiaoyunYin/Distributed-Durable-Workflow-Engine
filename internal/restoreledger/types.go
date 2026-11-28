// Package restoreledger records client acknowledgments and independent witnesses
// for the J7 restore and J10 promotion drills.
package restoreledger

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"durable-agent-execution-engine/internal/dur050"
)

const Schema = "j7.ack-ledger.v1"

// Clock is a monotonic nanosecond clock on the header's host and boot.
type Clock func() (int64, error)

func HostClock() (Clock, string, error) {
	if !dur050.HasSharedMonotonicClock() {
		return nil, "", errors.New("restore ledger requires the shared Linux monotonic clock")
	}
	raw, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return nil, "", err
	}
	boot := strings.TrimSpace(string(raw))
	if !identifier(boot) {
		return nil, "", errors.New("invalid host boot ID")
	}
	return dur050.SharedMonotonicNanoseconds, boot, nil
}

type Header struct {
	Schema         string `json:"schema"`
	Campaign       string `json:"campaign"`
	Drill          string `json:"drill"`
	RunID          uint64 `json:"run_id"`
	RunAttempt     uint32 `json:"run_attempt"`
	BootID         string `json:"boot_id"`
	Source         string `json:"source_instance"`
	Target         string `json:"target_instance"`
	Revision       string `json:"source_revision"`
	WorkloadSHA256 string `json:"workload_sha256"`
	TopologySHA256 string `json:"topology_sha256"`
	PolicySHA256   string `json:"policy_sha256"`
	MarkersSHA256  string `json:"markers_sha256,omitempty"`
}

func (h Header) valid() bool {
	return h.Schema == Schema && identifier(h.Campaign) && identifier(h.Drill) &&
		h.RunID > 0 && h.RunAttempt > 0 && identifier(h.BootID) &&
		identifier(h.Source) && identifier(h.Target) && h.Source != h.Target &&
		hexString(h.Revision, 40) && IsSHA256(h.WorkloadSHA256) && IsSHA256(h.TopologySHA256) &&
		IsSHA256(h.PolicySHA256) && (h.MarkersSHA256 == "" || IsSHA256(h.MarkersSHA256))
}

func (h Header) authority(id string) bool { return id == h.Source || id == h.Target }

type LeaseRef struct {
	PartitionID int16  `json:"partition_id"`
	OwnerID     string `json:"owner_id"`
	Epoch       int64  `json:"epoch"`
}

type ClaimRef struct {
	NodeID      string `json:"node_id"`
	Iteration   int    `json:"iteration"`
	Attempt     int64  `json:"attempt"`
	TokenSHA256 string `json:"token_sha256"`
}

const (
	Submit       = "submit"
	AcquireLease = "acquire_lease"
	RenewLease   = "renew_lease"
	ReleaseLease = "release_lease"
	OwnerWrite   = "owner_write"
	Claim        = "claim"
	Result       = "result"
)

type Operation struct {
	RequestID     string    `json:"request_id"`
	Kind          string    `json:"kind"`
	Phase         string    `json:"phase"`
	Authority     string    `json:"authority"`
	Sequence      int       `json:"sequence,omitempty"`
	WorkflowID    string    `json:"workflow_id,omitempty"`
	Namespace     string    `json:"namespace,omitempty"`
	SubmissionKey string    `json:"submission_key,omitempty"`
	RequestSHA256 string    `json:"request_sha256"`
	ScheduledNS   int64     `json:"scheduled_ns,omitempty"`
	Lease         *LeaseRef `json:"lease,omitempty"`
	Claim         *ClaimRef `json:"claim,omitempty"`
}

func (o Operation) valid(h Header) bool {
	if !identifier(o.RequestID) || !identifier(o.Phase) || !h.authority(o.Authority) ||
		!IsSHA256(o.RequestSHA256) || o.Sequence < 0 || o.ScheduledNS < 0 ||
		!validRefs(o.Lease, o.Claim) {
		return false
	}
	switch o.Kind {
	case Submit:
		return identifier(o.WorkflowID) && identifier(o.Namespace) && identifier(o.SubmissionKey)
	case AcquireLease:
		return true
	case RenewLease, ReleaseLease:
		return o.Lease != nil
	case OwnerWrite:
		return o.Lease != nil && identifier(o.WorkflowID)
	case Claim:
		return identifier(o.WorkflowID)
	case Result:
		return o.Claim != nil && identifier(o.WorkflowID)
	}
	return false
}

const (
	Acknowledged = "acknowledged"
	Rejected     = "rejected"
	Ambiguous    = "ambiguous"
	NotSent      = "not_sent"
	InFlight     = "in_flight"
)

type Outcome struct {
	RequestID      string    `json:"request_id"`
	Disposition    string    `json:"disposition"`
	SentNS         int64     `json:"sent_ns,omitempty"`
	CompletedNS    int64     `json:"response_complete_ns,omitempty"`
	HTTPStatus     int       `json:"http_status,omitempty"`
	ResponseSHA256 string    `json:"response_sha256,omitempty"`
	DurableID      string    `json:"durable_id,omitempty"`
	PayloadHash    string    `json:"payload_hash,omitempty"`
	Code           string    `json:"code,omitempty"`
	Lease          *LeaseRef `json:"lease,omitempty"`
	Claim          *ClaimRef `json:"claim,omitempty"`
}

func (o Outcome) valid(op Operation, sendSynced, now int64) bool {
	if sendSynced <= 0 || (o.HTTPStatus != 0 && (o.HTTPStatus < 100 || o.HTTPStatus > 599)) ||
		(o.PayloadHash != "" && !identifier(o.PayloadHash)) ||
		(o.ResponseSHA256 != "" && !IsSHA256(o.ResponseSHA256)) ||
		!validRefs(o.Lease, o.Claim) {
		return false
	}
	if o.Disposition == NotSent {
		return o.SentNS == 0 && o.CompletedNS == 0 && o.HTTPStatus == 0 &&
			o.ResponseSHA256 == "" && identifier(o.Code)
	}
	if o.SentNS < sendSynced || o.SentNS > now ||
		(o.CompletedNS != 0 && (o.CompletedNS < o.SentNS || o.CompletedNS > now ||
			!IsSHA256(o.ResponseSHA256))) ||
		(o.CompletedNS == 0 && o.ResponseSHA256 != "") {
		return false
	}
	switch o.Disposition {
	case Acknowledged:
		if o.CompletedNS == 0 || !identifier(o.DurableID) || o.Code != "" ||
			(o.HTTPStatus != 0 && (o.HTTPStatus < 200 || o.HTTPStatus > 299)) {
			return false
		}
		switch op.Kind {
		case Submit:
			return o.HTTPStatus == 201 && o.DurableID == op.WorkflowID
		case AcquireLease, RenewLease:
			return o.Lease != nil
		case Claim:
			return o.Claim != nil
		}
		return true
	case Rejected:
		return o.CompletedNS > 0 && identifier(o.Code) &&
			(o.HTTPStatus == 0 || (o.HTTPStatus >= 400 && o.HTTPStatus < 500))
	case Ambiguous:
		return identifier(o.Code)
	}
	return false
}

const (
	StateProbe      = "state_probe"
	WALObservation  = "wal_observation"
	AuthorityChange = "authority_change"
	ArchiveCut      = "archive_cut"
)

// Witnesses independently corroborate an operation or a phase milestone. A
// state probe or WAL observation does not turn a lost response into an ack.
type Witness struct {
	Kind        string    `json:"kind"`
	Phase       string    `json:"phase"`
	Authority   string    `json:"authority"`
	Former      string    `json:"former_authority,omitempty"`
	RequestID   string    `json:"request_id,omitempty"`
	StartedNS   int64     `json:"started_ns"`
	CompletedNS int64     `json:"completed_ns"`
	EvidenceSHA string    `json:"evidence_sha256"`
	Timeline    uint32    `json:"timeline,omitempty"`
	LSN         string    `json:"lsn,omitempty"`
	Position    string    `json:"position,omitempty"`
	Segment     string    `json:"segment,omitempty"`
	EndLSN      string    `json:"end_lsn,omitempty"`
	Lease       *LeaseRef `json:"lease,omitempty"`
	Claim       *ClaimRef `json:"claim,omitempty"`
}

func (w Witness) valid(h Header, now int64) bool {
	if !h.authority(w.Authority) || !identifier(w.Phase) || !IsSHA256(w.EvidenceSHA) ||
		w.StartedNS <= 0 || w.CompletedNS < w.StartedNS || w.CompletedNS > now ||
		!validRefs(w.Lease, w.Claim) {
		return false
	}
	switch w.Kind {
	case StateProbe:
		return true
	case AuthorityChange:
		return h.authority(w.Former) && w.Former != w.Authority
	case WALObservation:
		return w.Timeline > 0 && validLSN(w.LSN) &&
			(w.Position == "primary_flush" || w.Position == "standby_receive" ||
				w.Position == "standby_flush" || w.Position == "standby_replay")
	case ArchiveCut:
		return w.Authority == h.Source && w.archiveBoundaryValid()
	}
	return false
}

func (w Witness) archiveBoundaryValid() bool {
	const segmentSize = 16 << 20
	const segmentsPerLog = (1 << 32) / segmentSize
	point, pointOK := parseLSN(w.LSN)
	end, endOK := parseLSN(w.EndLSN)
	if w.Timeline == 0 || !pointOK || !endOK || point == 0 || end < point {
		return false
	}
	index := (point - 1) / segmentSize
	name := fmt.Sprintf("%08X%08X%08X", w.Timeline, index/segmentsPerLog, index%segmentsPerLog)
	return w.Segment == name && end == (index+1)*segmentSize
}

func validRefs(lease *LeaseRef, claim *ClaimRef) bool {
	return (lease == nil || (lease.PartitionID >= 0 && identifier(lease.OwnerID) && lease.Epoch > 0)) &&
		(claim == nil || (identifier(claim.NodeID) && claim.Iteration >= 0 &&
			claim.Attempt > 0 && IsSHA256(claim.TokenSHA256)))
}

func identifier(s string) bool {
	if s == "" || len(s) > 128 {
		return false
	}
	for _, c := range s {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' ||
			c == '-' || c == '_' || c == '.' || c == ':' || c == '/') {
			return false
		}
	}
	return true
}

func validLSN(s string) bool {
	_, ok := parseLSN(s)
	return ok
}

func parseLSN(s string) (uint64, bool) {
	left, right, ok := strings.Cut(s, "/")
	if !ok || !hexString(left, len(left)) || !hexString(right, len(right)) {
		return 0, false
	}
	upper, upperErr := strconv.ParseUint(left, 16, 32)
	lower, lowerErr := strconv.ParseUint(right, 16, 32)
	return upper<<32 | lower, upperErr == nil && lowerErr == nil && len(left) <= 8 && len(right) <= 8
}

func hexString(s string, length int) bool {
	if length == 0 || len(s) != length {
		return false
	}
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F') {
			return false
		}
	}
	return true
}

func IsSHA256(s string) bool { return hexString(s, sha256.Size*2) && s == strings.ToLower(s) }

func SHA256(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}
