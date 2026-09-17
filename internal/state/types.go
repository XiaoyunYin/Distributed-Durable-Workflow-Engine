// Package state implements the PostgreSQL-backed durable state repository.
//
// The package keeps scheduler-owned transitions separate from worker result
// receipts. Every scheduler mutation follows lease -> workflow -> node/attempt
// row locking, matching docs/CONTRACTS.md dur-002.v4.
package state

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrSubmissionConflict     = errors.New("submission key has a different payload")
	ErrLeaseNotOwned          = errors.New("scheduler lease is not owned or has expired")
	ErrRevisionConflict       = errors.New("workflow revision conflict")
	ErrInvalidTransition      = errors.New("invalid workflow transition")
	ErrAttemptNotCurrent      = errors.New("attempt is not current or claim is invalid")
	ErrNotTimedOut            = errors.New("attempt is not a timed-out non-cooperating attempt")
	ErrStaleClaim             = errors.New("claim is stale")
	ErrClaimRequestConflict   = errors.New("worker request ID belongs to another attempt")
	ErrResultConflict         = errors.New("result conflicts with the durable receipt")
	ErrEffectClassMismatch    = errors.New("attempt effect class does not match its definition")
	ErrMissingEffectClass     = errors.New("activity definition has no effect class")
	ErrPartitionMismatch      = errors.New("workflow partition does not match the frozen partition map")
	ErrResultNotConsumable    = errors.New("attempt has no unconsumed terminal result")
	ErrEvidenceConflict       = errors.New("late evidence conflicts with the durable evidence")
	ErrEffectIdentityMismatch = errors.New("retry effect identity does not match the first attempt")
	ErrWorkflowNotFound       = errors.New("workflow not found")
)

const (
	DefaultDispatchLease = time.Minute
	DefaultAttemptLease  = time.Minute
)

type WorkflowState string

const (
	StateRunnable                 WorkflowState = "RUNNABLE"
	StateWaitingActivity          WorkflowState = "WAITING_ACTIVITY"
	StateWaitingTimer             WorkflowState = "WAITING_TIMER"
	StateWaitingApproval          WorkflowState = "WAITING_APPROVAL"
	StatePausedUnsupportedVersion WorkflowState = "PAUSED_UNSUPPORTED_VERSION"
	StateReconciliationRequired   WorkflowState = "RECONCILIATION_REQUIRED"
	StateSucceeded                WorkflowState = "SUCCEEDED"
	StateFailed                   WorkflowState = "FAILED"
	StateRejected                 WorkflowState = "REJECTED"
	StateCanceled                 WorkflowState = "CANCELED"
	StateAbandoned                WorkflowState = "ABANDONED"
)

type EffectClass string

const (
	EffectPure           EffectClass = "PURE_ACTIVITY"
	EffectCooperating    EffectClass = "COOPERATING_EFFECT"
	EffectNonCooperating EffectClass = "NON_COOPERATING_EFFECT"
)

type AttemptState string

const (
	AttemptCreated         AttemptState = "CREATED"
	AttemptDispatchable    AttemptState = "DISPATCHABLE"
	AttemptClaimed         AttemptState = "CLAIMED"
	AttemptSucceeded       AttemptState = "SUCCEEDED"
	AttemptFailedRetryable AttemptState = "FAILED_RETRYABLE"
	AttemptFailedFinal     AttemptState = "FAILED_FINAL"
	AttemptTimedOut        AttemptState = "TIMED_OUT"
	AttemptReplaced        AttemptState = "REPLACED"
	AttemptOutcomeUnknown  AttemptState = "OUTCOME_UNKNOWN"
	AttemptCanceled        AttemptState = "CANCELED"
)

type Store struct {
	pool *pgxpool.Pool
}

type DefinitionInput struct {
	DefinitionID     string
	Version          int
	DefinitionHash   string
	Graph            json.RawMessage
	ActivityVersions json.RawMessage
	EffectClasses    json.RawMessage
}

type CreateWorkflowInput struct {
	WorkflowID            string
	Namespace             string
	SubmissionKey         string
	SubmissionPayloadHash string
	DefinitionID          string
	DefinitionVersion     int
	PartitionID           int16
	InitialNodeID         string
	InitialInput          json.RawMessage
	ActorID               string
}

type Workflow struct {
	WorkflowID        string
	Namespace         string
	SubmissionKey     string
	PayloadHash       string
	DefinitionID      string
	DefinitionVersion int
	PartitionID       int16
	State             WorkflowState
	Revision          int64
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

type Lease struct {
	PartitionID    int16
	OwnerID        string
	Epoch          int64
	LeaseExpiresAt time.Time
}

type LeaseRef struct {
	PartitionID int16
	OwnerID     string
	Epoch       int64
}

type OwnerTransitionInput struct {
	Lease            LeaseRef
	WorkflowID       string
	ExpectedRevision int64
	NewState         WorkflowState
	ActorID          string
	Reason           string
	NodeID           string
	Iteration        int
	NodeState        *WorkflowState
	AttemptNumber    *int64
}

type AttemptInput struct {
	Lease            LeaseRef
	WorkflowID       string
	NodeID           string
	Iteration        int
	ExpectedRevision int64
	EffectClass      EffectClass
	LogicalEffectKey string
	GrantScopeHash   string
	// HeartbeatDeadline is the initial dispatch deadline. ClaimAttempt replaces
	// it with a fresh worker lease; timeout redispatches and replacements also
	// arm a fresh dispatch deadline.
	HeartbeatDeadline time.Time
	ActorID           string
}

type Attempt struct {
	WorkflowID         string
	NodeID             string
	Iteration          int
	AttemptNumber      int64
	State              AttemptState
	EffectClass        EffectClass
	ClaimToken         string
	WorkerID           string
	WorkerRequestID    string
	HeartbeatDeadline  *time.Time
	LogicalEffectKey   string
	GrantScopeHash     string
	OutcomeDisposition string
	IsCurrent          bool
}

type ClaimInput struct {
	WorkflowID   string
	NodeID       string
	Iteration    int
	WorkerID     string
	RequestID    string
	AttemptLease time.Duration
}

type ClaimResult struct {
	AttemptNumber     int64
	ClaimToken        string
	EffectClass       EffectClass
	HeartbeatDeadline time.Time
}

type HeartbeatInput struct {
	WorkflowID    string
	NodeID        string
	Iteration     int
	AttemptNumber int64
	ClaimToken    string
	Extension     time.Duration
}

type ResultInput struct {
	WorkflowID        string
	NodeID            string
	Iteration         int
	AttemptNumber     int64
	ClaimToken        string
	AttemptState      AttemptState
	Payload           json.RawMessage
	EventType         string
	ReconciliationRef string
}

type ResultDisposition string

const (
	ResultAccepted           ResultDisposition = "ACCEPTED"
	ResultRecordedAsEvidence ResultDisposition = "RECORDED_AS_EVIDENCE"
)

type ResultReceipt struct {
	Disposition   ResultDisposition
	WorkflowID    string
	NodeID        string
	Iteration     int
	AttemptNumber int64
	AttemptState  AttemptState
	Payload       json.RawMessage
}

type TimeoutInput struct {
	Lease            LeaseRef
	WorkflowID       string
	NodeID           string
	Iteration        int
	AttemptNumber    int64
	ExpectedRevision int64
	ActorID          string
	DispatchLease    time.Duration
}

type TimeoutResult struct {
	AttemptNumber     int64
	ReplacementNumber *int64
	Redispatched      bool
	Reconciliation    bool
}

type LateEvidenceInput struct {
	WorkflowID        string
	NodeID            string
	Iteration         int
	AttemptNumber     int64
	ClaimToken        string
	ReconciliationRef string
	Payload           json.RawMessage
}

type TransitionRecord struct {
	TransitionID   string
	WorkflowID     string
	Revision       int64
	ActorKind      string
	ActorID        string
	SchedulerEpoch *int64
	NodeID         *string
	Iteration      *int
	AttemptNumber  *int64
	OldState       *WorkflowState
	NewState       WorkflowState
	Reason         string
	CreatedAt      time.Time
}

type LateEvidence struct {
	EvidenceID        string
	WorkflowID        string
	NodeID            string
	Iteration         int
	AttemptNumber     int64
	ReconciliationRef string
	RecordedAt        time.Time
}

type CreateWorkflowResult struct {
	Workflow Workflow
	Created  bool
}

type ConsumeResultInput struct {
	Lease            LeaseRef
	WorkflowID       string
	NodeID           string
	Iteration        int
	AttemptNumber    int64
	ExpectedRevision int64
	NewWorkflowState WorkflowState
	RetryDueAt       *time.Time
	ActorID          string
}

type ConsumeResult struct {
	Workflow     Workflow
	NodeState    WorkflowState
	AttemptState AttemptState
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

func (s *Store) Pool() *pgxpool.Pool {
	return s.pool
}

func (s *Store) Close() {
	if s.pool != nil {
		s.pool.Close()
	}
}

func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}
