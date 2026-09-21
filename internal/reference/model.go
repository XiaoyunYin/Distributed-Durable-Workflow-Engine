// Package reference contains a small contract-derived model used by the
// property tests. It intentionally has its own state types and transition
// rules; it does not call the production Store or Engine validators.
package reference

import "errors"

var (
	ErrStaleLease       = errors.New("stale lease")
	ErrTerminalWorkflow = errors.New("terminal workflow")
	ErrInvalidAttempt   = errors.New("invalid attempt")
	ErrNotClaimed       = errors.New("attempt is not claimed")
	ErrAlreadyApplied   = errors.New("result already applied")
)

type WorkflowState string

const (
	Runnable               WorkflowState = "RUNNABLE"
	WaitingActivity        WorkflowState = "WAITING_ACTIVITY"
	ReconciliationRequired WorkflowState = "RECONCILIATION_REQUIRED"
	Succeeded              WorkflowState = "SUCCEEDED"
	Failed                 WorkflowState = "FAILED"
	Canceled               WorkflowState = "CANCELED"
)

type AttemptState string

const (
	Unclaimed       AttemptState = "UNCLAIMED"
	Claimed         AttemptState = "CLAIMED"
	Completed       AttemptState = "COMPLETED"
	TimedOut        AttemptState = "TIMED_OUT"
	CanceledAttempt AttemptState = "CANCELED"
)

type EffectClass string

const (
	PureEffect        EffectClass = "PURE"
	CooperatingEffect EffectClass = "COOPERATING"
	UnknownEffect     EffectClass = "NON_COOPERATING"
)

type OpKind uint8

const (
	Claim OpKind = iota
	Complete
	Timeout
	Cancel
	Takeover
	Acknowledge
)

type Operation struct {
	Kind     OpKind
	Owner    string
	Epoch    int64
	Attempt  int
	Effect   EffectClass
	Applied  bool
	NewOwner string
}

type Snapshot struct {
	State          WorkflowState
	Revision       int64
	LeaseOwner     string
	LeaseEpoch     int64
	AttemptNumber  int
	AttemptState   AttemptState
	Effect         EffectClass
	OutcomeUnknown bool
	ResultAccepted bool
	Acked          bool
}

type Model struct {
	snapshot         Snapshot
	terminalRevision int64
}

func New() *Model {
	return &Model{snapshot: Snapshot{
		State: Runnable, Revision: 1, LeaseOwner: "owner-a", LeaseEpoch: 1,
		AttemptState: Unclaimed, Effect: PureEffect,
	}}
}

func (m *Model) Snapshot() Snapshot { return m.snapshot }

func (m *Model) Apply(op Operation) error {
	if op.Kind == Takeover {
		if op.NewOwner == "" || op.Epoch <= m.snapshot.LeaseEpoch {
			return ErrStaleLease
		}
		m.snapshot.LeaseOwner = op.NewOwner
		m.snapshot.LeaseEpoch = op.Epoch
		return nil
	}
	if op.Owner != m.snapshot.LeaseOwner || op.Epoch != m.snapshot.LeaseEpoch {
		return ErrStaleLease
	}
	if m.terminalRevision != 0 && op.Kind != Acknowledge {
		return ErrTerminalWorkflow
	}

	switch op.Kind {
	case Claim:
		if m.snapshot.State != Runnable && m.snapshot.State != WaitingActivity {
			return ErrTerminalWorkflow
		}
		if m.snapshot.AttemptState == Claimed {
			return ErrInvalidAttempt
		}
		if op.Attempt != 0 && op.Attempt != m.snapshot.AttemptNumber+1 {
			return ErrInvalidAttempt
		}
		m.snapshot.AttemptNumber++
		m.snapshot.AttemptState = Claimed
		m.snapshot.State = WaitingActivity
		m.snapshot.Revision++
	case Complete:
		if op.Attempt != m.snapshot.AttemptNumber {
			return ErrInvalidAttempt
		}
		if m.snapshot.ResultAccepted {
			return ErrAlreadyApplied
		}
		if m.snapshot.AttemptState != Claimed {
			return ErrNotClaimed
		}
		m.snapshot.AttemptState = Completed
		m.snapshot.ResultAccepted = true
		m.snapshot.State = Succeeded
		m.snapshot.Revision++
		m.terminalRevision = m.snapshot.Revision
	case Timeout:
		if op.Attempt != m.snapshot.AttemptNumber || m.snapshot.AttemptState != Claimed {
			return ErrInvalidAttempt
		}
		m.snapshot.AttemptState = TimedOut
		m.snapshot.Revision++
		if op.Effect == UnknownEffect {
			m.snapshot.Effect = UnknownEffect
			m.snapshot.OutcomeUnknown = true
			m.snapshot.State = ReconciliationRequired
		} else {
			m.snapshot.State = Runnable
		}
	case Cancel:
		if m.snapshot.State == Succeeded || m.snapshot.State == Failed || m.snapshot.State == Canceled {
			return ErrTerminalWorkflow
		}
		if m.snapshot.AttemptState == Claimed && op.Effect != PureEffect {
			m.snapshot.OutcomeUnknown = true
			m.snapshot.Effect = op.Effect
		}
		m.snapshot.AttemptState = CanceledAttempt
		m.snapshot.State = Canceled
		m.snapshot.Revision++
		m.terminalRevision = m.snapshot.Revision
	case Acknowledge:
		if m.snapshot.State == Succeeded || m.snapshot.State == Failed || m.snapshot.State == Canceled {
			return ErrTerminalWorkflow
		}
		m.snapshot.Acked = true
	default:
		return ErrInvalidAttempt
	}
	return nil
}

func (m *Model) Validate() error {
	s := m.snapshot
	if s.Revision < 1 || s.LeaseEpoch < 1 || s.LeaseOwner == "" {
		return errors.New("invalid durable identity")
	}
	if s.ResultAccepted && (s.AttemptState != Completed || s.State != Succeeded) {
		return errors.New("accepted result without completed attempt")
	}
	if s.OutcomeUnknown && s.Effect == PureEffect {
		return errors.New("pure activity has unknown external outcome")
	}
	if s.State == ReconciliationRequired && !s.OutcomeUnknown {
		return errors.New("reconciliation state lacks unknown outcome")
	}
	if s.State == Succeeded && !s.ResultAccepted {
		return errors.New("succeeded workflow lacks accepted result")
	}
	return nil
}
