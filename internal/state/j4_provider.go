package state

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	providerBudget      = 4
	providerHTTPTimeout = int64(2 * time.Second)
	providerRequestHash = "92b3bc136a6617117cfac28e4324b272b6bf6b72ab5d137a948cb9fc0d25e751"
)

var (
	ErrProviderInput    = errors.New("invalid J4 provider request")
	ErrProviderConflict = errors.New("J4 provider request conflicts with durable state")
	ErrProviderClock    = errors.New("J4 monotonic boot identity mismatch")
)

type ProviderClaim struct {
	WorkflowID, NodeID, ClaimToken, BootID string
	Iteration                              int
	Attempt                                int64
}

type ProviderOutcome struct {
	CallID            string
	Kind              string
	ObservedNS        int64
	RetryAfterSeconds int
}

type ProviderPermit struct {
	CallID     string `json:"call_id"`
	Sequence   int    `json:"sequence"`
	Attempt    int64  `json:"attempt"`
	IssuedNS   int64  `json:"issued_ns"`
	DeadlineNS int64  `json:"deadline_ns"`
}

type ProviderSchedule struct {
	BootID        string          `json:"boot_id"`
	EligibleNS    int64           `json:"eligible_ns"`
	OutcomeNS     int64           `json:"outcome_ns"`
	Permit        *ProviderPermit `json:"permit,omitempty"`
	TerminalState AttemptState    `json:"terminal_state,omitempty"`
	Payload       json.RawMessage `json:"payload,omitempty"`
}

// ProviderControl belongs to one private J4 listener and one fixture namespace.
// It uses the same workflow -> node -> attempt lock order as the result path.
type ProviderControl struct {
	store      *Store
	namespace  string
	bootID     string
	clock      func() (int64, error)
	fixedDelay bool
}

func NewProviderControl(ctx context.Context, store *Store, namespace string, fixedDelay bool) (*ProviderControl, error) {
	if store == nil || namespace == "" {
		return nil, errors.New("J4 provider control requires PostgreSQL and an explicit namespace")
	}
	var installed bool
	if err := store.pool.QueryRow(ctx, `SELECT
		EXISTS (SELECT 1 FROM engine.schema_migrations WHERE version = 25)
		AND to_regclass('engine.j4_provider_attempts') IS NOT NULL`).Scan(&installed); err != nil {
		return nil, fmt.Errorf("check J4 provider migration: %w", err)
	}
	if !installed {
		return nil, errors.New("J4 provider control requires migration j4_b2/000025")
	}
	bootID, clock, err := NewJ4HostClock()
	if err != nil {
		return nil, err
	}
	return &ProviderControl{store, namespace, bootID, clock, fixedDelay}, nil
}

func providerRetryDelay(sequence, retryAfterSeconds int, fixedDelay bool) int64 {
	sum := sha256.Sum256([]byte(fmt.Sprintf("j4-b2-v1:37:%d", sequence)))
	jitter := int64(binary.BigEndian.Uint32(sum[:4]) % 1001)
	delayMS := (int64(2000) << (sequence - 1)) + jitter
	if fixedDelay {
		delayMS = 3400
	}
	return max(delayMS*int64(time.Millisecond), int64(retryAfterSeconds)*int64(time.Second))
}

func (p *ProviderControl) lockClaim(ctx context.Context, tx pgx.Tx, c ProviderClaim) error {
	if c.WorkflowID == "" || c.ClaimToken == "" || c.NodeID != RecordedLLMActivity || c.Iteration != 0 || c.Attempt < 1 {
		return ErrProviderInput
	}
	if c.BootID != p.bootID {
		return ErrProviderClock
	}
	var active bool
	err := tx.QueryRow(ctx, `SELECT state = 'WAITING_ACTIVITY' FROM engine.workflow_executions
		WHERE workflow_id = $1 AND namespace = $2 FOR UPDATE`, c.WorkflowID, p.namespace).Scan(&active)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrStaleClaim
		}
		return err
	}
	if !active {
		return ErrStaleClaim
	}
	var current *int64
	err = tx.QueryRow(ctx, `SELECT current_attempt_number FROM engine.node_instances
		WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3 FOR UPDATE`,
		c.WorkflowID, c.NodeID, c.Iteration).Scan(&current)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrStaleClaim
		}
		return err
	}
	if current == nil || *current != c.Attempt {
		return ErrStaleClaim
	}
	err = tx.QueryRow(ctx, `SELECT is_current AND state = 'CLAIMED' AND effect_class = 'PURE_ACTIVITY'
		AND COALESCE(claim_token::text = $5 AND heartbeat_deadline > clock_timestamp(), false)
		FROM engine.activity_attempts
		WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3 AND attempt_number = $4 FOR UPDATE`,
		c.WorkflowID, c.NodeID, c.Iteration, c.Attempt, c.ClaimToken).Scan(&active)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && !active) {
		return ErrStaleClaim
	}
	return err
}

type providerRow struct {
	ProviderPermit
	requestID, bootID, outcome string
	observed, eligible         int64
	retryAfter                 int
	terminal                   AttemptState
	payload                    json.RawMessage
}

func latestProviderRow(ctx context.Context, tx pgx.Tx, c ProviderClaim) (providerRow, error) {
	var row providerRow
	err := tx.QueryRow(ctx, `SELECT call_id, sequence, attempt_number, issued_ns, deadline_ns, reserve_request_id, boot_id,
		COALESCE(outcome,''), COALESCE(observed_ns,0), COALESCE(eligible_ns,0), retry_after_seconds,
		COALESCE(terminal_state,''), terminal_payload
		FROM engine.j4_provider_attempts WHERE workflow_id = $1 AND node_id = $2 AND iteration = $3
		ORDER BY sequence DESC LIMIT 1 FOR UPDATE`, c.WorkflowID, c.NodeID, c.Iteration).Scan(
		&row.CallID, &row.Sequence, &row.Attempt, &row.IssuedNS, &row.DeadlineNS, &row.requestID, &row.bootID,
		&row.outcome, &row.observed, &row.eligible, &row.retryAfter, &row.terminal, &row.payload)
	if errors.Is(err, pgx.ErrNoRows) {
		err = nil
	}
	if err == nil && row.Sequence > 0 && row.bootID != c.BootID {
		err = ErrProviderClock
	}
	return row, err
}

func (p *ProviderControl) transaction(ctx context.Context, claim ProviderClaim, operation func(pgx.Tx, providerRow) (ProviderSchedule, error)) (ProviderSchedule, error) {
	tx, err := p.store.pool.Begin(ctx)
	if err != nil {
		return ProviderSchedule{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := p.lockClaim(ctx, tx, claim); err != nil {
		return ProviderSchedule{}, err
	}
	row, err := latestProviderRow(ctx, tx, claim)
	if err != nil {
		return ProviderSchedule{}, err
	}
	reply, err := operation(tx, row)
	if err != nil {
		return ProviderSchedule{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ProviderSchedule{}, err
	}
	reply.BootID = p.bootID
	return reply, nil
}

func (r providerRow) schedule() ProviderSchedule {
	reply := ProviderSchedule{EligibleNS: r.eligible, OutcomeNS: r.observed, TerminalState: r.terminal}
	if r.terminal != "" {
		reply.Payload = r.payload
	}
	return reply
}

// Reserve with an empty request ID reads and reconciles the schedule without
// granting a permit. A nonempty ID is stable across HTTP retries by one worker.
func (p *ProviderControl) Reserve(ctx context.Context, c ProviderClaim, requestID string) (ProviderSchedule, error) {
	if len(requestID) > 128 {
		return ProviderSchedule{}, ErrProviderInput
	}
	return p.transaction(ctx, c, func(tx pgx.Tx, row providerRow) (ProviderSchedule, error) {
		now, err := p.clock()
		if err != nil {
			return ProviderSchedule{}, err
		}
		if row.Sequence > 0 && row.outcome == "" {
			if row.Attempt == c.Attempt {
				switch requestID {
				case "":
					return ProviderSchedule{}, nil
				case row.requestID:
					return ProviderSchedule{Permit: &row.ProviderPermit}, nil
				default:
					return ProviderSchedule{}, ErrProviderConflict
				}
			}
			if now < row.DeadlineNS {
				return ProviderSchedule{EligibleNS: row.DeadlineNS}, nil
			}
			if err = p.persistOutcome(ctx, tx, &row, ProviderOutcome{Kind: "ambiguous", ObservedNS: row.DeadlineNS}); err != nil {
				return ProviderSchedule{}, err
			}
		}
		if requestID == "" || row.terminal != "" || now < row.eligible {
			return row.schedule(), nil
		}
		if row.Sequence >= providerBudget {
			return ProviderSchedule{}, ErrProviderConflict
		}
		permit := ProviderPermit{CallID: NewID(), Sequence: row.Sequence + 1,
			Attempt: c.Attempt, IssuedNS: now, DeadlineNS: now + providerHTTPTimeout}
		result, err := tx.Exec(ctx, `INSERT INTO engine.j4_provider_attempts
			(workflow_id, node_id, iteration, sequence, attempt_number, reserve_request_id, call_id, boot_id, issued_ns, deadline_ns)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
			ON CONFLICT (workflow_id,node_id,iteration,attempt_number,reserve_request_id) DO NOTHING`,
			c.WorkflowID, c.NodeID, c.Iteration, permit.Sequence, c.Attempt, requestID,
			permit.CallID, c.BootID, permit.IssuedNS, permit.DeadlineNS)
		if err != nil {
			return ProviderSchedule{}, err
		}
		if result.RowsAffected() != 1 {
			return ProviderSchedule{}, ErrProviderConflict
		}
		return ProviderSchedule{Permit: &permit}, nil
	})
}

func validateProviderOutcome(row providerRow, outcome ProviderOutcome, now int64) (ProviderOutcome, error) {
	if outcome.ObservedNS < row.IssuedNS || outcome.ObservedNS > now {
		return outcome, ErrProviderInput
	}
	switch outcome.Kind {
	case "timeout":
		if outcome.ObservedNS < row.DeadlineNS {
			return outcome, ErrProviderInput
		}
		outcome.ObservedNS = row.DeadlineNS
	case "rate_limited", "success", "mismatch":
		if outcome.ObservedNS >= row.DeadlineNS {
			return outcome, ErrProviderInput
		}
	default:
		return outcome, ErrProviderInput
	}
	if outcome.RetryAfterSeconds < 0 || outcome.RetryAfterSeconds > 2147483647 ||
		(outcome.Kind != "rate_limited" && outcome.RetryAfterSeconds != 0) {
		return outcome, ErrProviderInput
	}
	return outcome, nil
}

func (p *ProviderControl) Outcome(ctx context.Context, c ProviderClaim, outcome ProviderOutcome) (ProviderSchedule, error) {
	return p.transaction(ctx, c, func(tx pgx.Tx, row providerRow) (ProviderSchedule, error) {
		if row.Sequence == 0 || row.Attempt != c.Attempt || row.CallID != outcome.CallID {
			return ProviderSchedule{}, ErrProviderConflict
		}
		now, err := p.clock()
		if err != nil {
			return ProviderSchedule{}, err
		}
		normalized, err := validateProviderOutcome(row, outcome, now)
		if err != nil {
			return ProviderSchedule{}, err
		}
		if row.outcome != "" {
			if row.outcome != normalized.Kind || row.observed != normalized.ObservedNS || row.retryAfter != normalized.RetryAfterSeconds {
				return ProviderSchedule{}, ErrProviderConflict
			}
		} else if err := p.persistOutcome(ctx, tx, &row, normalized); err != nil {
			return ProviderSchedule{}, err
		}
		return row.schedule(), nil
	})
}

func (p *ProviderControl) persistOutcome(ctx context.Context, tx pgx.Tx, row *providerRow, outcome ProviderOutcome) error {
	row.outcome, row.observed, row.retryAfter = outcome.Kind, outcome.ObservedNS, outcome.RetryAfterSeconds
	switch outcome.Kind {
	case "success":
		row.terminal = AttemptSucceeded
		// Go owns the fixture payload and binds it to the durable permit.
		row.payload, _ = json.Marshal(map[string]string{
			"completion": "fixture alpha", "request_sha256": providerRequestHash, "provider_call_id": row.CallID,
		})
	case "mismatch":
		row.terminal, row.payload = AttemptFailedFinal, json.RawMessage(`{"reason":"RECORDED_REQUEST_MISMATCH"}`)
	default:
		row.eligible = row.observed + providerRetryDelay(row.Sequence, row.retryAfter, p.fixedDelay)
		if row.Sequence == providerBudget {
			row.terminal, row.payload = AttemptFailedFinal, json.RawMessage(`{"reason":"PROVIDER_BUDGET_EXHAUSTED"}`)
		}
	}
	_, err := tx.Exec(ctx, `UPDATE engine.j4_provider_attempts SET outcome=$2, observed_ns=$3, retry_after_seconds=$4,
		eligible_ns=NULLIF($5::bigint,0), terminal_state=NULLIF($6::text,''), terminal_payload=$7::jsonb
		WHERE call_id=$1`, row.CallID, row.outcome, row.observed, row.retryAfter, row.eligible, row.terminal, row.payload)
	return err
}

// CheckResult allows the existing receipt transaction to record only the
// immutable terminal outcome. The receipt still validates the current claim.
func (p *ProviderControl) CheckResult(ctx context.Context, input ResultInput) error {
	var state AttemptState
	var payload json.RawMessage
	err := p.store.pool.QueryRow(ctx, `SELECT a.terminal_state, a.terminal_payload
		FROM engine.j4_provider_attempts a JOIN engine.workflow_executions w USING (workflow_id)
		WHERE a.workflow_id=$1 AND a.node_id=$2 AND a.iteration=$3 AND w.namespace=$4
		AND a.terminal_state IS NOT NULL`,
		input.WorkflowID, input.NodeID, input.Iteration, p.namespace).Scan(&state, &payload)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && (state != input.AttemptState || !jsonEqual(payload, input.Payload))) {
		return ErrProviderConflict
	}
	return err
}
