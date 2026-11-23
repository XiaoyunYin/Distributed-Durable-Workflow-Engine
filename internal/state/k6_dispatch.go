package state

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"durable-agent-execution-engine/internal/k6receipt"
	"github.com/jackc/pgx/v5"
)

var ErrK6ResultPending = errors.New("K6 sink receipt is pending")
var ErrK6Unavailable = errors.New("K6 dispatch is unavailable")

// K6Dispatch contains grant material. It belongs only in the authenticated
// worker-control response and the private sink request.
type K6Dispatch struct {
	IntentID         string          `json:"intent_id"`
	GrantToken       string          `json:"grant_token"`
	WorkflowID       string          `json:"workflow_id"`
	NodeID           string          `json:"node_id"`
	Iteration        int             `json:"iteration"`
	LogicalKey       string          `json:"logical_key"`
	ScopeHash        string          `json:"scope_hash"`
	ResourceID       string          `json:"resource_id"`
	ExpectedRevision string          `json:"expected_revision"`
	Arguments        json.RawMessage `json:"arguments"`
	CanonicalHash    string          `json:"canonical_hash"`
	Attempt          int64           `json:"attempt"`
	Fence            int64           `json:"fence"`
	LookupFirst      bool            `json:"lookup_first"`
}

type k6Grant struct {
	K6Dispatch
	latest time.Time
	lockBy time.Time
}

type k6Querier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func (s *Store) K6Window(ctx context.Context, namespace string) (time.Time, error) {
	if !s.k6Approval {
		return time.Time{}, ErrK6Unavailable
	}
	var cutoff time.Time
	var live bool
	err := s.pool.QueryRow(ctx, `SELECT valid_until, clock_timestamp() < valid_until - uncertainty_us * interval '1 microsecond'
		FROM k6_auth.episode WHERE namespace=$1`, namespace).Scan(&cutoff, &live)
	if err != nil {
		return time.Time{}, err
	}
	if !live {
		return cutoff, ErrApprovalExpired
	}
	return cutoff, nil
}

// K6Intent reads the immutable operation binding. Its return value contains no
// grant token. The scheduler compares it with the sealed plan before reusing it.
func (s *Store) K6Intent(ctx context.Context, workflow string, iteration int, logicalKey string) (ApprovalIntent, error) {
	var intent ApprovalIntent
	err := s.pool.QueryRow(ctx, `SELECT intent_id::text, proposal_hash, decision, dispatch_status, COALESCE(grant_scope_hash,'')
		FROM engine.approval_action_intents WHERE workflow_id=$1 AND node_id=$2 AND iteration=$3 AND k6_logical_key=$4`,
		workflow, K6Activity, iteration, logicalKey).
		Scan(&intent.IntentID, &intent.ProposalHash, &intent.Decision, &intent.DispatchStatus, &intent.GrantScopeHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return ApprovalIntent{}, ErrApprovalNotFound
	}
	return intent, err
}

func (s *Store) k6DispatchGrant(ctx context.Context, db k6Querier, workflow string, iteration int, key, scope string) (k6Grant, error) {
	if !s.k6Approval || key == "" || scope == "" {
		return k6Grant{}, ErrGrantInvalid
	}
	g := k6Grant{K6Dispatch: K6Dispatch{WorkflowID: workflow, NodeID: K6Activity, Iteration: iteration, LogicalKey: key, ScopeHash: scope}}
	var proposal, version string
	var class EffectClass
	var sampled time.Time
	start := time.Now()
	err := db.QueryRow(ctx, `SELECT a.intent_id::text, a.grant_token::text, a.target, a.expected_resource_revision,
		a.canonical_arguments, a.proposal_hash, d.activity_versions->>$2::text, d.effect_classes->>$2::text,
		e.valid_until - e.uncertainty_us * interval '1 microsecond', clock_timestamp()
		FROM engine.approval_action_intents a
		JOIN engine.workflow_executions w USING (workflow_id)
		JOIN engine.workflow_definitions d ON (d.definition_id,d.version)=(w.definition_id,w.definition_version)
		JOIN k6_auth.episode e ON e.namespace=w.namespace
		WHERE a.workflow_id=$1 AND a.node_id=$2 AND a.iteration=$3 AND a.k6_logical_key=$4
		  AND a.grant_scope_hash=$5 AND a.decision='APPROVED' AND a.dispatch_status IN ('GRANTED','DISPATCHED')
		  AND a.valid_until=e.valid_until AND a.grant_expires_at=e.valid_until`,
		workflow, K6Activity, iteration, key, scope).Scan(&g.IntentID, &g.GrantToken, &g.ResourceID, &g.ExpectedRevision,
		&g.Arguments, &proposal, &version, &class, &g.latest, &sampled)
	if errors.Is(err, pgx.ErrNoRows) {
		return k6Grant{}, ErrGrantInvalid
	}
	if err != nil {
		return k6Grant{}, err
	}
	g.lockBy = start.Add(g.latest.Sub(sampled))
	if !time.Now().Before(g.lockBy) {
		return k6Grant{}, ErrApprovalExpired
	}
	actualProposal, err := K6ProposalHash(g.ResourceID, g.Arguments, g.ExpectedRevision)
	if version != "v1" || class != EffectCooperating || err != nil || proposal != actualProposal ||
		scope != K6GrantScopeHash(proposal, g.ExpectedRevision, key, g.ResourceID) {
		return k6Grant{}, ErrGrantInvalid
	}
	g.CanonicalHash, err = CanonicalPayloadHash(g.Arguments)
	return g, err
}

type k6Claim struct {
	ResultInput
	workerID string
}

type k6Attempt struct {
	key, scope, outcome string
	fence               int64
	proof               []byte
}

func currentK6Attempt(ctx context.Context, db k6Querier, claim k6Claim, live bool) (k6Attempt, error) {
	var attempt k6Attempt
	err := db.QueryRow(ctx, `SELECT a.logical_effect_key, a.grant_scope_hash, a.outcome_disposition,
		COALESCE(a.k6_fence,0), a.k6_proof
		FROM engine.activity_attempts a JOIN engine.node_instances n USING (workflow_id,node_id,iteration)
		JOIN engine.workflow_executions w USING (workflow_id)
		WHERE a.workflow_id=$1 AND a.node_id=$2 AND a.iteration=$3 AND a.attempt_number=$4
		  AND a.claim_token=$5 AND ($6='' OR a.worker_id=$6) AND a.is_current AND a.state='CLAIMED'
		  AND a.effect_class='COOPERATING_EFFECT' AND n.current_attempt_number=a.attempt_number
		  AND n.state='WAITING_ACTIVITY' AND w.state='WAITING_ACTIVITY'
		  AND (NOT $7 OR a.heartbeat_deadline > clock_timestamp())`,
		claim.WorkflowID, K6Activity, claim.Iteration, claim.AttemptNumber, claim.ClaimToken, claim.workerID, live).
		Scan(&attempt.key, &attempt.scope, &attempt.outcome, &attempt.fence, &attempt.proof)
	if errors.Is(err, pgx.ErrNoRows) {
		return k6Attempt{}, ErrAttemptNotCurrent
	}
	return attempt, err
}

func lockK6Workflow(ctx context.Context, tx pgx.Tx, workflow string) error {
	var id string
	return tx.QueryRow(ctx, `SELECT workflow_id FROM engine.workflow_executions WHERE workflow_id=$1 FOR UPDATE`, workflow).Scan(&id)
}

type K6Allocator interface {
	Allocate(context.Context, K6Dispatch) (int64, error)
}

func (s *Store) K6ClaimEnvelope(ctx context.Context, input ClaimInput, claim ClaimResult, allocator K6Allocator) (K6Dispatch, error) {
	if !s.k6Approval || !input.K6Authenticated || input.WorkerID == "" || input.NodeID != K6Activity ||
		claim.EffectClass != EffectCooperating || allocator == nil {
		return K6Dispatch{}, ErrGrantInvalid
	}
	id := k6Claim{ResultInput: ResultInput{WorkflowID: input.WorkflowID, NodeID: K6Activity, Iteration: input.Iteration,
		AttemptNumber: claim.AttemptNumber, ClaimToken: claim.ClaimToken}, workerID: input.WorkerID}
	a, err := currentK6Attempt(ctx, s.pool, id, true)
	if err != nil {
		return K6Dispatch{}, err
	}
	grant, err := s.k6DispatchGrant(ctx, s.pool, id.WorkflowID, id.Iteration, a.key, a.scope)
	if err != nil {
		return K6Dispatch{}, err
	}
	grant.Attempt = id.AttemptNumber
	grant.LookupFirst = grant.Attempt > 1
	ctx, cancel := context.WithDeadline(ctx, grant.lockBy)
	defer cancel()
	fence := a.fence
	if fence == 0 {
		fence, err = allocator.Allocate(ctx, grant.K6Dispatch)
		if err != nil || fence <= 0 {
			return K6Dispatch{}, ErrK6Unavailable
		}
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return K6Dispatch{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := lockK6Workflow(ctx, tx, id.WorkflowID); err != nil {
		return K6Dispatch{}, err
	}
	a, err = currentK6Attempt(ctx, tx, id, true)
	if err != nil {
		return K6Dispatch{}, err
	}
	if a.outcome == "OUTCOME_UNKNOWN" || len(a.proof) != 0 || (a.fence != 0 && a.fence != fence) {
		return K6Dispatch{}, ErrAttemptNotCurrent
	}
	tag, err := tx.Exec(ctx, `UPDATE engine.activity_attempts SET k6_fence=$5
		WHERE workflow_id=$1 AND node_id=$2 AND iteration=$3 AND attempt_number=$4
		  AND clock_timestamp() < $6 AND heartbeat_deadline > clock_timestamp()`,
		id.WorkflowID, K6Activity, id.Iteration, id.AttemptNumber, fence, grant.latest)
	if err != nil {
		return K6Dispatch{}, err
	}
	if tag.RowsAffected() != 1 {
		return K6Dispatch{}, ErrStaleClaim
	}
	if err := tx.Commit(ctx); err != nil {
		return K6Dispatch{}, err
	}
	grant.Fence = fence
	return grant.K6Dispatch, nil
}

// StageK6Result persists an uncertainty signal or an authenticated proof. Raw
// worker payloads are never written to state, history, or outbox.
func (s *Store) StageK6Result(ctx context.Context, input ResultInput, worker string, proof *k6receipt.Proof, key ed25519.PublicKey) error {
	if !s.k6Approval || input.NodeID != K6Activity || worker == "" {
		return ErrGrantInvalid
	}
	if (proof == nil && input.AttemptState != AttemptOutcomeUnknown) ||
		(proof != nil && input.AttemptState != AttemptSucceeded) {
		return ErrInvalidAttemptState
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := lockK6Workflow(ctx, tx, input.WorkflowID); err != nil {
		return err
	}
	a, err := currentK6Attempt(ctx, tx, k6Claim{input, worker}, true)
	if err != nil {
		return err
	}
	if proof == nil {
		if a.fence == 0 {
			return ErrStaleClaim
		}
		_, err = tx.Exec(ctx, `UPDATE engine.activity_attempts SET outcome_disposition='OUTCOME_UNKNOWN',
			updated_at=clock_timestamp() WHERE workflow_id=$1 AND node_id=$2 AND iteration=$3 AND attempt_number=$4
			AND k6_proof IS NULL`, input.WorkflowID, K6Activity, input.Iteration, input.AttemptNumber)
	} else {
		grant, grantErr := s.k6DispatchGrant(ctx, tx, input.WorkflowID, input.Iteration, a.key, a.scope)
		if grantErr != nil {
			return grantErr
		}
		if !validK6Proof(input, a, grant, *proof, key) {
			return ErrGrantInvalid
		}
		body, marshalErr := json.Marshal(proof)
		if marshalErr != nil {
			return marshalErr
		}
		tag, updateErr := tx.Exec(ctx, `UPDATE engine.activity_attempts SET k6_proof=$5, updated_at=clock_timestamp()
			WHERE workflow_id=$1 AND node_id=$2 AND iteration=$3 AND attempt_number=$4
			  AND clock_timestamp() < $6 AND (k6_proof IS NULL OR k6_proof = $5::jsonb)`,
			input.WorkflowID, K6Activity, input.Iteration, input.AttemptNumber, body, grant.latest)
		if updateErr != nil {
			return updateErr
		}
		if tag.RowsAffected() != 1 {
			return ErrResultConflict
		}
	}
	if err != nil {
		return err
	}
	var receiptID string
	if proof != nil {
		receiptID = proof.Receipt.ApplicationID
	}
	if err := captureResult(ctx, tx, input, "PENDING_VERIFICATION", receiptID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func validK6Proof(input ResultInput, a k6Attempt, grant k6Grant, proof k6receipt.Proof, key ed25519.PublicKey) bool {
	r := proof.Receipt
	revision, err := strconv.ParseInt(grant.ExpectedRevision, 10, 64)
	claim := k6receipt.Claim{NodeID: K6Activity, Iteration: input.Iteration, Attempt: input.AttemptNumber, Fence: a.fence}
	return err == nil && a.fence > 0 && (a.outcome != "OUTCOME_UNKNOWN" || proof.Operation == "lookup") &&
		r.WorkflowID == input.WorkflowID && r.IntentID == grant.IntentID && r.LogicalKey == grant.LogicalKey &&
		r.ScopeHash == grant.ScopeHash && r.ResourceID == grant.ResourceID && r.CanonicalHash == grant.CanonicalHash &&
		r.ExpectedRevision == revision && r.AppliedRevision > revision && r.AppliedRevision-1 == revision &&
		r.ApplicationID != "" && k6receipt.Verify(key, claim, proof)
}

type k6Completion struct {
	lease     LeaseRef
	key       ed25519.PublicKey
	lockBy    time.Time
	receiptID string
}

func (s *Store) CompleteK6Result(ctx context.Context, lease LeaseRef, attempt Attempt, key ed25519.PublicKey) error {
	_, err := s.recordResultReceipt(ctx, ResultInput{WorkflowID: attempt.WorkflowID, NodeID: K6Activity,
		Iteration: attempt.Iteration, AttemptNumber: attempt.AttemptNumber, ClaimToken: attempt.ClaimToken,
		AttemptState: AttemptSucceeded, EventType: "activity.result"}, &k6Completion{lease: lease, key: key})
	return err
}

func (s *Store) acknowledgeK6Result(ctx context.Context, tx pgx.Tx, input *ResultInput, completion *k6Completion) error {
	a, err := currentK6Attempt(ctx, tx, k6Claim{ResultInput: *input}, false)
	if err != nil {
		return err
	}
	if len(a.proof) == 0 {
		return ErrK6ResultPending
	}
	grant, err := s.k6DispatchGrant(ctx, tx, input.WorkflowID, input.Iteration, a.key, a.scope)
	if err != nil {
		return err
	}
	var proof k6receipt.Proof
	if json.Unmarshal(a.proof, &proof) != nil || !validK6Proof(*input, a, grant, proof, completion.key) {
		return ErrGrantInvalid
	}
	completion.lockBy = grant.lockBy
	ctx, cancel := context.WithDeadline(ctx, grant.lockBy)
	defer cancel()
	if err := markGrantDispatched(ctx, tx, grant.IntentID, grant.GrantToken, &grant.latest); err != nil {
		if errors.Is(err, ErrGrantInvalid) {
			return ErrApprovalExpired
		}
		return err
	}
	input.Payload, err = json.Marshal(proof.Receipt)
	completion.receiptID = proof.Receipt.ApplicationID
	return err
}

func (s *Store) ExpireK6Operation(ctx context.Context, lease LeaseRef, workflow Workflow, node NodeInstance, actor string) error {
	return s.ApplyOwnerTransition(ctx, OwnerTransitionInput{Lease: lease, WorkflowID: workflow.WorkflowID,
		ExpectedRevision: workflow.Revision, NewState: StateReconciliationRequired, ActorID: actor,
		Reason: "K6_GRANT_EXPIRED_OUTCOME_UNKNOWN", NodeID: node.NodeID, Iteration: node.Iteration, k6Expired: true})
}

func (s *Store) expireK6Attempt(ctx context.Context, tx pgx.Tx, input OwnerTransitionInput, namespace string, attempt *int64) error {
	if !s.k6Approval || input.NodeID != K6Activity {
		return ErrGrantInvalid
	}
	var expired bool
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp() >= valid_until - uncertainty_us * interval '1 microsecond'
		FROM k6_auth.episode WHERE namespace=$1`, namespace).Scan(&expired); err != nil {
		return err
	}
	if !expired {
		return ErrInvalidTransition
	}
	if attempt != nil {
		if _, err := tx.Exec(ctx, `UPDATE engine.activity_attempts SET state='OUTCOME_UNKNOWN',
			outcome_disposition='OUTCOME_UNKNOWN', is_current=false, updated_at=clock_timestamp()
			WHERE workflow_id=$1 AND node_id=$2 AND iteration=$3 AND attempt_number=$4 AND is_current`,
			input.WorkflowID, K6Activity, input.Iteration, *attempt); err != nil {
			return err
		}
	}
	ref := fmt.Sprintf("k6/%s/%s/%d", input.WorkflowID, K6Activity, input.Iteration)
	_, err := tx.Exec(ctx, `INSERT INTO engine.reconciliation_items
		(item_id,workflow_id,partition_id,node_id,iteration,attempt_number,kind,reference,detail)
		VALUES ($1,$2,$3,$4,$5,$6,'EXPIRED_ATTEMPT',$7,'{"outcome":"OUTCOME_UNKNOWN","reason":"K6_GRANT_EXPIRED"}')
		ON CONFLICT (kind,reference) DO NOTHING`, NewID(), input.WorkflowID, input.Lease.PartitionID,
		K6Activity, input.Iteration, attempt, ref)
	return err
}
