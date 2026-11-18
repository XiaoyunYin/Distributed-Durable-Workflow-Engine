package engine

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"unicode/utf8"

	"durable-agent-execution-engine/internal/state"
)

type K6Options struct {
	Plan       *K6Plan
	ReceiptKey ed25519.PublicKey
}

type K6Operation struct {
	SubmissionKey     string          `json:"submission_key"`
	DefinitionID      string          `json:"definition_id"`
	DefinitionVersion int             `json:"definition_version"`
	NodeID            string          `json:"node_id"`
	Iteration         int             `json:"iteration"`
	Resource          string          `json:"resource"`
	ExpectedRevision  string          `json:"expected_revision"`
	Arguments         json.RawMessage `json:"arguments"`
	LogicalKey        string          `json:"logical_key"`
}

type K6Plan struct {
	namespace  string
	operations map[string]K6Operation
}

func ParseK6Plan(raw []byte, digest, namespace string) (*K6Plan, error) {
	sum := sha256.Sum256(raw)
	if !utf8.Valid(raw) || hex.EncodeToString(sum[:]) != digest || state.ValidateK6Arguments(raw) != nil {
		return nil, errors.New("K6 sealed plan verification failed")
	}
	var doc struct {
		Namespace  string        `json:"namespace"`
		Operations []K6Operation `json:"operations"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&doc); err != nil || decoder.Decode(new(any)) != io.EOF ||
		namespace == "" || doc.Namespace != namespace || len(doc.Operations) == 0 {
		return nil, errors.New("invalid K6 sealed plan")
	}
	plan := &K6Plan{namespace: namespace, operations: make(map[string]K6Operation, len(doc.Operations))}
	keys := map[string]bool{}
	resources := map[string]bool{}
	for _, op := range doc.Operations {
		if !k6Identifier(op.SubmissionKey) || !k6Identifier(op.Resource) || !k6Identifier(op.LogicalKey) ||
			op.DefinitionID == "" || op.DefinitionVersion <= 0 || op.NodeID != state.K6Activity || op.Iteration < 0 ||
			op.ExpectedRevision != "0" || len(op.Arguments) > 48<<10 || state.ValidateK6Arguments(op.Arguments) != nil {
			return nil, errors.New("invalid K6 sealed operation")
		}
		if _, exists := plan.operations[op.SubmissionKey]; exists || keys[op.LogicalKey] || resources[op.Resource] {
			return nil, state.ErrEffectIdentityMismatch
		}
		keys[op.LogicalKey], resources[op.Resource] = true, true
		plan.operations[op.SubmissionKey] = op
	}
	return plan, nil
}

func k6Identifier(s string) bool {
	return s != "" && len(s) <= 512 && strings.IndexFunc(s, func(r rune) bool { return r <= ' ' || r == 127 }) < 0
}

func (p *K6Plan) operation(wf state.Workflow, node state.NodeInstance) (K6Operation, error) {
	op, found := p.operations[wf.SubmissionKey]
	if !found || wf.Namespace != p.namespace || wf.DefinitionID != op.DefinitionID || wf.DefinitionVersion != op.DefinitionVersion ||
		node.NodeID != op.NodeID || node.Iteration != op.Iteration {
		return K6Operation{}, state.ErrEffectIdentityMismatch
	}
	return op, nil
}

func (op K6Operation) matches(intent state.ApprovalIntent) bool {
	hash, err := state.K6ProposalHash(op.Resource, op.Arguments, op.ExpectedRevision)
	return err == nil && intent.ProposalHash == hash
}

func (e *Engine) prepareK6Activity(ctx context.Context, wf state.Workflow, node state.NodeInstance, lease state.LeaseRef, actor string) (bool, error) {
	op, err := e.K6.Plan.operation(wf, node)
	if err != nil {
		return false, err
	}
	intent, intentErr := e.Store.K6Intent(ctx, wf.WorkflowID, node.Iteration, op.LogicalKey)
	if intentErr == nil && !op.matches(intent) {
		return false, state.ErrEffectIdentityMismatch
	}
	cutoff, err := e.Store.K6Window(ctx, wf.Namespace)
	if errors.Is(err, state.ErrApprovalExpired) && node.State == state.StateWaitingApproval &&
		intentErr == nil && intent.Decision == "REJECTED" {
		return e.applyK6Approval(ctx, wf, node, lease, actor, op, intent)
	}
	if errors.Is(err, state.ErrApprovalExpired) {
		return true, e.Store.ExpireK6Operation(ctx, lease, wf, node, actor)
	}
	if err != nil {
		return false, err
	}
	err = intentErr
	if errors.Is(err, state.ErrApprovalNotFound) && node.State == state.StateRunnable {
		_, err = e.Store.CreateApprovalIntent(ctx, state.ApprovalIntentInput{Lease: lease, WorkflowID: wf.WorkflowID,
			NodeID: node.NodeID, Iteration: node.Iteration, ExpectedRevision: wf.Revision, Target: op.Resource,
			CanonicalArguments: op.Arguments, ExpectedResourceRevision: op.ExpectedRevision,
			ValidUntil: cutoff, ActorID: actor, K6LogicalKey: op.LogicalKey})
		var existing *state.K6ExistingIntent
		if errors.As(err, &existing) {
			intent, err = e.Store.K6Intent(ctx, wf.WorkflowID, node.Iteration, op.LogicalKey)
			if err == nil && intent.IntentID != existing.IntentID {
				return false, state.ErrEffectIdentityMismatch
			}
		} else {
			return err == nil, err
		}
	}
	if err != nil {
		return false, err
	}
	if !op.matches(intent) {
		return false, state.ErrEffectIdentityMismatch
	}
	if node.State == state.StateWaitingApproval {
		if intent.Decision == "PENDING" {
			return false, nil
		}
		return e.applyK6Approval(ctx, wf, node, lease, actor, op, intent)
	}
	if intent.Decision != "APPROVED" || (intent.DispatchStatus != "GRANTED" && intent.DispatchStatus != "DISPATCHED") {
		return false, state.ErrGrantInvalid
	}
	return false, nil
}

func (e *Engine) applyK6Approval(ctx context.Context, wf state.Workflow, node state.NodeInstance, lease state.LeaseRef,
	actor string, op K6Operation, intent state.ApprovalIntent) (bool, error) {
	_, err := e.Store.ApplyApproval(ctx, state.ApplyApprovalInput{Lease: lease, WorkflowID: wf.WorkflowID,
		NodeID: node.NodeID, Iteration: node.Iteration, IntentID: intent.IntentID,
		LogicalEffectKey: op.LogicalKey, ExpectedRevision: wf.Revision, ActorID: actor})
	var granted *state.K6AlreadyGranted
	if errors.As(err, &granted) && granted.IntentID == intent.IntentID {
		// ApplyApproval commits RUNNABLE together with its single grant.
		// The next scan will read the committed workflow state.
		return false, nil
	}
	return err == nil, err
}

func (e *Engine) recoverK6Attempt(ctx context.Context, wf state.Workflow, node state.NodeInstance,
	attempt state.Attempt, lease state.LeaseRef, actor string) (bool, error) {
	if attempt.State == state.AttemptClaimed && attempt.IsCurrent {
		err := e.Store.CompleteK6Result(ctx, lease, attempt, e.K6.ReceiptKey)
		switch {
		case err == nil:
			return true, e.boundary("result_recorded")
		case errors.Is(err, state.ErrApprovalExpired):
			return true, e.Store.ExpireK6Operation(ctx, lease, wf, node, actor)
		case !errors.Is(err, state.ErrK6ResultPending):
			return false, err
		}
	}
	_, err := e.Store.K6Window(ctx, wf.Namespace)
	if errors.Is(err, state.ErrApprovalExpired) {
		return true, e.Store.ExpireK6Operation(ctx, lease, wf, node, actor)
	}
	return false, err
}
