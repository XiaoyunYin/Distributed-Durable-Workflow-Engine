package state

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func (s *Store) CheckTenantRequesterSchema(ctx context.Context) error {
	var installed bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM engine.schema_migrations WHERE version = 26)`).Scan(&installed); err != nil {
		return fmt.Errorf("check J12 requester migration: %w", err)
	}
	if !installed {
		return errors.New("J12 requires requester migration j12/000026")
	}
	if _, err := s.pool.Exec(ctx, `SELECT workflow_id, issuer, subject FROM engine.j12_workflow_requesters LIMIT 0`); err != nil {
		return fmt.Errorf("J12 requester migration unavailable: %w", err)
	}
	return nil
}

func validateSubmissionReplay(ctx context.Context, tx pgx.Tx, workflow Workflow, input CreateWorkflowInput) error {
	if workflow.PayloadHash != input.SubmissionPayloadHash {
		return ErrSubmissionConflict
	}
	if input.Tenant == nil {
		return nil
	}
	var matches bool
	err := tx.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM engine.j12_workflow_requesters
		WHERE workflow_id = $1 AND issuer = $2 AND subject = $3
	)`, workflow.WorkflowID, input.Tenant.Issuer, input.Tenant.Subject).Scan(&matches)
	if err != nil {
		return fmt.Errorf("read J12 requester binding: %w", err)
	}
	if !matches {
		return ErrSubmissionConflict
	}
	return nil
}

func lockTenantApprovalWorkflow(ctx context.Context, tx pgx.Tx, intentID string, tenant TenantPrincipal) (string, error) {
	// Lock workflow before intent, as ApplyApproval does. The path intent ID
	// supplies the join, and foreign work has the same result as an absent ID.
	var workflowID, issuer, requester string
	err := tx.QueryRow(ctx, `
		SELECT w.workflow_id, COALESCE(r.issuer, ''), COALESCE(r.subject, '')
		FROM engine.approval_action_intents AS i
		JOIN engine.workflow_executions AS w ON w.workflow_id = i.workflow_id
		LEFT JOIN engine.j12_workflow_requesters AS r ON r.workflow_id = w.workflow_id
		WHERE i.intent_id = $1 AND w.namespace = $2
		FOR SHARE OF w`, intentID, tenant.Namespace).Scan(&workflowID, &issuer, &requester)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrApprovalNotFound
	}
	if err != nil {
		return "", fmt.Errorf("resolve J12 approval requester: %w", err)
	}
	if requester == "" || issuer != tenant.Issuer || requester == tenant.Subject {
		return "", ErrApprovalUnauthorized
	}
	return workflowID, nil
}
