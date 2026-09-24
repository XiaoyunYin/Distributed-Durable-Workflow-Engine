package state

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
)

const (
	dur050AdmissionActiveEnv = "DUR050_ADMISSION_MAX_ACTIVE"
	dur050AdmissionOutboxEnv = "DUR050_ADMISSION_MAX_PENDING_OUTBOX"
)

func admissionConfigFromEnvironment() (AdmissionGateConfig, error) {
	activeRaw, outboxRaw := os.Getenv(dur050AdmissionActiveEnv), os.Getenv(dur050AdmissionOutboxEnv)
	if activeRaw == "" && outboxRaw == "" {
		return AdmissionGateConfig{}, nil
	}
	if activeRaw == "" || outboxRaw == "" {
		return AdmissionGateConfig{}, fmt.Errorf("%s and %s must be set together", dur050AdmissionActiveEnv, dur050AdmissionOutboxEnv)
	}
	active, err := strconv.ParseInt(activeRaw, 10, 64)
	if err != nil {
		return AdmissionGateConfig{}, fmt.Errorf("parse %s: %w", dur050AdmissionActiveEnv, err)
	}
	outbox, err := strconv.ParseInt(outboxRaw, 10, 64)
	if err != nil {
		return AdmissionGateConfig{}, fmt.Errorf("parse %s: %w", dur050AdmissionOutboxEnv, err)
	}
	config := AdmissionGateConfig{MaxActiveWorkflows: active, MaxPendingOutbox: outbox}
	if err := config.validate(); err != nil {
		return AdmissionGateConfig{}, err
	}
	return config, nil
}

func (config AdmissionGateConfig) validate() error {
	if config.MaxActiveWorkflows < 0 || config.MaxPendingOutbox < 0 {
		return errors.New("DUR-050 admission limits cannot be negative")
	}
	if (config.MaxActiveWorkflows == 0) != (config.MaxPendingOutbox == 0) {
		return errors.New("both DUR-050 admission limits must be zero (disabled) or positive")
	}
	return nil
}

func (config AdmissionGateConfig) enabled() bool {
	return config.MaxActiveWorkflows > 0 && config.MaxPendingOutbox > 0
}

func isDur050Namespace(namespace string) bool {
	return strings.HasPrefix(namespace, "dur050-")
}

func (s *Store) lockAdmissionNamespace(ctx context.Context, tx pgx.Tx, namespace string) (bool, error) {
	if err := s.dur050Admission.validate(); err != nil {
		return false, err
	}
	if !s.dur050Admission.enabled() || !isDur050Namespace(namespace) {
		return false, nil
	}
	// Serialize only submissions in this campaign namespace. A transaction-
	// scoped advisory lock makes the count-and-reserve operation atomic across
	// API replicas without adding a product-wide admission dependency.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 50050))`, namespace); err != nil {
		return false, fmt.Errorf("lock DUR-050 admission namespace: %w", err)
	}
	return true, nil
}

func (s *Store) checkAdmissionCapacity(ctx context.Context, tx pgx.Tx, namespace string) error {
	var active int64
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM engine.dur050_admission_slots
		WHERE namespace = $1`, namespace).Scan(&active); err != nil {
		return fmt.Errorf("count DUR-050 active admission slots: %w", err)
	}
	if active >= s.dur050Admission.MaxActiveWorkflows {
		return ErrAdmissionLimit
	}
	pending, err := pendingOutboxCount(ctx, tx, namespace)
	if err != nil {
		return err
	}
	// A new accepted workflow writes one workflow.created event in this same
	// transaction, so equality already means that the next event would exceed
	// the configured bound.
	if pending >= s.dur050Admission.MaxPendingOutbox {
		return ErrAdmissionBackpressure
	}
	return nil
}

func (s *Store) checkOutboxInsertCapacity(ctx context.Context, tx pgx.Tx, namespace string) error {
	if err := s.dur050Admission.validate(); err != nil {
		return err
	}
	if !s.dur050Admission.enabled() || !isDur050Namespace(namespace) {
		return nil
	}
	if _, err := s.lockAdmissionNamespace(ctx, tx, namespace); err != nil {
		return err
	}
	pending, err := pendingOutboxCount(ctx, tx, namespace)
	if err != nil {
		return err
	}
	if pending >= s.dur050Admission.MaxPendingOutbox {
		return ErrAdmissionBackpressure
	}
	return nil
}

func pendingOutboxCount(ctx context.Context, tx pgx.Tx, namespace string) (int64, error) {
	var pending int64
	if err := tx.QueryRow(ctx, `
		SELECT count(*)
		FROM engine.workflow_executions AS w
		JOIN engine.outbox AS o USING (workflow_id)
		WHERE w.namespace = $1 AND o.publish_state IN ('PENDING', 'CLAIMED')`, namespace).Scan(&pending); err != nil {
		return 0, fmt.Errorf("count DUR-050 pending outbox events: %w", err)
	}
	return pending, nil
}

func insertAdmissionSlot(ctx context.Context, tx pgx.Tx, namespace, workflowID string) error {
	if _, err := tx.Exec(ctx, `INSERT INTO engine.dur050_admission_slots (workflow_id, namespace)
		VALUES ($1, $2)`, workflowID, namespace); err != nil {
		return fmt.Errorf("reserve DUR-050 admission slot: %w", err)
	}
	return nil
}

// releaseAdmissionSlotTx is called after the terminal workflow update but
// before commit, so a failure at this boundary rolls both writes back.
func (s *Store) releaseAdmissionSlotTx(ctx context.Context, tx pgx.Tx, namespace, workflowID string,
	oldState, newState WorkflowState) error {
	if !s.dur050Admission.enabled() || !isDur050Namespace(namespace) ||
		isTerminalWorkflowState(oldState) || !isTerminalWorkflowState(newState) {
		return nil
	}
	commandTag, err := tx.Exec(ctx, `DELETE FROM engine.dur050_admission_slots
		WHERE workflow_id = $1 AND namespace = $2`, workflowID, namespace)
	if err != nil {
		return fmt.Errorf("release DUR-050 admission slot: %w", err)
	}
	if commandTag.RowsAffected() != 1 {
		return fmt.Errorf("release DUR-050 admission slot: workflow %s has no active slot", workflowID)
	}
	return nil
}
