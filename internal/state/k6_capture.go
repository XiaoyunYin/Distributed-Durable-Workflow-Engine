//go:build dur053_k6

package state

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func configureK6Capture(config *pgxpool.Config) {
	if os.Getenv("DUR053_K6_CAPTURE") == "1" && config.ConnConfig.RuntimeParams["durable.k4_campaign"] == "on" {
		config.ConnConfig.RuntimeParams["durable.k6_capture"] = "on"
	}
}

func captureEnabled(conn *pgx.ConnConfig) bool {
	return conn.RuntimeParams["durable.k6_capture"] == "on"
}

func captureOwner(ctx context.Context, tx pgx.Tx, ref LeaseRef) (bool, error) {
	if !captureEnabled(tx.Conn().Config()) {
		return false, nil
	}
	_, err := tx.Exec(ctx, `SELECT set_config('durable.k4_owner', $1, true),
		set_config('durable.k6_owner_partition', $2::smallint::text, true),
		set_config('durable.k6_owner_epoch', $3::bigint::text, true)`,
		ref.OwnerID, ref.PartitionID, ref.Epoch)
	return true, err
}

func captureResult(ctx context.Context, tx pgx.Tx, input ResultInput, disposition, receiptID string) error {
	if !captureEnabled(tx.Conn().Config()) {
		return nil
	}
	token := sha256.Sum256([]byte(input.ClaimToken))
	_, err := tx.Exec(ctx, `
		INSERT INTO engine.k6_capture_queue (subject, operation, row_key, workflow_id, partition_id, after_image)
		SELECT 'result_request', $7, jsonb_build_array(a.workflow_id, a.node_id, a.iteration, a.attempt_number),
			a.workflow_id, w.partition_id, jsonb_build_object('worker_request_id', a.worker_request_id,
			'token_hash', $5::text, 'receipt_id', NULLIF($6::text, ''), 'attempt_state', $8::text)
		FROM engine.activity_attempts a JOIN engine.workflow_executions w USING (workflow_id)
		WHERE a.workflow_id=$1 AND a.node_id=$2 AND a.iteration=$3 AND a.attempt_number=$4`,
		input.WorkflowID, input.NodeID, input.Iteration, input.AttemptNumber,
		hex.EncodeToString(token[:]), receiptID, disposition, string(input.AttemptState))
	return err
}

func captureDelivery(ctx context.Context, tx pgx.Tx, message InboxMessage, disposition InboxDisposition) error {
	if !captureEnabled(tx.Conn().Config()) {
		return nil
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO engine.k6_capture_queue (subject, operation, row_key, after_image)
		VALUES ('physical_delivery', $6,
			jsonb_build_array($1::text, $2::text, $3::integer, $4::bigint),
			jsonb_build_object('event_id', NULLIF($5::text, '')))`,
		message.ConsumerID, message.Topic, message.Partition, message.Offset, message.EventID, string(disposition))
	return err
}

// CapturePublicationStart commits the engine's intent immediately before the
// broker call. Broker delivery and acknowledgement require separate witnesses.
func (s *Store) CapturePublicationStart(ctx context.Context, event OutboxEvent) error {
	if !captureEnabled(s.pool.Config().ConnConfig) {
		return nil
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO engine.k6_capture_queue (subject, operation, row_key, workflow_id, partition_id, after_image)
		VALUES ('publication_start', 'start', jsonb_build_array($1::text, $2::integer),
			$3, (SELECT partition_id FROM engine.workflow_executions WHERE workflow_id=$3),
			jsonb_build_object('topic', $4::text, 'relay_owner', $5::text))`,
		event.EventID, event.RelayAttempts, event.WorkflowID, event.Topic, event.ClaimOwner)
	return err
}
