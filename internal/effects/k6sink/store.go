package k6sink

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"durable-agent-execution-engine/internal/k6receipt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Receipt = k6receipt.Receipt

type delivery struct {
	operation, bodyHash, connectionHash string
	request                             Request
	auth                                authorization
	cooperating                         bool
}

type result struct {
	outcome    string
	body       []byte
	validUntil time.Time
}

type grantRecheck func(context.Context) (authorization, error)

type backend interface {
	execute(context.Context, delivery, grantRecheck) (result, error)
}

type Store struct{ pool *pgxpool.Pool }

func OpenStore(ctx context.Context, dsn string) (*Store, error) {
	pool, err := openPool(ctx, dsn, "k6_sink_service", "dur053-k6-sink", 4)
	if err != nil {
		return nil, err
	}
	return &Store{pool}, nil
}

func openPool(ctx context.Context, dsn, role, app string, maxConns int32) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if dsn == "" || err != nil || cfg.ConnConfig.User != role || cfg.ConnConfig.Database == "" {
		return nil, errUnavailable
	}
	cfg.ConnConfig.RuntimeParams["application_name"] = app
	cfg.ConnConfig.RuntimeParams["lock_timeout"] = "1000"
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = "2000"
	cfg.ConnConfig.RuntimeParams["synchronous_commit"] = "on"
	cfg.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = "5000"
	cfg.ConnConfig.RuntimeParams["timezone"] = "UTC"
	cfg.ConnConfig.RuntimeParams["search_path"] = "pg_catalog, pg_temp"
	cfg.MaxConns, cfg.MinConns, cfg.MinIdleConns = maxConns, 0, 0
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, errUnavailable
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, errUnavailable
	}
	return pool, nil
}

func (s *Store) Close() { s.pool.Close() }

func (s *Store) execute(ctx context.Context, d delivery, check grantRecheck) (result, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return result{}, errUnavailable
	}
	defer func() {
		rollback, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		defer cancel()
		_ = tx.Rollback(rollback)
	}()

	out, err := perform(ctx, tx, d, check)
	switch {
	case errors.Is(err, errDenied):
		return result{}, errDenied
	case err != nil:
		return result{}, errUnavailable
	}
	commit, cancel := context.WithDeadline(ctx, out.validUntil)
	defer cancel()
	if err := tx.Commit(commit); err != nil {
		return result{}, errUnavailable
	}
	return out, nil
}

const receiptColumns = `workflow_id, logical_key, resource_id, canonical_hash, scope_hash,
    intent_id::text AS intent_id, expected_revision, applied_revision, application_id::text AS application_id`

func scanReceipt(row pgx.Row, receipt *Receipt) error {
	return row.Scan(&receipt.WorkflowID, &receipt.LogicalKey, &receipt.ResourceID, &receipt.CanonicalHash,
		&receipt.ScopeHash, &receipt.IntentID, &receipt.ExpectedRevision, &receipt.AppliedRevision, &receipt.ApplicationID)
}

func perform(ctx context.Context, tx pgx.Tx, d delivery, check grantRecheck) (result, error) {
	r := d.request
	if d.operation == "allocate" {
		if _, err := tx.Exec(ctx, `SAVEPOINT allocate_resource`); err != nil {
			return result{}, err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO k6_sink.resources (resource_id) VALUES ($1) ON CONFLICT DO NOTHING`, r.ResourceID); err != nil {
			return result{}, err
		}
	}
	var revision, latestFence int64
	err := tx.QueryRow(ctx, `SELECT revision, latest_fence FROM k6_sink.resources WHERE resource_id = $1 FOR UPDATE`, r.ResourceID).
		Scan(&revision, &latestFence)
	if errors.Is(err, pgx.ErrNoRows) {
		return result{}, errDenied // An issued allocation always has a resource row.
	}
	if err != nil {
		return result{}, err
	}

	var issuedFence int64
	err = tx.QueryRow(ctx, `SELECT fence FROM k6_sink.allocations
		WHERE intent_id = $1 AND grant_hash = $2 AND attempt = $3 AND resource_id = $4`,
		r.IntentID, d.auth.grantHash, r.Attempt, r.ResourceID).Scan(&issuedFence)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return result{}, err
	}
	if d.operation != "allocate" && (issuedFence == 0 || issuedFence != r.Fence) {
		return result{}, errDenied
	}

	matches, err := bindRequest(ctx, tx, r.RequestID, hash([]byte(d.operation+"\x00"+d.bodyHash)))
	if err != nil {
		return result{}, err
	}
	// Same-key competitors can present different authorized targets. Lock
	// the stable key across resources and re-read the receipt under both locks.
	if d.operation != "allocate" && matches {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(603053, hashtext($1))`, r.WorkflowID+"\x1f"+r.LogicalKey); err != nil {
			return result{}, err
		}
	}
	checked, err := check(ctx)
	if err != nil {
		return result{}, err
	}
	if checked.deadline.Before(d.auth.deadline) {
		d.auth.deadline = checked.deadline
	}
	d.auth.grant.DBTime = checked.grant.DBTime
	ctx, cancel := context.WithDeadline(ctx, d.auth.deadline)
	defer cancel()
	if !matches {
		// Roll back absent-resource creation on an allocation replay with a
		// conflicting body. There can be no such creation on worker calls.
		if d.operation == "allocate" {
			if _, err := tx.Exec(ctx, `ROLLBACK TO SAVEPOINT allocate_resource`); err != nil {
				return result{}, err
			}
		}
		return recordCall(ctx, tx, d, "REQUEST_CONFLICT", nil)
	}
	if d.operation == "allocate" {
		if d.cooperating && issuedFence == 0 {
			var superseded bool
			err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT FROM k6_sink.allocations
				WHERE intent_id = $1 AND grant_hash = $2 AND attempt > $3)`,
				r.IntentID, d.auth.grantHash, r.Attempt).Scan(&superseded)
			if err != nil {
				return result{}, err
			}
			if superseded {
				return result{}, errDenied
			}
		}
		return allocate(ctx, tx, d, issuedFence)
	}

	if d.cooperating && d.operation == "apply" && r.Attempt > 1 {
		// The allocation was issued before this lookup. The resource lock
		// excludes any old-fence application between the lookup and this write.
		var observedAbsent bool
		err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT FROM k6_sink.lookup_misses
			WHERE intent_id=$1 AND grant_hash=$2 AND attempt=$3)`,
			r.IntentID, d.auth.grantHash, r.Attempt).Scan(&observedAbsent)
		if err != nil {
			return result{}, err
		}
		if !observedAbsent {
			return result{}, errDenied
		}
	}

	var receipt Receipt
	err = scanReceipt(tx.QueryRow(ctx, `SELECT `+receiptColumns+` FROM k6_sink.receipts
		WHERE workflow_id = $1 AND logical_key = $2`, r.WorkflowID, r.LogicalKey), &receipt)
	switch {
	case err == nil:
		if receipt.ResourceID != r.ResourceID || receipt.IntentID != r.IntentID ||
			receipt.CanonicalHash != d.auth.canonicalHash || receipt.ScopeHash != r.ScopeHash ||
			receipt.ExpectedRevision != d.auth.revision {
			return recordCall(ctx, tx, d, "KEY_CONFLICT", nil)
		}
		outcome := "DUPLICATE"
		if d.operation == "lookup" {
			outcome = "FOUND"
		}
		return recordCall(ctx, tx, d, outcome, &receipt)
	case !errors.Is(err, pgx.ErrNoRows):
		return result{}, err
	case d.operation == "lookup":
		if d.cooperating && r.Attempt > 1 && issuedFence == latestFence {
			if _, err := tx.Exec(ctx, `INSERT INTO k6_sink.lookup_misses (intent_id, grant_hash, attempt)
				SELECT $1,$2,$3 WHERE clock_timestamp() < $4 ON CONFLICT DO NOTHING`,
				r.IntentID, d.auth.grantHash, r.Attempt, d.auth.sinkCutoff); err != nil {
				return result{}, err
			}
		}
		return recordCall(ctx, tx, d, "MISSING", nil)
	case issuedFence != latestFence:
		return recordCall(ctx, tx, d, "STALE_FENCE", nil)
	case revision != d.auth.revision:
		return recordCall(ctx, tx, d, "REVISION_CONFLICT", nil)
	}

	return apply(ctx, tx, d)
}

func bindRequest(ctx context.Context, tx pgx.Tx, requestID, bodyHash string) (bool, error) {
	if _, err := tx.Exec(ctx, `INSERT INTO k6_sink.request_bindings (request_id, body_hash)
		VALUES ($1, $2) ON CONFLICT DO NOTHING`, requestID, bodyHash); err != nil {
		return false, err
	}
	var originalHash string
	if err := tx.QueryRow(ctx, `SELECT body_hash FROM k6_sink.request_bindings WHERE request_id = $1`,
		requestID).Scan(&originalHash); err != nil {
		return false, err
	}
	return sameBody(originalHash, bodyHash), nil
}

func sameBody(originalHash, bodyHash string) bool {
	return originalHash == bodyHash
}

func allocate(ctx context.Context, tx pgx.Tx, d delivery, issuedFence int64) (result, error) {
	r := d.request
	if issuedFence == 0 {
		err := tx.QueryRow(ctx, `UPDATE k6_sink.resources SET latest_fence = latest_fence + 1
			WHERE resource_id = $1 AND clock_timestamp() < $2
			RETURNING latest_fence`, r.ResourceID, d.auth.sinkCutoff).Scan(&issuedFence)
		if errors.Is(err, pgx.ErrNoRows) {
			return result{}, errDenied
		}
		if err != nil {
			return result{}, err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO k6_sink.allocations (intent_id, grant_hash, attempt, resource_id, fence)
			VALUES ($1, $2, $3, $4, $5)`, r.IntentID, d.auth.grantHash, r.Attempt, r.ResourceID, issuedFence); err != nil {
			return result{}, err
		}
	}
	out, err := recordCall(ctx, tx, d, "ALLOCATED", nil)
	if err != nil {
		return result{}, err
	}
	out.body, err = json.Marshal(struct {
		Fence int64 `json:"fence"`
	}{issuedFence})
	return out, err
}

func apply(ctx context.Context, tx pgx.Tx, d delivery) (result, error) {
	r := d.request
	if err := updateResource(ctx, tx, d); err != nil {
		return result{}, err
	}

	var receipt Receipt
	err := scanReceipt(tx.QueryRow(ctx, `INSERT INTO k6_sink.receipts
		(workflow_id, logical_key, resource_id, canonical_hash, scope_hash, intent_id, expected_revision, applied_revision)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $7::bigint + 1)
		RETURNING `+receiptColumns, r.WorkflowID, r.LogicalKey, r.ResourceID, d.auth.canonicalHash, r.ScopeHash,
		r.IntentID, d.auth.revision), &receipt)
	if err != nil {
		return result{}, err
	}
	return recordCall(ctx, tx, d, "APPLIED", &receipt)
}

func updateResource(ctx context.Context, tx pgx.Tx, d delivery) error {
	r := d.request
	tag, err := tx.Exec(ctx, `UPDATE k6_sink.resources
		SET state = $2, revision = revision + 1, logical_key = $3, intent_id = $4
		WHERE resource_id = $1 AND revision = $5 AND clock_timestamp() < $6`,
		r.ResourceID, d.auth.grant.CanonicalArguments, r.LogicalKey, r.IntentID, d.auth.revision, d.auth.sinkCutoff)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errDenied
	}
	return nil
}

func recordCall(ctx context.Context, tx pgx.Tx, d delivery, outcome string, receipt *Receipt) (result, error) {
	r := d.request
	out := result{outcome: outcome, validUntil: d.auth.deadline}
	var applicationID, responseHash *string
	if receipt != nil {
		body, err := json.Marshal(receipt)
		if err != nil {
			return result{}, err
		}
		out.body = body
		digest := hash(body)
		applicationID, responseHash = &receipt.ApplicationID, &digest
	}
	// The resource lock orders call IDs. This final insert dates the applied
	// transaction independently of any subsequent response delivery.
	var arguments json.RawMessage
	if d.cooperating {
		arguments = r.Arguments
	}
	tag, err := tx.Exec(ctx, `WITH sampled AS MATERIALIZED (SELECT clock_timestamp() AS at)
		INSERT INTO k6_sink.calls (operation, workflow_id, logical_key, attempt,
		intent_id, resource_id, message_id, request_id, body_hash, connection_hash, outcome, application_id, response_hash,
		committed_at, grant_read_at, grant_hash, node_id, iteration, scope_hash, expected_revision, arguments, fence)
		SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
		CASE WHEN $11 = 'APPLIED' THEN at END, $15, $16, $17, $18, $19, $20, $21, $22
		FROM sampled WHERE at < $14`,
		d.operation, r.WorkflowID, r.LogicalKey, r.Attempt, r.IntentID, r.ResourceID, r.MessageID, r.RequestID,
		d.bodyHash, d.connectionHash, outcome, applicationID, responseHash, d.auth.sinkCutoff,
		d.auth.grant.DBTime, d.auth.grantHash, r.NodeID, r.Iteration, r.ScopeHash, r.ExpectedRevision, arguments, r.Fence)
	if err != nil {
		return result{}, err
	}
	if tag.RowsAffected() != 1 || !time.Now().Before(d.auth.deadline) {
		return result{}, errDenied
	}
	return out, nil
}
