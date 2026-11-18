package k6auth

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	DecisionRole = "k6_decision_writer"
	ReaderRole   = "k6_sink_grant_reader"
)

type DatabaseDecisionWriter struct{ pool *pgxpool.Pool }
type DatabaseGrantReader struct{ pool *pgxpool.Pool }

func OpenDecisionWriter(ctx context.Context, dsn string) (*DatabaseDecisionWriter, error) {
	pool, err := openDatabaseRole(ctx, dsn, DecisionRole)
	if err != nil {
		return nil, err
	}
	return &DatabaseDecisionWriter{pool}, nil
}

func OpenGrantReader(ctx context.Context, dsn string) (*DatabaseGrantReader, error) {
	pool, err := openDatabaseRole(ctx, dsn, ReaderRole)
	if err != nil {
		return nil, err
	}
	return &DatabaseGrantReader{pool}, nil
}

func openDatabaseRole(ctx context.Context, dsn, role string) (*pgxpool.Pool, error) {
	if dsn == "" {
		return nil, ErrDenied
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil || cfg.ConnConfig.User != role || cfg.ConnConfig.Database == "" || cfg.ConnConfig.Password == "" ||
		len(cfg.ConnConfig.Fallbacks) != 0 || cfg.ConnConfig.TLSConfig == nil || cfg.ConnConfig.TLSConfig.InsecureSkipVerify ||
		cfg.ConnConfig.TLSConfig.ServerName == "" {
		return nil, ErrDenied
	}
	cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeCacheStatement
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, ErrDenied
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, ErrDenied
	}
	return pool, nil
}

func (w *DatabaseDecisionWriter) Close() { w.pool.Close() }
func (r *DatabaseGrantReader) Close()    { r.pool.Close() }

func (w *DatabaseDecisionWriter) Decide(ctx context.Context, request DecisionRequest, subject string) (DecisionIdentity, error) {
	var identity DecisionIdentity
	var code string
	var intentID, proposalHash, decision *string
	var requestedIntent *string
	if request.IntentID != "" {
		requestedIntent = &request.IntentID
	}
	if err := w.pool.QueryRow(ctx, `SELECT code, intent_id::text, proposal_hash, decision, subject, decided_at
		FROM k6_auth.approval_decision($1, $2, $3, $4, $5, $6, $7)`, requestedIntent, request.WorkflowID,
		request.NodeID, request.Iteration, request.ProposalHash, subject, request.Decision).
		Scan(&code, &intentID, &proposalHash, &decision, &identity.Subject, &identity.DecidedAt); err != nil {
		return DecisionIdentity{}, ErrDenied
	}
	if code == "EXPIRED" {
		return DecisionIdentity{}, ErrExpired
	}
	if code != "OK" || intentID == nil || proposalHash == nil || decision == nil {
		return DecisionIdentity{}, ErrDenied
	}
	identity.IntentID, identity.ProposalHash, identity.Decision = *intentID, *proposalHash, *decision
	return identity, nil
}

type ExactGrantRequest struct {
	IntentID, Token, WorkflowID, NodeID string
	Iteration                           int32
	LogicalKey, ScopeHash, Target       string
}

type ExactGrant struct {
	IntentID, WorkflowID, NodeID        string
	Iteration                           int32
	ProposalHash, Target                string
	CanonicalArguments                  json.RawMessage
	ExpectedResourceRevision            string
	Decision, DispatchStatus, ScopeHash string
	ExpiresAt, DBTime                   time.Time
}

func (r *DatabaseGrantReader) ReadExactGrant(ctx context.Context, input ExactGrantRequest) (ExactGrant, error) {
	var grant ExactGrant
	rows, err := r.pool.Query(ctx, `SELECT intent_id::text, workflow_id, node_id, iteration, proposal_hash, target,
		canonical_arguments, expected_resource_revision, decision, dispatch_status, grant_scope_hash, grant_expires_at, db_time
		FROM k6_auth.read_exact_grant($1, $2, $3, $4, $5, $6, $7, $8)`, input.IntentID, input.Token, input.WorkflowID,
		input.NodeID, input.Iteration, input.LogicalKey, input.ScopeHash, input.Target)
	if err != nil {
		return ExactGrant{}, ErrDenied
	}
	_, err = pgx.CollectExactlyOneRow(rows, func(row pgx.CollectableRow) (struct{}, error) {
		return struct{}{}, row.Scan(&grant.IntentID, &grant.WorkflowID, &grant.NodeID, &grant.Iteration,
			&grant.ProposalHash, &grant.Target, &grant.CanonicalArguments, &grant.ExpectedResourceRevision,
			&grant.Decision, &grant.DispatchStatus, &grant.ScopeHash, &grant.ExpiresAt, &grant.DBTime)
	})
	if err != nil {
		return ExactGrant{}, ErrDenied
	}
	return grant, nil
}
