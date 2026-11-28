// dur053-k6-fixture drives the real engine store and a pod-side grant probe.
package main

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"syscall"
	"time"

	"durable-agent-execution-engine/internal/invariants"
	"durable-agent-execution-engine/internal/k6auth"
	"durable-agent-execution-engine/internal/k6auth/smokeplan"
	"durable-agent-execution-engine/internal/partition"
	"durable-agent-execution-engine/internal/state"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type input struct {
	DSN         string
	Episode     k6auth.Episode
	Expiration  time.Time
	Token       string
	Plan        smokeplan.Plan
	Cutoff      time.Time
	Uncertainty time.Duration
	EngineRoles []string
	IntentID    string
	Lease       state.LeaseRef
	Grant       k6auth.ExactGrantRequest
	Probe       string
	TraceSocket bool
}

func stage(code string, err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return fmt.Errorf("%s_%s", code, pgErr.Code)
	}
	return errors.New(code)
}

func connect(ctx context.Context, dsn string) (*pgx.Conn, error) {
	c, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, stage("ENGINE_CONNECT", err)
	}
	return c, nil
}

func engine(ctx context.Context, action string, in input) (any, error) {
	conn, err := connect(ctx, in.DSN)
	if err != nil {
		return nil, err
	}
	defer conn.Close(ctx)
	switch action {
	case "clock":
		var db time.Time
		before := time.Now().UTC()
		err := conn.QueryRow(ctx, `SELECT pg_catalog.clock_timestamp()`).Scan(&db)
		after := time.Now().UTC()
		if err != nil {
			return nil, stage("ENGINE_CLOCK", err)
		}
		return map[string]time.Time{"before": before, "db": db, "after": after}, nil
	case "provision":
		credentials, err := k6auth.Provision(ctx, conn, in.Episode.Namespace, in.Cutoff, in.Uncertainty, in.EngineRoles...)
		if err != nil {
			return nil, stage("ENGINE_PROVISION", err)
		}
		return credentials, nil
	case "reset":
		if err := k6auth.Deprovision(ctx, conn, in.Episode.Namespace); err != nil {
			return nil, stage("ENGINE_RESET", err)
		}
		var absent bool
		if err := conn.QueryRow(ctx, `SELECT NOT EXISTS (SELECT FROM k6_auth.episode)
			AND NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname IN ($1,$2))`, k6auth.ReaderRole, k6auth.DecisionRole).Scan(&absent); err != nil || !absent {
			return nil, stage("ENGINE_RESET_VERIFY", err)
		}
		return map[string]bool{"episode_and_logins_absent": true}, nil
	case "connect-acl":
		var explicit bool
		if err := conn.QueryRow(ctx, `SELECT EXISTS (
			SELECT FROM pg_catalog.pg_database d CROSS JOIN LATERAL pg_catalog.aclexplode(d.datacl) a
			JOIN pg_catalog.pg_roles r ON r.oid = a.grantee
			WHERE d.datname = pg_catalog.current_database() AND r.rolname = current_user AND a.privilege_type = 'CONNECT')`).Scan(&explicit); err != nil || !explicit {
			return nil, stage("ENGINE_EXPLICIT_CONNECT", err)
		}
		return map[string]bool{"explicit_connect": true}, nil
	case "snapshot":
		var digest string
		err := conn.QueryRow(ctx, `SELECT encode(sha256(convert_to(jsonb_build_array(
			(SELECT jsonb_agg(w ORDER BY workflow_id) FROM engine.workflow_executions w),
			(SELECT jsonb_agg(n ORDER BY workflow_id, node_id, iteration) FROM engine.node_instances n),
			(SELECT jsonb_agg(i ORDER BY intent_id) FROM engine.approval_action_intents i),
			(SELECT jsonb_agg(h ORDER BY to_jsonb(h)::text) FROM engine.transition_history h),
			(SELECT jsonb_agg(o ORDER BY to_jsonb(o)::text) FROM engine.outbox o),
			(SELECT jsonb_agg(c ORDER BY request_id) FROM engine.cancellation_requests c),
			(SELECT jsonb_agg(e ORDER BY workflow_id, logical_effect_key) FROM effects.effect_records e),
			(SELECT to_jsonb(e) FROM k6_auth.episode e)
		)::text, 'UTF8')), 'hex')`).Scan(&digest)
		if err != nil {
			return nil, stage("ENGINE_SNAPSHOT", err)
		}
		return map[string]string{"sha256": digest}, nil
	}
	return nil, errors.New("ENGINE_ACTION")
}

func storeAction(ctx context.Context, action string, in input) (any, error) {
	cfg, err := pgxpool.ParseConfig(in.DSN)
	if err != nil {
		return nil, stage("STORE_CONFIG", err)
	}
	cfg.MaxConns = 2
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, stage("STORE_POOL", err)
	}
	defer pool.Close()
	store := state.NewWithOptions(pool, state.StoreOptions{K6Approval: action != "foreign"})
	switch action {
	case "export":
		trace, err := invariants.LoadNamespace(ctx, store, in.Plan.Namespace)
		if err != nil {
			return nil, stage("CHECKER_EXPORT", err)
		}
		if len(trace.Workflows) != 1 {
			return nil, errors.New("CHECKER_EXPORT_WORKFLOW_COUNT")
		}
		return map[string]int{"workflow_count": len(trace.Workflows)}, nil
	case "apply":
		return apply(ctx, store, in)
	case "intent", "foreign":
		return create(ctx, store, action, in)
	}
	return nil, errors.New("STORE_ACTION")
}

func create(ctx context.Context, store *state.Store, action string, in input) (any, error) {
	p := in.Plan
	part, err := partition.ID(p.WorkflowID)
	if err != nil {
		return nil, stage("FIXTURE_PARTITION", err)
	}
	const schedulerOwner = "00000000-0053-4000-8000-000000000006"
	lease, acquired, err := store.AcquireLease(ctx, int16(part), schedulerOwner, 10*time.Minute)
	if err != nil || !acquired {
		return nil, stage("FIXTURE_LEASE", err)
	}
	in.Lease = state.LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch}
	if err := store.CreateDefinition(ctx, state.DefinitionInput{
		DefinitionID: p.WorkflowID, Version: 1, DefinitionHash: p.WorkflowID,
		Graph:            []byte(`{"entry":"root","nodes":[{"id":"root","kind":"activity"}]}`),
		ActivityVersions: []byte(`{"root":"1"}`), EffectClasses: []byte(`{"root":"COOPERATING_EFFECT"}`),
	}); err != nil {
		return nil, stage("FIXTURE_DEFINITION", err)
	}
	workflow, err := store.CreateWorkflow(ctx, state.CreateWorkflowInput{
		WorkflowID: p.WorkflowID, Namespace: p.Namespace, SubmissionKey: p.WorkflowID, SubmissionPayloadHash: "sub-v1:" + p.WorkflowID,
		DefinitionID: p.WorkflowID, DefinitionVersion: 1, PartitionID: lease.PartitionID,
		InitialNodeID: p.NodeID, InitialInput: []byte(`{}`), ActorID: "k6-smoke-submitter",
	})
	if err != nil {
		return nil, stage("FIXTURE_WORKFLOW", err)
	}
	intent, err := store.CreateApprovalIntent(ctx, state.ApprovalIntentInput{
		Lease: in.Lease, WorkflowID: p.WorkflowID, NodeID: p.NodeID, ExpectedRevision: workflow.Workflow.Revision,
		Target: p.Target, ExpectedResourceRevision: p.ResourceRevision, ValidUntil: in.Cutoff,
		CanonicalArguments: p.Arguments, ActorID: lease.OwnerID,
	})
	if err != nil {
		return nil, stage("FIXTURE_INTENT", err)
	}
	if action == "intent" {
		return map[string]any{"intent_id": intent.IntentID, "proposal_hash": intent.ProposalHash, "lease": in.Lease}, nil
	}
	// A foreign namespace has legitimately populated engine rows, as in the
	// B1 namespace test. Only the K6 namespace is installed for this reader.
	if _, err := store.RecordApprovalDecision(ctx, state.ApprovalDecisionInput{
		IntentID: intent.IntentID, ApproverID: "foreign-fixture", ProposalHash: intent.ProposalHash, Decision: "APPROVED",
	}); err != nil {
		return nil, stage("FOREIGN_DECISION", err)
	}
	in.IntentID = intent.IntentID
	return apply(ctx, store, in)
}

func apply(ctx context.Context, store *state.Store, in input) (any, error) {
	p := in.Plan
	request := state.ApplyApprovalInput{
		Lease: in.Lease, WorkflowID: p.WorkflowID, NodeID: p.NodeID, IntentID: in.IntentID,
		LogicalEffectKey: p.Key, ExpectedRevision: 2, ActorID: in.Lease.OwnerID,
	}
	grant, err := store.ApplyApproval(ctx, request)
	if err != nil || grant.GrantToken == "" || !grant.ExpiresAt.Equal(in.Cutoff) {
		return nil, stage("FIXTURE_APPLY", err)
	}
	var count int
	err = store.Pool().QueryRow(ctx, `SELECT count(*) FROM engine.approval_action_intents
		WHERE workflow_id = $1 AND node_id = $2 AND iteration = 0 AND grant_token IS NOT NULL`,
		p.WorkflowID, p.NodeID).Scan(&count)
	if err != nil || count != 1 {
		return nil, stage("FIXTURE_GRANT_COUNT", err)
	}
	return k6auth.ExactGrantRequest{
		IntentID: grant.IntentID, Token: grant.GrantToken, WorkflowID: p.WorkflowID, NodeID: p.NodeID,
		LogicalKey: p.Key, ScopeHash: grant.GrantScopeHash, Target: p.Target,
	}, nil
}

func probe(ctx context.Context, in input) map[string]any {
	code := func(value string, err error) map[string]any {
		return map[string]any{"code": value, "diagnostic": connectDiagnostic(err)}
	}
	if in.Probe == "read" || in.Probe == "empty" {
		reader, err := k6auth.OpenGrantReader(ctx, in.DSN)
		if err != nil {
			return code("READER_CONNECT_FAILED", err)
		}
		defer reader.Close()
		if in.Probe == "empty" {
			// Count the exact function's rows below to distinguish a clean
			// mismatch from a failing connection, ACL, or query.
			return sqlProbe(ctx, in)
		}
		grant, err := reader.ReadExactGrant(ctx, in.Grant)
		if err != nil {
			return code("READER_READ_FAILED", err)
		}
		p := in.Plan
		hash, err := state.K6ProposalHash(grant.Target, grant.CanonicalArguments, grant.ExpectedResourceRevision)
		expected, planErr := state.K6ProposalHash(p.Target, p.Arguments, p.ResourceRevision)
		if err != nil || planErr != nil || hash != expected || grant.ProposalHash != expected ||
			grant.IntentID != in.Grant.IntentID || grant.WorkflowID != p.WorkflowID || grant.NodeID != p.NodeID ||
			grant.Iteration != 0 || grant.Target != p.Target || grant.ExpectedResourceRevision != p.ResourceRevision ||
			grant.ScopeHash != in.Grant.ScopeHash || grant.Decision != "APPROVED" || grant.DispatchStatus != "GRANTED" ||
			!grant.ExpiresAt.Equal(in.Cutoff) || !grant.DBTime.Before(grant.ExpiresAt) {
			return code("READER_TUPLE_MISMATCH", nil)
		}
		return code("EXACT_GRANT", nil)
	}
	return sqlProbe(ctx, in)
}

func classifyConnectError(err error) string {
	var pgErr *pgconn.PgError
	var verifyErr x509.UnknownAuthorityError
	var nameErr x509.HostnameError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	if errors.As(err, &verifyErr) || errors.As(err, &nameErr) {
		return "TLS_VERIFY_FAILED"
	}
	if isTimeoutError(err) {
		return "TCP_DIAL_TIMEOUT"
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		switch errno {
		case syscall.ECONNREFUSED:
			return "TCP_DIAL_REFUSED"
		case syscall.EHOSTUNREACH, syscall.ENETUNREACH:
			return "TCP_DIAL_UNREACHABLE"
		}
	}
	return "CONNECT_FAILED"
}

func isTimeoutError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || os.IsTimeout(err) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func connectDiagnostic(err error) map[string]any {
	types := []string{}
	for current := err; current != nil; current = errors.Unwrap(current) {
		types = append(types, fmt.Sprintf("%T", current))
	}
	diagnostic := map[string]any{"error_types": types, "timeout": isTimeoutError(err)}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		diagnostic["op"] = opErr.Op
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		name := fmt.Sprintf("ERRNO_%d", errno)
		switch errno {
		case syscall.ECONNREFUSED:
			name = "ECONNREFUSED"
		case syscall.EHOSTUNREACH:
			name = "EHOSTUNREACH"
		case syscall.ENETUNREACH:
			name = "ENETUNREACH"
		case syscall.ETIMEDOUT:
			name = "ETIMEDOUT"
		}
		diagnostic["errno"] = name
	}
	return diagnostic
}

func sqlProbe(ctx context.Context, in input) map[string]any {
	result := map[string]any{"diagnostic": connectDiagnostic(nil)}
	conn, err := connectProbe(ctx, in, result)
	if err != nil {
		result["code"] = classifyConnectError(err)
		result["diagnostic"] = connectDiagnostic(err)
		return result
	}
	defer conn.Close(ctx)
	g := in.Grant
	const exact = `SELECT count(*) FROM k6_auth.read_exact_grant($1,$2,$3,$4,$5,$6,$7,$8)`
	switch in.Probe {
	case "empty", "network", "connect":
		var count int
		err = conn.QueryRow(ctx, exact, g.IntentID, g.Token, g.WorkflowID, g.NodeID, g.Iteration, g.LogicalKey, g.ScopeHash, g.Target).Scan(&count)
		result["code"] = fmt.Sprintf("EXACT_ROWS_%d", count)
	case "sql":
		_, err = conn.Exec(ctx, `CREATE TEMP TABLE k6_denied (id integer)`)
	case "table":
		_, err = conn.Exec(ctx, `SELECT grant_token FROM engine.approval_action_intents WHERE intent_id = $1`, g.IntentID)
	case "list":
		_, err = conn.Exec(ctx, `SELECT * FROM k6_auth.episode_grants`)
	case "set-role":
		_, err = conn.Exec(ctx, `SET ROLE k6_decision_writer`)
	case "insert":
		_, err = conn.Exec(ctx, `INSERT INTO engine.approval_action_intents (intent_id) VALUES ($1)`, g.IntentID)
	case "update":
		_, err = conn.Exec(ctx, `UPDATE engine.approval_action_intents SET dispatch_status = 'DISPATCHED' WHERE intent_id = $1`, g.IntentID)
	default:
		result["code"] = "UNKNOWN_PROBE"
	}
	if err != nil {
		result["code"] = "QUERY_FAILED"
		result["diagnostic"] = connectDiagnostic(err)
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			result["code"] = pgErr.Code
		}
	} else if result["code"] == "" {
		result["code"] = "UNEXPECTED_SQL_SUCCESS"
	}
	return result
}

func connectProbe(ctx context.Context, in input, result map[string]any) (*pgx.Conn, error) {
	if !in.TraceSocket {
		return pgx.Connect(ctx, in.DSN)
	}
	config, err := pgx.ParseConfig(in.DSN)
	if err != nil {
		return nil, err
	}
	dial := config.DialFunc
	var local *net.TCPAddr
	dials := 0
	config.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
		dials++
		conn, err := dial(ctx, network, addr)
		if err == nil {
			local, _ = conn.LocalAddr().(*net.TCPAddr)
		}
		return conn, err
	}
	conn, err := pgx.ConnectConfig(ctx, config)
	var rejection *pgconn.PgError
	if dials == 1 && local != nil && errors.As(err, &rejection) && rejection.Code == "28P01" {
		result["rejected_connection"] = map[string]any{
			"client_addr": local.IP.String(), "client_port": local.Port,
			"role": config.User, "application": config.RuntimeParams["application_name"],
		}
	}
	return conn, err
}

func run(ctx context.Context, action string, in input) (any, error) {
	switch action {
	case "token-request":
		return k6auth.BuildTokenRequest(in.Episode.SecretName, in.Episode.SecretUID)
	case "token-window":
		if err := k6auth.ValidateIssuedToken(in.Episode, in.Token, in.Expiration, time.Now()); err != nil {
			return nil, errors.New("TOKEN_WINDOW_OR_BINDING_INVALID")
		}
		return map[string]bool{"valid": true}, nil
	case "intent", "apply", "foreign", "export":
		return storeAction(ctx, action, in)
	case "probe":
		return probe(ctx, in), nil
	default:
		return engine(ctx, action, in)
	}
}

func main() {
	var in input
	if len(os.Args) != 2 || json.NewDecoder(io.LimitReader(os.Stdin, 128<<10)).Decode(&in) != nil {
		fmt.Fprintln(os.Stderr, "FIXTURE_INPUT")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	out, err := run(ctx, os.Args[1], in)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if json.NewEncoder(os.Stdout).Encode(out) != nil {
		os.Exit(1)
	}
}
