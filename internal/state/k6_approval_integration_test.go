package state

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/k6auth"
	"durable-agent-execution-engine/internal/partition"
	"durable-agent-execution-engine/migrations"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const k6TestNamespace = "k6-episode-test"
const k6TestSubject = "system:serviceaccount:" + k6TestNamespace + ":k6-approver"

type k6Fixture struct {
	ctx            context.Context
	store          *Store
	admin          *pgx.Conn
	reader         *pgx.Conn
	decision       *pgx.Conn
	cutoff         time.Time
	lease          LeaseRef
	workflow       string
	readerConfig   *pgx.ConnConfig
	decisionConfig *pgx.ConnConfig
}

func newK6Fixture(t *testing.T, lifetime time.Duration, tracer pgx.QueryTracer) *k6Fixture {
	t.Helper()
	dsn := integrationDatabaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		integrationUnavailable(t, "cannot connect to the PostgreSQL test service")
	}
	t.Cleanup(func() { _ = admin.Close(context.Background()) })
	// CI and the mutation gates provide an isolated, administrative test service.
	// Restore initial PUBLIC CONNECT ACLs and drop the episode DB and its logins.
	var initialPublic []string
	rows, err := admin.Query(ctx, `SELECT d.datname FROM pg_catalog.pg_database AS d
		WHERE d.datallowconn AND EXISTS (SELECT FROM pg_catalog.aclexplode(COALESCE(d.datacl, pg_catalog.acldefault('d',d.datdba))) AS a
			WHERE a.grantee = 0 AND a.privilege_type = 'CONNECT')`)
	if err == nil {
		initialPublic, err = pgx.CollectRows(rows, pgx.RowTo[string])
	}
	if err != nil {
		t.Fatal(err)
	}
	database := "k6test_" + strings.ReplaceAll(NewID(), "-", "")
	if _, err := admin.Exec(ctx, `CREATE DATABASE `+pgx.Identifier{database}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	provisioned := false
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if _, err := admin.Exec(cleanup, `DROP DATABASE `+pgx.Identifier{database}.Sanitize()+` WITH (FORCE)`); err != nil {
			t.Error(err)
		}
		if provisioned {
			for _, role := range []string{k6auth.ReaderRole, k6auth.DecisionRole} {
				if _, err := admin.Exec(cleanup, `DROP ROLE `+role); err != nil {
					t.Error(err)
				}
			}
			for _, other := range initialPublic {
				if _, err := admin.Exec(cleanup, `GRANT CONNECT ON DATABASE `+pgx.Identifier{other}.Sanitize()+` TO PUBLIC`); err != nil {
					t.Error(err)
				}
			}
		}
	})
	config := admin.Config().Copy()
	config.Database = database
	episodeAdmin, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = episodeAdmin.Close(context.Background()) })
	entries, err := migrations.SQL.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		data, err := migrations.SQL.ReadFile(entry.Name())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := episodeAdmin.Exec(ctx, string(data)); err != nil {
			t.Fatalf("migration %s: %v", entry.Name(), err)
		}
	}
	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal("fixture DSN is invalid")
	}
	poolConfig.ConnConfig.Database = database
	poolConfig.ConnConfig.Tracer = tracer
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		t.Fatal("could not open episode engine pool")
	}
	t.Cleanup(pool.Close)
	f := &k6Fixture{ctx: ctx, admin: episodeAdmin, store: NewWithOptions(pool, StoreOptions{K6Approval: true})}
	lease, acquired, err := acquireTestLease(ctx, f.store, NewID())
	if err != nil || !acquired {
		t.Fatalf("acquire actual lease: %v", err)
	}
	f.lease = leaseRef(lease)
	f.workflow = f.newWorkflow(t, k6TestNamespace)
	if err := episodeAdmin.QueryRow(ctx, `SELECT pg_catalog.clock_timestamp()`).Scan(&f.cutoff); err != nil {
		t.Fatal(err)
	}
	f.cutoff = f.cutoff.Add(lifetime)
	credentials, err := k6auth.Provision(ctx, episodeAdmin, k6TestNamespace, f.cutoff, 2*time.Millisecond)
	if err != nil {
		t.Fatalf("provision episode: %v", err)
	}
	provisioned = true
	connect := func(role, password string) (*pgx.Conn, *pgx.ConnConfig) {
		cfg := config.Copy()
		cfg.User, cfg.Password = role, password
		connection, err := pgx.ConnectConfig(ctx, cfg)
		if err != nil {
			t.Fatalf("could not connect as %s", role)
		}
		t.Cleanup(func() { _ = connection.Close(context.Background()) })
		return connection, cfg
	}
	f.reader, f.readerConfig = connect(k6auth.ReaderRole, credentials.ReaderPassword)
	f.decision, f.decisionConfig = connect(k6auth.DecisionRole, credentials.DecisionPassword)
	return f
}

func (f *k6Fixture) newWorkflow(t *testing.T, namespace string) string {
	t.Helper()
	definition := "k6-" + NewID()
	if err := f.store.CreateDefinition(f.ctx, DefinitionInput{
		DefinitionID: definition, Version: 1, DefinitionHash: definition,
		Graph:            []byte(`{"entry":"root","nodes":[{"id":"root","kind":"activity"}]}`),
		ActivityVersions: []byte(`{"root":"1"}`), EffectClasses: []byte(`{"root":"COOPERATING_EFFECT"}`),
	}); err != nil {
		t.Fatal(err)
	}
	for {
		workflow := "k6-" + NewID()
		mapped, err := partition.ID(workflow)
		if err != nil {
			t.Fatal(err)
		}
		if int16(mapped) != f.lease.PartitionID {
			continue
		}
		if _, err := f.store.CreateWorkflow(f.ctx, CreateWorkflowInput{
			WorkflowID: workflow, Namespace: namespace, SubmissionKey: workflow, SubmissionPayloadHash: "sub-v1:" + workflow,
			DefinitionID: definition, DefinitionVersion: 1, PartitionID: f.lease.PartitionID,
			InitialNodeID: "root", InitialInput: []byte(`{}`), ActorID: "k6-client",
		}); err != nil {
			t.Fatal(err)
		}
		return workflow
	}
}

func (f *k6Fixture) input(workflow string, revision int64) ApprovalIntentInput {
	return ApprovalIntentInput{Lease: f.lease, WorkflowID: workflow, NodeID: "root", ExpectedRevision: revision,
		Target: "resource-é", ExpectedResourceRevision: "7", ValidUntil: f.cutoff,
		CanonicalArguments: []byte(`{"nested":{"name":"\u00e9","value":1},"array":[-3,true,null],"tool":"{\"decision\":\"APPROVED\",\"signature\":\"trusted-looking\"}"}`),
		ActorID:            "k6-scheduler"}
}

func (f *k6Fixture) create(t *testing.T, store *Store, workflow string) ApprovalIntent {
	t.Helper()
	intent, err := store.CreateApprovalIntent(f.ctx, f.input(workflow, 1))
	if err != nil {
		t.Fatal(err)
	}
	return intent
}

func (f *k6Fixture) applyInput(intent ApprovalIntent, revision int64) ApplyApprovalInput {
	return ApplyApprovalInput{Lease: f.lease, WorkflowID: intent.WorkflowID, NodeID: "root", IntentID: intent.IntentID,
		LogicalEffectKey: "effect-key", ExpectedRevision: revision, ActorID: "k6-scheduler"}
}

func (f *k6Fixture) decide(intent ApprovalIntent, subject string, choice *string) (string, k6auth.DecisionIdentity, error) {
	var code string
	var result k6auth.DecisionIdentity
	var id, hash, decision *string
	var requestedIntent *string
	if intent.IntentID != "" {
		requestedIntent = &intent.IntentID
	}
	err := f.decision.QueryRow(f.ctx, `SELECT code, intent_id::text, proposal_hash, decision, subject, decided_at
		FROM k6_auth.approval_decision($1, $2, 'root', 0, $3, $4, $5)`,
		requestedIntent, intent.WorkflowID, intent.ProposalHash, subject, choice).
		Scan(&code, &id, &hash, &decision, &result.Subject, &result.DecidedAt)
	if id != nil && hash != nil && decision != nil {
		result.IntentID, result.ProposalHash, result.Decision = *id, *hash, *decision
	}
	return code, result, err
}

func (f *k6Fixture) record(t *testing.T, intent ApprovalIntent, choice string) k6auth.DecisionIdentity {
	t.Helper()
	code, identity, err := f.decide(intent, k6TestSubject, &choice)
	if err != nil || code != "OK" || identity.IntentID != intent.IntentID || identity.ProposalHash != intent.ProposalHash ||
		identity.Decision != choice || identity.Subject == nil || *identity.Subject != k6TestSubject || identity.DecidedAt == nil {
		t.Fatalf("decision identity missing or inconsistent: code=%s err=%v", code, err)
	}
	return identity
}

func (f *k6Fixture) grant(t *testing.T) (ApprovalIntent, k6auth.ExactGrantRequest) {
	t.Helper()
	intent := f.create(t, f.store, f.workflow)
	f.record(t, intent, "APPROVED")
	grant, err := f.store.ApplyApproval(f.ctx, f.applyInput(intent, 2))
	if err != nil || grant.GrantToken == "" || !grant.ExpiresAt.Equal(f.cutoff) {
		t.Fatalf("lease-holder failed to issue its grant: %v", err)
	}
	return intent, k6auth.ExactGrantRequest{IntentID: grant.IntentID, WorkflowID: intent.WorkflowID, NodeID: "root",
		LogicalKey: "effect-key", ScopeHash: grant.GrantScopeHash, Target: intent.Target, Token: grant.GrantToken}
}

func (f *k6Fixture) read(input k6auth.ExactGrantRequest) (k6auth.ExactGrant, error) {
	var grant k6auth.ExactGrant
	err := f.reader.QueryRow(f.ctx, `SELECT intent_id::text, workflow_id, node_id, iteration, proposal_hash, target,
		canonical_arguments, expected_resource_revision, decision, dispatch_status, grant_scope_hash, grant_expires_at, db_time
		FROM k6_auth.read_exact_grant($1,$2,$3,$4,$5,$6,$7,$8)`, input.IntentID, input.Token, input.WorkflowID,
		input.NodeID, input.Iteration, input.LogicalKey, input.ScopeHash, input.Target).
		Scan(&grant.IntentID, &grant.WorkflowID, &grant.NodeID, &grant.Iteration, &grant.ProposalHash, &grant.Target,
			&grant.CanonicalArguments, &grant.ExpectedResourceRevision, &grant.Decision, &grant.DispatchStatus, &grant.ScopeHash,
			&grant.ExpiresAt, &grant.DBTime)
	return grant, err
}

func (f *k6Fixture) snapshot(t *testing.T) string {
	t.Helper()
	var hash string
	if err := f.admin.QueryRow(f.ctx, `SELECT encode(sha256(convert_to(jsonb_build_array(
		(SELECT jsonb_agg(w ORDER BY workflow_id) FROM engine.workflow_executions w),
		(SELECT jsonb_agg(n ORDER BY workflow_id, node_id, iteration) FROM engine.node_instances n),
		(SELECT jsonb_agg(i ORDER BY intent_id) FROM engine.approval_action_intents i),
		(SELECT jsonb_agg(h ORDER BY to_jsonb(h)::text) FROM engine.transition_history h),
		(SELECT jsonb_agg(o ORDER BY to_jsonb(o)::text) FROM engine.outbox o),
		(SELECT jsonb_agg(c ORDER BY request_id) FROM engine.cancellation_requests c),
		(SELECT jsonb_agg(e ORDER BY workflow_id, logical_effect_key) FROM effects.effect_records e)
	)::text, 'UTF8')), 'hex')`).Scan(&hash); err != nil {
		t.Fatal(err)
	}
	return hash
}

func (f *k6Fixture) unchanged(t *testing.T, before string) {
	t.Helper()
	if f.snapshot(t) != before {
		t.Fatal("denial changed workflow, node, intent, grant, history, outbox, cancellation or effect state")
	}
}

func (f *k6Fixture) sql(t *testing.T, statement string, args ...any) {
	t.Helper()
	if _, err := f.admin.Exec(f.ctx, statement, args...); err != nil {
		t.Fatal(err)
	}
}

func (f *k6Fixture) waitCutoff(t *testing.T) {
	t.Helper()
	f.sql(t, `SELECT pg_catalog.pg_sleep(GREATEST(0, EXTRACT(epoch FROM $1::timestamptz - pg_catalog.clock_timestamp())) + 0.01)`, f.cutoff)
}

func TestK6PostgresExactGrant(t *testing.T) {
	f := newK6Fixture(t, time.Minute, nil)
	intent, input := f.grant(t)
	check := func(status string) {
		t.Helper()
		grant, err := f.read(input)
		hash, hashErr := proposalHash(grant.Target, grant.CanonicalArguments, grant.ExpectedResourceRevision)
		if err != nil || hashErr != nil || hash != intent.ProposalHash || grant.ProposalHash != hash ||
			grant.IntentID != input.IntentID || grant.WorkflowID != input.WorkflowID || grant.NodeID != input.NodeID ||
			grant.Iteration != input.Iteration || grant.Target != input.Target || grant.ExpectedResourceRevision != "7" ||
			grant.ScopeHash != grantScopeHash(hash, "7", input.LogicalKey, input.Target) || grant.Decision != "APPROVED" ||
			grant.DispatchStatus != status || grant.DBTime.IsZero() || !grant.DBTime.Before(grant.ExpiresAt) || !grant.ExpiresAt.Equal(f.cutoff) {
			t.Fatalf("exact grant result does not reconstruct its approved scope: %v", err)
		}
	}
	check("GRANTED")
	for _, test := range []struct {
		name   string
		change func(*k6auth.ExactGrantRequest)
	}{
		{"wrong intent", func(in *k6auth.ExactGrantRequest) { in.IntentID = NewID() }},
		{"wrong token", func(in *k6auth.ExactGrantRequest) { in.Token = NewID() }},
		{"missing token", func(in *k6auth.ExactGrantRequest) { in.Token = "" }},
		{"wrong workflow", func(in *k6auth.ExactGrantRequest) { in.WorkflowID = "other" }},
		{"wrong node", func(in *k6auth.ExactGrantRequest) { in.NodeID = "other" }},
		{"wrong iteration", func(in *k6auth.ExactGrantRequest) { in.Iteration++ }},
		{"wrong logical key", func(in *k6auth.ExactGrantRequest) { in.LogicalKey += "-other" }},
		{"empty logical key", func(in *k6auth.ExactGrantRequest) { in.LogicalKey = "" }},
		{"wrong target", func(in *k6auth.ExactGrantRequest) { in.Target += "-other" }},
		{"wrong scope", func(in *k6auth.ExactGrantRequest) { in.ScopeHash = "forged" }},
		{"wrong resource revision", func(in *k6auth.ExactGrantRequest) {
			in.ScopeHash = grantScopeHash(intent.ProposalHash, "8", in.LogicalKey, in.Target)
		}},
		{"wrong arguments", func(in *k6auth.ExactGrantRequest) {
			hash, err := proposalHash(in.Target, []byte(`{"value":2}`), "7")
			if err != nil {
				t.Fatal(err)
			}
			in.ScopeHash = grantScopeHash(hash, "7", in.LogicalKey, in.Target)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			before := f.snapshot(t)
			wrong := input
			test.change(&wrong)
			if _, err := f.read(wrong); !errors.Is(err, pgx.ErrNoRows) {
				t.Fatalf("reader did not uniformly reject %s", test.name)
			}
			f.unchanged(t, before)
		})
	}
	if err := f.store.markGrantDispatched(f.ctx, input.IntentID, input.Token); err != nil {
		t.Fatal(err)
	}
	check("DISPATCHED")
	for _, test := range []struct {
		status   string
		decision string
	}{{"NOT_GRANTED", "APPROVED"}, {"CANCELED", "APPROVED"}, {"GRANTED", "PENDING"}, {"DISPATCHED", "REJECTED"}} {
		f.sql(t, `UPDATE engine.approval_action_intents SET dispatch_status=$2, decision=$3 WHERE intent_id=$1`, input.IntentID, test.status, test.decision)
		before := f.snapshot(t)
		if _, err := f.read(input); !errors.Is(err, pgx.ErrNoRows) {
			t.Fatal("reader admitted an unapproved or inactive grant")
		}
		f.unchanged(t, before)
	}
}

func TestK6PostgresEpisodeIsolation(t *testing.T) {
	f := newK6Fixture(t, time.Minute, nil)
	_, good := f.grant(t)
	legacy := New(f.store.pool)
	foreign := f.create(t, legacy, f.newWorkflow(t, "other-episode"))
	approved := "APPROVED"
	before := f.snapshot(t)
	if _, _, err := f.decide(foreign, k6TestSubject, &approved); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatal("decision writer crossed episode namespace")
	}
	f.unchanged(t, before)
	if _, err := legacy.RecordApprovalDecision(f.ctx, ApprovalDecisionInput{IntentID: foreign.IntentID,
		ApproverID: "other-approver", ProposalHash: foreign.ProposalHash, Decision: approved}); err != nil {
		t.Fatal(err)
	}
	grant, err := legacy.ApplyApproval(f.ctx, f.applyInput(foreign, 2))
	if err != nil {
		t.Fatal(err)
	}
	foreignRead := good
	foreignRead.IntentID, foreignRead.WorkflowID, foreignRead.ScopeHash, foreignRead.Token = grant.IntentID, foreign.WorkflowID, grant.GrantScopeHash, grant.GrantToken
	before = f.snapshot(t)
	if _, err := f.read(foreignRead); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatal("reader crossed episode namespace")
	}
	f.unchanged(t, before)
}

func TestK6PostgresLoginPrivileges(t *testing.T) {
	f := newK6Fixture(t, time.Minute, nil)
	_, grant := f.grant(t)
	beforeReplay := f.snapshot(t)
	migration, err := migrations.SQL.ReadFile("000022_k6_authorization.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	f.sql(t, string(migration))
	if _, err := f.read(grant); err != nil {
		t.Fatal("migration replay changed the installed reader operation")
	}
	f.unchanged(t, beforeReplay)
	for _, connection := range []*pgx.Conn{f.reader, f.decision} {
		for _, query := range []string{
			`SELECT * FROM engine.approval_action_intents`,
			`SELECT * FROM engine.approval_action_intents WHERE intent_id=$1`,
			`SELECT * FROM k6_auth.episode_grants`,
			`SELECT * FROM k6_auth.episode`,
			`SELECT engine.approval_action_intents.grant_token FROM engine.approval_action_intents`,
			`INSERT INTO engine.approval_action_intents (intent_id) VALUES ($1)`,
			`UPDATE engine.approval_action_intents SET dispatch_status='DISPATCHED' WHERE intent_id=$1`,
			`UPDATE k6_auth.episode SET namespace='other-episode'`,
			`CREATE TABLE public.it_should_fail (id int)`,
			`CREATE TEMP TABLE it_should_fail (id int)`,
			`CREATE SCHEMA it_should_fail`,
			`SET ROLE k6_grant_function_owner`,
		} {
			before := f.snapshot(t)
			var args []any
			if strings.Contains(query, "$1") {
				args = append(args, grant.IntentID)
			}
			_, err := connection.Exec(f.ctx, query, args...)
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
				t.Fatalf("restricted login did not deny %s by privilege", query)
			}
			f.unchanged(t, before)
		}
		before := f.snapshot(t)
		var canConnect, privileged bool
		if err := connection.QueryRow(f.ctx, `SELECT has_database_privilege(current_user, current_database(), 'CONNECT'),
			rolinherit OR rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls
			FROM pg_catalog.pg_roles WHERE rolname=current_user`).Scan(&canConnect, &privileged); err != nil || !canConnect || privileged {
			t.Fatal("restricted role flags or CONNECT are incorrect")
		}
		f.unchanged(t, before)
	}
	for _, cfg := range []*pgx.ConnConfig{f.readerConfig, f.decisionConfig} {
		before := f.snapshot(t)
		other := cfg.Copy()
		other.Database = "postgres"
		connected, err := pgx.ConnectConfig(f.ctx, other)
		if err == nil {
			_ = connected.Close(f.ctx)
			t.Fatal("episode login connected to another database")
		}
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
			t.Fatal("other database connection failed for a reason other than its CONNECT ACL")
		}
		f.unchanged(t, before)
	}
	for _, test := range []struct {
		conn *pgx.Conn
		role string
		sql  string
	}{{f.reader, f.reader.Config().User, `SELECT * FROM k6_auth.approval_decision(pg_catalog.gen_random_uuid(),'workflow','node',0,'hash','subject','APPROVED')`},
		{f.decision, f.decision.Config().User, `SELECT * FROM k6_auth.read_exact_grant(pg_catalog.gen_random_uuid(),'token','workflow','node',0,'key','scope','target')`},
		{f.reader, f.reader.Config().User, `SELECT k6_auth.grant_is_live(pg_catalog.clock_timestamp(),pg_catalog.clock_timestamp())`}} {
		before := f.snapshot(t)
		_, err := test.conn.Exec(f.ctx, test.sql)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
			t.Fatalf("role %q could execute %s (error: %v)", test.role, test.sql, err)
		}
		f.unchanged(t, before)
	}
}

func TestK6PostgresDecisionAndRedispatch(t *testing.T) {
	f := newK6Fixture(t, time.Minute, nil)
	intent := f.create(t, f.store, f.workflow)
	before := f.snapshot(t)
	lookup := intent
	lookup.IntentID = ""
	if code, identity, err := f.decide(lookup, k6TestSubject, nil); err != nil || code != "OK" || identity.Decision != "PENDING" ||
		identity.Subject != nil || identity.DecidedAt != nil || identity.IntentID != intent.IntentID || identity.ProposalHash != intent.ProposalHash {
		t.Fatal("pending lookup did not return the exact proposal identity")
	}
	f.unchanged(t, before)
	approved := "APPROVED"
	if _, _, err := f.decide(lookup, k6TestSubject, &approved); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatal("approval recorded without the exact intent identity")
	}
	f.unchanged(t, before)
	if retry, err := f.store.CreateApprovalIntent(f.ctx, f.input(f.workflow, 2)); err != nil || retry.IntentID != intent.IntentID {
		t.Fatal("pending exact retry did not deduplicate")
	}
	f.unchanged(t, before)
	wrong := f.input(f.workflow, 2)
	wrong.Target += "-changed"
	if _, err := f.store.CreateApprovalIntent(f.ctx, wrong); !errors.Is(err, ErrApprovalMismatch) {
		t.Fatal("pending retry admitted a different proposal")
	}
	f.unchanged(t, before)
	wrong = f.input(f.workflow, 2)
	wrong.ValidUntil = wrong.ValidUntil.Add(time.Minute)
	if _, err := f.store.CreateApprovalIntent(f.ctx, wrong); !errors.Is(err, ErrApprovalMismatch) {
		t.Fatal("caller extended the provisioned cutoff")
	}
	f.unchanged(t, before)
	rejected := "REJECTED"
	badHash := intent
	badHash.ProposalHash = "wrong-hash"
	for _, bad := range []struct {
		intent  ApprovalIntent
		subject string
	}{{badHash, k6TestSubject}, {intent, "worker"}} {
		if _, _, err := f.decide(bad.intent, bad.subject, &approved); !errors.Is(err, pgx.ErrNoRows) {
			t.Fatal("decision writer admitted a wrong proposal hash or subject")
		}
		f.unchanged(t, before)
	}
	if _, err := f.store.RecordApprovalDecision(f.ctx, ApprovalDecisionInput{IntentID: intent.IntentID,
		ApproverID: k6TestSubject, Decision: approved, ProposalHash: intent.ProposalHash}); !errors.Is(err, ErrApprovalUnauthorized) {
		t.Fatal("legacy decision route bypassed the K6 decision writer")
	}
	f.unchanged(t, before)
	if _, err := f.store.ApplyApproval(f.ctx, f.applyInput(intent, 2)); !errors.Is(err, ErrApprovalMismatch) {
		t.Fatal("signed-looking tool text or a pending proposal authorized a grant")
	}
	f.unchanged(t, before)
	first := f.record(t, intent, approved)
	before = f.snapshot(t)
	if retry := f.record(t, intent, approved); !retry.DecidedAt.Equal(*first.DecidedAt) {
		t.Fatal("exact decision retry replaced its identity")
	}
	f.unchanged(t, before)
	if _, _, err := f.decide(intent, k6TestSubject, &rejected); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatal("decision writer changed a recorded decision")
	}
	f.unchanged(t, before)
	if _, _, err := f.decide(intent, k6TestSubject, nil); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatal("pending lookup returned a decided proposal")
	}
	f.unchanged(t, before)
	var existing *K6ExistingIntent
	if _, err := f.store.CreateApprovalIntent(f.ctx, f.input(f.workflow, 2)); !errors.As(err, &existing) || existing.IntentID != intent.IntentID {
		t.Fatal("redispatch replaced the non-pending intent")
	}
	f.unchanged(t, before)
	stale := f.applyInput(intent, 2)
	stale.Lease.Epoch++
	if _, err := f.store.ApplyApproval(f.ctx, stale); !errors.Is(err, ErrLeaseNotOwned) {
		t.Fatalf("stale lease admitted: %v", err)
	}
	f.unchanged(t, before)
	if _, err := f.store.ApplyApproval(f.ctx, f.applyInput(intent, 1)); !errors.Is(err, ErrRevisionConflict) {
		t.Fatal("stale workflow revision admitted")
	}
	f.unchanged(t, before)
	if _, err := f.store.ApplyApproval(f.ctx, f.applyInput(intent, 2)); err != nil {
		t.Fatal(err)
	}
	before = f.snapshot(t)
	var already *K6AlreadyGranted
	if _, err := f.store.ApplyApproval(f.ctx, f.applyInput(intent, 3)); !errors.As(err, &already) || already.IntentID != intent.IntentID {
		t.Fatal("redispatch re-minted an already granted intent")
	}
	f.unchanged(t, before)
	if _, err := f.store.CreateApprovalIntent(f.ctx, f.input(f.workflow, 3)); !errors.As(err, &existing) || existing.IntentID != intent.IntentID {
		t.Fatal("redispatch replaced the granted intent")
	}
	f.unchanged(t, before)
}

func TestK6PostgresCancellationAndRejection(t *testing.T) {
	f := newK6Fixture(t, time.Minute, nil)
	for _, decision := range []string{"APPROVED", "REJECTED"} {
		workflow := f.newWorkflow(t, k6TestNamespace)
		intent := f.create(t, f.store, workflow)
		f.record(t, intent, decision)
		request, err := f.store.RequestCancellation(f.ctx, CancellationRequestInput{WorkflowID: workflow, ClientKey: "k6-cancel", ObservedRevision: 2})
		if err != nil {
			t.Fatal(err)
		}
		before := f.snapshot(t)
		if _, err := f.store.ApplyApproval(f.ctx, f.applyInput(intent, 2)); !errors.Is(err, ErrCancellationConflict) {
			t.Fatal("decision was applied ahead of pending cancellation")
		}
		f.unchanged(t, before)
		if _, err := f.store.ApplyCancellationRequest(f.ctx, f.lease, request.RequestID, "k6-scheduler"); err != nil {
			t.Fatal(err)
		}
	}
	intent := f.create(t, f.store, f.workflow)
	f.record(t, intent, "REJECTED")
	grant, err := f.store.ApplyApproval(f.ctx, f.applyInput(intent, 2))
	workflow, getErr := f.store.GetWorkflow(f.ctx, f.workflow)
	if err != nil || getErr != nil || grant.GrantToken != "" || workflow.State != StateRejected {
		t.Fatal("lease-holder rejection did not commit without a grant")
	}
}

func TestK6PostgresCutoffRecovery(t *testing.T) {
	f := newK6Fixture(t, 3*time.Second, nil)
	intent, grant := f.grant(t)
	unusedWorkflow := f.newWorkflow(t, k6TestNamespace)
	hash, err := canonicalPayloadHash(intent.CanonicalArguments)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.RecordUnknownEffect(f.ctx, f.workflow, grant.LogicalKey, hash, grant.ScopeHash, 1); err != nil {
		t.Fatal(err)
	}
	if err := f.store.ApplyOwnerTransition(f.ctx, OwnerTransitionInput{Lease: f.lease, WorkflowID: f.workflow, ExpectedRevision: 3,
		NewState: StateWaitingActivity, ActorID: "k6-scheduler", Reason: "SCHEDULE_ACTIVITY", NodeID: "root", NodeState: ptrState(StateWaitingActivity)}); err != nil {
		t.Fatal(err)
	}
	if err := f.store.ApplyOwnerTransition(f.ctx, OwnerTransitionInput{Lease: f.lease, WorkflowID: f.workflow, ExpectedRevision: 4,
		NewState: StateReconciliationRequired, ActorID: "k6-scheduler", Reason: "OUTCOME_UNKNOWN", NodeID: "root", NodeState: ptrState(StateReconciliationRequired)}); err != nil {
		t.Fatal(err)
	}
	before := f.snapshot(t)
	f.waitCutoff(t)
	var boundary bool
	if err := f.admin.QueryRow(f.ctx, `SELECT k6_auth.grant_is_live($1::timestamptz,$1::timestamptz)`, f.cutoff).Scan(&boundary); err != nil || boundary {
		t.Fatal("reader admitted DB time equal to grant expiry")
	}
	if _, err := f.read(grant); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatal("reader admitted the expired grant")
	}
	f.unchanged(t, before)
	if _, err := f.store.CreateApprovalIntent(f.ctx, f.input(unusedWorkflow, 1)); !errors.Is(err, ErrApprovalExpired) {
		t.Fatal("scheduler created a first intent after cutoff")
	}
	f.unchanged(t, before)
	approved := "APPROVED"
	if code, identity, err := f.decide(intent, k6TestSubject, &approved); err != nil || code != "EXPIRED" || identity.IntentID != "" {
		t.Fatal("same-actor decision retry was accepted after cutoff")
	}
	f.unchanged(t, before)
	var existing *K6ExistingIntent
	if _, err := f.store.CreateApprovalIntent(f.ctx, f.input(f.workflow, 5)); !errors.As(err, &existing) || existing.IntentID != intent.IntentID {
		t.Fatal("post-cutoff recovery did not retain the original intent identity")
	}
	f.unchanged(t, before)
	var already *K6AlreadyGranted
	if _, err := f.store.ApplyApproval(f.ctx, f.applyInput(intent, 5)); !errors.As(err, &already) || already.IntentID != intent.IntentID {
		t.Fatal("post-cutoff recovery did not retain the original grant identity")
	}
	f.unchanged(t, before)
	var observedWorkflow, observedEffect string
	if err := f.admin.QueryRow(f.ctx, `SELECT w.state, e.outcome FROM engine.workflow_executions w
		JOIN effects.effect_records e ON e.workflow_id=w.workflow_id WHERE w.workflow_id=$1`, f.workflow).
		Scan(&observedWorkflow, &observedEffect); err != nil || observedWorkflow != "RECONCILIATION_REQUIRED" || observedEffect != "OUTCOME_UNKNOWN" {
		t.Fatal("post-cutoff recovery lost the unresolved outcome")
	}
}

type k6ExpiryTracer struct {
	match  string
	cutoff time.Time
}

func (tr *k6ExpiryTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if tr.match != "" && strings.Contains(data.SQL, tr.match) {
		time.Sleep(time.Until(tr.cutoff) + 30*time.Millisecond)
	}
	return ctx
}

func (*k6ExpiryTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func TestK6PostgresIssuanceDBTime(t *testing.T) {
	for _, test := range []struct{ name, match, failure string }{
		{"DB sample", "SELECT pg_catalog.clock_timestamp()", "process time admitted grant issuance"},
		{"protected write", "SET dispatch_status = 'GRANTED'", "grant minted after cutoff at protected write"},
		{"clock uncertainty", "", "clock bracket spanning cutoff admitted grant issuance"},
	} {
		t.Run(test.name, func(t *testing.T) {
			tracer := &k6ExpiryTracer{}
			f := newK6Fixture(t, 3*time.Second, tracer)
			intent := f.create(t, f.store, f.workflow)
			f.record(t, intent, "APPROVED")
			if test.match == "" {
				f.sql(t, `UPDATE k6_auth.episode SET uncertainty_us=4000000`)
			}
			before := f.snapshot(t)
			tracer.match, tracer.cutoff = test.match, f.cutoff
			if _, err := f.store.ApplyApproval(f.ctx, f.applyInput(intent, 2)); !errors.Is(err, ErrApprovalExpired) {
				t.Fatalf("%s: %v", test.failure, err)
			}
			f.unchanged(t, before)
		})
	}
}

func TestK6PostgresStoredArguments(t *testing.T) {
	f := newK6Fixture(t, time.Minute, nil)
	before := f.snapshot(t)
	for _, arguments := range []string{`{"n":1e2}`, `{"n":1.0}`, `{"n":-0}`, `{"n":1E400}`, `{"n":1,"n":2}`} {
		input := f.input(f.workflow, 1)
		input.CanonicalArguments = json.RawMessage(arguments)
		if _, err := f.store.CreateApprovalIntent(f.ctx, input); !errors.Is(err, ErrK6ArgumentsNotCanonical) {
			t.Fatalf("noncanonical arguments reached intent creation: %v", err)
		}
		f.unchanged(t, before)
	}
	f.sql(t, `CREATE FUNCTION k6_auth.test_normalize() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN NEW.canonical_arguments = '{"changed":true}'::jsonb; RETURN NEW; END; $$;
		CREATE TRIGGER k6_test_normalize BEFORE INSERT ON engine.approval_action_intents
		FOR EACH ROW EXECUTE FUNCTION k6_auth.test_normalize()`)
	if _, err := f.store.CreateApprovalIntent(f.ctx, f.input(f.workflow, 1)); !errors.Is(err, ErrK6ArgumentsNotCanonical) {
		t.Fatal("stored argument hash mismatch was committed")
	}
	f.unchanged(t, before)
	f.sql(t, `DROP TRIGGER k6_test_normalize ON engine.approval_action_intents`)
	intent := f.create(t, f.store, f.workflow)
	var stored json.RawMessage
	if err := f.admin.QueryRow(f.ctx, `SELECT canonical_arguments FROM engine.approval_action_intents WHERE intent_id=$1`, intent.IntentID).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	hash, err := proposalHash(intent.Target, stored, intent.ExpectedResourceRevision)
	if err != nil || hash != intent.ProposalHash {
		t.Fatal("accepted nested canonical arguments changed through jsonb")
	}
}

func TestK6ValidateArguments(t *testing.T) {
	for _, s := range []string{`1e2`, `1.0`, `-0`, `1E400`, `{"n":1,"\u006e":2}`, `{"n":1,"n":2}`,
		`9007199254740992`, `-9007199254740992`, `+1`, `01`, `{} {}`, `{"a":[{"x":0,"x":1}]}`} {
		if !errors.Is(ValidateK6Arguments([]byte(s)), ErrK6ArgumentsNotCanonical) {
			t.Errorf("invalid sealed-plan value %s accepted", s)
		}
	}
	for _, s := range []string{`{"é":[9007199254740991,-9007199254740991,0,null,true,false,{"label":"é"}]}`,
		`{"\u00e9":[9007199254740991,-9007199254740991,0,null,true,false,{"label":"\u00e9"}]}`, `[]`, `null`, `"é"`, `0`} {
		if err := ValidateK6Arguments([]byte(s)); err != nil {
			t.Errorf("valid sealed-plan value %s refused: %v", s, err)
		}
	}
	first, err := proposalHash("target", []byte(`{"é":"é"}`), "7")
	second, anotherErr := proposalHash("target", []byte(`{"\u00e9":"\u00e9"}`), "7")
	if err != nil || anotherErr != nil || first != second {
		t.Fatal("literal and escaped Unicode changed the canonical proposal hash")
	}
}
