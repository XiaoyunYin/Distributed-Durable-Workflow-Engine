package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/invariants/k6collect"
	"durable-agent-execution-engine/internal/invariants/k6live"
	"durable-agent-execution-engine/internal/invariants/k6v1"
	"github.com/jackc/pgx/v5"
)

func TestK6AuthorityLiveParity(t *testing.T) {
	if os.Getenv("DURABLE_K6_VERDICT_ARTIFACT") == "" {
		t.Skip("requires hosted PostgreSQL and network-isolated checker")
	}
	for _, tc := range []struct{ name, code, sinkSQL, engineSQL string }{
		{name: "Clean"},
		{name: "ConfirmationClean"},
		{name: "ConfirmationWrongUID", code: "required_proof_missing:control_runtime"},
		{name: "ConfirmationRuntimeChanged", code: "CONTROL_RUNTIME_CHANGED"},
		{name: "ConfirmationFaultConfigured", code: "required_proof_missing:control_fault_inventory"},
		{name: "ConfirmationLateOpening", code: "required_proof_missing:control_window"},
		{name: "ConfirmationStartOutside", code: "required_proof_missing:control_window"},
		{name: "ConfirmationMissingRole", code: "required_proof_missing:control_runtime"},
		{name: "ConfirmationSourceDrop", code: "required_proof_missing:" + k6v1.ControlPath},
		{name: "WithheldPastExpiry"},
		{name: "ExpiredWithheld", code: "I10_EXPIRED_WRITE"},
		{"Unapproved", "I10_UNAPPROVED_WRITE", "", "UPDATE engine.approval_action_intents SET decision='PENDING'"},
		{"WrongActor", "I10_WRONG_ACTOR", "", "UPDATE engine.approval_action_intents SET approver_id='wrong-actor'"},
		{"WrongToken", "I10_WRONG_TOKEN", "grant_hash=repeat('a',64)", ""},
		{"WrongTarget", "I10_WRONG_TARGET", "resource_id='other-resource'", ""},
		{"WrongArgument", "I10_WRONG_ARGUMENT", `arguments='{"position":4,"value":"k6"}'`, ""},
		{"WrongKey", "I10_WRONG_KEY", "logical_key='other-key'", ""},
		{"WrongScope", "I10_WRONG_SCOPE", "scope_hash=repeat('a',64)", ""},
		{"WrongRevision", "I10_WRONG_REVISION", "expected_revision='1'", ""},
		{"WrongOperation", "I10_WRONG_OPERATION", "node_id='other-node'", ""},
		{"StaleFence", "I10_STALE_FENCE", "fence=2", ""},
		{name: "ReplacedGrant", code: "I10_GRANT_REPLACED"},
		{name: "LookupGrantChanged", code: "I10_GRANT_CONTINUITY"},
		{"CancellationOrder", "I10_CANCELLATION_ORDER", "", "INSERT INTO engine.cancellation_requests(request_id,workflow_id,client_key,observed_revision,status,created_at,applied_at) SELECT gen_random_uuid(),workflow_id,'cancel',2,'APPLIED',created_at,created_at FROM engine.workflow_executions"},
		{name: "CancellationLosesGrantRace"},
		{name: "CancellationPendingAtGrant", code: "I10_CANCELLATION_ORDER"},
		{name: "Unacknowledged", code: "I10_DISPATCH_UNACKNOWLEDGED"},
		{"CommitTimeMissing", "required_proof_missing:sink_commit_transaction", "committed_at=NULL", ""},
		{"ReadMissing", "required_proof_missing:exact_grant_read", "grant_read_at=NULL", ""},
		{name: "OpenMissing", code: "required_proof_missing:relay_ingress_open"},
		{"ReadAfterWrite", "I10_WRITE_BRACKET", "grant_read_at=committed_at+interval '1 second'", ""},
		{"WrongTransaction", "I10_WRITE_TRANSACTION", "transaction_id='999999999'", ""},
		{"GrantDecisionBinding", "I10_GRANT_DECISION_BINDING", "", `ALTER TABLE engine.k6_write_capture DISABLE TRIGGER k6_write_immutable;
			UPDATE engine.k6_write_capture SET before_image=jsonb_set(before_image,'{approver_id}','"other-actor"')
			WHERE subject='engine.approval_action_intents' AND after_image->>'dispatch_status'='GRANTED'`},
		{"ClaimRetired", "I10_WRITE_CLAIM", "", `ALTER TABLE engine.k6_write_capture DISABLE TRIGGER k6_write_immutable;
			UPDATE engine.k6_write_capture SET after_image=jsonb_set(after_image,'{is_current}','false')
			WHERE subject='engine.activity_attempts' AND before_image->>'state'='DISPATCHABLE'`},
		{name: "AckWrongOrder", code: "I10_DISPATCH_ACK_ORDER"},
		{"AckWrongReceipt", "I10_DISPATCH_RECEIPT", "", `UPDATE engine.activity_attempts
			SET k6_proof=jsonb_set(k6_proof,'{receipt,application_id}',to_jsonb(gen_random_uuid()))`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			engine, dsn := k6TestDatabase(t)
			bindK6Database(t, engine, "engine-instance", "reset-a")
			sink, sinkDSN := k6SinkDatabase(t, engine)
			execK6SQL(t, engine, string(getFile(t, captureMigration+"up.sql")))
			m := k6Manifest(engine.Config().Database, false)
			m.Ordinal, m.Seed, m.Cell, m.Episode, m.Workload = 90, 530091, "medium-12", "medium-12-0", "COOPERATING_EFFECT"
			m.Sink = &k6v1.DatabaseIdentity{Database: sink.Config().Database, Instance: "sink-instance"}
			m.Instrumentation = []string{k6v1.WriteCapture}
			control := strings.HasPrefix(tc.name, "Confirmation")
			if control {
				m.Ordinal, m.Seed, m.Cell, m.Episode = 113, 530114, "medium-16", "medium-16-3"
			}
			m.RequiredSources = m.SourcePaths()
			plan, arrivals := k6Sources(m, time.Now().Add(-time.Minute), "resource")
			workflow := &plan.Windows[0].Submissions[0]
			arguments := json.RawMessage(`{"position":3,"value":"k6"}`)
			workflow.Effect = &k6v1.PlannedEffect{Node: "pure.echo", LogicalKey: "key", ExpectedRevision: "0", Arguments: arguments}
			sources := parityExternal(t, &m, true, workflow.Key)
			recovered := tc.name == "WithheldPastExpiry" || tc.name == "ExpiredWithheld"
			shortGrant := recovered || tc.name == "ReplacedGrant"
			cutoff := time.Now().Add(2 * time.Minute).Truncate(time.Microsecond)
			if shortGrant {
				cutoff = time.Now().Add(5 * time.Second).Truncate(time.Microsecond)
			}
			argsHash := k6v1.Hash(arguments)
			proposal := k6v1.Hash([]byte("resource\x00" + argsHash + "\x000"))
			scope := k6v1.Hash([]byte(proposal + "\x000\x00key\x00resource"))
			d := &sources.Decisions.Rows[0].Data
			d.ProposalHash = proposal
			d.DecidedUTC = seedAuthorityClaim(t, engine, *workflow, proposal, scope, cutoff, tc.name)
			sources.Decisions.Rows[0].Position.UTC = d.DecidedUTC
			execK6SQL(t, sink, "BEGIN")
			seedParitySink(t, sink, workflow.Key, sources)
			execK6SQL(t, sink, "UPDATE k6_sink.receipts SET canonical_hash=$1,scope_hash=$2", argsHash, scope)
			opened := time.Now().Truncate(time.Microsecond)
			read := opened.Add(time.Microsecond)
			written := read.Add(time.Microsecond)
			if tc.name == "ExpiredWithheld" {
				written = cutoff.Add(time.Microsecond)
			}
			if !read.Before(cutoff) {
				t.Fatal("fixture must read the grant before expiry")
			}
			apply := &sources.Ingress.Rows[1]
			if tc.name != "OpenMissing" {
				apply.Data.OpenSequence, apply.Data.OpenedUTC = 1, k6v1.UTC(opened)
			}
			execK6SQL(t, sink, `UPDATE k6_sink.calls SET grant_hash=$1,node_id='pure.echo',iteration=0,scope_hash=$2,
				expected_revision='0',arguments=$3,fence=1,grant_read_at=$4,committed_at=CASE WHEN outcome='APPLIED' THEN $5::timestamptz END`,
				apply.Data.Traffic.GrantHash, scope, arguments, read, written)
			if tc.sinkSQL != "" {
				execK6SQL(t, sink, "UPDATE k6_sink.calls SET "+tc.sinkSQL+" WHERE outcome='APPLIED'")
			}
			if tc.name == "LookupGrantChanged" {
				execK6SQL(t, sink, "UPDATE k6_sink.calls SET grant_hash=repeat('a',64) WHERE operation='lookup'")
			}
			execK6SQL(t, sink, "COMMIT")
			if shortGrant {
				time.Sleep(time.Until(cutoff.Add(time.Millisecond)))
			}
			operation := "apply"
			if recovered {
				operation = "lookup"
				apply.Position.UTC = k6v1.UTC(time.Now())
				sources.Ingress.Rows[2].Position.UTC = apply.Position.UTC
			}
			proof := provisionFixtureReceipt(t, engine, sink, &m, workflow.Key, operation, 1)
			if tc.name == "AckWrongOrder" {
				execK6SQL(t, engine, "UPDATE engine.approval_action_intents SET dispatch_status='DISPATCHED'")
			}
			finishAuthorityFixture(t, engine, workflow.Key, proof.Receipt.ApplicationID, tc.name != "Unacknowledged" && tc.name != "AckWrongOrder")
			if tc.name == "ReplacedGrant" {
				execK6SQL(t, engine, "BEGIN;"+fixtureAuthorityOwner+"UPDATE engine.approval_action_intents SET grant_token=gen_random_uuid(),grant_expires_at=clock_timestamp()+interval '1 minute'; COMMIT")
			}
			if tc.engineSQL != "" {
				execK6SQL(t, engine, tc.engineSQL)
			}
			sealFixtureStream(t, sources.Decisions, time.Now())
			changeEffectFixture(t, engine, sink, &sources, tc.name, recovered)
			seedAuthorityBroker(t, m, &sources)
			cfg := k6live.Config{Schema: k6live.ConfigVersion, Manifest: m, DatabaseURL: dsn, SinkDatabaseURL: sinkDSN,
				ExternalDir: t.TempDir(), EpisodeDir: filepath.Join(t.TempDir(), "public")}
			if control {
				writeConfirmationFixture(t, cfg, &arrivals, &sources, tc.name)
			}
			cfg = writeK6Sources(t, cfg, plan, arrivals)
			publishFixtureFrames(t, cfg, sources)
			path, stdout, diag, code := invokeLiveK6(t, cfg)
			if diag != "" {
				t.Fatalf("live diagnostics: %s, stdout: %s", diag, stdout)
			}
			r := verifyVerdictParity(t, path, stdout, code)
			if control {
				if tc.code == "" {
					want := k6v1.Confirmation{Status: "confirmed", Reason: "cell_predicate_satisfied"}
					if code != k6v1.Pass || r.Confirmation != want || len(r.Findings) != 0 {
						t.Fatalf("control did not pass: exit %d, %+v, %+v", code, r.Confirmation, r.Findings)
					}
				} else if code != k6v1.Fail || len(r.Findings) != 1 ||
					r.Findings[0].Source != k6v1.ControlPath || r.Findings[0].Code != tc.code ||
					r.Confirmation.Reason != tc.code {
					t.Fatalf("control fault result: exit %d, %+v, %+v", code, r.Confirmation, r.Findings)
				}
				return
			}
			if tc.code == "" {
				if code != k6v1.Incomplete || len(r.Findings) != 0 || r.Labels[7].Outcome != "covered/pass" || r.Labels[9].Outcome != "covered/pass" {
					t.Fatalf("effect awards: %+v, %+v, findings: %+v", r.Labels[7], r.Labels[9], r.Findings)
				}
				return
			}
			if code != k6v1.Fail {
				t.Fatalf("faulty effect exited %d", code)
			}
			switch tc.name {
			case "Unacknowledged", "AckWrongOrder", "AckWrongReceipt":
				requirePredicateFinding(t, r, k6v1.Violation, "DispatchAcknowledged", tc.code)
			case "CommitTimeMissing", "ReadMissing", "OpenMissing":
				requirePredicateFinding(t, r, k6v1.MissingProof, "AuthorizedAtWrite", tc.code)
			default:
				requirePredicateFinding(t, r, k6v1.Violation, "AuthorizedAtWrite", tc.code)
			}
		})
	}
}

func writeConfirmationFixture(t *testing.T, cfg k6live.Config, arrivals *k6v1.Arrivals, sources *k6v1.External, variant string) {
	t.Helper()
	if variant == "ConfirmationSourceDrop" {
		return
	}
	now := time.Now()
	workload := k6v1.UTC(now.Add(-5 * time.Second))
	attempt := &arrivals.Rows[0].Attempts[0]
	attempt.SentUTC, attempt.ObservedUTC, arrivals.SealedUTC = workload, workload, workload
	ledger := sources.Sessions[paritySession]
	for i := 1; i < len(ledger.Rows); i++ {
		ledger.Rows[i].Position.UTC = workload
	}
	if variant == "ConfirmationStartOutside" {
		ledger.Rows[1].Position.UTC = k6v1.UTC(now.Add(-11 * time.Second))
	}
	sealFixtureStream(t, &ledger, now)
	sources.Sessions[paritySession] = ledger
	worker := ledger.Rows[0].Data.Session.Runtime()
	if variant == "ConfirmationWrongUID" {
		worker.PodUID = "wrong-uid"
	}
	pods := []k6v1.ControlPod{{Role: "worker", Runtime: worker, EnvNames: []string{}}}
	for _, role := range []string{"api", "scheduler", "worker-control", "relay", "k6-sink", "k6-effect-relay"} {
		if variant == "ConfirmationMissingRole" && role == "scheduler" {
			continue
		}
		pods = append(pods, k6v1.ControlPod{Role: role, EnvNames: []string{},
			Runtime: k6v1.RuntimeIdentity{PodUID: role, ContainerID: role, StartedUTC: worker.StartedUTC}})
	}
	slices.SortFunc(pods, func(a, b k6v1.ControlPod) int { return strings.Compare(a.Runtime.PodUID, b.Runtime.PodUID) })
	opened := k6v1.UTC(now.Add(-10 * time.Second))
	if variant == "ConfirmationLateOpening" {
		opened = k6v1.UTC(now.Add(-time.Second))
	}
	before := k6v1.ControlSnapshot{BeforeUTC: opened, AfterUTC: opened, CRDList: "10", PodList: "10",
		Pods: pods, ChaosCRDs: []string{}}
	after := before
	after.BeforeUTC, after.AfterUTC, after.PodList = k6v1.UTC(now), k6v1.UTC(now), "11"
	after.Pods = slices.Clone(before.Pods)
	if variant == "ConfirmationRuntimeChanged" {
		after.Pods[0].Runtime.ContainerID = "replacement"
	}
	if variant == "ConfirmationFaultConfigured" {
		after.ChaosCRDs = []string{"podchaos.chaos-mesh.org"}
	}
	control := k6v1.ControlObservation{Schema: k6v1.ControlVersion, Binding: k6v1.Binding(cfg.Manifest), Before: before, After: after}
	path := filepath.Join(cfg.ExternalDir, k6v1.ControlPath)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	putFile(t, path, canonical(t, control))
}

const fixtureAuthorityOwner = `SET LOCAL durable.k4_owner = '` + captureOwnerID + `';
	SET LOCAL durable.k6_owner_partition = '0'; SET LOCAL durable.k6_owner_epoch = '1';`

func seedAuthorityBroker(t *testing.T, m k6v1.Manifest, sources *k6v1.External) {
	t.Helper()
	at := time.Now()
	stream := k6collect.NewStream[k6v1.BrokerCopyRow](m, "broker-copies", "fixture-kafka", at)
	parts := []k6v1.BrokerPartition{
		{Topic: "durable-agent.events.v1", TopicID: "fixture-events"},
		{Topic: "durable-agent.tasks.v1", TopicID: "fixture-tasks"},
	}
	// The outbox was captured with publication admission closed. Both physical
	// ranges are empty at this cut.
	for _, p := range parts {
		k6collect.Append(&stream, at, k6v1.BrokerCopyRow{PartitionIdentity: k6v1.PartitionIdentity{TopicID: p.TopicID}, Range: &p, Complete: true})
	}
	sources.Broker.Rows[0].Data.Partitions = parts
	sealFixtureStream(t, sources.Broker, at)
	sealFixtureStream(t, &stream, at)
	sources.BrokerCopies = &stream
}

func fixtureAuthorityHistory(t *testing.T, db *pgx.Conn, revision int, reason, state, event string, attempt *int) {
	t.Helper()
	execK6SQL(t, db, `INSERT INTO engine.transition_history(transition_id,workflow_id,revision,actor_kind,actor_id,
		scheduler_epoch,node_id,iteration,attempt_number,old_state,new_state,reason)
		SELECT gen_random_uuid(),workflow_id,$1,CASE WHEN $1=1 THEN 'client' WHEN $2='ATTEMPT_RESULT_RECORDED' THEN 'worker' ELSE 'scheduler' END,
		CASE WHEN $2='ATTEMPT_RESULT_RECORDED' THEN 'worker' ELSE 'actor' END,
		CASE WHEN $1=1 OR $2='ATTEMPT_RESULT_RECORDED' THEN NULL ELSE 1 END,'pure.echo',CASE WHEN $1=1 THEN NULL ELSE 0 END,
		$4,CASE WHEN $1=1 THEN NULL ELSE state END,$3,$2 FROM engine.workflow_executions;
		UPDATE engine.workflow_executions SET state=$3,revision=$1;
		INSERT INTO engine.outbox(event_id,workflow_id,aggregate_revision,event_type,topic,payload)
		SELECT gen_random_uuid(),workflow_id,$1,$5,
			CASE WHEN $5='attempt.dispatch' THEN 'durable-agent.tasks.v1' ELSE 'durable-agent.events.v1' END,
			CASE $5 WHEN 'attempt.dispatch' THEN jsonb_build_object('node_id','pure.echo','iteration',0,'attempt_number',$4::int)
			WHEN 'graph.advanced' THEN jsonb_build_object('workflow_id',workflow_id,'from_node_id','pure.echo','next',NULL) ELSE '{}'::jsonb END
		FROM engine.workflow_executions WHERE $5<>''`, pgx.QueryExecModeSimpleProtocol, revision, reason, state, attempt, event)
}

const insertAuthorityCancellation = `INSERT INTO engine.cancellation_requests(request_id, workflow_id, client_key, observed_revision)
	VALUES (gen_random_uuid(), $1, 'cancel', 2)`

// Start the actual FK-blocked insert and wait for PostgreSQL to identify the
// grant transaction as its blocker before allowing the grant to commit.
func blockedAuthorityCancellation(t *testing.T, grant *pgx.Conn, workflow string) func() {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	peer, err := pgx.ConnectConfig(ctx, grant.Config().Copy())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = peer.Close(context.Background()) })
	execK6SQL(t, peer, "SET durable.k4_campaign='on'; SET durable.k6_capture='on'")
	execK6SQL(t, grant, "SELECT workflow_id FROM engine.workflow_executions WHERE workflow_id=$1 FOR UPDATE", workflow)
	grantPID, peerPID := int32(grant.PgConn().PID()), int32(peer.PgConn().PID())
	inserted := make(chan error, 1)
	go func() {
		_, err := peer.Exec(ctx, insertAuthorityCancellation, workflow)
		inserted <- err
	}()
	for {
		var blocked bool
		if err := grant.QueryRow(ctx, `SELECT $1 = ANY(pg_blocking_pids($2))`, grantPID, peerPID).Scan(&blocked); err != nil {
			t.Fatalf("observe cancellation blocked by grant: %v", err)
		}
		if blocked {
			break
		}
		select {
		case err := <-inserted:
			t.Fatalf("cancellation returned before grant released the workflow lock: %v", err)
		case <-time.After(5 * time.Millisecond):
		}
	}
	return func() {
		t.Helper()
		defer cancel()
		if err := <-inserted; err != nil {
			t.Fatalf("commit cancellation after grant: %v", err)
		}
		execK6SQL(t, grant, "UPDATE engine.cancellation_requests SET status='BEST_EFFORT',applied_at=clock_timestamp()")
		var ordered bool
		if err := grant.QueryRow(t.Context(), `SELECT c.created_at < a.finalized_at AND a.event_order < i.event_order AND i.event_order < u.event_order
			FROM engine.cancellation_requests c
			JOIN engine.transition_history h ON h.workflow_id=c.workflow_id AND h.reason='APPROVAL_GRANTED'
			JOIN engine.k4_audit a ON a.transition_id=h.transition_id
			JOIN engine.k6_write_capture i ON i.subject='engine.cancellation_requests' AND i.row_key=jsonb_build_array(c.request_id) AND i.operation='insert'
			JOIN engine.k6_write_capture u ON u.subject=i.subject AND u.row_key=i.row_key AND u.operation='update'`).Scan(&ordered); err != nil || !ordered {
			t.Fatalf("cancellation sample and commit must straddle the grant: ordered=%t, err=%v", ordered, err)
		}
	}
}

func seedAuthorityClaim(t *testing.T, db *pgx.Conn, item k6v1.Submission, proposal, scope string, cutoff time.Time, variant string) string {
	t.Helper()
	execK6SQL(t, db, `INSERT INTO engine.workflow_definitions(definition_id,version,definition_hash,graph,activity_versions,effect_classes)
		VALUES ('definition',1,'hash','{"nodes":["pure.echo"]}','{"pure.echo":"v1"}','{"pure.echo":"COOPERATING_EFFECT"}');
		SET durable.k4_campaign='on'; SET durable.k4_session='session'; SET durable.k4_pod_uid='uid';
		SET durable.k4_container_id='container'; SET durable.k6_capture='on';
		BEGIN;
		UPDATE engine.partition_leases SET owner_id='`+captureOwnerID+`',epoch=1,lease_expires_at=now()+interval '1 hour' WHERE partition_id=0;
		INSERT INTO engine.k4_audit_queue(kind,partition_id,owner_id,epoch,session_id,pod_uid,container_id)
		VALUES ('acquire',0,'`+captureOwnerID+`',1,'session','uid','container'); COMMIT;`)
	execK6SQL(t, db, `BEGIN; INSERT INTO engine.workflow_executions
		(workflow_id,namespace,submission_key,submission_payload_hash,definition_id,definition_version,partition_id,state,revision)
		VALUES ($1,'episode',$1,$2,'definition',1,0,'RUNNABLE',1);
		INSERT INTO engine.node_instances(workflow_id,node_id,iteration,state,input) VALUES ($1,'pure.echo',0,'RUNNABLE','{}')`,
		pgx.QueryExecModeSimpleProtocol, item.Key, "sub-v1:"+item.BodySHA256)
	fixtureAuthorityHistory(t, db, 1, "WORKFLOW_CREATED", "RUNNABLE", "workflow.created", nil)
	execK6SQL(t, db, "COMMIT; BEGIN;"+fixtureAuthorityOwner)
	execK6SQL(t, db, `INSERT INTO k6_auth.episode(namespace,approver_subject,valid_until,uncertainty_us) VALUES
		('episode','system:serviceaccount:episode:k6-approver',$1,0);
		INSERT INTO engine.approval_action_intents(intent_id,workflow_id,node_id,iteration,proposal_hash,target,canonical_arguments,
			expected_resource_revision,valid_until,k6_logical_key)
		VALUES ($2,$3,'pure.echo',0,$4,'resource',$5,'0',$1,'key');
		UPDATE engine.node_instances SET state='WAITING_APPROVAL',revision=1`, pgx.QueryExecModeSimpleProtocol,
		cutoff, parityIntent, item.Key, proposal, string(item.Effect.Arguments))
	fixtureAuthorityHistory(t, db, 2, "APPROVAL_REQUESTED", "WAITING_APPROVAL", "approval.requested", nil)
	execK6SQL(t, db, "COMMIT")
	var decided time.Time
	if err := db.QueryRow(t.Context(), `UPDATE engine.approval_action_intents SET decision='APPROVED',
		approver_id='system:serviceaccount:episode:k6-approver',decided_at=clock_timestamp() RETURNING decided_at`).Scan(&decided); err != nil {
		t.Fatal(err)
	}
	if variant == "CancellationPendingAtGrant" {
		execK6SQL(t, db, insertAuthorityCancellation, item.Key)
	}
	execK6SQL(t, db, "BEGIN;"+fixtureAuthorityOwner)
	var finishCancellation func()
	if variant == "CancellationLosesGrantRace" {
		finishCancellation = blockedAuthorityCancellation(t, db, item.Key)
	}
	execK6SQL(t, db, `UPDATE engine.approval_action_intents SET dispatch_status='GRANTED',grant_token=$1,
		grant_scope_hash=$2,grant_expires_at=valid_until; UPDATE engine.node_instances SET state='RUNNABLE',revision=2`,
		pgx.QueryExecModeSimpleProtocol, grantSecret, scope)
	fixtureAuthorityHistory(t, db, 3, "APPROVAL_GRANTED", "RUNNABLE", "approval.granted", nil)
	execK6SQL(t, db, "COMMIT")
	if finishCancellation != nil {
		finishCancellation()
	}
	execK6SQL(t, db, "BEGIN;"+fixtureAuthorityOwner+"UPDATE engine.node_instances SET state='WAITING_ACTIVITY',revision=3")
	fixtureAuthorityHistory(t, db, 4, "ACTIVITY_SCHEDULED", "WAITING_ACTIVITY", "", nil)
	execK6SQL(t, db, "COMMIT; BEGIN;"+fixtureAuthorityOwner)
	execK6SQL(t, db, `INSERT INTO engine.activity_attempts(workflow_id,node_id,iteration,attempt_number,state,effect_class,logical_effect_key,grant_scope_hash)
		VALUES ($1,'pure.echo',0,1,'DISPATCHABLE','COOPERATING_EFFECT','key',$2);
		UPDATE engine.node_instances SET current_attempt_number=1`, pgx.QueryExecModeSimpleProtocol, item.Key, scope)
	attempt := 1
	fixtureAuthorityHistory(t, db, 5, "ATTEMPT_CREATED", "WAITING_ACTIVITY", "attempt.dispatch", &attempt)
	execK6SQL(t, db, "COMMIT")
	execK6SQL(t, db, `UPDATE engine.activity_attempts SET state='CLAIMED',claim_token=$1,worker_id='worker',worker_request_id='request',
		heartbeat_deadline=clock_timestamp()+interval '2 minutes',updated_at=clock_timestamp(),k6_fence=1`, claimSecret)
	return k6v1.UTC(decided)
}

func finishAuthorityFixture(t *testing.T, db *pgx.Conn, workflow, receipt string, acknowledge bool) {
	t.Helper()
	execK6SQL(t, db, "BEGIN")
	if acknowledge {
		execK6SQL(t, db, "UPDATE engine.approval_action_intents SET dispatch_status='DISPATCHED'")
	}
	execK6SQL(t, db, `UPDATE engine.activity_attempts SET state='SUCCEEDED',is_current=false,
		outcome_disposition='KNOWN_SUCCESS',updated_at=clock_timestamp();
		INSERT INTO engine.k6_capture_queue(subject,operation,row_key,workflow_id,partition_id,after_image)
		VALUES ('result_request','ACCEPTED',jsonb_build_array($1::text,'pure.echo',0,1),$1,0,
			jsonb_build_object('token_hash',$2::text,'worker_request_id','request','receipt_id',$3::text,'attempt_state','SUCCEEDED'))`,
		pgx.QueryExecModeSimpleProtocol, workflow, k6v1.Hash([]byte(claimSecret)), receipt)
	attempt := 1
	fixtureAuthorityHistory(t, db, 6, "ATTEMPT_RESULT_RECORDED", "WAITING_ACTIVITY", "activity.result", &attempt)
	execK6SQL(t, db, "COMMIT; BEGIN;"+fixtureAuthorityOwner+"UPDATE engine.node_instances SET state='SUCCEEDED',current_attempt_number=NULL,revision=4")
	fixtureAuthorityHistory(t, db, 7, "RESULT_CONSUMED", "RUNNABLE", "workflow.result_consumed", &attempt)
	execK6SQL(t, db, "COMMIT; BEGIN;"+fixtureAuthorityOwner+"UPDATE engine.node_instances SET revision=5")
	fixtureAuthorityHistory(t, db, 8, "GRAPH_ADVANCED", "SUCCEEDED", "graph.advanced", nil)
	execK6SQL(t, db, "COMMIT")
}
