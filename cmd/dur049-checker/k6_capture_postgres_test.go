package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/invariants/k6live"
	"durable-agent-execution-engine/internal/invariants/k6v1"
	"github.com/jackc/pgx/v5"
)

const captureMigration = "../../migrations/k6_capture/000024_k6_capture."
const captureOwnerID = "00000000-0000-0000-0000-000000000001"

func TestK6SourceCaptureParity(t *testing.T) {
	if os.Getenv("DURABLE_K6_VERDICT_ARTIFACT") == "" {
		t.Skip("requires hosted PostgreSQL and network-isolated checker")
	}
	engine, dsn := k6TestDatabase(t)
	bindK6Database(t, engine, "engine-instance", "reset-a")
	assertInstalled := func(want bool) {
		t.Helper()
		var installed, migrated bool
		err := engine.QueryRow(t.Context(), `SELECT to_regclass('engine.k6_write_capture') IS NOT NULL,
			EXISTS (SELECT 1 FROM engine.schema_migrations WHERE version=24)`).Scan(&installed, &migrated)
		if err != nil || installed != want || migrated != want {
			t.Fatalf("capture table/ledger %t/%t, expected %t: %v", installed, migrated, want, err)
		}
	}
	assertInstalled(false)
	up, down := string(getFile(t, captureMigration+"up.sql")), string(getFile(t, captureMigration+"down.sql"))
	execK6SQL(t, engine, up)
	assertInstalled(true)
	execK6SQL(t, engine, down)
	execK6SQL(t, engine, down)
	assertInstalled(false)
	execK6SQL(t, engine, up)
	execK6SQL(t, engine, up)
	assertInstalled(true)
	execK6SQL(t, engine, `UPDATE engine.partition_leases SET updated_at=updated_at WHERE partition_id=0`)
	assertCaptureCount(t, engine, 0)

	m := k6Manifest(engine.Config().Database, false)
	m.Cell, m.Episode, m.Ordinal, m.Seed = "hook-5", "hook-5-0", 25, 530026
	m.Instrumentation = []string{k6v1.WriteCapture}
	m.RequiredSources = k6v1.RequiredSources(false, false, true)
	plan, arrivals := k6Sources(m, time.Now().Add(-time.Minute), "pure.echo")
	item := plan.Windows[0].Submissions[0]
	seedCapture(t, engine, item)
	execK6SQL(t, engine, `CREATE TEMP TABLE clean_capture AS TABLE engine.k6_write_capture;
		CREATE TEMP TABLE clean_owners AS TABLE engine.k6_owner_write_transactions;
		ALTER TABLE engine.k6_write_capture DISABLE TRIGGER k6_write_immutable;
		ALTER TABLE engine.k6_owner_write_transactions DISABLE TRIGGER k6_owner_immutable`)

	for _, tc := range []struct {
		name, change, predicate, code, relation, keyQuery string
	}{
		{name: "Clean"},
		{"MissingWrite", "DELETE FROM engine.k6_write_capture WHERE subject='engine.node_instances' AND operation='insert'",
			"EngineCaptureClosure", "CAPTURE_IMAGE_CHAIN", k6v1.WriteCapture,
			"SELECT event_order FROM clean_capture WHERE subject='engine.node_instances' AND operation='update' ORDER BY event_order LIMIT 1"},
		{"WrongTransaction", "UPDATE engine.k6_write_capture SET transaction_id='999999999' WHERE subject='engine.node_instances' AND operation='insert'",
			"EngineCaptureClosure", "CAPTURE_NODE_TRANSACTION", k6v1.WriteCapture,
			"SELECT event_order FROM clean_capture WHERE subject='engine.node_instances' AND operation='insert'"},
		{"OmittedOwnerClass", "DELETE FROM engine.k6_owner_write_transactions WHERE subject='engine.timers'",
			"OwnerAtCommit", "OWNER_INVENTORY_WRITE_MISSING", k6v1.WriteCapture,
			"SELECT event_order FROM clean_capture WHERE subject='engine.timers'"},
		{"StaleEpoch", "UPDATE engine.k6_owner_write_transactions SET epoch=epoch-1 WHERE subject='engine.timers'",
			"OwnerAtCommit", "OWNER_INVENTORY_STALE_EPOCH", k6v1.OwnerWrites,
			"SELECT event_order FROM clean_owners WHERE subject='engine.timers'"},
		{"ResultMismatch", "UPDATE engine.k6_write_capture SET after_image=jsonb_set(after_image,'{token_hash}',to_jsonb(repeat('a',64))) WHERE subject='result_request'",
			"CurrentClaimWinner", "CAPTURE_RESULT_IDENTITY", k6v1.WriteCapture,
			"SELECT event_order FROM clean_capture WHERE subject='result_request'"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.change != "" {
				execK6SQL(t, engine, tc.change)
				t.Cleanup(func() {
					execK6SQL(t, engine, `TRUNCATE engine.k6_owner_write_transactions, engine.k6_write_capture;
						INSERT INTO engine.k6_write_capture SELECT * FROM clean_capture;
						INSERT INTO engine.k6_owner_write_transactions SELECT * FROM clean_owners`)
				})
			}
			cfg := writeK6Sources(t, k6live.Config{Schema: k6live.ConfigVersion, DatabaseURL: dsn, Manifest: m,
				EpisodeDir: filepath.Join(t.TempDir(), "public")}, plan, arrivals)
			path, stdout, diag, code := invokeLiveK6(t, cfg)
			if code != k6v1.Fail || diag != "" {
				t.Fatalf("capture fixture exit %d, diagnostics %s: %s", code, diag, stdout)
			}
			r := verifyVerdictParity(t, path, stdout, code)
			if tc.change == "" {
				if slices.ContainsFunc(r.Findings, func(f k6v1.Finding) bool { return f.Kind == k6v1.Violation }) {
					t.Fatalf("clean capture fixture: %s", stdout)
				}
				return
			}
			var order int64
			if err := engine.QueryRow(t.Context(), tc.keyQuery).Scan(&order); err != nil {
				t.Fatal(err)
			}
			key, _ := json.Marshal(order)
			requireOrderingFinding(t, r, tc.predicate, tc.code, k6v1.Violation, tc.relation, []any{json.Number(key)})
		})
	}
	var count int
	if err := engine.QueryRow(t.Context(), "SELECT count(*) FROM engine.k6_write_capture").Scan(&count); err != nil {
		t.Fatal(err)
	}
	execK6SQL(t, engine, `BEGIN;
		UPDATE engine.activity_attempts SET updated_at=clock_timestamp();
		SET CONSTRAINTS ALL IMMEDIATE;
		ROLLBACK`)
	assertCaptureCount(t, engine, count)
	measureCapture(t, engine)
}

func assertCaptureCount(t *testing.T, conn *pgx.Conn, want int) {
	t.Helper()
	var got int
	err := conn.QueryRow(t.Context(), "SELECT count(*) FROM engine.k6_write_capture").Scan(&got)
	if err != nil || got != want {
		t.Fatalf("committed capture rows %d, expected %d: %v", got, want, err)
	}
}

func seedCapture(t *testing.T, conn *pgx.Conn, item k6v1.Submission) {
	t.Helper()
	execK6SQL(t, conn, `
		INSERT INTO engine.workflow_definitions(definition_id,version,definition_hash,graph,activity_versions,effect_classes)
			VALUES ('definition',1,'hash','{"nodes":["pure.echo"]}','{"pure.echo":"v1"}','{"pure.echo":"PURE_ACTIVITY"}');
		SET durable.k4_campaign = 'on';
		SET durable.k4_session = 'session';
		SET durable.k4_pod_uid = 'uid';
		SET durable.k4_container_id = 'container';
		SET durable.k6_capture = 'on';
		BEGIN;
		UPDATE engine.partition_leases SET owner_id='`+captureOwnerID+`', epoch=1, lease_expires_at=now()+interval '1 hour' WHERE partition_id=0;
		INSERT INTO engine.k4_audit_queue(kind,partition_id,owner_id,epoch,session_id,pod_uid,container_id)
			VALUES ('acquire',0,'`+captureOwnerID+`',1,'session','uid','container');
		COMMIT;`)
	inTx := func(stmt string, args ...any) {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		if _, err := tx.Exec(ctx, stmt, append([]any{pgx.QueryExecModeSimpleProtocol}, args...)...); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
	inTx(`INSERT INTO engine.workflow_executions
		(workflow_id,namespace,submission_key,submission_payload_hash,definition_id,definition_version,partition_id,state,revision)
		VALUES ($1,'episode',$1,$2,'definition',1,0,'RUNNABLE',1);
		INSERT INTO engine.node_instances(workflow_id,node_id,iteration,state,input)
		VALUES ($1,'pure.echo',0,'RUNNABLE','{}');
		INSERT INTO engine.transition_history(transition_id,workflow_id,revision,actor_kind,actor_id,new_state,reason)
		VALUES (md5('h1')::uuid,$1,1,'client','client','RUNNABLE','WORKFLOW_CREATED');
		INSERT INTO engine.outbox(event_id,workflow_id,aggregate_revision,event_type,payload)
		VALUES (md5('o1')::uuid,$1,1,'workflow.created','{}')`, item.Key, "sub-v1:"+item.BodySHA256)
	const fence = `SET LOCAL durable.k4_owner = '` + captureOwnerID + `';
		SET LOCAL durable.k6_owner_partition = '0';
		SET LOCAL durable.k6_owner_epoch = '1';`
	inTx(fence+`
		UPDATE engine.workflow_executions SET state='WAITING_ACTIVITY',revision=2;
		UPDATE engine.node_instances SET state='WAITING_ACTIVITY',current_attempt_number=1,revision=1;
		INSERT INTO engine.activity_attempts(workflow_id,node_id,iteration,attempt_number,state,effect_class)
		VALUES ($1,'pure.echo',0,1,'DISPATCHABLE','PURE_ACTIVITY');
		INSERT INTO engine.timers(timer_id,workflow_id,node_id,iteration,due_at,purpose,owning_revision)
		VALUES (md5('timer')::uuid,$1,'pure.echo',0,now(),'RETRY_BACKOFF',2);
		INSERT INTO engine.transition_history(transition_id,workflow_id,revision,actor_kind,actor_id,scheduler_epoch,node_id,iteration,attempt_number,old_state,new_state,reason)
		VALUES (md5('h2')::uuid,$1,2,'scheduler','actor',1,'pure.echo',0,1,'RUNNABLE','WAITING_ACTIVITY','ATTEMPT_CREATED');
		INSERT INTO engine.outbox(event_id,workflow_id,aggregate_revision,event_type,topic,payload)
		VALUES (md5('o2')::uuid,$1,2,'attempt.dispatch','durable-agent.tasks.v1','{"node_id":"pure.echo","iteration":0,"attempt_number":1}')`, item.Key)
	inTx(`UPDATE engine.activity_attempts SET state='CLAIMED',claim_token=$1,worker_id='worker',worker_request_id='request'`, claimSecret)
	inTx(`UPDATE engine.activity_attempts SET state='SUCCEEDED',is_current=false,result='5';
		UPDATE engine.workflow_executions SET revision=3;
		INSERT INTO engine.transition_history(transition_id,workflow_id,revision,actor_kind,actor_id,node_id,iteration,attempt_number,old_state,new_state,reason)
		VALUES (md5('h3')::uuid,$1,3,'worker','worker','pure.echo',0,1,'WAITING_ACTIVITY','WAITING_ACTIVITY','ATTEMPT_RESULT_RECORDED');
		INSERT INTO engine.outbox(event_id,workflow_id,aggregate_revision,event_type,payload)
		VALUES (md5('o3')::uuid,$1,3,'activity.result','5');
		INSERT INTO engine.k6_capture_queue(subject,operation,row_key,workflow_id,partition_id,after_image)
		VALUES ('result_request','ACCEPTED',jsonb_build_array($1::text,'pure.echo',0,1),$1,0,
		        jsonb_build_object('token_hash',$2::text,'worker_request_id','request','receipt_id',NULL,'attempt_state','SUCCEEDED'))`,
		item.Key, k6v1.Hash([]byte(claimSecret)))
	inTx(fence+`
		UPDATE engine.workflow_executions SET state='RUNNABLE',revision=4;
		UPDATE engine.node_instances SET state='SUCCEEDED',accepted_result='5',revision=2;
		INSERT INTO engine.transition_history(transition_id,workflow_id,revision,actor_kind,actor_id,scheduler_epoch,node_id,iteration,attempt_number,old_state,new_state,reason)
		VALUES (md5('h4')::uuid,$1,4,'scheduler','actor',1,'pure.echo',0,1,'WAITING_ACTIVITY','RUNNABLE','RESULT_CONSUMED');
		INSERT INTO engine.outbox(event_id,workflow_id,aggregate_revision,event_type,payload)
		VALUES (md5('o4')::uuid,$1,4,'workflow.result_consumed','{}')`, item.Key)
	inTx(fence+`UPDATE engine.workflow_executions SET state='SUCCEEDED',revision=5;
		INSERT INTO engine.transition_history(transition_id,workflow_id,revision,actor_kind,actor_id,scheduler_epoch,old_state,new_state,reason)
		VALUES (md5('h5')::uuid,$1,5,'scheduler','actor',1,'RUNNABLE','SUCCEEDED','WORKFLOW_ADVANCED')`, item.Key)
}
