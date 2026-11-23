package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/invariants/k6collect"
	"durable-agent-execution-engine/internal/invariants/k6live"
	"durable-agent-execution-engine/internal/invariants/k6v1"
	"github.com/jackc/pgx/v5"
)

const testTaskTopic = "durable-agent.tasks.v1"

// These are real live database exports. Each named case alters one visible
// obligation, mark, disposition, result or broker observation from its twin.
func TestK6SourceTransportParity(t *testing.T) {
	if os.Getenv("DURABLE_K6_VERDICT_ARTIFACT") == "" {
		t.Skip("requires the hosted PostgreSQL verdict artifact and network-isolated replay step")
	}
	id := func(n int) string { return fmt.Sprintf("00000000-0000-0000-0000-%012d", n) }
	for _, tc := range []struct {
		name, predicate, code, relation string
		index, witness                  int
		change                          string
	}{
		{name: "OutsideCeiling"},
		{"N7RetryEvent", "DispatchDisposition", "DISPATCH_MULTIPLE_EVENTS", "engine.outbox", 5, 23, `
			INSERT INTO engine.outbox(event_id,workflow_id,aggregate_revision,event_type,topic,payload,relay_attempts)
			SELECT '00000000-0000-0000-0000-000000000023',workflow_id,aggregate_revision,event_type,topic,payload,relay_attempts
			FROM engine.outbox WHERE aggregate_revision=3;
			UPDATE engine.outbox_publications SET event_id='00000000-0000-0000-0000-000000000023'
			WHERE relay_attempt=1 AND NOT broker_acknowledged`},
		{"N7MissingOutbox", "DispatchDisposition", "DISPATCH_OUTBOX_MISSING", k6History, 5, 4,
			"DELETE FROM engine.outbox WHERE aggregate_revision=4"},
		{"N7MissingDisposition", "DispatchDisposition", "DISPATCH_DISPOSITION_MISSING", "engine.outbox", 5, 13,
			"DELETE FROM engine.event_inbox WHERE consumer_id='runtime-workers-v1'"},
		{"N7ExtraDisposition", "DispatchDisposition", "DISPATCH_INBOX_UNMATCHED", "engine.event_inbox", 5, 13, `
			INSERT INTO engine.event_inbox(consumer_id,event_id,disposition,topic,kafka_partition,kafka_offset)
			SELECT 'unregistered-consumer',event_id,disposition,topic,kafka_partition,kafka_offset
			FROM engine.event_inbox WHERE consumer_id='runtime-workers-v1'`},
		{"N21ResultReplay", "LostResultReplay", "RESULT_REAPPLIED", k6History, 4, 5, `
			UPDATE engine.transition_history SET actor_kind='worker', actor_id='worker', scheduler_epoch=NULL,
				reason='ATTEMPT_RESULT_RECORDED' WHERE revision=5`},
		{"N21UnresolvedReapply", "UnknownEffectVisible", "UNRESOLVED_EFFECT_REAPPLIED", k6History, 8, 5, `
			UPDATE engine.transition_history SET actor_kind='worker', actor_id='worker', scheduler_epoch=NULL,
				reason='ATTEMPT_RESULT_RECORDED' WHERE revision=5`},
		{"N8LowerGap", "LowerOffsetsDisposed", "OFFSET_BEYOND_DURABLE_DISPOSITION", k6v1.BrokerPath, 6, 1, ""},
		{"N21ShutdownFailure", "ShutdownDisposition", "SHUTDOWN_RECORDED_AS_FAILURE", k6History, 12, 4,
			"UPDATE engine.activity_attempts SET state='FAILED_RETRYABLE'"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			engine, dsn := k6TestDatabase(t)
			bindK6Database(t, engine, "engine-instance", "reset-a")
			m := k6Manifest(engine.Config().Database, false)
			if tc.name == "N8LowerGap" {
				m.Cell, m.Episode, m.Ordinal, m.Seed = "hook-3", "hook-3-0", 15, 530016
			}
			plan, arrivals := k6Sources(m, time.Now().Add(-time.Minute), "pure.echo")
			item := plan.Windows[0].Submissions[0]
			seedTransportParity(t, engine, item)
			if tc.name == "N21UnresolvedReapply" {
				seedTransportUncertainty(t, engine)
			}
			if tc.change != "" {
				execK6SQL(t, engine, tc.change)
			}
			// Keep the audit consistently bound when the fault changes the
			// actor of the underlying state-changing result write.
			execK6SQL(t, engine, `INSERT INTO engine.k4_audit
				(event_order,kind,at,partition_id,owner_id,epoch,session_id,pod_uid,container_id,transaction_id,transition_id,finalized_at)
				SELECT 10*revision,'history',created_at,0,CASE WHEN scheduler_epoch IS NULL THEN '' ELSE 'owner' END,
				       COALESCE(scheduler_epoch,0),'session','uid','container',revision::text,transition_id,clock_timestamp()
				FROM engine.transition_history;
				INSERT INTO engine.k4_audit
				(event_order,kind,at,partition_id,owner_id,epoch,session_id,pod_uid,container_id,transaction_id,claim_identity,finalized_at)
				SELECT 35,'claim',now(),0,'',0,'session','uid','container','claim',
				       jsonb_build_object('workflow_id',workflow_id,'node_id',node_id,'iteration',iteration,'attempt_number',attempt_number,
				      'token_hash',encode(sha256(convert_to(claim_token::text,'UTF8')),'hex'),'worker_id',worker_id,'worker_request_id',worker_request_id),now()
				FROM engine.activity_attempts WHERE attempt_number=1`)
			cfg := k6live.Config{Schema: k6live.ConfigVersion, DatabaseURL: dsn, EpisodeDir: filepath.Join(t.TempDir(), "public")}
			if tc.name == "N8LowerGap" || tc.name == "N21ShutdownFailure" {
				sources := parityExternal(t, &m, false, item.Key)
				transportParityObservations(t, &sources, tc.name)
				cfg.ExternalDir = t.TempDir()
				cfg.Manifest = m
				publishFixtureFrames(t, cfg, sources)
			}
			cfg.Manifest = m
			cfg = writeK6Sources(t, cfg, plan, arrivals)
			path, stdout, diag, code := invokeLiveK6(t, cfg)
			if code != k6v1.Fail || diag != "" {
				t.Fatalf("live exit %d, diagnostics %s: %s", code, diag, stdout)
			}
			r := verifyVerdictParity(t, path, stdout, code)
			if tc.name == "OutsideCeiling" {
				if r.Safety == "FAIL" {
					t.Fatalf("well-formed rows failed safety: %+v", r.Findings)
				}
				for _, idx := range []int{4, 6, 8, 12} {
					requireTransportCeiling(t, r.Labels[idx])
				}
				return
			}
			key := []any{id(tc.witness)}
			switch tc.relation {
			case k6History:
				key = []any{item.Key, json.Number(fmt.Sprint(tc.witness)), id(tc.witness)}
			case "engine.event_inbox":
				key = []any{"unregistered-consumer", id(tc.witness)}
			case k6v1.BrokerPath:
				key = []any{int64(3), "tasks-id", int64(0), int64(1)}
			}
			requireOrderingFinding(t, r, tc.predicate, tc.code, k6v1.Violation, tc.relation, key)
			label := r.Labels[tc.index]
			if r.Safety != "FAIL" || !slices.ContainsFunc(label.Witnesses, func(w k6v1.Witness) bool {
				return slices.Equal(w.RowKey, key)
			}) {
				t.Fatalf("missing linked safety violation: %+v", label)
			}
			if tc.index == 5 || tc.index == 6 {
				if label.Outcome != "covered/fail" || label.Reason != "contract_violation" {
					t.Fatalf("in-ceiling label failed to record violation: %+v", label)
				}
			} else {
				requireTransportCeiling(t, label)
			}
		})
	}
}

func requireTransportCeiling(t *testing.T, label k6v1.Label) {
	t.Helper()
	if label.Registered || label.Outcome != "NOT COVERED" || label.Reason != "outside_registry_ceiling" {
		t.Fatalf("registry ceiling changed: %+v", label)
	}
}

func seedTransportParity(t *testing.T, db *pgx.Conn, item k6v1.Submission) {
	t.Helper()
	execK6SQL(t, db, `INSERT INTO engine.workflow_definitions(definition_id,version,definition_hash,graph,activity_versions,effect_classes)
		VALUES ('definition',1,'hash','{"nodes":["pure.echo"]}','{"pure.echo":"v1"}','{"pure.echo":"PURE_ACTIVITY"}')`)
	minimalExecution(t, db, item)
	execK6SQL(t, db, `UPDATE engine.workflow_executions SET revision=6;
		DELETE FROM engine.transition_history`)
	execK6SQL(t, db, `INSERT INTO engine.node_instances(workflow_id,node_id,iteration,state,input,accepted_result)
		VALUES ($1,'pure.echo',0,'SUCCEEDED','{}','5')`, item.Key)
	execK6SQL(t, db, `INSERT INTO engine.activity_attempts
		(workflow_id,node_id,iteration,attempt_number,state,effect_class,claim_token,worker_id,worker_request_id,result)
		VALUES ($1,'pure.echo',0,1,'SUCCEEDED','PURE_ACTIVITY',$2,'worker','request','5')`, item.Key, claimSecret)
	execK6SQL(t, db, `INSERT INTO engine.transition_history
		(transition_id,workflow_id,revision,actor_kind,actor_id,scheduler_epoch,old_state,new_state,reason,node_id,iteration,attempt_number)
		SELECT ('00000000-0000-0000-0000-00000000000'||rev)::uuid,$1,rev,actor,
		       CASE WHEN actor='worker' THEN 'worker' ELSE 'actor' END,epoch,old,new,reason,
		       CASE WHEN attempt IS NOT NULL THEN 'pure.echo' END,CASE WHEN attempt IS NOT NULL THEN 0 END,attempt
		FROM (VALUES (1,'client',NULL,NULL,'RUNNABLE','WORKFLOW_CREATED',NULL),
		             (2,'scheduler',1,'RUNNABLE','WAITING_ACTIVITY','ACTIVITY_SCHEDULED',NULL),
		             (3,'scheduler',1,'WAITING_ACTIVITY','WAITING_ACTIVITY','ATTEMPT_CREATED',1),
		             (4,'worker',NULL,'WAITING_ACTIVITY','WAITING_ACTIVITY','ATTEMPT_RESULT_RECORDED',1),
		             (5,'scheduler',1,'WAITING_ACTIVITY','WAITING_ACTIVITY','ACTIVITY_WAITING',1),
		             (6,'scheduler',1,'WAITING_ACTIVITY','FAILED','WORKFLOW_ADVANCED',NULL)) v(rev,actor,epoch,old,new,reason,attempt)`, item.Key)
	execK6SQL(t, db, `UPDATE engine.workflow_executions SET state='FAILED';
		INSERT INTO engine.outbox(event_id,workflow_id,aggregate_revision,event_type,topic,payload,publish_state,relay_attempts,published_at)
		SELECT ('00000000-0000-0000-0000-00000000001'||rev)::uuid,workflow_id,rev,typ,topic,payload::jsonb,v.state,attempts,
		       CASE WHEN v.state='PUBLISHED' THEN clock_timestamp() END
		FROM engine.workflow_executions CROSS JOIN (VALUES
		  (1,'workflow.created','durable-agent.events.v1','{}','PUBLISHED',1),
		  (3,'attempt.dispatch','durable-agent.tasks.v1','{"node_id":"pure.echo","iteration":0,"attempt_number":1}','PUBLISHED',2),
		  (4,'activity.result','durable-agent.events.v1','{}','PENDING',0)) v(rev,typ,topic,payload,state,attempts);
		INSERT INTO engine.outbox_publications(publication_id,event_id,relay_owner,relay_attempt,broker_acknowledged)
		SELECT md5(event_id::text||n)::uuid,event_id,'00000000-0000-0000-0000-000000000099',n,n=relay_attempts
		FROM engine.outbox CROSS JOIN LATERAL generate_series(1,relay_attempts) n;
		INSERT INTO engine.event_inbox(consumer_id,event_id,disposition,topic,kafka_partition,kafka_offset)
		SELECT CASE WHEN aggregate_revision=3 THEN 'runtime-workers-v1' ELSE 'runtime-schedulers-v1' END,
		       event_id,'ACCEPTED',topic,0,0 FROM engine.outbox WHERE publish_state='PUBLISHED';
		INSERT INTO engine.scheduler_wakeups(wakeup_id,workflow_id,partition_id,event_id,reason,state,consumed_at)
		SELECT md5('wake')::uuid,workflow_id,0,event_id,'EVENT:workflow.created','CONSUMED',now()
		FROM engine.outbox WHERE aggregate_revision=1;
		INSERT INTO engine.consumer_offsets(consumer_id,topic,kafka_partition,next_offset)
		SELECT consumer_id,topic,0,1 FROM engine.event_inbox`)
}

func seedTransportUncertainty(t *testing.T, db *pgx.Conn) {
	t.Helper()
	execK6SQL(t, db, `UPDATE engine.activity_attempts SET effect_class='NON_COOPERATING_EFFECT',state='TIMED_OUT',
		outcome_disposition='OUTCOME_UNKNOWN',is_current=false;
		UPDATE engine.transition_history SET new_state='RECONCILIATION_REQUIRED',reason='TIMEOUT_RECONCILIATION_REQUIRED',
			actor_kind='scheduler',actor_id='actor',scheduler_epoch=1 WHERE revision=4;
		UPDATE engine.transition_history SET old_state='RECONCILIATION_REQUIRED',new_state='RECONCILIATION_REQUIRED',attempt_number=2 WHERE revision=5;
		UPDATE engine.transition_history SET old_state='RECONCILIATION_REQUIRED',new_state='ABANDONED' WHERE revision=6;
		UPDATE engine.workflow_executions SET state='ABANDONED';
		UPDATE engine.outbox SET event_type='reconciliation.required' WHERE aggregate_revision=4;
		INSERT INTO engine.activity_attempts(workflow_id,node_id,iteration,attempt_number,state,effect_class,result)
		SELECT workflow_id,node_id,iteration,2,'SUCCEEDED','NON_COOPERATING_EFFECT','5' FROM engine.activity_attempts WHERE attempt_number=1;
		INSERT INTO engine.reconciliation_items(item_id,workflow_id,partition_id,node_id,iteration,attempt_number,kind,reference)
		SELECT md5('expired')::uuid,workflow_id,0,node_id,iteration,1,'EXPIRED_ATTEMPT','attempt/'||workflow_id||'/pure.echo/0/1'
		FROM engine.activity_attempts WHERE attempt_number=1`)
}

func transportParityObservations(t *testing.T, sources *k6v1.External, variant string) {
	t.Helper()
	at := time.Now().Add(-time.Second)
	if variant == "N8LowerGap" {
		stream := sources.Broker
		row := stream.Rows[0].Data
		row.Group = "runtime-workers-v1"
		row.Partitions = []k6v1.BrokerPartition{{Topic: testTaskTopic, TopicID: "tasks-id"}}
		stream.Rows[0].Data = row
		for i, committed := range []int64{1, 2} {
			next := row
			instant := at.Add(time.Duration(i) * time.Millisecond)
			next.BeforeUTC, next.AfterUTC = k6v1.UTC(instant), k6v1.UTC(instant)
			next.Partitions = []k6v1.BrokerPartition{{Topic: testTaskTopic, TopicID: "tasks-id", End: 2, Committed: &committed}}
			k6collect.Append(stream, instant, next)
		}
		sealFixtureStream(t, stream, time.Now())
		return
	}
	for id, ledger := range sources.Sessions {
		ledger.Rows[len(ledger.Rows)-1].Data.Kind = "abort"
		sealFixtureStream(t, &ledger, time.Now())
		session := ledger.Rows[0].Data.Session
		k6collect.Append(&sources.Roster, at, k6v1.WatchRow{Kind: "death", From: "2", Cursor: "3", Session: session,
			Death: &k6v1.Death{Observer: sources.Roster.Header.Source, ObservedUTC: k6v1.UTC(at), Receiver: ledger.Header.Source,
				FinalSequence: ledger.Seal.Last, FinalSHA256: ledger.Seal.SHA256}})
		sources.Sessions[id] = ledger
	}
	sealFixtureStream(t, &sources.Roster, time.Now())
}
