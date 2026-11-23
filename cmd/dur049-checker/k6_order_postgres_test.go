package main

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/invariants/k6live"
	"durable-agent-execution-engine/internal/invariants/k6v1"
)

func requireOrderingFinding(t *testing.T, r k6v1.Result, predicate, code string, kind k6v1.FindingKind, relation string, key []any) {
	t.Helper()
	path := relation
	if relation != k6v1.ManifestPath {
		path = k6v1.RelationPath(relation)
	}
	for _, finding := range r.Findings {
		w := finding.Witness
		if finding.Code == code && finding.Kind == kind && finding.Predicate == predicate && finding.Source == path &&
			w != nil && w.Path == path && slices.Equal(w.RowKey, key) && len(w.SHA256) == 64 {
			return
		}
	}
	t.Fatalf("wanted %s %s %s at %s %v, got %+v", kind, predicate, code, path, key, r.Findings)
}

func TestK6SourceOrderingVerdictParity(t *testing.T) {
	engine, dsn := k6TestDatabase(t)
	bindK6Database(t, engine, "engine-instance", "reset-a")
	m := k6Manifest(engine.Config().Database, false)
	m.Cell, m.Episode, m.Ordinal, m.Seed = "hook-5", "hook-5-0", 25, 530026
	cfg := k6live.Config{Schema: k6live.ConfigVersion, DatabaseURL: dsn, Manifest: m}
	plan, arrivals := k6Sources(m, time.Now().Add(-time.Minute), "pure.echo")
	item := plan.Windows[0].Submissions[0]
	execK6SQL(t, engine, `INSERT INTO engine.workflow_definitions(definition_id,version,definition_hash,graph,activity_versions,effect_classes)
		VALUES ('definition',1,'hash','{"nodes":["pure.echo"]}','{"pure.echo":"v1"}','{"pure.echo":"PURE_ACTIVITY"}')`)
	execK6SQL(t, engine, `INSERT INTO engine.workflow_executions
		(workflow_id,namespace,submission_key,submission_payload_hash,definition_id,definition_version,partition_id,state,revision)
		VALUES ($1,'episode',$1,$2,'definition',1,0,'SUCCEEDED',7)`, item.Key, "sub-v1:"+item.BodySHA256)
	execK6SQL(t, engine, `INSERT INTO engine.node_instances(workflow_id,node_id,iteration,state,input,accepted_result)
		VALUES ($1,'pure.echo',0,'SUCCEEDED','{}','5')`, item.Key)
	execK6SQL(t, engine, `INSERT INTO engine.activity_attempts
		(workflow_id,node_id,iteration,attempt_number,state,effect_class,claim_token,worker_id,worker_request_id,is_current,result)
		VALUES ($1,'pure.echo',0,1,'TIMED_OUT','PURE_ACTIVITY',$2,'worker','old-request',false,NULL),
		       ($1,'pure.echo',0,2,'SUCCEEDED','PURE_ACTIVITY',$3,'worker','new-request',false,'5')`,
		item.Key, claimSecret, "deadbeef-0000-0000-0000-000000000024")
	execK6SQL(t, engine, `INSERT INTO engine.transition_history
		(transition_id,workflow_id,revision,actor_kind,actor_id,old_state,new_state,reason,scheduler_epoch,node_id,iteration,attempt_number)
		SELECT ('00000000-0000-0000-0000-00000000000'||v.rev::text)::uuid,$1,v.rev,v.actor,
		       CASE v.actor WHEN 'worker' THEN 'worker' ELSE 'actor' END,v.old,v.new,v.reason,v.epoch,
		       'pure.echo',0,v.attempt
		FROM (VALUES
			(1,'client',NULL,'RUNNABLE','WORKFLOW_CREATED',NULL,NULL),
			(2,'scheduler','RUNNABLE','WAITING_ACTIVITY','ACTIVITY_SCHEDULED',1,NULL),
			(3,'scheduler','WAITING_ACTIVITY','WAITING_ACTIVITY','ATTEMPT_CREATED',1,1),
			(4,'scheduler','WAITING_ACTIVITY','WAITING_ACTIVITY','TIMEOUT_REPLACEMENT',1,1),
			(5,'worker','WAITING_ACTIVITY','WAITING_ACTIVITY','ATTEMPT_RESULT_RECORDED',NULL,2),
			(6,'scheduler','WAITING_ACTIVITY','RUNNABLE','RESULT_CONSUMED',1,2),
			(7,'scheduler','RUNNABLE','SUCCEEDED','WORKFLOW_ADVANCED',1,NULL)
		) v(rev,actor,old,new,reason,epoch,attempt)`, item.Key)
	// The raw audit rows serve as fixture commit order. Wall timestamps all
	// come from the database and are deliberately not used for ordering.
	execK6SQL(t, engine, `INSERT INTO engine.k4_audit
		(event_order,kind,at,partition_id,owner_id,epoch,session_id,pod_uid,container_id,transaction_id,transition_id,finalized_at)
		SELECT (ARRAY[20,30,40,60,80,90,100])[revision::int], 'history',created_at,0,
		       CASE WHEN scheduler_epoch IS NULL THEN '' ELSE 'owner' END,COALESCE(scheduler_epoch,0),
		       'session','uid','container',revision::text,transition_id,clock_timestamp()
		FROM engine.transition_history;
		INSERT INTO engine.k4_audit(event_order,kind,at,partition_id,owner_id,epoch,session_id,pod_uid,container_id,transaction_id,finalized_at)
		VALUES (10,'acquire',now(),0,'owner',1,'session','uid','container','10',now()),
		       (110,'acquire',now(),0,'successor',2,'session','uid','container','110',now());
		INSERT INTO engine.k4_audit
		(event_order,kind,at,partition_id,owner_id,epoch,session_id,pod_uid,container_id,transaction_id,claim_identity,finalized_at)
		SELECT CASE attempt_number WHEN 1 THEN 50 ELSE 70 END,'claim',now(),0,'',0,
		       'session','uid','container',(attempt_number+50)::text,
		       jsonb_build_object('workflow_id',workflow_id,'node_id',node_id,'iteration',iteration,'attempt_number',attempt_number,
		       'token_hash',encode(sha256(convert_to(claim_token::text,'UTF8')),'hex'),'worker_id',worker_id,'worker_request_id',worker_request_id),now()
		FROM engine.activity_attempts;
		ALTER TABLE engine.k4_audit DISABLE TRIGGER k4_audit_no_mutation`)
	verify := func(t *testing.T) k6v1.Result {
		t.Helper()
		cfg.EpisodeDir = filepath.Join(t.TempDir(), "public")
		cfg = writeK6Sources(t, cfg, plan, arrivals)
		path, out, diag, code := invokeLiveK6(t, cfg)
		if code != k6v1.Fail || diag != "" {
			t.Fatalf("live exit %d, diagnostics %s: %s", code, diag, out)
		}
		return verifyVerdictParity(t, path, out, code)
	}
	historyKey := func(revision int) []any {
		return []any{item.Key, json.Number(fmt.Sprint(revision)), fmt.Sprintf("00000000-0000-0000-0000-%012d", revision)}
	}
	t.Run("CleanObservedOrder", func(t *testing.T) {
		r := verify(t)
		if r.Safety == "FAIL" || r.Completeness != "FAIL" || r.RelationIntegrity != "covered/fail" {
			t.Fatalf("ordering baseline: %+v", r.Findings)
		}
		for _, tc := range []struct {
			index, revision int
			name, source    string
		}{
			{1, 1, "AtomicHistory", "node_attempt_write_transactions"},
			{2, 2, "OwnerAtCommit", "owner_write_transactions"},
			{3, 5, "CurrentClaimWinner", "result_commit_identity"},
		} {
			code := "required_proof_missing:" + tc.source
			requireOrderingFinding(t, r, tc.name, code, k6v1.MissingProof, k6History, historyKey(tc.revision))
			if label := r.Labels[tc.index]; label.Outcome != "NOT COVERED" || label.Reason != code {
				t.Fatalf("missing proof label: %+v", label)
			}
		}
	})
	t.Run("N5OldEpoch", func(t *testing.T) {
		execK6SQL(t, engine, "UPDATE engine.k4_audit SET event_order=59 WHERE event_order=110")
		t.Cleanup(func() { execK6SQL(t, engine, "UPDATE engine.k4_audit SET event_order=110 WHERE event_order=59") })
		r := verify(t)
		requireOrderingFinding(t, r, "OwnerAtCommit", "OWNER_NOT_CURRENT_AT_COMMIT", k6v1.Violation, "engine.k4_audit", []any{json.Number("60")})
		if label := r.Labels[2]; label.Outcome != "covered/fail" || label.Reason != "contract_violation" {
			t.Fatalf("OwnerAtCommit label: %+v", label)
		}
	})
	for _, stale := range []string{"N6SupersededToken", "N22FinalCurrentFlags"} {
		t.Run(stale, func(t *testing.T) {
			if stale == "N22FinalCurrentFlags" {
				// Both twins advertise the old attempt as current. The sole
				// fault is still changing the attempt named by the result.
				execK6SQL(t, engine, "UPDATE engine.activity_attempts SET is_current=true WHERE attempt_number=1")
				t.Cleanup(func() {
					execK6SQL(t, engine, "UPDATE engine.activity_attempts SET is_current=false WHERE attempt_number=1")
				})
				if r := verify(t); r.Safety == "FAIL" {
					t.Fatalf("endpoint current flag overrode clean commit order: %+v", r.Findings)
				}
			}
			execK6SQL(t, engine, "UPDATE engine.transition_history SET attempt_number=1 WHERE revision=5")
			t.Cleanup(func() { execK6SQL(t, engine, "UPDATE engine.transition_history SET attempt_number=2 WHERE revision=5") })
			r := verify(t)
			requireOrderingFinding(t, r, "CurrentClaimWinner", "RESULT_SUPERSEDED_AT_COMMIT", k6v1.Violation, k6History, historyKey(5))
			if label := r.Labels[3]; label.Outcome != "covered/fail" || label.Reason != "contract_violation" {
				t.Fatalf("CurrentClaimWinner label: %+v", label)
			}
		})
	}
	t.Run("N6CanceledResult", func(t *testing.T) {
		execK6SQL(t, engine, `UPDATE engine.transition_history
			SET new_state='CANCELED',reason='CANCELED_ALL_ACTIVE_NODES',node_id=NULL,iteration=NULL WHERE revision=7;
			UPDATE engine.workflow_executions SET state='CANCELED'`)
		if r := verify(t); r.Safety == "FAIL" {
			t.Fatalf("result preceding cancellation: %+v", r.Findings)
		}
		execK6SQL(t, engine, "UPDATE engine.k4_audit SET event_order=75 WHERE event_order=100")
		r := verify(t)
		requireOrderingFinding(t, r, "CurrentClaimWinner", "RESULT_AFTER_CANCELLATION", k6v1.Violation, k6History, historyKey(5))
		if label := r.Labels[3]; label.Outcome != "covered/fail" || label.Reason != "contract_violation" {
			t.Fatalf("CurrentClaimWinner label: %+v", label)
		}
	})
}
