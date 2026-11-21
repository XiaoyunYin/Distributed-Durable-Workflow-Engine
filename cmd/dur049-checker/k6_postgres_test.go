package main

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"durable-agent-execution-engine/internal/invariants/k6live"
	"durable-agent-execution-engine/internal/invariants/k6v1"
	"durable-agent-execution-engine/migrations"
	"github.com/jackc/pgx/v5"
)

func k6TestDatabase(t *testing.T) (*pgx.Conn, string) {
	t.Helper()
	if os.Getenv("DURABLE_RUN_INTEGRATION") != "1" && os.Getenv("DURABLE_REQUIRE_DATABASE") != "1" {
		t.Skip("PostgreSQL integration requires DURABLE_RUN_INTEGRATION=1 or service CI")
	}
	raw := os.Getenv("DURABLE_DATABASE_URL")
	if raw == "" {
		values := map[string]string{}
		for _, line := range strings.Split(string(getFile(t, "../../.env")), "\n") {
			if key, value, ok := strings.Cut(strings.TrimSpace(line), "="); ok {
				values[key] = strings.Trim(value, "\"")
			}
		}
		port := values["POSTGRES_PORT"]
		if port == "" {
			port = "5432"
		}
		raw = (&url.URL{
			Scheme: "postgresql", User: url.UserPassword(values["POSTGRES_USER"], values["POSTGRES_PASSWORD"]),
			Host: "127.0.0.1:" + port, Path: "/" + values["POSTGRES_DB"], RawQuery: "sslmode=disable",
		}).String()
	}
	config, err := pgx.ParseConfig(raw)
	if err != nil {
		t.Fatal("cannot parse integration database config")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	admin, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		t.Fatal("cannot open integration database")
	}
	name := "k6g1_" + strings.ReplaceAll(filepath.Base(t.TempDir()), "-", "_") + "_" + strings.ReplaceAll(time.Now().UTC().Format("150405.000000000"), ".", "_")
	nameSQL := pgx.Identifier{name}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+nameSQL+" TEMPLATE template0"); err != nil {
		admin.Close(ctx)
		t.Fatal("cannot create K6 integration database:", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		defer admin.Close(ctx)
		if _, err := admin.Exec(ctx, "DROP DATABASE "+nameSQL+" WITH (FORCE)"); err != nil {
			t.Error("cannot drop K6 integration database:", err)
		}
	})
	dsn, err := url.Parse(raw)
	if err != nil {
		t.Fatal("could not parse test PostgreSQL URL")
	}
	dsn.Path, dsn.RawPath = "/"+name, ""
	conn, err := pgx.Connect(ctx, dsn.String())
	if err != nil {
		t.Fatal("cannot connect to isolated K6 test database")
	}
	t.Cleanup(func() { conn.Close(context.Background()) })
	entries, err := fs.ReadDir(migrations.SQL, ".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		migration, err := migrations.SQL.ReadFile(entry.Name())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Exec(ctx, string(migration)); err != nil {
			t.Fatalf("%s failed: %v", entry.Name(), err)
		}
	}
	return conn, dsn.String()
}

func execK6SQL(t *testing.T, conn *pgx.Conn, query string, args ...any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := conn.Exec(ctx, query, args...); err != nil {
		t.Fatal(err)
	}
}

const (
	claimSecret  = "deadbeef-0000-0000-0000-000000000022"
	grantSecret  = "deadbeef-0000-0000-0000-000000000023"
	nestedSecret = "K6_SENTINEL_CREDENTIAL_42"
	k6History    = "engine.transition_history"
)

func seedK6Postgres(t *testing.T, conn *pgx.Conn) {
	t.Helper()
	execK6SQL(t, conn, `
		INSERT INTO engine.workflow_definitions(definition_id,version,definition_hash,graph,activity_versions,effect_classes)
		VALUES ('definition',1,'hash','{"nodes":["pure.echo"]}','{"pure.echo":"v1"}','{"pure.echo":"PURE_ACTIVITY"}');
		INSERT INTO engine.workflow_executions(workflow_id,namespace,submission_key,submission_payload_hash,definition_id,definition_version,partition_id,state,revision)
		VALUES ('workflow','episode','submission','hash','definition',1,0,'SUCCEEDED',1001);
		INSERT INTO engine.transition_history(transition_id,workflow_id,revision,actor_kind,actor_id,old_state,new_state,reason)
		SELECT md5(i::text)::uuid,'workflow',i,'scheduler','actor',CASE WHEN i>1 THEN 'RUNNABLE' END,
		       CASE WHEN i=1001 THEN 'SUCCEEDED' ELSE 'RUNNABLE' END, CASE WHEN i=1 THEN 'WORKFLOW_CREATED' ELSE 'WORKFLOW_ADVANCED' END
		FROM generate_series(1,1001) i;
		INSERT INTO engine.node_instances(workflow_id,node_id,iteration,state,input,accepted_result)
		VALUES ('workflow','pure.echo',0,'SUCCEEDED','{"cell":"hook-0"}','{"position":3,"value":"k6"}');
		INSERT INTO engine.activity_attempts(workflow_id,node_id,iteration,attempt_number,state,effect_class,claim_token,worker_id,worker_request_id,k6_proof)
		VALUES ('workflow','pure.echo',0,1,'SUCCEEDED','PURE_ACTIVITY','`+claimSecret+`','worker','request',
			jsonb_build_object('operation','apply','signature',repeat('A',86),'receipt',
				jsonb_build_object('workflow_id','workflow','logical_key','key','resource_id','resource',
					'canonical_hash',repeat('c',64),'scope_hash',repeat('d',64),
					'intent_id','00000000-0000-0000-0000-000000000001','expected_revision',0,
					'applied_revision',1,'application_id','00000000-0000-0000-0000-000000000002')));
		INSERT INTO engine.approval_action_intents(intent_id,workflow_id,node_id,iteration,proposal_hash,target,canonical_arguments,valid_until,grant_token,decision_reason)
		VALUES ('00000000-0000-0000-0000-000000000001','workflow','pure.echo',0,'hash','resource','{"position":3,"value":"k6"}',
			clock_timestamp(),'`+grantSecret+`','`+nestedSecret+`');
		INSERT INTO engine.attempt_result_evidence(evidence_id,workflow_id,node_id,iteration,attempt_number,claim_token,reconciliation_reference,payload,disposition)
		VALUES ('00000000-0000-0000-0000-000000000003','workflow','pure.echo',0,1,'`+claimSecret+`','ref',
			jsonb_build_object('error','`+nestedSecret+`','type','RuntimeError'),'RECORDED_AS_EVIDENCE');
		INSERT INTO engine.k4_audit(kind,at,partition_id,owner_id,epoch,session_id,pod_uid,container_id,transaction_id,claim_identity,finalized_at)
		VALUES ('claim',clock_timestamp(),0,'',0,'session','uid','container','1',
			jsonb_build_object('workflow_id','workflow','node_id','pure.echo','iteration',0,'attempt_number',1,
			'token_hash',encode(sha256(convert_to('`+claimSecret+`','UTF8')),'hex'),'worker_id','worker','worker_request_id','request'),clock_timestamp());
		INSERT INTO engine.reconciliation_items(item_id,kind,reference,detail)
		VALUES ('00000000-0000-0000-0000-000000000004','POISON_RECORD','topic/0/0',jsonb_build_object('reason','`+nestedSecret+`'));
		INSERT INTO source_corpus.documents(document_id,version,source_type,service,title,source_text,corpus_version)
		VALUES ('doc','1','runbook','fixture','fixture','`+nestedSecret+`','1');
	`)
}

func liveK6(t *testing.T, conn *pgx.Conn, dsn string, instrumented bool) (string, []byte, string, int) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "public")
	config := k6live.Config{
		Schema: k6live.ConfigVersion, DatabaseURL: dsn, EpisodeDir: dir, Manifest: k6Manifest(conn.Config().Database, instrumented),
	}
	config.Manifest.Cell, config.Manifest.Episode, config.Manifest.Workload = "medium-12", "medium-12-0", "COOPERATING_EFFECT"
	config.Manifest.Ordinal, config.Manifest.Seed = 90, 530091
	path := filepath.Join(t.TempDir(), "private.json")
	putFile(t, path, canonical(t, config))
	var out, diag bytes.Buffer
	code := k6Mode([]string{k6Live, path}, &out, &diag)
	noK6Secrets(t, append(bytes.Clone(out.Bytes()), diag.Bytes()...))
	return dir, out.Bytes(), diag.String(), code
}

func noK6Secrets(t *testing.T, data []byte) {
	t.Helper()
	for _, secret := range []string{claimSecret, grantSecret, nestedSecret} {
		if bytes.Contains(data, []byte(secret)) {
			t.Fatal("raw credential leaked from K6 publication")
		}
	}
}

func inspectK6Output(t *testing.T, dir string, live []byte, wantCode int) fstest.MapFS {
	t.Helper()
	var offline, diag bytes.Buffer
	if got := k6Mode([]string{k6Offline, dir}, &offline, &diag); got != wantCode || diag.Len() != 0 || !bytes.Equal(live, offline.Bytes()) {
		t.Fatalf("live/offline result differs: offline exit %d, diagnostic %q\nlive %s\noffline %s", got, &diag, live, &offline)
	}
	bundle := fstest.MapFS{}
	root := os.DirFS(dir)
	err := fs.WalkDir(root, ".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		raw, err := fs.ReadFile(root, path)
		if err == nil {
			noK6Secrets(t, raw)
			bundle[path] = &fstest.MapFile{Data: raw}
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return bundle
}

func requireK6Finding(t *testing.T, result k6v1.Result, code string) {
	t.Helper()
	for _, issue := range result.Findings {
		if issue.Code == code {
			return
		}
	}
	t.Fatalf("expected %s, got %+v", code, result.Findings)
}

func TestK6EngineExport(t *testing.T) {
	conn, dsn := k6TestDatabase(t)
	seedK6Postgres(t, conn)
	dir, out, diag, code := liveK6(t, conn, dsn, false)
	if code != k6v1.Incomplete || diag != "" {
		t.Fatalf("clean fixture exit %d: %s %s", code, diag, out)
	}
	bundle := inspectK6Output(t, dir, out, k6v1.Incomplete)
	result := k6v1.Verify(bundle)
	if result.RelationIntegrity != "covered/pass" || len(result.Labels) != 13 ||
		!slices.Equal(result.InvariantsCovered, []string{"I2", "I11"}) || len(result.Findings) != 0 ||
		result.Namespace == nil || result.Namespace.NamespaceExecutions != 1 || result.Namespace.SQLNamespaceExecutions != 1 {
		t.Fatalf("clean fixture: %+v", result)
	}
	for i, label := range result.Labels {
		if label.ID != "I"+strconv.Itoa(i+1) {
			t.Fatalf("unexpected label order: %+v", label)
		}
		if i != 1 && i != 10 && (label.Outcome != "NOT COVERED" || label.Reason != "slice_not_implemented:G1") {
			t.Fatalf("unimplemented label: %+v", label)
		}
	}
	historyPath := k6v1.RelationPath(k6History)
	if got := bytes.Count(bundle[historyPath].Data, []byte{'\n'}); got != 1001 {
		t.Fatalf("got %d history rows", got)
	}
	if len(bundle[k6v1.RelationPath("effects.effect_records")].Data) != 0 {
		t.Fatal("empty effect relation was not exported as empty bytes")
	}
	t.Run("TokenHashing", func(t *testing.T) {
		for _, tc := range []struct{ relation, key, token string }{
			{"engine.activity_attempts", "claim_token_hash", claimSecret},
			{"engine.attempt_result_evidence", "claim_token_hash", claimSecret},
			{"engine.approval_action_intents", "grant_token_hash", grantSecret},
			{"engine.k4_audit", "token_hash", claimSecret},
		} {
			raw := bundle[k6v1.RelationPath(tc.relation)].Data
			if !bytes.Contains(raw, []byte(`"`+tc.key+`":"`+k6v1.Hash([]byte(tc.token))+`"`)) {
				t.Fatalf("token digest missing in %s", tc.relation)
			}
		}
	})

	t.Run("SealedIntegrity", func(t *testing.T) {
		sealed := bundle[k6v1.IndexPath]
		for _, tc := range []struct {
			name, path, finding string
			damage              func([]byte) []byte
		}{
			{"digest", historyPath, "INDEX_DIGEST", func(raw []byte) []byte {
				return bytes.Replace(raw, []byte(`"actor_id":"actor"`), []byte(`"actor_id":"other"`), 1)
			}},
			{"count", k6v1.SnapshotPath, "INDEX_DIGEST", func(raw []byte) []byte { return bytes.Replace(raw, []byte(`"count":1001`), []byte(`"count":1000`), 1) }},
			{"lf", historyPath, "RELATION_FRAMING", func(raw []byte) []byte { return raw[:len(raw)-1] }},
		} {
			t.Run(tc.name, func(t *testing.T) {
				old := bundle[tc.path]
				bundle[tc.path] = &fstest.MapFile{Data: tc.damage(old.Data)}
				defer func() { bundle[tc.path] = old }()
				requireK6Finding(t, k6v1.Verify(bundle), tc.finding)
				if bundle[k6v1.IndexPath] != sealed {
					t.Fatal("changed sealed index")
				}
			})
		}
		for _, spec := range k6v1.EngineRelations(false) {
			path := k6v1.RelationPath(spec.Name)
			old := bundle[path]
			delete(bundle, path)
			requireK6Finding(t, k6v1.Verify(bundle), "required_proof_missing:"+path)
			bundle[path] = old
		}
	})

	t.Run("ResealedMutants", func(t *testing.T) {
		lines := bytes.SplitAfter(bundle[historyPath].Data, []byte{'\n'})
		for _, tc := range []struct {
			name      string
			change    func([][]byte) [][]byte
			countBump int64
			findings  []string
		}{
			{"missing-final", func(raw [][]byte) [][]byte { return raw[:1000] }, 0, []string{"HISTORY_FINAL_REVISION", "TERMINAL_FINAL_UNIQUE"}},
			{"interior-gap", func(raw [][]byte) [][]byte { return slices.Delete(raw, 9, 10) }, 0, []string{"HISTORY_REVISION_GAP"}},
			{"illegal-edge", func(raw [][]byte) [][]byte {
				raw[1] = bytes.Replace(raw[1], []byte(`"new_state":"RUNNABLE"`), []byte(`"new_state":"REJECTED"`), 1)
				return raw
			}, 0, []string{"HISTORY_EDGE"}},
			{"double-terminal", func(raw [][]byte) [][]byte {
				raw[1] = bytes.Replace(raw[1], []byte(`"new_state":"RUNNABLE"`), []byte(`"new_state":"SUCCEEDED"`), 1)
				return raw
			}, 0, []string{"TERMINAL_FINAL_UNIQUE"}},
			{"out-of-order", func(raw [][]byte) [][]byte { raw[9], raw[10] = raw[10], raw[9]; return raw }, 0, []string{"RELATION_ORDER_OR_DUPLICATE"}},
			{"duplicate-key", func(raw [][]byte) [][]byte { raw[9] = raw[8]; return raw }, 0, []string{"RELATION_ORDER_OR_DUPLICATE"}},
			{"bad-sql-count", func(raw [][]byte) [][]byte { return raw }, 1, []string{"RELATION_COUNT"}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				mutant := cloneBundle(bundle)
				raw := bytes.Join(tc.change(slices.Clone(lines)), nil)
				resealK6Relation(t, mutant, k6History, raw, int64(bytes.Count(raw, []byte{'\n'}))+tc.countBump)
				mutant["checker.json"] = &fstest.MapFile{Data: []byte(`{"status":"PASS"}`)}
				verdict := k6v1.Verify(mutant)
				for _, code := range tc.findings {
					requireK6Finding(t, verdict, code)
				}
				if verdict.ExitCode() != k6v1.Fail {
					t.Fatal("resealed mutant accepted")
				}
			})
		}
	})

	t.Run("LiveN1", func(t *testing.T) {
		execK6SQL(t, conn, "DELETE FROM engine.transition_history WHERE revision=1001")
		dir, out, diag, code := liveK6(t, conn, dsn, false)
		if code != k6v1.Fail || diag != "" {
			t.Fatalf("live N1 exit %d: %s", code, diag)
		}
		verdict := k6v1.Verify(inspectK6Output(t, dir, out, code))
		requireK6Finding(t, verdict, "HISTORY_FINAL_REVISION")
		requireK6Finding(t, verdict, "TERMINAL_FINAL_UNIQUE")
		execK6SQL(t, conn, `INSERT INTO engine.transition_history(transition_id,workflow_id,revision,actor_kind,actor_id,old_state,new_state,reason)
			VALUES (md5('1001')::uuid,'workflow',1001,'scheduler','actor','RUNNABLE','SUCCEEDED','WORKFLOW_ADVANCED')`)
	})
	t.Run("LiveIllegalEdge", func(t *testing.T) {
		execK6SQL(t, conn, "UPDATE engine.transition_history SET new_state='REJECTED' WHERE revision=2")
		dir, out, diag, code := liveK6(t, conn, dsn, false)
		execK6SQL(t, conn, "UPDATE engine.transition_history SET new_state='RUNNABLE' WHERE revision=2")
		if code != k6v1.Fail || diag != "" {
			t.Fatalf("live illegal edge exit %d: %s", code, diag)
		}
		verdict := k6v1.Verify(inspectK6Output(t, dir, out, code))
		requireK6Finding(t, verdict, "HISTORY_EDGE")
		if verdict.RelationIntegrity != "covered/pass" || verdict.Labels[1].Outcome != "covered/fail" {
			t.Fatal("illegal edge did not reach the independent history predicate")
		}
	})
	t.Run("NestedSecrets", func(t *testing.T) {
		for _, setter := range []string{
			`k6_proof || jsonb_build_object('credential',$1::text)`,
			`jsonb_set(k6_proof,'{receipt,credential}',to_jsonb($1::text))`,
			`jsonb_set(k6_proof,'{receipt,applied_revision}',jsonb_build_array(jsonb_build_object('bearer',$1::text)))`,
			`jsonb_set(k6_proof,'{receipt,applied_revision}',to_jsonb(jsonb_build_object('bearer',$1::text)::text))`,
		} {
			execK6SQL(t, conn, "CREATE TEMP TABLE restore_proof AS SELECT k6_proof FROM engine.activity_attempts")
			execK6SQL(t, conn, "UPDATE engine.activity_attempts SET k6_proof="+setter, nestedSecret)
			// Temporary tables are census-visible from other sessions. Drop
			// this one before taking the checker snapshot.
			var original string
			if err := conn.QueryRow(context.Background(), "SELECT k6_proof::text FROM restore_proof").Scan(&original); err != nil {
				t.Fatal(err)
			}
			execK6SQL(t, conn, "DROP TABLE restore_proof")
			dir, _, diag, code := liveK6(t, conn, dsn, false)
			if code != k6v1.Fail || !strings.Contains(diag, "K6_ENGINE_PUBLIC_ROW_SCHEMA engine.activity_attempts") {
				t.Fatalf("nested input did not fail at the public row boundary: %d %s", code, diag)
			}
			if err := fs.WalkDir(os.DirFS(dir), ".", func(path string, entry fs.DirEntry, err error) error {
				if err == nil && !entry.IsDir() {
					noK6Secrets(t, getFile(t, filepath.Join(dir, filepath.FromSlash(path))))
				}
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(filepath.Join(dir, k6v1.IndexPath)); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("invalid projection left a sealed bundle")
			}
			execK6SQL(t, conn, "UPDATE engine.activity_attempts SET k6_proof=$1::jsonb", original)
		}
	})
	t.Run("WholeNamespace", func(t *testing.T) {
		execK6SQL(t, conn, `
			INSERT INTO engine.workflow_executions(workflow_id,namespace,submission_key,submission_payload_hash,definition_id,definition_version,partition_id,state,revision)
			VALUES ('foreign','other','submission','hash','definition',1,0,'SUCCEEDED',2);
			INSERT INTO engine.transition_history(transition_id,workflow_id,revision,actor_kind,actor_id,old_state,new_state,reason)
			VALUES (md5('foreign1')::uuid,'foreign',1,'client','actor',NULL,'RUNNABLE','WORKFLOW_CREATED'),
			       (md5('foreign2')::uuid,'foreign',2,'scheduler','actor','RUNNABLE','SUCCEEDED','WORKFLOW_ADVANCED');
			INSERT INTO effects.effect_records(workflow_id,logical_effect_key,argument_hash,attempt_number,outcome)
			VALUES ('orphan','key','hash',1,'OUTCOME_UNKNOWN')`)
		dir, out, diag, code := liveK6(t, conn, dsn, false)
		if code != k6v1.Fail || diag != "" {
			t.Fatalf("whole namespace fixture exit %d: %s", code, diag)
		}
		result := k6v1.Verify(inspectK6Output(t, dir, out, code))
		requireK6Finding(t, result, "FOREIGN_EXECUTION")
		requireK6Finding(t, result, "ORPHAN_WORKFLOW_ROW")
		seen := result.Namespace
		if seen == nil || seen.Executions != 2 || seen.NamespaceExecutions != 1 || seen.SQLNamespaceExecutions != 1 ||
			seen.ForeignExecutions != 1 || seen.OrphanRows != 1 {
			t.Fatalf("whole-namespace counts disagree: %+v", seen)
		}
	})
}

func cloneBundle(bundle fstest.MapFS) fstest.MapFS {
	clone := fstest.MapFS{}
	for path, value := range bundle {
		clone[path] = value
	}
	return clone
}

func resealK6Relation(t *testing.T, bundle fstest.MapFS, relation string, raw []byte, count int64) {
	t.Helper()
	path := k6v1.RelationPath(relation)
	bundle[path] = &fstest.MapFile{Data: raw}
	var snap k6v1.Snapshot
	if err := k6v1.DecodeStrict(bundle[k6v1.SnapshotPath].Data, &snap); err != nil {
		t.Fatal(err)
	}
	for i := range snap.Relations {
		r := &snap.Relations[i]
		if r.Relation == relation {
			r.SHA256, r.Length, r.Count = k6v1.Hash(raw), int64(len(raw)), count
		}
	}
	for i := range snap.Census {
		row := &snap.Census[i]
		if row.Name == relation {
			digest := k6v1.Hash(raw)
			row.Count, row.SHA256 = &count, &digest
		}
	}
	bundle[k6v1.SnapshotPath] = &fstest.MapFile{Data: canonical(t, snap)}
	var index k6v1.Index
	if err := k6v1.DecodeStrict(bundle[k6v1.IndexPath].Data, &index); err != nil {
		t.Fatal(err)
	}
	for i := range index.Files {
		f := &index.Files[i]
		data := bundle[f.Path].Data
		f.SHA256, f.Length = k6v1.Hash(data), int64(len(data))
	}
	bundle[k6v1.IndexPath] = &fstest.MapFile{Data: canonical(t, index)}
}

func TestK6Census(t *testing.T) {
	conn, dsn := k6TestDatabase(t)
	seedK6Postgres(t, conn)
	dir, out, diag, code := liveK6(t, conn, dsn, false)
	if code != k6v1.Incomplete || diag != "" {
		t.Fatalf("K6 census positive rejected: %d %s", code, diag)
	}
	bundle := inspectK6Output(t, dir, out, code)
	var snap k6v1.Snapshot
	if err := k6v1.DecodeStrict(bundle[k6v1.SnapshotPath].Data, &snap); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, item := range snap.Census {
		if item.Name == "source_corpus.documents" {
			found = item.Classification == k6v1.Excluded && item.Reason == k6v1.FixtureCorpus &&
				item.Count != nil && *item.Count == 1 && item.SHA256 != nil && *item.SHA256 != k6v1.Hash(nil)
		}
	}
	if !found {
		t.Fatal("fixture corpus was not classified with its own count and digest")
	}
	execK6SQL(t, conn, "CREATE TABLE public.extra_k6_relation(payload text); INSERT INTO public.extra_k6_relation VALUES ('one')")
	dir, out, diag, code = liveK6(t, conn, dsn, false)
	if code != k6v1.Fail || diag != "" {
		t.Fatalf("unclassified relation accepted: %d %s", code, diag)
	}
	verdict := k6v1.Verify(inspectK6Output(t, dir, out, code))
	requireK6Finding(t, verdict, "RELATION_UNCLASSIFIED")
	if verdict.Findings[0].Source != "public.extra_k6_relation" {
		t.Fatalf("census failed to identify extra relation: %+v", verdict.Findings)
	}
	execK6SQL(t, conn, `DROP TABLE public.extra_k6_relation; CREATE SCHEMA k6_f3;
		CREATE TABLE k6_f3.deadline_writes (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, written_at timestamptz,
			application text, session text, branch text, partition_id int, owner_id text, epoch bigint,
			workflow_id text, node_id text, iteration int, attempt_number bigint, worker_id text, token_hash text,
			row_id text, deadline timestamptz, old_deadline timestamptz);
		INSERT INTO k6_f3.deadline_writes(written_at,application,branch,row_id)
		VALUES (clock_timestamp(),'runtime','renewal','row')`)
	dir, out, _, code = liveK6(t, conn, dsn, false)
	if code != k6v1.Fail {
		t.Fatal("undeclared instrumentation accepted")
	}
	bundle = inspectK6Output(t, dir, out, code)
	requireK6Finding(t, k6v1.Verify(bundle), "RELATION_UNCLASSIFIED")
	if _, found := bundle[k6v1.RelationPath(k6v1.DeadlineWrites)]; found {
		t.Fatal("undeclared instrumentation was exported")
	}
	dir, out, diag, code = liveK6(t, conn, dsn, true)
	if code != k6v1.Incomplete || diag != "" {
		t.Fatalf("declared instrumentation rejected: %d %s", code, diag)
	}
	bundle = inspectK6Output(t, dir, out, code)
	path := k6v1.RelationPath(k6v1.DeadlineWrites)
	if len(bundle[path].Data) == 0 {
		t.Fatal("declared instrumentation row missing")
	}
	delete(bundle, path)
	requireK6Finding(t, k6v1.Verify(bundle), "required_proof_missing:"+path)
}
