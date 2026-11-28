package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"durable-agent-execution-engine/internal/restoreledger"
)

func TestPostgres18Media(t *testing.T) {
	pgBin := os.Getenv("DURABLE_J7_TEST_PG_BIN")
	if pgBin == "" {
		t.Skip("set DURABLE_J7_TEST_PG_BIN to the pinned PostgreSQL 18.6 binaries")
	}
	if runtime.GOOS != "linux" || !filepath.IsAbs(pgBin) {
		t.Fatal("J7 PostgreSQL fixture requires Linux and an absolute tool path")
	}
	root := t.TempDir()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	data, archive := filepath.Join(root, "data"), filepath.Join(root, "wal")
	writer := filepath.Join(root, "j7-media")
	if out, err := exec.CommandContext(ctx, "go", "build", "-p", "1", "-o", writer, ".").CombinedOutput(); err != nil {
		t.Fatalf("build archive writer: %v\n%s", err, out)
	}
	pg := func(name string, args ...string) {
		t.Helper()
		out, err := exec.CommandContext(ctx, filepath.Join(pgBin, name), args...).CombinedOutput()
		if err != nil {
			t.Fatalf("%s: %v\n%s", name, err, out)
		}
	}
	pg("initdb", "--auth=trust", "--username=j7_fixture", "--no-locale", "--encoding=UTF8",
		"--wal-segsize=16", "--pgdata="+data)
	if err := os.Mkdir(archive, 0o700); err != nil {
		t.Fatal(err)
	}
	shellWord := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'" }
	gucString := func(s string) string {
		return "'" + strings.ReplaceAll(strings.ReplaceAll(s, "\\", "\\\\"), "'", "''") + "'"
	}
	archiveCommand := strings.Join([]string{
		shellWord(writer), "archive", shellWord(archive), "'%p'", "'%f'",
	}, " ")
	settings := fmt.Sprintf(`
listen_addresses = ''
unix_socket_directories = %s
port = 55432
wal_level = replica
archive_mode = on
archive_timeout = 0
archive_command = %s
fsync = on
synchronous_commit = on
full_page_writes = on
`, gucString(root), gucString(archiveCommand))
	config, err := os.OpenFile(filepath.Join(data, "postgresql.conf"), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := config.WriteString(settings)
	if closeErr := config.Close(); writeErr != nil || closeErr != nil {
		t.Fatalf("write fixture settings: %v %v", writeErr, closeErr)
	}
	log := filepath.Join(root, "postgres.log")
	t.Cleanup(func() {
		if _, err := os.Stat(filepath.Join(data, "postmaster.pid")); err != nil {
			return
		}
		stopCtx, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		out, err := exec.CommandContext(stopCtx, filepath.Join(pgBin, "pg_ctl"), "-D", data,
			"-w", "-m", "immediate", "stop").CombinedOutput()
		if err != nil {
			t.Errorf("stop fixture PostgreSQL: %v\n%s", err, out)
		}
	})
	pg("pg_ctl", "-D", data, "-l", log, "-w", "start")
	service := filepath.Join(root, "pg_service.conf")
	format := "[%s]\nhost=%s\nport=55432\ndbname=postgres\nuser=%s\n"
	dataSource := fmt.Sprintf(format, "fixture-source", root, "j7_fixture")
	backupSource := fmt.Sprintf(format, "fixture-backup", root, "j7_backup")
	if err := os.WriteFile(service, []byte(dataSource+backupSource), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PGSERVICEFILE", service)
	pg("psql", "--no-psqlrc", "--set=ON_ERROR_STOP=1", "--dbname=service=fixture-source",
		"--command=CREATE ROLE j7_backup LOGIN REPLICATION")
	receipt, err := prepareBackup(ctx, prepareInput{
		pgBin: pgBin, sourceService: "fixture-source", backupService: "fixture-backup",
		backup: filepath.Join(root, "base"), archive: archive, drill: "integration",
	}, execTool, syncDirectory)
	if err != nil {
		postgresLog, _ := os.ReadFile(log)
		t.Fatalf("prepare physical backup with archived WAL: %v\n%s", err, postgresLog)
	}
	if len(receipt.Anchors) != 2 || len(receipt.Archive) < 2 ||
		receipt.Anchors[0].Segment == receipt.Anchors[1].Segment || receipt.VerifyBackupText == "" {
		t.Fatalf("incomplete real media evidence: %+v", receipt)
	}
	t.Run("final_archive_cut", func(t *testing.T) {
		testPostgresBoundary(t, ctx, pgBin, archive, receipt)
	})
}

func testPostgresBoundary(t *testing.T, ctx context.Context, pgBin, archive string, backup backupReceipt) {
	t.Helper()
	f := fixtureForFault(t, 12)
	source := f.source
	source.pgBin, source.archive, source.service = pgBin, archive, "fixture-source"
	source.run, source.syncDir = execTool, syncDirectory
	markers, err := json.Marshal(ledgerMarkers(f.plan, f.report))
	if err != nil {
		t.Fatal(err)
	}
	setup := fmt.Sprintf(`
CREATE SCHEMA engine;
CREATE TABLE engine.workflow_executions (
    workflow_id text PRIMARY KEY, namespace text, submission_key text,
    definition_id text, definition_version int, submission_payload_hash text, state text);
CREATE TABLE engine.node_instances (
    workflow_id text, node_id text, iteration int, input jsonb, state text, accepted_result jsonb);
CREATE TABLE engine.activity_attempts (
    workflow_id text, node_id text, iteration int, attempt_number int, is_current bool,
    effect_class text, state text, outcome_disposition text);
INSERT INTO engine.workflow_executions
SELECT fixture->>'id', fixture->>'namespace', fixture->>'submission_key',
       fixture->>'definition_id', 1, fixture->>'submission_payload_hash', 'SUCCEEDED'
FROM jsonb_array_elements(%[1]s) AS fixture;
INSERT INTO engine.node_instances
SELECT fixture->>'id', 'pure.echo', 0, fixture->'input', 'SUCCEEDED', fixture->'input'
FROM jsonb_array_elements(%[1]s) AS fixture;`, sqlJSON(markers))
	if _, err := source.psql(ctx, setup); err != nil {
		t.Fatalf("seed pinned PostgreSQL readback fixture: %v", err)
	}
	cut, err := closeBoundary(ctx, source, backup, testDigest, f.plan, f.report, testOrigin)
	if err != nil || len(cut.Inventory) < 3 || cut.Point.Segment == backup.Anchors[1].Segment ||
		cut.EndLSN == "" || len(cut.PreSwitch.Markers) != 12 {
		t.Fatalf("witness pinned PostgreSQL archive cut: %+v, %v", cut, err)
	}
	if _, err := source.psql(ctx, `
INSERT INTO engine.workflow_executions VALUES ('e1', 'j7', 'e1', 'effect', 1, 'test', 'SUCCEEDED');
INSERT INTO engine.node_instances VALUES ('e1', 'effect', 0, '{}', 'SUCCEEDED', '{"receipt":"fixture"}');
INSERT INTO engine.activity_attempts VALUES ('e1', 'effect', 0, 1, true, 'COOPERATING_EFFECT', 'SUCCEEDED', 'KNOWN_SUCCESS');
`); err != nil {
		t.Fatal(err)
	}
	sample, err := source.sample(ctx, ledgerMarkers(f.plan, f.report), "e1")
	flush, flushErr := parseLSN(sample.FlushLSN)
	end, endErr := parseLSN(cut.EndLSN)
	if err != nil || flushErr != nil || endErr != nil || flush <= end || flush-end >= walSegmentBytes ||
		!restoreledger.IsSHA256(sample.EffectSHA256) {
		t.Fatalf("pinned PostgreSQL did not expose the unarchived tail: %+v, %v", sample, err)
	}
}
