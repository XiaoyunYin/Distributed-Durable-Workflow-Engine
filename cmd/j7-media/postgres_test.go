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
	writer := os.Getenv("DURABLE_J7_TEST_MEDIA_BINARY")
	if writer == "" {
		writer = filepath.Join(root, "j7-media")
		if out, err := exec.CommandContext(ctx, "go", "build", "-p", "1", "-o", writer, ".").CombinedOutput(); err != nil {
			t.Fatalf("build archive writer: %v\n%s", err, out)
		}
	} else if !filepath.IsAbs(writer) {
		t.Fatal("the hosted media fixture binary path must be absolute")
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
		testPostgresBoundary(t, ctx, pgBin, archive, filepath.Join(root, "base"), receipt, shellWord)
	})
}

func testPostgresBoundary(t *testing.T, ctx context.Context, pgBin, archive, backupDir string, backup backupReceipt, shellWord func(string) string) {
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
    workflow_id text, node_id text, iteration int, input jsonb, state text, accepted_result jsonb,
    current_attempt_number bigint);
CREATE TABLE engine.activity_attempts (
    workflow_id text, node_id text, iteration int, attempt_number int, is_current bool,
    effect_class text, state text, outcome_disposition text);
INSERT INTO engine.workflow_executions
SELECT fixture->>'id', fixture->>'namespace', fixture->>'submission_key',
       fixture->>'definition_id', 1, fixture->>'submission_payload_hash', 'SUCCEEDED'
FROM jsonb_array_elements(%[1]s) AS fixture;
INSERT INTO engine.node_instances (workflow_id, node_id, iteration, input, state, accepted_result)
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
INSERT INTO engine.node_instances VALUES ('e1', 'effect', 0, '{}', 'SUCCEEDED', '{"receipt":"fixture"}', 1);
INSERT INTO engine.activity_attempts VALUES ('e1', 'effect', 0, 1, true, 'COOPERATING_EFFECT', 'SUCCEEDED', 'KNOWN_SUCCESS');
`); err != nil {
		t.Fatal(err)
	}
	if pending, err := source.sample(ctx, ledgerMarkers(f.plan, f.report), "e1"); err != nil || pending.EffectSHA256 != "" {
		t.Fatalf("current effect attempt must not count as a consumed source result: %+v, %v", pending, err)
	}
	if _, err := source.psql(ctx, "UPDATE engine.activity_attempts SET is_current=false; UPDATE engine.node_instances SET current_attempt_number=NULL WHERE workflow_id='e1';"); err != nil {
		t.Fatal(err)
	}
	sample, err := source.sample(ctx, ledgerMarkers(f.plan, f.report), "e1")
	flush, flushErr := parseLSN(sample.FlushLSN)
	end, endErr := parseLSN(cut.EndLSN)
	if err != nil || flushErr != nil || endErr != nil || flush <= end || flush-end >= walSegmentBytes ||
		!restoreledger.IsSHA256(sample.EffectSHA256) {
		t.Fatalf("pinned PostgreSQL did not expose the unarchived tail: %+v, %v", sample, err)
	}
	t.Run("isolated_archive_replay", func(t *testing.T) {
		testPostgresReplay(t, ctx, source, cut, backup, backupDir, shellWord)
	})
}

func testPostgresReplay(t *testing.T, ctx context.Context, source mediaSource, cut cutReceipt,
	backup backupReceipt, backupDir string, shellWord func(string) string) {
	t.Helper()
	wal, err := verifyReplayMedia(ctx, source, backup, cut, outageReceipt{Inventory: cut.Inventory}, backupDir)
	if err != nil {
		t.Fatalf("verify actual PostgreSQL WAL through C: %v", err)
	}
	root := t.TempDir()
	target := filepath.Join(root, "target")
	if err := copyBaseBackup(backupDir, target, os.Getuid(), os.Getgid()); err != nil {
		t.Fatal(err)
	}
	// This opt-in fixture runs PostgreSQL without Docker. The production
	// helper's allowlist and checked copy have their own platform tests.
	cmd := filepath.Join(root, "restore-segment")
	script := "#!/bin/sh\nset -eu\ncase \"$1\" in\n"
	for _, file := range cut.Inventory {
		script += shellWord(file.Name) + "|\n"
	}
	script = strings.TrimSuffix(script, "|\n") + ")\n" +
		"cp -- " + shellWord(source.archive) + "/\"$1\" \"$2\"\nprintf 'j7-archive restored %s\\n' \"$1\";;\n" +
		"*) printf 'j7-archive absent %s\\n' \"$1\"; exit 1;;\nesac\n"
	script = strings.ReplaceAll(script, "|\n", "|")
	if err := os.WriteFile(cmd, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	quote := func(v string) string {
		return "'" + strings.ReplaceAll(strings.ReplaceAll(v, "\\", "\\\\"), "'", "''") + "'"
	}
	config := strings.Replace(recoveryConf, "unix_socket_directories = '/tmp'", "unix_socket_directories = "+quote(root), 1)
	config = strings.Replace(config, "'/j7/bin/j7-media restore-wal \"%f\" \"%p\"'", quote(shellWord(cmd)+" '%f' '%p'"), 1)
	for name, value := range map[string]string{"postgresql.conf": config, "postgresql.auto.conf": "",
		"pg_hba.conf": "local all j7_fixture trust\n", "recovery.signal": ""} {
		if err := os.WriteFile(filepath.Join(target, name), []byte(value), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	log := filepath.Join(root, "target.log")
	t.Cleanup(func() {
		if _, err := os.Stat(filepath.Join(target, "postmaster.pid")); err != nil {
			return
		}
		stop, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if out, err := exec.CommandContext(stop, filepath.Join(source.pgBin, "pg_ctl"), "-D", target, "-w", "-m", "immediate", "stop").CombinedOutput(); err != nil {
			t.Errorf("stop isolated PostgreSQL fixture: %v\n%s", err, out)
		}
	})
	if raw, err := exec.CommandContext(ctx, filepath.Join(source.pgBin, "pg_ctl"), "-D", target, "-l", log, "-w", "start").CombinedOutput(); err != nil {
		logs, _ := os.ReadFile(log)
		t.Fatalf("start isolated PostgreSQL fixture: %v\n%s\n%s", err, raw, logs)
	}
	service := "host=" + quote(root) + " user=j7_fixture dbname=postgres"
	raw, err := exec.CommandContext(ctx, filepath.Join(source.pgBin, "psql"), "--no-psqlrc",
		"--no-password", "--tuples-only", "--no-align", "--set=ON_ERROR_STOP=1", "--dbname="+service,
		"--command=SELECT pg_last_wal_replay_lsn(), (SELECT timeline_id FROM pg_control_checkpoint()), pg_is_in_recovery(), (SELECT count(*) FROM engine.workflow_executions);").CombinedOutput()
	if err != nil {
		t.Fatalf("observe restored fixture: %v\n%s", err, raw)
	}
	fields := strings.Split(strings.TrimSpace(string(raw)), "|")
	if len(fields) != 4 || fields[1] != "2" || fields[2] != "f" || fields[3] != "12" {
		t.Fatalf("replayed unexpected population or timeline: %q", raw)
	}
	replayed, replayErr := parseLSN(fields[0])
	end, endErr := parseLSN(wal.End)
	if replayErr != nil || endErr != nil || replayed != end {
		t.Fatalf("padded replay LSN %s differs from archive end %s", fields[0], wal.End)
	}
	logs, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkReplaySegments(string(logs), cut, wal); err != nil {
		t.Fatalf("recovery process did not witness sealed archive: %v\n%s", err, logs)
	}
	history, err := os.ReadFile(filepath.Join(target, "pg_wal", "00000002.history"))
	if err != nil {
		t.Fatal(err)
	}
	if err := matchTimelineHistory(string(history), backup.Source.Timeline, end); err != nil {
		t.Fatal(err)
	}
}
