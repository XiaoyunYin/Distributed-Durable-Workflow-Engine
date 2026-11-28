package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
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
}
