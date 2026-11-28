package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"durable-agent-execution-engine/internal/restoreledger"
)

const (
	targetVolumeDir = "/var/lib/postgresql"
	targetData      = targetVolumeDir + "/j7data"
	targetBackup    = "/j7/base"
	targetBinary    = "/j7/bin/j7-media"
	targetManifest  = "/j7/replay.json"
	targetSocket    = "/tmp"
	observerRole    = "j7_observer"
)

type replayManifest struct {
	Files    []archivedFile `json:"files"`
	Timeline uint32         `json:"source_timeline"`
	Database string         `json:"database"`
	Backup   string         `json:"backup_manifest_sha256"`
}

type walProof struct {
	Start       string `json:"start_lsn"`
	End         string `json:"padded_end_lsn"`
	FinalRecord string `json:"final_switch_record_lsn"`
	NextSegment string `json:"expected_absent_segment"`
	Dump        string `json:"pg_waldump_output"`
	Verify      string `json:"pg_verifybackup_output"`
}

var walDumpRow = regexp.MustCompile(`(?m)^rmgr: +([[:alnum:]_]+)[^\n]*?lsn: +([[:xdigit:]]+/[[:xdigit:]]+),[^\n]*?desc: +([^\r\n]+)`)

func verifyReplayMedia(ctx context.Context, s mediaSource, backup backupReceipt, c cutReceipt,
	loss outageReceipt, backupDir string) (walProof, error) {
	var proof walProof
	index, end, err := cutGeometry(c.Point, backup.Source.Timeline, c.PreSwitch.FlushLSN)
	if err != nil {
		return proof, err
	}
	gotEnd, err := parseLSN(c.EndLSN)
	if err != nil || gotEnd != end {
		return proof, errors.New("archive has an invalid exclusive segment end")
	}
	files, err := inventory(s.archive, backup, index)
	if err != nil {
		return proof, err
	}
	if !slices.Equal(files, c.Inventory) || !slices.Equal(files, loss.Inventory) {
		return proof, errors.New("archive bytes do not match the sealed cut and loss inventories")
	}
	_, digest, size, err := inspectBackup(backupDir, backup.Source)
	if err != nil {
		return proof, err
	}
	if digest != backup.ManifestSHA256 || size != backup.BackupBytes {
		return proof, errors.New("base backup bytes or manifest differ from the prepared backup")
	}
	for _, name := range []string{"pg_verifybackup", "pg_waldump"} {
		out, err := s.run(ctx, filepath.Join(s.pgBin, name), "--version")
		fields := strings.Fields(string(out))
		if err != nil || len(fields) < 3 || fields[0] != name || fields[1] != "(PostgreSQL)" || fields[2] != postgresVersion {
			return proof, fmt.Errorf("%s must be PostgreSQL %s", name, postgresVersion)
		}
	}
	checked, err := s.run(ctx, filepath.Join(s.pgBin, "pg_verifybackup"),
		"--format=plain", "--wal-directory="+s.archive, backupDir)
	proof.Verify = string(checked)
	if err != nil {
		return proof, fmt.Errorf("reverify base backup with sealed archive: %w", err)
	}
	proof.Start, proof.End = backup.RequiredWAL[0].Start, c.EndLSN
	proof.NextSegment = segmentName(backup.Source.Timeline, index+1)
	// The quiet pass reads all record CRCs through the exclusive end. The
	// filtered pass exposes the terminal SWITCH and the named restore point.
	args := []string{"--path=" + s.archive, "--timeline=" + fmt.Sprint(backup.Source.Timeline),
		"--start=" + proof.Start, "--end=" + proof.End}
	if _, err := s.run(ctx, filepath.Join(s.pgBin, "pg_waldump"), append(slices.Clone(args), "--quiet")...); err != nil {
		return proof, fmt.Errorf("decode contiguous WAL through C: %w", err)
	}
	dump, err := s.run(ctx, filepath.Join(s.pgBin, "pg_waldump"), append(args, "--rmgr=XLOG")...)
	proof.Dump = string(dump)
	if err != nil {
		return proof, fmt.Errorf("extract final WAL switch: %w", err)
	}
	proof.FinalRecord, err = checkedFinalRecord(proof.Dump, c.Point, end)
	return proof, err
}

func checkedFinalRecord(dump string, point anchor, end uint64) (string, error) {
	rows := walDumpRow.FindAllStringSubmatch(dump, -1)
	if len(rows) < 2 {
		return "", errors.New("pg_waldump did not expose the named point and terminal switch")
	}
	var named int
	pointLSN, pointErr := parseLSN(point.LSN)
	switchLSN, switchErr := parseLSN(point.SwitchLSN)
	for _, row := range rows {
		pos, err := parseLSN(row[2])
		if err == nil && row[1] == "XLOG" && strings.TrimSpace(row[3]) == "RESTORE_POINT "+point.Name &&
			pos < pointLSN && pos/walSegmentBytes == (end-1)/walSegmentBytes {
			named++
		}
	}
	last := rows[len(rows)-1]
	start, err := parseLSN(last[2])
	if named != 1 || pointErr != nil || switchErr != nil || err != nil || last[1] != "XLOG" ||
		strings.TrimSpace(last[3]) != "SWITCH" || start < pointLSN || start >= switchLSN ||
		start/walSegmentBytes != (end-1)/walSegmentBytes {
		return "", errors.New("WAL does not end at the witnessed restore-point segment switch")
	}
	return last[2], nil
}

var errWALAbsent = errors.New("WAL is outside the sealed archive")

func replayWAL(archive, name, dest string, manifest replayManifest) error {
	if _, err := archiveSizeLimit(name); err != nil {
		return err
	}
	wantDestination := "pg_wal/RECOVERYXLOG"
	if strings.HasSuffix(name, ".history") {
		wantDestination = "pg_wal/RECOVERYHISTORY"
	}
	if dest != wantDestination {
		return errors.New("unexpected PostgreSQL restore destination")
	}
	pos := slices.IndexFunc(manifest.Files, func(f archivedFile) bool { return f.Name == name })
	if pos < 0 {
		return errWALAbsent
	}
	source, err := openRegular(filepath.Join(archive, name), os.O_RDONLY)
	if err != nil {
		return err
	}
	defer source.Close()
	temporary, err := os.CreateTemp(filepath.Dir(dest), ".j7-replay-")
	if err != nil {
		return err
	}
	defer os.Remove(temporary.Name())
	got, err := streamArchive(temporary, source, name)
	if err != nil || got != manifest.Files[pos] {
		return errors.Join(fmt.Errorf("sealed WAL changed: %s", name), err, temporary.Close())
	}
	if err := errors.Join(temporary.Sync(), temporary.Close()); err != nil {
		return err
	}
	return os.Rename(temporary.Name(), dest)
}

func runReplayWAL(args []string) int {
	if len(args) != 2 {
		return 126
	}
	var m replayManifest
	_, err := readFaultJSON(targetManifest, &m)
	if err == nil {
		err = replayWAL(archiveDest, args[0], args[1], m)
	}
	switch {
	case err == nil:
		fmt.Fprintf(os.Stderr, "j7-archive restored %s\n", args[0])
		return 0
	case errors.Is(err, errWALAbsent):
		fmt.Fprintf(os.Stderr, "j7-archive absent %s\n", args[0])
		return 1
	default:
		fmt.Fprintln(os.Stderr, "j7-archive integrity error:", err)
		return 126
	}
}

func stageTarget(ctx context.Context) error {
	var manifest replayManifest
	if _, err := readFaultJSON(targetManifest, &manifest); err != nil {
		return err
	}
	if !fixtureName.MatchString(manifest.Database) || manifest.Timeline == 0 {
		return errors.New("invalid isolated recovery placement manifest")
	}
	entries, err := os.ReadDir(targetVolumeDir)
	if err != nil || len(entries) != 0 {
		return errors.New("target volume must be fresh and empty")
	}
	u, err := user.Lookup("postgres")
	if err != nil {
		return err
	}
	uid, uidErr := strconv.Atoi(u.Uid)
	gid, gidErr := strconv.Atoi(u.Gid)
	if uidErr != nil || gidErr != nil || uid <= 0 || gid <= 0 {
		return errors.New("invalid image PostgreSQL UID or GID")
	}
	if err := copyBaseBackup(targetBackup, targetData, uid, gid); err != nil {
		return err
	}
	// Verify the actual copy before replacing its server configuration.
	f, err := openRegular(filepath.Join(targetData, "backup_manifest"), os.O_RDONLY)
	if err != nil {
		return err
	}
	raw, err := io.ReadAll(io.LimitReader(f, maxManifestSize+1))
	if err := errors.Join(err, f.Close()); err != nil {
		return err
	}
	if len(raw) > maxManifestSize || restoreledger.SHA256(raw) != manifest.Backup {
		return errors.New("copied backup manifest differs from verified backup")
	}
	out, err := exec.CommandContext(ctx, "pg_verifybackup", "--format=plain",
		"--wal-directory="+archiveDest, targetData).CombinedOutput()
	if err != nil {
		return fmt.Errorf("verify copied base backup: %w: %s", err, out)
	}
	fmt.Print(string(out))
	for _, name := range []string{"standby.signal", "recovery.signal", "postmaster.pid"} {
		if _, err := os.Lstat(filepath.Join(targetData, name)); !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("base backup contains disallowed entry %s", name)
		}
	}
	config := map[string]string{
		"postgresql.conf":      recoveryConf,
		"postgresql.auto.conf": "",
		"pg_hba.conf":          "local " + manifest.Database + " " + observerRole + " peer map=j7-readonly\nlocal all all reject\nhost all all 0.0.0.0/0 reject\nhost all all ::/0 reject\n",
		"pg_ident.conf":        "j7-readonly postgres " + observerRole + "\n",
		"recovery.signal":      "",
	}
	for name, content := range config {
		if err := writeTargetFile(filepath.Join(targetData, name), strings.NewReader(content), uid, gid, false); err != nil {
			return err
		}
	}
	if err := syncDirectory(targetData); err != nil {
		return err
	}
	return syncDirectory(targetVolumeDir)
}

const recoveryConf = `
listen_addresses = ''
unix_socket_directories = '/tmp'
unix_socket_permissions = 0700
port = 5432
max_connections = 100
max_worker_processes = 8
max_wal_senders = 10
max_prepared_transactions = 0
max_locks_per_transaction = 64
wal_level = replica
archive_mode = off
primary_conninfo = ''
primary_slot_name = ''
hot_standby = off
recovery_target_timeline = 'current'
restore_command = '/j7/bin/j7-media restore-wal "%f" "%p"'
default_transaction_read_only = on
autovacuum = off
lc_messages = 'C'
log_min_messages = log
log_timezone = 'UTC'
timezone = 'UTC'
`

func copyBaseBackup(src, dest string, uid, gid int) error {
	dirs := []string{}
	var total int64
	if err := filepath.WalkDir(src, func(path string, item fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		to := filepath.Join(dest, rel)
		if item.IsDir() {
			dirs = append(dirs, to)
			return errors.Join(os.Mkdir(to, 0o700), os.Chown(to, uid, gid))
		}
		f, err := openRegular(path, os.O_RDONLY)
		if err != nil {
			return err
		}
		defer f.Close()
		limited := &io.LimitedReader{R: f, N: maxBackupBytes - total + 1}
		first := limited.N
		err = writeTargetFile(to, limited, uid, gid, true)
		total += first - limited.N
		if total > maxBackupBytes {
			return errors.New("target copy exceeds 512 MiB")
		}
		return err
	}); err != nil {
		return err
	}
	for i := len(dirs) - 1; i >= 0; i-- {
		if err := syncDirectory(dirs[i]); err != nil {
			return err
		}
	}
	return nil
}

func writeTargetFile(to string, content io.Reader, uid, gid int, fresh bool) error {
	flags := os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	if fresh {
		flags |= os.O_EXCL
	}
	f, err := os.OpenFile(to, flags, 0o600)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(f, content)
	return errors.Join(copyErr, f.Chown(uid, gid), f.Sync(), f.Close())
}
