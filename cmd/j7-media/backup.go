package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

const (
	postgresVersion = "18.6"
	serverVersion   = 180006
	maxDatabaseSize = 128 << 20
	maxBackupBytes  = 512 << 20
	maxManifestSize = 16 << 20
)

type sourceState struct {
	Version         int    `json:"server_version_num"`
	SystemID        uint64 `json:"system_identifier"`
	Timeline        uint32 `json:"timeline"`
	InRecovery      bool   `json:"in_recovery"`
	WALLevel        string `json:"wal_level"`
	ArchiveMode     string `json:"archive_mode"`
	ArchiveTimeout  string `json:"archive_timeout"`
	SegmentBytes    int64  `json:"wal_segment_bytes"`
	Fsync           string `json:"fsync"`
	SyncCommit      string `json:"synchronous_commit"`
	FullPageWrites  string `json:"full_page_writes"`
	HasArchiveCmd   bool   `json:"has_archive_command"`
	ArchiveLibrary  string `json:"archive_library"`
	DataDirectory   string `json:"data_directory"`
	ExtraTablespace bool   `json:"extra_tablespace"`
	DatabaseBytes   int64  `json:"database_bytes"`
}

const sourceSQL = `
SELECT json_build_object(
    'server_version_num', current_setting('server_version_num')::integer,
    'system_identifier', (SELECT system_identifier FROM pg_control_system()),
    'timeline', (SELECT timeline_id FROM pg_control_checkpoint()),
    'in_recovery', pg_is_in_recovery(),
    'wal_level', current_setting('wal_level'),
    'archive_mode', current_setting('archive_mode'),
    'archive_timeout', (SELECT setting FROM pg_settings WHERE name = 'archive_timeout'),
    'wal_segment_bytes', pg_size_bytes(current_setting('wal_segment_size')),
    'fsync', current_setting('fsync'),
    'synchronous_commit', current_setting('synchronous_commit'),
    'full_page_writes', current_setting('full_page_writes'),
    'has_archive_command', current_setting('archive_command') <> '',
    'archive_library', current_setting('archive_library'),
    'data_directory', current_setting('data_directory'),
    'database_bytes', pg_database_size(current_database()),
    'extra_tablespace', EXISTS (SELECT FROM pg_tablespace WHERE spcname NOT IN ('pg_default', 'pg_global'))
);`

func (s sourceState) validate() error {
	if s.Version != serverVersion || s.SystemID == 0 || s.Timeline == 0 || s.InRecovery ||
		s.WALLevel != "replica" || s.ArchiveMode != "on" || s.ArchiveTimeout != "0" ||
		s.SegmentBytes != walSegmentBytes || s.Fsync != "on" || s.SyncCommit != "on" ||
		s.FullPageWrites != "on" || !s.HasArchiveCmd || s.ArchiveLibrary != "" ||
		s.DataDirectory == "" || s.ExtraTablespace || s.DatabaseBytes <= 0 || s.DatabaseBytes > maxDatabaseSize {
		return errors.New("source PostgreSQL settings or identity differ from the J7 media profile")
	}
	return nil
}

type walRange struct {
	Timeline uint32 `json:"Timeline"`
	Start    string `json:"Start-LSN"`
	End      string `json:"End-LSN"`
}

type backupManifest struct {
	Version  int    `json:"PostgreSQL-Backup-Manifest-Version"`
	SystemID uint64 `json:"System-Identifier"`
	Files    []struct {
		Algorithm string `json:"Checksum-Algorithm"`
		Digest    string `json:"Checksum"`
	} `json:"Files"`
	WAL []walRange `json:"WAL-Ranges"`
}

type anchor struct {
	Name      string `json:"name"`
	LSN       string `json:"lsn"`
	Segment   string `json:"segment"`
	SwitchLSN string `json:"switch_lsn"`
}

type backupReceipt struct {
	Source           sourceState    `json:"source"`
	BackupBytes      int64          `json:"backup_bytes"`
	ManifestSHA256   string         `json:"backup_manifest_sha256"`
	RequiredWAL      []walRange     `json:"required_wal"`
	Anchors          []anchor       `json:"anchors"`
	Archive          []archivedFile `json:"archive_through_second_anchor"`
	VerifyBackupText string         `json:"pg_verifybackup_output"`
}

type prepareInput struct {
	pgBin, sourceService, backupService, backup, archive, drill string
}

var fixtureName = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,31}$`)

type runTool func(context.Context, string, ...string) ([]byte, error)

func execTool(ctx context.Context, name string, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, name, args...).Output()
}

func (in prepareInput) validatePaths() error {
	if !fixtureName.MatchString(in.drill) || !fixtureName.MatchString(in.sourceService) ||
		!fixtureName.MatchString(in.backupService) {
		return errors.New("drill and libpq service names must be lowercase fixture identifiers")
	}
	if !filepath.IsAbs(in.pgBin) || !filepath.IsAbs(in.backup) || !filepath.IsAbs(in.archive) {
		return errors.New("PostgreSQL binary, backup and archive paths must be absolute")
	}
	archive, err := filepath.EvalSymlinks(in.archive)
	if err != nil {
		return fmt.Errorf("resolve archive: %w", err)
	}
	info, err := os.Stat(archive)
	if err != nil || !info.IsDir() {
		return errors.New("archive must be an existing directory")
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(in.backup))
	if err != nil {
		return fmt.Errorf("resolve backup parent: %w", err)
	}
	backup := filepath.Join(parent, filepath.Base(in.backup))
	if within(archive, backup) || within(backup, archive) {
		return errors.New("backup and archive must use disjoint paths")
	}
	if _, err := os.Lstat(backup); !errors.Is(err, os.ErrNotExist) {
		return errors.New("base backup path must be new")
	}
	return nil
}

func within(root, path string) bool {
	relative, err := filepath.Rel(root, path)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func inspectBackup(dir string, source sourceState) (backupManifest, string, int64, error) {
	var bytes int64
	err := filepath.WalkDir(dir, func(path string, item fs.DirEntry, err error) error {
		if err != nil || item.IsDir() {
			return err
		}
		info, err := item.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("nonregular base backup entry %q", path)
		}
		bytes += info.Size()
		if bytes > maxBackupBytes {
			return errors.New("base backup exceeds 512 MiB")
		}
		return nil
	})
	if err != nil {
		return backupManifest{}, "", 0, err
	}
	f, err := openRegular(filepath.Join(dir, "backup_manifest"), os.O_RDONLY)
	if err != nil {
		return backupManifest{}, "", 0, err
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, maxManifestSize+1))
	if err != nil {
		return backupManifest{}, "", 0, err
	}
	var manifest backupManifest
	if len(raw) > maxManifestSize || json.Unmarshal(raw, &manifest) != nil || manifest.Version != 2 ||
		manifest.SystemID != source.SystemID || len(manifest.Files) == 0 || len(manifest.WAL) != 1 ||
		manifest.WAL[0].Timeline != source.Timeline {
		return backupManifest{}, "", 0, errors.New("invalid backup manifest or source identity mismatch")
	}
	for _, file := range manifest.Files {
		if file.Algorithm != "SHA256" || len(file.Digest) != 2*sha256.Size {
			return backupManifest{}, "", 0, errors.New("backup manifest requires SHA256 for each file")
		}
	}
	walDir := filepath.Join(dir, "pg_wal")
	entries, err := os.ReadDir(walDir)
	if err != nil {
		return backupManifest{}, "", 0, err
	}
	for _, entry := range entries {
		if !entry.IsDir() || (entry.Name() != "archive_status" && entry.Name() != "summaries") {
			return backupManifest{}, "", 0, errors.New("base backup has bundled WAL or unexpected WAL files")
		}
	}
	hash := sha256.Sum256(raw)
	return manifest, hex.EncodeToString(hash[:]), bytes, nil
}

func awaitArchive(ctx context.Context, dir, name string) error {
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	path := filepath.Join(dir, name)
	for {
		info, err := os.Lstat(path)
		if err == nil {
			if !info.Mode().IsRegular() || info.Size() != walSegmentBytes {
				return fmt.Errorf("archive segment %s is incomplete or nonregular", name)
			}
			return nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("await archive segment %s: %w", name, ctx.Err())
		case <-tick.C:
		}
	}
}

func prepareBackup(ctx context.Context, in prepareInput, run runTool, syncDir func(string) error) (backupReceipt, error) {
	var receipt backupReceipt
	if err := in.validatePaths(); err != nil {
		return receipt, err
	}
	for _, tool := range []string{"psql", "pg_basebackup", "pg_verifybackup"} {
		out, err := run(ctx, filepath.Join(in.pgBin, tool), "--version")
		fields := strings.Fields(string(out))
		if err != nil || len(fields) < 3 || fields[0] != tool ||
			fields[1] != "(PostgreSQL)" || fields[2] != postgresVersion {
			return receipt, fmt.Errorf("%s must be PostgreSQL %s", tool, postgresVersion)
		}
	}
	psql := func(commands ...string) ([]byte, error) {
		args := []string{"--no-psqlrc", "--no-password", "--tuples-only",
			"--no-align", "--set=ON_ERROR_STOP=1", "--dbname=service=" + in.sourceService}
		for _, command := range commands {
			args = append(args, "--command="+command)
		}
		return run(ctx, filepath.Join(in.pgBin, "psql"), args...)
	}
	source, err := psql(sourceSQL)
	if err != nil {
		return receipt, fmt.Errorf("probe backup source: %w", err)
	}
	if err := json.Unmarshal(source, &receipt.Source); err != nil {
		return receipt, fmt.Errorf("decode source settings: %w", err)
	}
	if err := receipt.Source.validate(); err != nil {
		return receipt, err
	}
	if _, err := run(ctx, filepath.Join(in.pgBin, "pg_basebackup"), "--no-password",
		"--dbname=service="+in.backupService, "--pgdata="+in.backup, "--format=plain",
		"--wal-method=none", "--manifest-checksums=SHA256", "--no-clean"); err != nil {
		return receipt, fmt.Errorf("take base backup: %w", err)
	}
	if err := syncDir(filepath.Dir(in.backup)); err != nil {
		return receipt, fmt.Errorf("sync base backup directory entry: %w", err)
	}
	manifest, digest, bytes, err := inspectBackup(in.backup, receipt.Source)
	if err != nil {
		return receipt, err
	}
	receipt.BackupBytes, receipt.ManifestSHA256, receipt.RequiredWAL = bytes, digest, manifest.WAL
	start, startErr := parseLSN(manifest.WAL[0].Start)
	end, endErr := parseLSN(manifest.WAL[0].End)
	if startErr != nil || endErr != nil || end <= start {
		return receipt, errors.New("invalid manifest WAL positions")
	}
	backupNames, err := segmentRange(receipt.Source.Timeline, start/walSegmentBytes, (end-1)/walSegmentBytes)
	if err != nil {
		return receipt, err
	}
	for _, name := range backupNames {
		if err := awaitArchive(ctx, in.archive, name); err != nil {
			return receipt, err
		}
	}
	verified, err := run(ctx, filepath.Join(in.pgBin, "pg_verifybackup"),
		"--format=plain", "--wal-directory="+in.archive, in.backup)
	if err != nil {
		var failed *exec.ExitError
		if errors.As(err, &failed) && len(failed.Stderr) != 0 {
			return receipt, fmt.Errorf("verify base backup and required WAL: %w: %s",
				err, strings.TrimSpace(string(failed.Stderr)))
		}
		return receipt, fmt.Errorf("verify base backup and required WAL: %w", err)
	}
	receipt.VerifyBackupText = strings.TrimSpace(string(verified))

	lastIndex := (end - 1) / walSegmentBytes
	for ordinal := 1; ordinal <= 2; ordinal++ {
		point, index, err := archiveAnchor(ctx, in, ordinal, receipt.Source, psql)
		if err != nil {
			return receipt, err
		}
		if index < lastIndex || (ordinal == 2 && index == lastIndex) {
			return receipt, fmt.Errorf("archive anchor %d is out of order", ordinal)
		}
		lastIndex = index
		receipt.Anchors = append(receipt.Anchors, point)
	}
	names, err := segmentRange(receipt.Source.Timeline, start/walSegmentBytes, lastIndex)
	if err != nil {
		return receipt, err
	}
	receipt.Archive, err = readArchive(in.archive, names)
	if err != nil {
		return receipt, err
	}
	// The file was synced before publication. Sync the containing directory
	// ourselves so an observed segment is durable even if its archive command
	// has not yet returned to PostgreSQL.
	if err := syncDir(in.archive); err != nil {
		return receipt, fmt.Errorf("sync verified archive inventory: %w", err)
	}
	return receipt, nil
}

func archiveAnchor(ctx context.Context, in prepareInput, ordinal int, source sourceState,
	psql func(...string) ([]byte, error)) (anchor, uint64, error) {
	point := anchor{Name: fmt.Sprintf("j7-%s-anchor-%d", in.drill, ordinal)}
	// One connection witnesses the restore point and then switches WAL.
	mark := fmt.Sprintf(`
WITH point AS MATERIALIZED (SELECT pg_create_restore_point('%s') AS lsn)
SELECT json_build_object('lsn', lsn, 'segment', pg_walfile_name(lsn),
    'system_identifier', (SELECT system_identifier FROM pg_control_system()))
FROM point;`, point.Name)
	raw, err := psql(mark, "SELECT pg_switch_wal()")
	if err != nil {
		return point, 0, fmt.Errorf("create and switch archive anchor %d: %w", ordinal, err)
	}
	observed, switchLSN, found := strings.Cut(strings.TrimSpace(string(raw)), "\n")
	var identity struct {
		LSN     string `json:"lsn"`
		Segment string `json:"segment"`
		ID      uint64 `json:"system_identifier"`
	}
	if !found || json.Unmarshal([]byte(observed), &identity) != nil || identity.ID != source.SystemID {
		return point, 0, fmt.Errorf("archive anchor %d belongs to an unverified source", ordinal)
	}
	point.LSN, point.Segment, point.SwitchLSN = identity.LSN, identity.Segment, strings.TrimSpace(switchLSN)
	lsn, err := parseLSN(point.LSN)
	if err != nil || lsn == 0 {
		return point, 0, fmt.Errorf("invalid archive anchor %d LSN", ordinal)
	}
	index := (lsn - 1) / walSegmentBytes
	if point.Segment != segmentName(source.Timeline, index) {
		return point, 0, fmt.Errorf("archive anchor %d timeline or segment mismatch", ordinal)
	}
	switched, err := parseLSN(point.SwitchLSN)
	if err != nil || switched <= lsn {
		return point, 0, fmt.Errorf("WAL switch did not follow archive anchor %d", ordinal)
	}
	return point, index, awaitArchive(ctx, in.archive, point.Segment)
}
