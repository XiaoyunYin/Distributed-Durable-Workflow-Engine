package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestPrepareMedia(t *testing.T) {
	for _, tc := range []struct {
		name string
		want string
	}{
		{"verified", ""},
		{"wrong tool version", "must be PostgreSQL 18.6"},
		{"unsafe source", "settings or identity differ"},
		{"wrong backup identity", "source identity mismatch"},
		{"bundled WAL", "bundled WAL"},
		{"checksum failure", "verify base backup and required WAL"},
		{"wrong anchor identity", "belongs to an unverified source"},
		{"missing archive segment", "read archive"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			in := prepareInput{
				pgBin: root, backup: filepath.Join(root, "base"), archive: filepath.Join(root, "wal"),
				sourceService: "fixture-source", backupService: "fixture-backup", drill: "r1",
			}
			if err := os.Mkdir(in.archive, 0o700); err != nil {
				t.Fatal(err)
			}
			var ordinal int
			var verified bool
			const sourceID = 7974252375362733099
			run := func(_ context.Context, tool string, args ...string) ([]byte, error) {
				name := filepath.Base(tool)
				if slices.Equal(args, []string{"--version"}) {
					version := postgresVersion
					if tc.name == "wrong tool version" && name == "pg_verifybackup" {
						version = "17.9"
					}
					return fmt.Appendf(nil, "%s (PostgreSQL) %s\n", name, version), nil
				}
				switch name {
				case "psql":
					if strings.Contains(args[len(args)-1], "server_version_num") {
						source := sourceState{
							Version: serverVersion, SystemID: sourceID, Timeline: 1,
							WALLevel: "replica", ArchiveMode: "on", ArchiveTimeout: "0",
							SegmentBytes: walSegmentBytes, Fsync: "on", SyncCommit: "on",
							FullPageWrites: "on", HasArchiveCmd: true, DataDirectory: "/fixture/data",
							DatabaseBytes: 12 << 20,
						}
						if tc.name == "unsafe source" {
							source.Fsync = "off"
						}
						return json.Marshal(source)
					}
					if !verified {
						t.Fatal("anchor was requested before backup verification")
					}
					ordinal++
					if ordinal > 2 || !strings.Contains(args[len(args)-2], fmt.Sprintf("j7-r1-anchor-%d", ordinal)) ||
						args[len(args)-1] != "--command=SELECT pg_switch_wal()" {
						t.Fatalf("unexpected archive anchor commands: %v", args)
					}
					index := uint64(ordinal + 1)
					if tc.name == "missing archive segment" && ordinal == 2 {
						index++
					}
					wal := segmentName(1, index)
					writeSegment(t, in.archive, wal, walSegmentBytes, byte(index))
					id := uint64(sourceID)
					if tc.name == "wrong anchor identity" {
						id++
					}
					return fmt.Appendf(nil, "{\"lsn\":\"0/%X\",\"segment\":\"%s\",\"system_identifier\":%d}\n0/%X\n",
						index*walSegmentBytes+0x50, wal, id, (index+1)*walSegmentBytes), nil
				case "pg_basebackup":
					for _, flag := range []string{"--format=plain", "--wal-method=none", "--manifest-checksums=SHA256", "--dbname=service=fixture-backup"} {
						if !slices.Contains(args, flag) {
							t.Fatalf("base backup missing %s: %v", flag, args)
						}
					}
					if err := os.MkdirAll(filepath.Join(in.backup, "pg_wal"), 0o700); err != nil {
						t.Fatal(err)
					}
					id := uint64(sourceID)
					if tc.name == "wrong backup identity" {
						id++
					}
					manifest := fmt.Sprintf(`{"PostgreSQL-Backup-Manifest-Version":2,"System-Identifier":%d,"Files":[{"Checksum-Algorithm":"SHA256","Checksum":"%s"}],"WAL-Ranges":[{"Timeline":1,"Start-LSN":"0/1000028","End-LSN":"0/2000000"}]}`, id, strings.Repeat("a", 64))
					if err := os.WriteFile(filepath.Join(in.backup, "backup_manifest"), []byte(manifest), 0o600); err != nil {
						t.Fatal(err)
					}
					writeSegment(t, in.archive, segmentName(1, 1), walSegmentBytes, 1)
					if tc.name == "bundled WAL" {
						writeSegment(t, filepath.Join(in.backup, "pg_wal"), segmentName(1, 1), walSegmentBytes, 5)
					}
					return nil, nil
				case "pg_verifybackup":
					if !slices.Equal(args, []string{"--format=plain", "--wal-directory=" + in.archive, in.backup}) {
						t.Fatalf("verify called with unexpected args: %v", args)
					}
					if tc.name == "checksum failure" {
						return nil, errors.New("injected checksum mismatch")
					}
					verified = true
					return []byte("backup successfully verified\n"), nil
				default:
					t.Fatalf("unexpected binary: %s", tool)
					return nil, nil
				}
			}
			var dirs []string
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			receipt, err := prepareBackup(ctx, in, run, func(dir string) error {
				dirs = append(dirs, dir)
				return nil
			})
			if tc.want != "" {
				if err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("prepare = %v, want %q", err, tc.want)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(receipt.Archive) != 3 || len(receipt.Anchors) != 2 ||
				receipt.Archive[2].Name != receipt.Anchors[1].Segment ||
				receipt.VerifyBackupText != "backup successfully verified" ||
				receipt.Source.SystemID != sourceID || len(receipt.ManifestSHA256) != 64 ||
				!slices.Equal(dirs, []string{root, in.archive}) {
				t.Fatalf("incomplete receipt %+v or directory syncs %v", receipt, dirs)
			}
		})
	}
}

func TestAwaitArchiveHonorsCancellation(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := awaitArchive(ctx, dir, segmentName(1, 1)); !errors.Is(err, context.Canceled) {
		t.Fatalf("await missing WAL = %v", err)
	}
}
