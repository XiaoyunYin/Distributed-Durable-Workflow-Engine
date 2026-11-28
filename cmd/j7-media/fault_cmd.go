package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"durable-agent-execution-engine/internal/j7markers"
	"durable-agent-execution-engine/internal/restoreledger"
)

type faultFiles struct {
	PGBin        string      `json:"pg_bin"`
	Source       string      `json:"source_service"`
	Archive      string      `json:"archive"`
	BackupDir    string      `json:"backup_directory"`
	Backup       string      `json:"backup_receipt"`
	BackupSHA256 string      `json:"backup_receipt_sha256"`
	Ledger       string      `json:"ledger"`
	Plan         string      `json:"marker_plan"`
	OriginNS     int64       `json:"marker_origin_ns"`
	Binding      string      `json:"source_binding"`
	Cut          string      `json:"archive_cut"`
	Outage       string      `json:"source_loss"`
	EffectID     string      `json:"source_effect_workflow_id"`
	Resources    faultConfig `json:"resources"`
}

func (f faultFiles) validate() error {
	if !fixtureName.MatchString(f.Source) || !restoreledger.IsSHA256(f.BackupSHA256) || f.EffectID == "" ||
		f.OriginNS <= 0 {
		return errors.New("fault configuration requires the pinned source service, backup digest and E1 workflow ID")
	}
	if err := f.Resources.validate(); err != nil {
		return err
	}
	paths := []string{f.PGBin, f.Archive, f.BackupDir, f.Backup, f.Ledger, f.Plan, f.Binding, f.Cut, f.Outage}
	seen := make(map[string]bool)
	for _, path := range paths {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path || seen[path] {
			return errors.New("fault paths must be distinct and absolute")
		}
		seen[path] = true
	}
	for _, path := range paths[3:] {
		parent, err := filepath.EvalSymlinks(filepath.Dir(path))
		if err != nil || parent != filepath.Dir(path) || within(f.Archive, path) || within(f.BackupDir, path) {
			return errors.New("fault evidence must use independent, canonical host paths")
		}
	}
	archive, err := filepath.EvalSymlinks(f.Archive)
	if err != nil || archive != f.Archive {
		return errors.New("archive bind path must be canonical on the controller host")
	}
	backup, err := filepath.EvalSymlinks(f.BackupDir)
	if err != nil || backup != f.BackupDir || within(f.Archive, backup) || within(backup, f.Archive) {
		return errors.New("base backup path must be canonical and disjoint from the archive")
	}
	return nil
}

func readFaultJSON[T any](file string, to *T) (string, error) {
	f, err := openRegular(file, os.O_RDONLY)
	if err != nil {
		return "", err
	}
	defer f.Close()
	const limit = 2 << 20
	raw, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return "", err
	}
	if len(raw) > limit {
		return "", errors.New("fault metadata exceeds two MiB")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(to); err != nil {
		return "", err
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return "", errors.New("fault metadata must contain exactly one JSON value")
	}
	return restoreledger.SHA256(raw), nil
}

func readLiveLedger(file, boot string) (restoreledger.Report, error) {
	f, err := openRegular(file, os.O_RDONLY)
	if err != nil {
		return restoreledger.Report{}, err
	}
	defer f.Close()
	report, err := restoreledger.Read(f, "", true)
	if err != nil {
		return report, err
	}
	if report.Sealed || report.Header.BootID != boot {
		return report, errors.New("source action requires the live ledger on the current Linux boot")
	}
	return report, nil
}

// Create the receipt exclusively before the action. On either outcome retain
// partial evidence and fsync it. An existing receipt blocks a replay.
func faultReceipt[T any](path string, action func() (T, error)) (T, string, error) {
	var blank T
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return blank, "", err
	}
	if err := file.Sync(); err != nil {
		return blank, "", errors.Join(err, file.Close())
	}
	if err := syncDirectory(filepath.Dir(path)); err != nil {
		return blank, "", errors.Join(err, file.Close())
	}
	receipt, actionErr := action()
	raw, encodeErr := json.Marshal(receipt)
	if encodeErr != nil {
		return receipt, "", errors.Join(actionErr, encodeErr, file.Close())
	}
	raw = append(raw, '\n')
	_, writeErr := file.Write(raw)
	return receipt, restoreledger.SHA256(raw), errors.Join(actionErr, writeErr, file.Sync(), file.Close())
}

func runFault(ctx context.Context, args []string, out, diagnostics io.Writer) error {
	flags := flag.NewFlagSet(args[0], flag.ContinueOnError)
	flags.SetOutput(diagnostics)
	config := flags.String("config", "", "absolute path of the private J7 fault configuration JSON")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 || !filepath.IsAbs(*config) {
		return errors.New("source operation requires one absolute --config path")
	}
	now, boot, err := restoreledger.HostClock()
	if err != nil {
		return err
	}
	var files faultFiles
	configSHA, err := readFaultJSON(*config, &files)
	if err != nil {
		return err
	}
	if err := files.validate(); err != nil {
		return err
	}
	var backup backupReceipt
	backupSHA, err := readFaultJSON(files.Backup, &backup)
	if err != nil {
		return err
	}
	if backupSHA != files.BackupSHA256 {
		return errors.New("backup receipt digest differs from the sealed fault configuration")
	}
	if err := backup.Source.validate(); err != nil {
		return err
	}
	report, err := readLiveLedger(files.Ledger, boot)
	if err != nil {
		return err
	}
	var plan j7markers.Plan
	if _, err := readFaultJSON(files.Plan, &plan); err != nil {
		return err
	}
	planSHA, err := plan.Digest()
	if err != nil || report.Header.MarkersSHA256 != planSHA {
		return errors.New("fault marker plan does not match the live ledger")
	}
	source := mediaSource{pgBin: files.PGBin, service: files.Source, archive: files.Archive,
		run: execTool, now: now, syncDir: syncDirectory}
	docker := localDocker{run: execTool}
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	emit := json.NewEncoder(out)
	if args[0] == "source-preflight" {
		_, sha, err := faultReceipt(files.Binding, func() (sourceBinding, error) {
			_, digest, size, err := inspectBackup(files.BackupDir, backup.Source)
			if err != nil {
				return sourceBinding{}, err
			}
			if digest != backup.ManifestSHA256 || size != backup.BackupBytes {
				return sourceBinding{}, errors.New("preflight base backup differs from verified media")
			}
			binding, err := preflightSource(ctx, source, docker, files.Resources, report.Header, backup.Source, backupSHA)
			binding.ConfigSHA256 = configSHA
			if err == nil {
				err = files.checkBinding(binding, *config)
			}
			return binding, err
		})
		if err != nil {
			return err
		}
		return emit.Encode(map[string]string{"source_binding_sha256": sha})
	}
	var binding sourceBinding
	bindingSHA, err := readFaultJSON(files.Binding, &binding)
	if err != nil {
		return err
	}
	if binding.Schema != "j7.source-binding.v1" || binding.BootID != boot ||
		binding.Authority != report.Header.Source || binding.BackupSHA256 != backupSHA ||
		binding.ConfigSHA256 != configSHA || binding.Run != files.Resources {
		return errors.New("preflight receipt does not match the sealed fault configuration")
	}
	if err := files.checkBinding(binding, *config); err != nil {
		return err
	}
	if args[0] == "cut" {
		r, sha, err := faultReceipt(files.Cut, func() (cutReceipt, error) {
			return closeBoundary(ctx, source, backup, backupSHA, plan, report, files.OriginNS)
		})
		if err != nil {
			return err
		}
		return emit.Encode(archiveCutWitness(r, sha, plan.Markers[11].RequestID))
	}
	var cut cutReceipt
	cutSHA, err := readFaultJSON(files.Cut, &cut)
	if err != nil {
		return err
	}
	_, sha, err := faultReceipt(files.Outage, func() (outageReceipt, error) {
		return loseSource(ctx, source, docker, files.Resources, backup, backupSHA, binding, bindingSHA,
			cut, cutSHA, plan, report, files.OriginNS, files.EffectID)
	})
	if err != nil {
		return err
	}
	return emit.Encode(map[string]string{"source_loss_sha256": sha})
}

func (f faultFiles) checkBinding(b sourceBinding, config string) error {
	if b.ObservedNS <= 0 || b.ObservedNS >= f.OriginNS {
		return errors.New("source preflight must finish before marker admission")
	}
	for _, file := range []string{config, f.PGBin, f.BackupDir, f.Backup, f.Ledger, f.Plan, f.Binding, f.Cut, f.Outage} {
		if within(b.Volume.Mountpoint, file) {
			return errors.New("source-loss tooling, configuration and evidence must survive volume deletion")
		}
	}
	return nil
}

func preflightSource(ctx context.Context, source mediaSource, docker localDocker, cfg faultConfig,
	h restoreledger.Header, want sourceState, backupSHA string) (sourceBinding, error) {
	for _, tool := range []string{"psql", "pg_isready"} {
		out, err := source.run(ctx, filepath.Join(source.pgBin, tool), "--version")
		fields := strings.Fields(string(out))
		if err != nil || len(fields) < 3 || fields[0] != tool ||
			fields[1] != "(PostgreSQL)" || fields[2] != postgresVersion {
			return sourceBinding{}, fmt.Errorf("%s must be PostgreSQL %s", tool, postgresVersion)
		}
	}
	raw, err := source.psql(ctx, sourceSQL)
	if err != nil {
		return sourceBinding{}, fmt.Errorf("verify preflight PostgreSQL data directory: %w", err)
	}
	var live sourceState
	if err := json.Unmarshal(raw, &live); err != nil {
		return sourceBinding{}, err
	}
	if err := live.validate(); err != nil {
		return sourceBinding{}, err
	}
	if live.SystemID != want.SystemID || live.Timeline != want.Timeline || live.DataDirectory != want.DataDirectory {
		return sourceBinding{}, errors.New("preflight source differs from verified backup")
	}
	return docker.bind(ctx, source, cfg, h, live, backupSHA)
}
