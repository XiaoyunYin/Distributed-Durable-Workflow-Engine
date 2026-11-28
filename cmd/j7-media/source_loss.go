package main

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"durable-agent-execution-engine/internal/j7markers"
	"durable-agent-execution-engine/internal/restoreledger"
)

type outageReceipt struct {
	Schema        string                `json:"schema"`
	BootID        string                `json:"boot_id"`
	Source        string                `json:"source_instance"`
	BackupSHA256  string                `json:"backup_receipt_sha256"`
	BindingSHA256 string                `json:"source_binding_sha256"`
	CutSHA256     string                `json:"cut_receipt_sha256"`
	LedgerSHA256  string                `json:"outage_input_ledger_sha256"`
	EffectProof   restoreledger.Witness `json:"source_effect_receipt_witness"`
	WriterFreeze  restoreledger.Witness `json:"writer_freeze"`
	FinalSource   sourceSample          `json:"final_source"`
	FlushPastCut  uint64                `json:"flush_bytes_past_cut"`
	M15AckNS      int64                 `json:"m15_ack_ns"`
	KillNS        int64                 `json:"kill_requested_ns,omitempty"`
	KillAckNS     int64                 `json:"kill_ack_ns,omitempty"`
	LastSuccessNS int64                 `json:"last_sql_success_ns,omitempty"`
	SQLDown       sqlDown               `json:"sql_down"`
	Exit          exitObservation       `json:"container_exit"`
	T0NS          int64                 `json:"t0_ns,omitempty"`
	ContainerGone int64                 `json:"container_removed_ns,omitempty"`
	VolumeRemoved int64                 `json:"volume_removed_ns,omitempty"`
	VolumeAbsent  int64                 `json:"volume_absent_ns,omitempty"`
	ArchiveSealNS int64                 `json:"archive_sealed_ns,omitempty"`
	Inventory     []archivedFile        `json:"sealed_archive_inventory,omitempty"`
}

type exitObservation struct {
	State      containerState `json:"state"`
	ObservedNS int64          `json:"observed_ns"`
}

type sqlDown struct {
	FirstFailedStartNS int64 `json:"first_failed_start_ns"`
	FirstFailedNS      int64 `json:"first_failed_ns"`
	NoResponseNS       int64 `json:"no_response_confirmed_ns"`
	LastSuccessNS      int64 `json:"last_success_ns"`
	PsqlExitCode       int   `json:"psql_exit_code"`
	PgIsReadyExitCode  int   `json:"pg_isready_exit_code"`
}

func archiveCutWitness(cut cutReceipt, digest, requestID string) restoreledger.Witness {
	return restoreledger.Witness{
		Kind: restoreledger.ArchiveCut, Phase: "markers", Authority: cut.Source,
		RequestID: requestID, StartedNS: cut.StartedNS, CompletedNS: cut.CompletedNS, EvidenceSHA: digest,
		Timeline: cut.PreSwitch.Timeline, LSN: cut.Point.LSN, Segment: cut.Point.Segment, EndLSN: cut.EndLSN,
	}
}

func verifyOutageCut(cut cutReceipt, cutSHA, backupSHA string, backup backupReceipt,
	plan j7markers.Plan, report restoreledger.Report) (uint64, error) {
	h := report.Header
	if cut.Schema != "j7.archive-cut.v1" || cut.Source != h.Source || cut.BootID != h.BootID ||
		cut.BackupSHA256 != backupSHA || cut.PreSwitch.SystemID != backup.Source.SystemID ||
		cut.PreSwitch.Timeline != backup.Source.Timeline || !restoreledger.IsSHA256(cut.LedgerSHA256) {
		return 0, errors.New("archive cut belongs to another source, drill, or backup")
	}
	point, end, err := cutGeometry(cut.Point, backup.Source.Timeline, cut.PreSwitch.FlushLSN)
	recordedEnd, endErr := parseLSN(cut.EndLSN)
	if err != nil || endErr != nil || end != recordedEnd || len(cut.Inventory) < 3 {
		return 0, errors.New("archive cut geometry or frozen inventory is invalid")
	}
	expected := archiveCutWitness(cut, cutSHA, plan.Markers[11].RequestID)
	for _, w := range report.Witnesses {
		if w.Witness == expected {
			return point, nil
		}
	}
	return 0, errors.New("archive cut receipt does not match the live marker ledger")
}

func phaseWitness(report restoreledger.Report, phase string, earliest, latest int64) (restoreledger.Witness, error) {
	var result restoreledger.Witness
	for _, w := range report.Witnesses {
		if w.Phase != phase {
			continue
		}
		if result.Kind != "" || w.Kind != restoreledger.StateProbe || w.Authority != report.Header.Source ||
			w.RequestID != "" || w.CompletedNS < earliest || w.RecordedNS > latest {
			return result, fmt.Errorf("source %s witness is duplicated or unbound", phase)
		}
		result = w.Witness
	}
	if result.Kind == "" {
		return result, fmt.Errorf("source %s witness is required before fault", phase)
	}
	return result, nil
}

func bindingMatches(a, b sourceBinding) bool {
	return a.Schema == b.Schema && a.BootID == b.BootID && a.Authority == b.Authority &&
		a.BackupSHA256 == b.BackupSHA256 && a.Run == b.Run && maps.Equal(a.Labels, b.Labels) &&
		a.Image == b.Image && a.StartedAt == b.StartedAt && a.Data == b.Data && a.Archive == b.Archive &&
		sameVolume(a.Volume, b.Volume)
}

func sameVolume(a, b sourceVolume) bool {
	return a.Name == b.Name && a.Mountpoint == b.Mountpoint && a.CreatedAt == b.CreatedAt &&
		a.Driver == b.Driver && a.Scope == b.Scope && maps.Equal(a.Options, b.Options) && maps.Equal(a.Labels, b.Labels)
}

func loseSource(ctx context.Context, source mediaSource, docker localDocker, f faultConfig,
	backup backupReceipt, backupSHA string, binding sourceBinding, bindingSHA string, cut cutReceipt, cutSHA string,
	plan j7markers.Plan, report restoreledger.Report, origin int64, effectID string) (outageReceipt, error) {
	h := report.Header
	r := outageReceipt{Schema: "j7.source-loss.v1", BootID: h.BootID, Source: h.Source,
		BackupSHA256: backupSHA, BindingSHA256: bindingSHA, CutSHA256: cutSHA, LedgerSHA256: report.Digest}
	var err error
	r.M15AckNS, err = j7markers.BoundaryAck(plan, origin, report, true)
	if err != nil {
		return r, err
	}
	cutIndex, err := verifyOutageCut(cut, cutSHA, backupSHA, backup, plan, report)
	if err != nil {
		return r, err
	}
	now, err := source.now()
	if err != nil {
		return r, err
	}
	r.EffectProof, err = phaseWitness(report, "post-cut-effect", cut.CompletedNS, now)
	if err != nil {
		return r, err
	}
	// The controller may close old-writer admission while the final marker
	// request drains. Permit that proof to precede M15 so shutdown of the
	// old application roles does not consume its two-second kill window.
	r.WriterFreeze, err = phaseWitness(report, "writers-frozen", cut.CompletedNS, now)
	if err != nil {
		return r, err
	}
	preKillCtx, cancel, err := phaseDeadline(ctx, now, r.M15AckNS, killWindow)
	if err != nil {
		return r, fmt.Errorf("M15 source kill: %w", err)
	}
	defer cancel()
	live, err := docker.bind(preKillCtx, source, f, h, backup.Source, backupSHA)
	if err != nil {
		return r, err
	}
	if binding.ObservedNS <= 0 || binding.ObservedNS >= cut.StartedNS || !bindingMatches(binding, live) {
		return r, errors.New("live source container and volume do not match the sealed preflight")
	}
	want := ledgerMarkers(plan, report)
	r.FinalSource, err = source.sample(preKillCtx, want, effectID)
	if err != nil {
		return r, err
	}
	r.LastSuccessNS = r.FinalSource.CompletedNS
	if err := r.FinalSource.check(backup.Source, want, 8); err != nil {
		return r, err
	}
	if !restoreledger.IsSHA256(r.FinalSource.EffectSHA256) {
		return r, errors.New("the original cooperating result must be visible on the source before loss")
	}
	end := (cutIndex + 1) * walSegmentBytes
	flush, flushErr := parseLSN(r.FinalSource.FlushLSN)
	insert, insertErr := parseLSN(r.FinalSource.InsertLSN)
	if flushErr != nil || insertErr != nil || flush <= end || insert < flush || insert-end >= walSegmentBytes {
		return r, errors.New("flushed unarchived source tail exceeds the single next open segment or is missing")
	}
	r.FlushPastCut = flush - end
	requestNS, err := source.now()
	if err != nil {
		return r, err
	}
	if requestNS < r.LastSuccessNS || requestNS-r.M15AckNS >= int64(killWindow) || preKillCtx.Err() != nil {
		return r, errors.New("source kill request missed the M15 deadline")
	}
	r.KillNS = requestNS
	confirmCtx, stopConfirm := context.WithTimeout(ctx, confirmWindow)
	defer stopConfirm()
	out, err := docker.command(confirmCtx, "container", "kill", "--signal=SIGKILL", f.Container)
	if err != nil {
		return r, fmt.Errorf("kill the sealed source container: %w", err)
	}
	if strings.TrimSpace(string(out)) != f.Container {
		return r, errors.New("source SIGKILL acknowledgment did not identify the sealed container")
	}
	r.KillAckNS, err = source.now()
	if err != nil {
		return r, err
	}
	type downResult struct {
		observation sqlDown
		err         error
	}
	down := make(chan downResult, 1)
	go func() {
		obs, err := source.awaitUnavailable(confirmCtx, r.LastSuccessNS, backup.Source.SystemID)
		down <- downResult{obs, err}
	}()
	r.Exit, err = docker.observeExit(confirmCtx, f, binding, source.now)
	failed := <-down
	r.SQLDown = failed.observation
	r.LastSuccessNS = r.SQLDown.LastSuccessNS
	if err := errors.Join(err, failed.err); err != nil {
		return r, err
	}
	r.T0NS = max(r.SQLDown.NoResponseNS, r.Exit.ObservedNS)
	if r.T0NS-r.KillNS > int64(confirmWindow) || r.SQLDown.FirstFailedNS-r.LastSuccessNS > int64(3*time.Second) {
		return r, errors.New("confirmed outage exceeded the two-second gate or three-second probe bracket")
	}
	if err := removeSource(ctx, source, docker, binding, &r); err != nil {
		return r, err
	}
	r.Inventory, err = inventory(source.archive, backup, cutIndex)
	if err != nil {
		return r, err
	}
	if !slices.Equal(r.Inventory, cut.Inventory) {
		return r, errors.New("post-loss archive inventory differs from the pre-loss cut")
	}
	if err := source.syncDir(source.archive); err != nil {
		return r, err
	}
	r.ArchiveSealNS, err = source.now()
	return r, err
}

func (d localDocker) observeExit(ctx context.Context, f faultConfig, b sourceBinding,
	now restoreledger.Clock) (exitObservation, error) {
	var evidence exitObservation
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		c, err := d.container(ctx, f.Container)
		if err != nil {
			return evidence, err
		}
		evidence.State = c.State
		if killedSource(c.State, b.StartedAt) && c.RestartCount == 0 {
			evidence.ObservedNS, err = now()
			return evidence, err
		}
		if !runningSource(c.State) || c.State.StartedAt != b.StartedAt || c.RestartCount != 0 {
			return evidence, errors.New("source exited without the expected SIGKILL or restarted")
		}
		select {
		case <-ctx.Done():
			return evidence, fmt.Errorf("await source SIGKILL exit: %w", ctx.Err())
		case <-tick.C:
		}
	}
}

func exitCode(err error) int {
	if err == nil {
		return 0
	}
	var status interface{ ExitCode() int }
	if errors.As(err, &status) {
		return status.ExitCode()
	}
	return -1
}

func (s mediaSource) awaitUnavailable(ctx context.Context, lastSuccess int64, systemID uint64) (sqlDown, error) {
	obs := sqlDown{LastSuccessNS: lastSuccess}
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		start, err := s.now()
		if err != nil {
			return obs, err
		}
		raw, sqlErr := s.psql(ctx, "SELECT system_identifier FROM pg_control_system();")
		finish, err := s.now()
		if err != nil {
			return obs, err
		}
		if sqlErr == nil {
			if strings.TrimSpace(string(raw)) != fmt.Sprint(systemID) || obs.FirstFailedNS != 0 {
				return obs, errors.New("source SQL endpoint changed identity or reappeared")
			}
			obs.LastSuccessNS = finish
		} else if code := exitCode(sqlErr); code == 2 {
			// psql's connection failure and pg_isready's no-response exit
			// must both be observed. A SQL/permission error is not an outage.
			if obs.FirstFailedNS == 0 {
				obs.FirstFailedStartNS, obs.FirstFailedNS, obs.PsqlExitCode = start, finish, code
			}
			_, err := s.run(ctx, filepath.Join(s.pgBin, "pg_isready"),
				"--quiet", "--timeout=1", "--dbname=service="+s.service)
			obs.PgIsReadyExitCode = exitCode(err)
			if obs.PgIsReadyExitCode == 2 {
				obs.NoResponseNS, err = s.now()
				return obs, err
			}
			if code := obs.PgIsReadyExitCode; code != -1 && code != 1 {
				return obs, errors.New("pg_isready did not report an unavailable source")
			}
		} else {
			return obs, fmt.Errorf("SQL probe did not fail with a connection error: %w", sqlErr)
		}
		select {
		case <-ctx.Done():
			return obs, fmt.Errorf("await shared client-path SQL unavailability: %w", ctx.Err())
		case <-tick.C:
		}
	}
}

func removeSource(ctx context.Context, source mediaSource, d localDocker, b sourceBinding, r *outageReceipt) error {
	now, err := source.now()
	if err != nil {
		return err
	}
	ctx, cancel, err := phaseDeadline(ctx, now, r.T0NS, deleteWindow)
	if err != nil {
		return fmt.Errorf("source deletion: %w", err)
	}
	defer cancel()
	f := b.Run
	c, err := d.container(ctx, f.Container)
	if err != nil {
		return err
	}
	v, err := d.volume(ctx, f.SourceVolume)
	if err != nil {
		return err
	}
	data, archive, err := ownedMounts(c, v, f, b.Labels, r.FinalSource.DataDirectory, source.archive)
	if err != nil || data != b.Data || archive != b.Archive || !sameVolume(v, b.Volume) || c.Image != b.Image ||
		!killedSource(c.State, b.StartedAt) || c.RestartCount != 0 {
		return errors.New("sealed exited source or mount inventory changed before deletion")
	}
	if err := d.checkVolumeSet(ctx, f, []string{f.Container}, false); err != nil {
		return err
	}
	if err := removeDockerObject(ctx, d, "container", f.Container, source.now, &r.ContainerGone); err != nil {
		return err
	}
	if err := d.checkVolumeSet(ctx, f, nil, false); err != nil {
		return err
	}
	if err := removeDockerObject(ctx, d, "volume", f.SourceVolume, source.now, &r.VolumeRemoved); err != nil {
		return err
	}
	if err := d.checkVolumeSet(ctx, f, nil, true); err != nil {
		return err
	}
	r.VolumeAbsent, err = source.now()
	if err == nil && (r.VolumeAbsent-r.T0NS > int64(deleteWindow) || ctx.Err() != nil) {
		err = errors.New("source volume absence missed the 30-second gate")
	}
	return err
}

func removeDockerObject(ctx context.Context, d localDocker, kind, id string, now restoreledger.Clock, stamp *int64) error {
	out, err := d.command(ctx, kind, "rm", id)
	if err != nil {
		return fmt.Errorf("remove sealed source %s %s: %w", kind, id, err)
	}
	if strings.TrimSpace(string(out)) != id {
		return fmt.Errorf("Docker did not acknowledge removal of sealed source %s %s", kind, id)
	}
	*stamp, err = now()
	return err
}
