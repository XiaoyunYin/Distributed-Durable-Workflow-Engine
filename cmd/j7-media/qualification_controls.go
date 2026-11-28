package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"durable-agent-execution-engine/internal/invariants"
	"durable-agent-execution-engine/internal/restoreledger"
)

func changeMarkerRows(before, source invariants.Trace, id string) (invariants.Trace, error) {
	out := before
	out.Snapshot = make(map[string]invariants.SnapshotTable, len(before.Snapshot))
	for name, table := range before.Snapshot {
		rows := slices.Clone(table.Rows)
		rowID := func(raw json.RawMessage) string {
			var ref struct {
				ID string `json:"workflow_id"`
			}
			_ = json.Unmarshal(raw, &ref) // checked by SnapshotIntegrity
			return ref.ID
		}
		rows = slices.DeleteFunc(rows, func(row json.RawMessage) bool { return rowID(row) == id })
		for _, row := range source.Snapshot[name].Rows {
			if rowID(row) == id {
				rows = append(rows, row)
			}
		}
		raw, err := json.Marshal(rows)
		if err != nil {
			return out, err
		}
		out.Snapshot[name] = invariants.SnapshotTable{Count: len(rows), SHA256: restoreledger.SHA256(raw), Rows: rows}
	}
	out.Workflows = mergeMarkerProjection(out.Workflows, source.Workflows, id, func(r invariants.WorkflowRecord) string { return r.WorkflowID })
	out.History = mergeMarkerProjection(out.History, source.History, id, func(r invariants.HistoryRecord) string { return r.WorkflowID })
	out.Results = mergeMarkerProjection(out.Results, source.Results, id, func(r invariants.AcceptedResult) string { return r.WorkflowID })
	out.Submissions = mergeMarkerProjection(out.Submissions, source.Submissions, id, func(r invariants.SubmissionRecord) string { return r.WorkflowID })
	out.Attempts = mergeMarkerProjection(out.Attempts, source.Attempts, id, func(r invariants.AttemptRecord) string { return r.WorkflowID })
	out.Outbox = mergeMarkerProjection(out.Outbox, source.Outbox, id, func(r invariants.OutboxRecord) string { return r.WorkflowID })
	out.Wakeups = mergeMarkerProjection(out.Wakeups, source.Wakeups, id, func(r invariants.WakeupRecord) string { return r.WorkflowID })
	out.Checkpoints = mergeMarkerProjection(out.Checkpoints, source.Checkpoints, id, func(r invariants.CheckpointRecord) string { return r.WorkflowID })
	out.Timers = mergeMarkerProjection(out.Timers, source.Timers, id, func(r invariants.TimerRecord) string { return r.WorkflowID })
	return out, invariants.SnapshotIntegrity(out)
}

func mergeMarkerProjection[T any](before, tail []T, id string, key func(T) string) []T {
	rows := slices.DeleteFunc(slices.Clone(before), func(row T) bool { return key(row) == id })
	for _, row := range tail {
		if key(row) == id {
			rows = append(rows, row)
		}
	}
	return rows
}

func checkControlCode(err error, expected string) error {
	if err == nil || !strings.HasPrefix(err.Error(), expected+":") {
		return fmt.Errorf("control %s did not reject for the registered reason: %v", expected, err)
	}
	return nil
}

func (d drillInputs) checkControls(source invariants.Trace, archive, scratch string) ([]string, error) {
	var v drillVerdict
	if _, err := d.checkPrewrite(&v); err != nil {
		return nil, err
	}
	if err := invariants.SnapshotIntegrity(source); err != nil {
		return nil, err
	}
	want := ledgerMarkers(d.plan, d.ledger)
	for i, control := range []struct {
		id   string
		tail invariants.Trace
	}{
		{want[6].ID, invariants.Trace{}}, {want[14].ID, source},
	} {
		copy, err := changeMarkerRows(d.pre, control.tail, control.id)
		if err != nil {
			return nil, err
		}
		_, err = verifyPrewrite(copy, want, d.cohorts, d.proof, d.sinkBefore)
		if err := checkControlCode(err, qualificationControls[i]); err != nil {
			return nil, err
		}
	}
	last, _, err := cutGeometry(d.cut.Point, d.backup.Source.Timeline, d.cut.PreSwitch.FlushLSN)
	if err != nil {
		return nil, err
	}
	orig, err := inventory(archive, d.backup, last)
	if err != nil || !slices.Equal(orig, d.loss.Inventory) {
		return nil, errors.New("healthy archive sibling does not match witnessed media")
	}
	copyDir, err := os.MkdirTemp(scratch, "j7-archive-control-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(copyDir)
	gap := segmentName(d.backup.Source.Timeline, last-1)
	for _, f := range orig {
		if f.Name == gap {
			continue
		}
		raw, err := evidenceFile(archive, f.Name)
		if err != nil {
			return nil, err
		}
		if err := writeEvidenceBytes(filepath.Join(copyDir, f.Name), raw); err != nil {
			return nil, err
		}
	}
	_, gapErr := inventory(copyDir, d.backup, last)
	if err := checkControlCode(gapErr, qualificationControls[2]); err != nil {
		return nil, err
	}
	var rows []renderedConsumer
	if err := d.b.decode("consumers.json", &rows); err != nil {
		return nil, err
	}
	if err := verifyGenerationRender(rows, d.freshKafka.Target.RuntimeEndpoint); err != nil {
		return nil, err
	}
	for i := range rows {
		if rows[i].Role == "scheduler" {
			for j, entry := range rows[i].Env {
				if entry.Name == "RUNTIME_KAFKA_GENERATION" {
					rows[i].Env = slices.Delete(rows[i].Env, j, j+1)
					break
				}
			}
		}
	}
	if err := checkControlCode(verifyGenerationRender(rows, d.freshKafka.Target.RuntimeEndpoint), qualificationControls[3]); err != nil {
		return nil, err
	}
	return qualificationControls, nil
}

func runQualificationControls(args []string, out, diagnostics io.Writer) error {
	flags := flag.NewFlagSet(args[0], flag.ContinueOnError)
	flags.SetOutput(diagnostics)
	dir := flags.String("dir", "", "indexed qualification evidence")
	index := flags.String("index-sha256", "", "digest of its evidence index")
	archive := flags.String("archive", "", "original private archive directory")
	tail := flags.String("source-tail", "", "source observer snapshot taken before deletion")
	tailSHA := flags.String("source-tail-sha256", "", "digest of its source observer witness")
	scratch := flags.String("scratch", "", "private directory for archive control copies")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 || !filepath.IsAbs(*tail) {
		return errors.New("qualification controls require private canonical inputs")
	}
	if err := errors.Join(checkedEvidenceDir(*archive), checkedEvidenceDir(*scratch)); err != nil {
		return err
	}
	if within(*archive, *scratch) {
		return errors.New("qualification control copies must stay outside the original archive")
	}
	b, err := openBundle(*dir, *index)
	if err != nil || b.index.Projection {
		return errors.New("qualification controls require the private observed sibling")
	}
	d, err := readDrill(b)
	if err != nil {
		return err
	}
	var v drillVerdict
	if err := b.decode("verdict.json", &v); err != nil || v.Status != "passed" {
		return errors.New("qualification controls require the passing independent sibling verdict")
	}
	if err := phaseSourceTail(d, *tailSHA); err != nil {
		return err
	}
	source, err := loadTraceArtifact(*tail, *tailSHA)
	if err != nil {
		return err
	}
	codes, err := d.checkControls(source, *archive, *scratch)
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(struct {
		Codes   []string `json:"codes"`
		Healthy bool     `json:"healthy_sibling"`
	}{codes, true})
}

func phaseSourceTail(d drillInputs, expected string) error {
	w, err := phaseWitness(d.ledger, "j7-source-tail", d.cut.CompletedNS, d.loss.KillNS)
	if err != nil || w.EvidenceSHA != expected || w.Timeline != d.backup.Source.Timeline {
		return errors.New("post-cut M15 control source lacks a ledger-bound observer export")
	}
	return nil
}
