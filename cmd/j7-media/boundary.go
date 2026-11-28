package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"durable-agent-execution-engine/internal/j7markers"
	"durable-agent-execution-engine/internal/restoreledger"
)

const (
	cutWindow     = 3 * time.Second
	killWindow    = 2 * time.Second
	confirmWindow = 2 * time.Second
	deleteWindow  = 30 * time.Second
)

type mediaSource struct {
	pgBin, service, archive string
	run                     runTool
	now                     restoreledger.Clock
	syncDir                 func(string) error
}

func (s mediaSource) psql(ctx context.Context, commands ...string) ([]byte, error) {
	args := []string{"--no-psqlrc", "--no-password", "--tuples-only", "--no-align",
		"--set=ON_ERROR_STOP=1", "--dbname=service=" + s.service}
	for _, command := range commands {
		args = append(args, "--command="+command)
	}
	return s.run(ctx, filepath.Join(s.pgBin, "psql"), args...)
}

type sourceSample struct {
	StartedNS     int64        `json:"started_ns"`
	CompletedNS   int64        `json:"completed_ns"`
	SystemID      uint64       `json:"system_identifier"`
	Timeline      uint32       `json:"timeline"`
	DataDirectory string       `json:"data_directory"`
	FlushLSN      string       `json:"flush_lsn"`
	InsertLSN     string       `json:"insert_lsn"`
	DatabaseBytes int64        `json:"database_bytes"`
	ClusterBytes  int64        `json:"cluster_database_bytes"`
	WALBytes      int64        `json:"wal_bytes"`
	Markers       []markerSeen `json:"markers"`
	EffectSHA256  string       `json:"effect_result_sha256,omitempty"`
}

type markerExpected struct {
	ID            string          `json:"id"`
	Namespace     string          `json:"namespace"`
	Key           string          `json:"submission_key"`
	Definition    string          `json:"definition_id"`
	Input         json.RawMessage `json:"input"`
	PayloadSHA256 string          `json:"submission_payload_hash"`
}

type markerSeen struct {
	ID      string `json:"id"`
	State   string `json:"state"`
	Matches bool   `json:"matches"`
}

func ledgerMarkers(plan j7markers.Plan, report restoreledger.Report) []markerExpected {
	var markers []markerExpected
	for _, x := range report.Exchanges {
		if x.Operation.Phase != "markers" {
			continue
		}
		m := plan.Markers[x.Operation.Sequence-1]
		markers = append(markers, markerExpected{
			ID: m.WorkflowID, Namespace: plan.Fixture.Namespace, Key: m.SubmissionKey,
			Definition: plan.Fixture.DefinitionID, Input: m.ExpectedOutput, PayloadSHA256: x.Outcome.PayloadHash,
		})
	}
	return markers
}

// Parameters are hex-encoded JSON parsed as values by PostgreSQL. Fixture
// names and IDs never become SQL syntax.
func sqlJSON(raw []byte) string {
	return "convert_from(decode('" + hex.EncodeToString(raw) + "', 'hex'), 'UTF8')::jsonb"
}

func (s mediaSource) sample(ctx context.Context, want []markerExpected, effectID string) (sourceSample, error) {
	var sample sourceSample
	started, err := s.now()
	if err != nil {
		return sample, err
	}
	input, err := json.Marshal(struct {
		Markers []markerExpected `json:"markers"`
		Effect  string           `json:"effect"`
	}{want, effectID})
	if err != nil {
		return sample, err
	}
	query := fmt.Sprintf(`
WITH fixture AS (SELECT %s AS inputs),
    markers AS (
        SELECT expected->>'id' AS id, w.state,
            COALESCE(w.namespace = expected->>'namespace'
                AND w.submission_key = expected->>'submission_key'
                AND w.definition_id = expected->>'definition_id' AND w.definition_version = 1
                AND w.submission_payload_hash = expected->>'submission_payload_hash'
                AND n.input = expected->'input', false) AS matches,
            ordinal
        FROM fixture CROSS JOIN LATERAL
             jsonb_array_elements(inputs->'markers') WITH ORDINALITY AS items(expected, ordinal)
        LEFT JOIN engine.workflow_executions w ON w.workflow_id = expected->>'id'
        LEFT JOIN engine.node_instances n ON n.workflow_id = w.workflow_id
            AND n.node_id = 'pure.echo' AND n.iteration = 0
    ),
    effect AS (
        SELECT encode(sha256(convert_to(n.accepted_result::text, 'UTF8')), 'hex') AS digest
        FROM fixture JOIN engine.workflow_executions w ON w.workflow_id = inputs->>'effect'
        JOIN engine.node_instances n ON n.workflow_id = w.workflow_id
        JOIN engine.activity_attempts a ON a.workflow_id = n.workflow_id
            AND a.node_id = n.node_id AND a.iteration = n.iteration
        WHERE w.state = 'SUCCEEDED' AND n.state = 'SUCCEEDED'
          AND a.attempt_number = 1 AND a.is_current AND a.effect_class = 'COOPERATING_EFFECT'
          AND a.state = 'SUCCEEDED' AND a.outcome_disposition = 'KNOWN_SUCCESS'
          AND n.accepted_result IS NOT NULL
    )
SELECT json_build_object(
    'system_identifier', (SELECT system_identifier FROM pg_control_system()),
    'timeline', (SELECT timeline_id FROM pg_control_checkpoint()),
    'data_directory', current_setting('data_directory'),
    'flush_lsn', pg_current_wal_flush_lsn(), 'insert_lsn', pg_current_wal_insert_lsn(),
    'database_bytes', pg_database_size(current_database()),
    'cluster_database_bytes', (SELECT sum(pg_database_size(oid)) FROM pg_database),
    'wal_bytes', (SELECT COALESCE(sum(size), 0) FROM pg_ls_waldir()),
    'markers', (SELECT json_agg(json_build_object('id', id, 'state', state, 'matches', matches)
                               ORDER BY ordinal) FROM markers),
    'effect_result_sha256', (SELECT digest FROM effect)
);`, sqlJSON(input))
	out, err := s.psql(ctx, query)
	if err != nil {
		return sample, fmt.Errorf("source SQL visibility probe: %w", err)
	}
	complete, err := s.now()
	if err != nil {
		return sample, err
	}
	if err := json.Unmarshal(out, &sample); err != nil {
		return sample, fmt.Errorf("decode source visibility: %w", err)
	}
	sample.StartedNS, sample.CompletedNS = started, complete
	return sample, nil
}

func (p sourceSample) check(source sourceState, want []markerExpected, terminal int) error {
	if p.StartedNS <= 0 || p.CompletedNS < p.StartedNS || p.SystemID != source.SystemID ||
		p.Timeline != source.Timeline || p.DataDirectory != source.DataDirectory ||
		p.DatabaseBytes <= 0 || p.DatabaseBytes > maxDatabaseSize ||
		p.ClusterBytes < p.DatabaseBytes || p.WALBytes < walSegmentBytes || len(p.Markers) != len(want) {
		return errors.New("source visibility probe has mismatched identity, resources or population")
	}
	for i, row := range p.Markers {
		if !row.Matches || row.ID != want[i].ID || row.State == "" ||
			(i < terminal && row.State != "SUCCEEDED") {
			return fmt.Errorf("source marker %s fails visibility or terminal expectation", want[i].ID)
		}
	}
	return nil
}

type cutReceipt struct {
	Schema       string                `json:"schema"`
	BootID       string                `json:"boot_id"`
	Source       string                `json:"source_instance"`
	BackupSHA256 string                `json:"backup_receipt_sha256"`
	LedgerSHA256 string                `json:"cut_input_ledger_sha256"`
	AckNS        int64                 `json:"m12_ack_ns"`
	StartedNS    int64                 `json:"started_ns"`
	CompletedNS  int64                 `json:"completed_ns"`
	Cohorts      restoreledger.Witness `json:"pre_cut_cohorts"`
	PreSwitch    sourceSample          `json:"pre_switch"`
	Point        anchor                `json:"restore_point"`
	EndLSN       string                `json:"exclusive_end_lsn"`
	Inventory    []archivedFile        `json:"archive_inventory"`
}

func closeBoundary(ctx context.Context, s mediaSource, backup backupReceipt, backupSHA string,
	plan j7markers.Plan, report restoreledger.Report, origin int64) (cutReceipt, error) {
	r := cutReceipt{Schema: "j7.archive-cut.v1", BootID: report.Header.BootID, Source: report.Header.Source,
		BackupSHA256: backupSHA, LedgerSHA256: report.Digest}
	var err error
	r.AckNS, err = j7markers.BoundaryAck(plan, origin, report, false)
	if err != nil {
		return r, err
	}
	r.StartedNS, err = s.now()
	if err != nil {
		return r, err
	}
	ctx, cancel, err := phaseDeadline(ctx, r.StartedNS, r.AckNS, cutWindow)
	if err != nil {
		return r, fmt.Errorf("M12 archive cut: %w", err)
	}
	defer cancel()
	r.Cohorts, err = phaseWitness(report, "pre-cut-cohorts", 1, r.AckNS)
	if err != nil {
		return r, err
	}
	if err := backup.Source.validate(); err != nil {
		return r, err
	}
	want := ledgerMarkers(plan, report)
	r.PreSwitch, err = s.sample(ctx, want, "")
	if err != nil {
		return r, err
	}
	if err := r.PreSwitch.check(backup.Source, want, 8); err != nil {
		return r, err
	}
	r.Point.Name = "j7-" + restoreledger.SHA256([]byte(plan.Fixture.Campaign + "\x00" + plan.Fixture.Drill))[:32] + "-cut"
	condition := fmt.Sprintf("(SELECT system_identifier FROM pg_control_system()) = %d AND "+
		"(SELECT timeline_id FROM pg_control_checkpoint()) = %d AND NOT pg_is_in_recovery()",
		backup.Source.SystemID, backup.Source.Timeline)
	mark := fmt.Sprintf(`WITH point AS MATERIALIZED (
SELECT CASE WHEN %s THEN pg_create_restore_point('%s') END AS lsn)
SELECT json_build_object('lsn', lsn, 'segment', pg_walfile_name(lsn)) FROM point;`, condition, r.Point.Name)
	out, err := s.psql(ctx, mark, "SELECT CASE WHEN "+condition+" THEN pg_switch_wal() END;")
	if err != nil {
		return r, fmt.Errorf("create named restore point and switch WAL: %w", err)
	}
	raw, switched, found := strings.Cut(strings.TrimSpace(string(out)), "\n")
	observed := anchor{}
	if !found || json.Unmarshal([]byte(raw), &observed) != nil {
		return r, errors.New("missing restore-point and switch evidence")
	}
	r.Point.LSN, r.Point.Segment, r.Point.SwitchLSN = observed.LSN, observed.Segment, strings.TrimSpace(switched)
	index, end, err := cutGeometry(r.Point, backup.Source.Timeline, r.PreSwitch.FlushLSN)
	if err != nil {
		return r, err
	}
	r.EndLSN = fmt.Sprintf("%X/%X", end>>32, uint32(end))
	if err := awaitArchive(ctx, s.archive, r.Point.Segment); err != nil {
		return r, err
	}
	if err := awaitArchivePublication(ctx, s.archive); err != nil {
		return r, err
	}
	r.Inventory, err = inventory(s.archive, backup, index)
	if err != nil {
		return r, err
	}
	if err := s.syncDir(s.archive); err != nil {
		return r, fmt.Errorf("sync final archive cut: %w", err)
	}
	r.CompletedNS, err = s.now()
	if err == nil && (r.CompletedNS < r.StartedNS || r.CompletedNS-r.AckNS > int64(cutWindow) || ctx.Err() != nil) {
		err = errors.New("archive proof missed the M12 cut deadline")
	}
	return r, err
}

func awaitArchivePublication(ctx context.Context, dir string) error {
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		files, err := os.ReadDir(dir)
		if err != nil {
			return err
		}
		if !slices.ContainsFunc(files, func(file os.DirEntry) bool {
			return strings.HasPrefix(file.Name(), ".j7-archive-")
		}) {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("archive publication still staged: %w", ctx.Err())
		case <-tick.C:
		}
	}
}

func phaseDeadline(ctx context.Context, now, origin int64, window time.Duration) (context.Context, context.CancelFunc, error) {
	if origin <= 0 || now < origin || now-origin >= int64(window) {
		return nil, nil, errors.New("phase deadline elapsed or origin is invalid")
	}
	bounded, cancel := context.WithTimeout(ctx, window-time.Duration(now-origin))
	return bounded, cancel, nil
}

func cutGeometry(p anchor, timeline uint32, flushLSN string) (uint64, uint64, error) {
	point, pointErr := parseLSN(p.LSN)
	switched, switchErr := parseLSN(p.SwitchLSN)
	flush, flushErr := parseLSN(flushLSN)
	if pointErr != nil || switchErr != nil || flushErr != nil || point == 0 || flush >= point {
		return 0, 0, errors.New("restore point does not follow the visible source flush")
	}
	index := (point - 1) / walSegmentBytes
	end := (index + 1) * walSegmentBytes
	if p.Segment != segmentName(timeline, index) || switched <= point || switched > end {
		return 0, 0, errors.New("WAL switch did not close the restore-point segment")
	}
	return index, end, nil
}

// Inventory accounts for every file on the private archive filesystem. Data
// segments may start before the backup range, but must be contiguous through C.
func inventory(dir string, backup backupReceipt, last uint64) ([]archivedFile, error) {
	if len(backup.RequiredWAL) != 1 || len(backup.Archive) < 2 || len(backup.Anchors) != 2 ||
		backup.RequiredWAL[0].Timeline != backup.Source.Timeline {
		return nil, errors.New("incomplete verified backup and anchor inventory")
	}
	firstLSN, err := parseLSN(backup.RequiredWAL[0].Start)
	if err != nil {
		return nil, err
	}
	first := firstLSN / walSegmentBytes
	if last < first || last-first < 2 {
		return nil, errors.New("archive requires three contiguous segments and an interior control segment")
	}
	anchorLSN, err := parseLSN(backup.Anchors[1].LSN)
	if err != nil || anchorLSN == 0 || (anchorLSN-1)/walSegmentBytes >= last {
		return nil, errors.New("final cut must follow both prepared archive anchors")
	}
	minimum := first
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	segments := make(map[string]bool)
	for _, entry := range entries {
		name := entry.Name()
		if _, err := archiveSizeLimit(name); err != nil || len(name) < 24 ||
			!strings.HasPrefix(name, fmt.Sprintf("%08X", backup.Source.Timeline)) {
			return nil, fmt.Errorf("unexpected file in archive: %s", name)
		}
		log, _ := strconv.ParseUint(name[8:16], 16, 32)
		seg, _ := strconv.ParseUint(name[16:24], 16, 32)
		index := log*segmentsPerLog + seg
		if index > last {
			return nil, fmt.Errorf("unexpected WAL beyond cut: %s", name)
		}
		if len(name) == 24 {
			minimum = min(minimum, index)
			segments[name] = true
		}
		names = append(names, name)
	}
	required, err := segmentRange(backup.Source.Timeline, minimum, last)
	if err != nil {
		return nil, err
	}
	for _, name := range required {
		if !segments[name] {
			return nil, fmt.Errorf("WAL_ARCHIVE_GAP: %s", name)
		}
	}
	for _, name := range names {
		if !segments[name[:24]] {
			return nil, fmt.Errorf("archive metadata has no matching segment: %s", name)
		}
	}
	files, err := readArchive(dir, names)
	if err != nil {
		return nil, err
	}
	var total int64
	for _, file := range files {
		total += file.Size
	}
	if total > maxArchiveBytes {
		return nil, errors.New("archive exceeds 512 MiB")
	}
	for _, prior := range backup.Archive {
		if !slices.Contains(files, prior) {
			return nil, fmt.Errorf("prepared archive segment changed: %s", prior.Name)
		}
	}
	return files, nil
}
