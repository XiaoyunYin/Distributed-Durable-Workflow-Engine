package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"durable-agent-execution-engine/internal/dur050"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var columns = []string{"record_type", "sequence", "workflow_id", "scheduled_at_utc", "observed_at_utc", "state", "created_at_db", "updated_at_db", "terminal_transition_at_db", "query_duration_ms", "poll_gap_ms", "max_poll_gap_ms", "max_preterminal_gap_ms", "observer_qps", "valid", "reason", "scheduled_at_monotonic_ns", "observed_at_monotonic_ns"}

type workflowSnapshot struct {
	ID         string
	State      string
	CreatedAt  time.Time
	UpdatedAt  time.Time
	TerminalAt *time.Time
}

func main() {
	mode := flag.String("mode", "", "fine (one workflow every 50ms) or batch (IDs once per second)")
	workflowID := flag.String("workflow-id", "", "preassigned workflow ID for fine mode")
	workflowIDsPath := flag.String("workflow-ids-file", "", "newline-delimited workflow IDs for batch mode")
	outputPath := flag.String("output", "", "new CSV output path; existing files are never overwritten")
	databaseURL := flag.String("database-url", os.Getenv("DUR050_OBSERVER_DATABASE_URL"), "read-only PostgreSQL DSN; defaults to DUR050_OBSERVER_DATABASE_URL")
	maximumDuration := flag.Duration("timeout", 30*time.Minute, "maximum observation duration")
	flag.Parse()
	if err := run(*mode, *workflowID, *workflowIDsPath, *outputPath, *databaseURL, *maximumDuration); err != nil {
		log.Fatal(err)
	}
}

func run(mode, workflowID, workflowIDsPath, outputPath, databaseURL string, maximumDuration time.Duration) error {
	if outputPath == "" || databaseURL == "" || maximumDuration <= 0 {
		return errors.New("-output, a read-only database URL, and a positive -timeout are required")
	}
	if !dur050.HasSharedMonotonicClock() {
		return errors.New("DUR-050 measurement requires the shared Linux CLOCK_MONOTONIC clock")
	}
	if mode != "fine" && mode != "batch" {
		return errors.New("-mode must be fine or batch")
	}
	if mode == "fine" && workflowID == "" {
		return errors.New("-workflow-id is required in fine mode")
	}
	var ids []string
	if mode == "batch" {
		if workflowIDsPath == "" {
			return errors.New("-workflow-ids-file is required in batch mode")
		}
		var err error
		ids, err = readWorkflowIDs(workflowIDsPath)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			return errors.New("batch observer needs at least one workflow ID")
		}
	}

	file, err := os.OpenFile(outputPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create immutable observer output: %w", err)
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	if err := writer.Write(columns); err != nil {
		return err
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), maximumDuration)
	defer cancel()
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return fmt.Errorf("parse observer database URL: %w", err)
	}
	config.MaxConns = 2
	config.MinConns = 1
	if config.ConnConfig.RuntimeParams == nil {
		config.ConnConfig.RuntimeParams = make(map[string]string)
	}
	config.ConnConfig.RuntimeParams["default_transaction_read_only"] = "on"
	config.ConnConfig.RuntimeParams["application_name"] = "dur050-read-only-observer"
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return fmt.Errorf("open observer pool: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("connect as read-only observer: %w", err)
	}
	if err := assertReadOnly(ctx, pool); err != nil {
		return err
	}

	switch mode {
	case "fine":
		return observeFine(ctx, pool, writer, workflowID)
	case "batch":
		return observeBatch(ctx, pool, writer, ids)
	default:
		return errors.New("unsupported observer mode")
	}
}

func assertReadOnly(ctx context.Context, pool *pgxpool.Pool) error {
	var readOnly bool
	if err := pool.QueryRow(ctx, "SHOW transaction_read_only").Scan(&readOnly); err != nil {
		return fmt.Errorf("verify observer read-only mode: %w", err)
	}
	if !readOnly {
		return errors.New("observer connection is not transaction-read-only")
	}
	return nil
}

func observeFine(ctx context.Context, pool *pgxpool.Pool, writer *csv.Writer, workflowID string) error {
	return observe(ctx, writer, dur050.PilotPollInterval, dur050.PilotMaxPollGap,
		[]string{workflowID}, func(ctx context.Context, ids []string) (map[string]workflowSnapshot, error) {
			var snapshot workflowSnapshot
			err := pool.QueryRow(ctx, `SELECT w.workflow_id, w.state, w.created_at, w.updated_at, terminal.created_at
				FROM engine.workflow_executions AS w
				LEFT JOIN LATERAL (
					SELECT created_at FROM engine.transition_history
					WHERE workflow_id = w.workflow_id
						AND new_state IN ('SUCCEEDED', 'FAILED', 'REJECTED', 'CANCELED', 'ABANDONED')
					ORDER BY revision DESC LIMIT 1
				) AS terminal ON true
				WHERE w.workflow_id = $1`, workflowID).Scan(
				&snapshot.ID, &snapshot.State, &snapshot.CreatedAt, &snapshot.UpdatedAt, &snapshot.TerminalAt)
			if errors.Is(err, pgx.ErrNoRows) {
				return map[string]workflowSnapshot{}, nil
			}
			if err != nil {
				return nil, err
			}
			return map[string]workflowSnapshot{snapshot.ID: snapshot}, nil
		})
}

func observeBatch(ctx context.Context, pool *pgxpool.Pool, writer *csv.Writer, ids []string) error {
	return observe(ctx, writer, dur050.BatchPollInterval, 2*dur050.BatchPollInterval, ids,
		func(ctx context.Context, ids []string) (map[string]workflowSnapshot, error) {
			rows, err := pool.Query(ctx, `SELECT w.workflow_id, w.state, w.created_at, w.updated_at, terminal.created_at
				FROM engine.workflow_executions AS w
				LEFT JOIN LATERAL (
					SELECT created_at FROM engine.transition_history
					WHERE workflow_id = w.workflow_id
						AND new_state IN ('SUCCEEDED', 'FAILED', 'REJECTED', 'CANCELED', 'ABANDONED')
					ORDER BY revision DESC LIMIT 1
				) AS terminal ON true
				WHERE w.workflow_id = ANY($1::text[])`, ids)
			if err != nil {
				return nil, err
			}
			defer rows.Close()
			result := make(map[string]workflowSnapshot, len(ids))
			for rows.Next() {
				var snapshot workflowSnapshot
				if err := rows.Scan(&snapshot.ID, &snapshot.State, &snapshot.CreatedAt, &snapshot.UpdatedAt, &snapshot.TerminalAt); err != nil {
					return nil, err
				}
				result[snapshot.ID] = snapshot
			}
			return result, rows.Err()
		})
}

func observe(ctx context.Context, writer *csv.Writer, interval, maximumGap time.Duration, ids []string,
	query func(context.Context, []string) (map[string]workflowSnapshot, error)) error {
	started := time.Now()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	// Include the startup-to-first-read interval too; otherwise a stalled first
	// query could look like a zero-gap sample simply because no previous poll
	// timestamp exists yet.
	lastObserved := started
	var sequence, terminalWorkflowsSeen, failedQueries int64
	var maxGap time.Duration
	maxGapBeforeTerminal := make(map[string]time.Duration, len(ids))
	terminalSeen := make(map[string]bool, len(ids))
	allTerminal := false
	for !allTerminal {
		select {
		case <-ctx.Done():
			return fmt.Errorf("observer timed out after %s: %w", time.Since(started), ctx.Err())
		case scheduled := <-ticker.C:
			sequence++
			scheduledMonoNow, err := dur050.SharedMonotonicNanoseconds()
			if err != nil {
				return fmt.Errorf("read shared monotonic clock before observer query: %w", err)
			}
			scheduledMono := scheduledMonoNow - time.Since(scheduled).Nanoseconds()
			queryStarted := time.Now()
			snapshots, err := query(ctx, ids)
			observed := time.Now()
			observedMono, clockErr := dur050.SharedMonotonicNanoseconds()
			if clockErr != nil {
				return fmt.Errorf("read shared monotonic clock after observer query: %w", clockErr)
			}
			queryDuration := observed.Sub(queryStarted)
			gap := observed.Sub(lastObserved)
			if gap > maxGap {
				maxGap = gap
			}
			for _, id := range ids {
				if gap > maxGapBeforeTerminal[id] {
					maxGapBeforeTerminal[id] = gap
				}
			}
			lastObserved = observed
			if err != nil {
				failedQueries++
				if writeErr := writeRow(writer, append([]string{"poll_error", fmt.Sprint(sequence), "", scheduled.UTC().Format(time.RFC3339Nano), observed.UTC().Format(time.RFC3339Nano), "", "", "", "", fmt.Sprintf("%.3f", float64(queryDuration.Microseconds())/1000), fmt.Sprintf("%.3f", float64(gap.Microseconds())/1000), "", "", "", "false", err.Error()}, fmt.Sprint(scheduledMono), fmt.Sprint(observedMono))); writeErr != nil {
					return writeErr
				}
				continue
			}
			allTerminal = true
			for _, id := range ids {
				snapshot, found := snapshots[id]
				state := "NOT_FOUND"
				createdAt, updatedAt, terminalAt := "", "", ""
				if found {
					state = snapshot.State
					createdAt = snapshot.CreatedAt.UTC().Format(time.RFC3339Nano)
					updatedAt = snapshot.UpdatedAt.UTC().Format(time.RFC3339Nano)
					if snapshot.TerminalAt != nil {
						terminalAt = snapshot.TerminalAt.UTC().Format(time.RFC3339Nano)
					}
					if dur050.IsTerminalWorkflowState(state) {
						if !terminalSeen[id] {
							terminalWorkflowsSeen++
							terminalSeen[id] = true
							preterminalGap := maxGapBeforeTerminal[id]
							terminalValid := dur050.PollGapValid(preterminalGap, maximumGap)
							terminalReason := ""
							if !terminalValid {
								terminalReason = "preterminal_observation_gap_exceeded"
							}
							if writeErr := writeRow(writer, append([]string{"first_terminal_observation", fmt.Sprint(sequence), id, scheduled.UTC().Format(time.RFC3339Nano), observed.UTC().Format(time.RFC3339Nano), state, createdAt, updatedAt, terminalAt, fmt.Sprintf("%.3f", float64(queryDuration.Microseconds())/1000), fmt.Sprintf("%.3f", float64(gap.Microseconds())/1000), "", fmt.Sprintf("%.3f", float64(preterminalGap.Microseconds())/1000), "", fmt.Sprint(terminalValid), terminalReason}, fmt.Sprint(scheduledMono), fmt.Sprint(observedMono))); writeErr != nil {
								return writeErr
							}
						}
					} else {
						allTerminal = false
					}
				} else {
					allTerminal = false
				}
				valid := dur050.PollGapValid(gap, maximumGap)
				reason := ""
				if !valid {
					reason = "observer_gap_exceeded"
				}
				if writeErr := writeRow(writer, append([]string{"snapshot", fmt.Sprint(sequence), id, scheduled.UTC().Format(time.RFC3339Nano), observed.UTC().Format(time.RFC3339Nano), state, createdAt, updatedAt, terminalAt, fmt.Sprintf("%.3f", float64(queryDuration.Microseconds())/1000), fmt.Sprintf("%.3f", float64(gap.Microseconds())/1000), "", "", "", fmt.Sprint(valid), reason}, fmt.Sprint(scheduledMono), fmt.Sprint(observedMono))); writeErr != nil {
					return writeErr
				}
			}
		}
	}
	elapsed := time.Since(started)
	qps := float64(sequence) / elapsed.Seconds()
	observedMono, err := dur050.SharedMonotonicNanoseconds()
	if err != nil {
		return fmt.Errorf("read shared monotonic clock for observer summary: %w", err)
	}
	valid := failedQueries == 0 && (maxGap == 0 || dur050.PollGapValid(maxGap, maximumGap))
	reason := ""
	if failedQueries != 0 {
		reason = "observer_query_failures"
	} else if !valid {
		reason = "observer_sampling_gap_exceeded"
	}
	if err := writeRow(writer, append([]string{"summary", fmt.Sprint(sequence), "", "", time.Now().UTC().Format(time.RFC3339Nano), "", "", "", "", "", "", fmt.Sprintf("%.3f", float64(maxGap.Microseconds())/1000), "", fmt.Sprintf("%.6f", qps), fmt.Sprint(valid), fmt.Sprintf("queries=%d terminal_workflows_seen=%d failed_queries=%d %s", sequence, terminalWorkflowsSeen, failedQueries, reason)}, "", fmt.Sprint(observedMono))); err != nil {
		return err
	}
	if !valid {
		return fmt.Errorf("observer run invalid: %s (max poll gap %s, failed queries %d)", reason, maxGap, failedQueries)
	}
	return nil
}

func writeRow(writer *csv.Writer, row []string) error {
	for len(row) < len(columns) {
		row = append(row, "")
	}
	if len(row) != len(columns) {
		return fmt.Errorf("observer row has %d fields, want %d", len(row), len(columns))
	}
	if err := writer.Write(row); err != nil {
		return err
	}
	writer.Flush()
	return writer.Error()
}

func readWorkflowIDs(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open workflow IDs: %w", err)
	}
	defer file.Close()
	var ids []string
	seen := make(map[string]struct{})
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		id := strings.TrimSpace(scanner.Text())
		if id == "" || strings.HasPrefix(id, "#") {
			continue
		}
		if _, duplicate := seen[id]; duplicate {
			return nil, fmt.Errorf("duplicate workflow ID %q", id)
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	if err := scanner.Err(); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return ids, nil
}
