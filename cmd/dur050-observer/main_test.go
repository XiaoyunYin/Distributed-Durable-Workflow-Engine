package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFineObserverInvalidatesSlowFirstTerminalObservation(t *testing.T) {
	var output bytes.Buffer
	writer := csv.NewWriter(&output)
	query := func(_ context.Context, ids []string) (map[string]workflowSnapshot, error) {
		time.Sleep(60 * time.Millisecond)
		return map[string]workflowSnapshot{
			ids[0]: {ID: ids[0], State: "SUCCEEDED", CreatedAt: time.Now(), UpdatedAt: time.Now()},
		}, nil
	}
	err := observe(context.Background(), writer, 50*time.Millisecond, 100*time.Millisecond,
		[]string{"wf-1"}, query)
	if err == nil || !strings.Contains(err.Error(), "observer run invalid") {
		t.Fatalf("slow first terminal observation error = %v, want invalid-run error", err)
	}
	rows, parseErr := csv.NewReader(strings.NewReader(output.String())).ReadAll()
	if parseErr != nil {
		t.Fatal(parseErr)
	}
	var sawInvalidTerminal, sawInvalidSummary bool
	for _, row := range rows {
		if len(row) < len(columns) {
			continue
		}
		if row[0] == "first_terminal_observation" {
			sawInvalidTerminal = row[14] == "false" && row[15] == "preterminal_observation_gap_exceeded"
		}
		if row[0] == "summary" {
			sawInvalidSummary = row[14] == "false" && strings.Contains(row[15], "observer_sampling_gap_exceeded")
		}
	}
	if !sawInvalidTerminal || !sawInvalidSummary {
		t.Fatalf("slow first poll was not preserved as invalid evidence: terminal=%t summary=%t rows=%s",
			sawInvalidTerminal, sawInvalidSummary, output.String())
	}
}

func TestFineObserverInvalidatesTerminalRowAfterEarlierSamplingGap(t *testing.T) {
	var output bytes.Buffer
	writer := csv.NewWriter(&output)
	var poll int
	query := func(_ context.Context, ids []string) (map[string]workflowSnapshot, error) {
		poll++
		if poll == 1 {
			time.Sleep(60 * time.Millisecond)
			return map[string]workflowSnapshot{ids[0]: {ID: ids[0], State: "RUNNABLE"}}, nil
		}
		return map[string]workflowSnapshot{ids[0]: {ID: ids[0], State: "SUCCEEDED"}}, nil
	}
	err := observe(context.Background(), writer, 50*time.Millisecond, 100*time.Millisecond,
		[]string{"wf-gap"}, query)
	if err == nil || !strings.Contains(err.Error(), "observer run invalid") {
		t.Fatalf("gapped fine observer error = %v, want invalid-run error", err)
	}
	rows, parseErr := csv.NewReader(strings.NewReader(output.String())).ReadAll()
	if parseErr != nil {
		t.Fatal(parseErr)
	}
	for _, row := range rows {
		if row[0] == "first_terminal_observation" {
			if row[14] != "false" || row[15] != "preterminal_observation_gap_exceeded" || row[12] == "" {
				t.Fatalf("terminal row did not retain its earlier excessive gap: %v", row)
			}
			return
		}
	}
	t.Fatalf("terminal observation row missing: %s", output.String())
}

func TestFineObserverAcceptsPollsWithinGapBound(t *testing.T) {
	var output bytes.Buffer
	writer := csv.NewWriter(&output)
	query := func(_ context.Context, ids []string) (map[string]workflowSnapshot, error) {
		return map[string]workflowSnapshot{
			ids[0]: {ID: ids[0], State: "SUCCEEDED", CreatedAt: time.Now(), UpdatedAt: time.Now()},
		}, nil
	}
	err := observe(context.Background(), writer, time.Millisecond, 100*time.Millisecond,
		[]string{"wf-1"}, query)
	if err != nil {
		t.Fatalf("fast first terminal observation: %v", err)
	}
	if !strings.Contains(output.String(), "first_terminal_observation") || !strings.Contains(output.String(), ",true,") {
		t.Fatalf("valid first terminal observation missing from output: %s", output.String())
	}
}

func TestBatchObserverWaitsForDoneMarkerAndKeepsAcceptedWorkUntilTerminal(t *testing.T) {
	var output bytes.Buffer
	writer := csv.NewWriter(&output)
	polls := 0
	submissionsDone := false
	query := func(_ context.Context, ids []string) (map[string]workflowSnapshot, error) {
		polls++
		if polls == 1 {
			return map[string]workflowSnapshot{ids[0]: {ID: ids[0], State: "RUNNABLE"}}, nil
		}
		submissionsDone = true
		return map[string]workflowSnapshot{ids[0]: {ID: ids[0], State: "SUCCEEDED"}}, nil
	}
	completionDone := func() (bool, error) { return submissionsDone, nil }
	err := observeUntil(context.Background(), writer, time.Millisecond, 100*time.Millisecond,
		[]string{"accepted-workflow", "rejected-workflow"}, completionDone, query)
	if err != nil {
		t.Fatalf("batch observer with completion marker: %v", err)
	}
	if polls != 3 {
		t.Fatalf("batch observer polls=%d; want one post-marker query before classifying absent IDs", polls)
	}
	rows, err := csv.NewReader(strings.NewReader(output.String())).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	var sawTerminal, sawNotFound, sawSummary bool
	for _, row := range rows {
		if len(row) < len(columns) {
			continue
		}
		switch {
		case row[0] == "first_terminal_observation" && row[2] == "accepted-workflow" && row[5] == "SUCCEEDED":
			sawTerminal = true
		case row[0] == "snapshot" && row[2] == "rejected-workflow" && row[5] == "NOT_FOUND":
			sawNotFound = true
		case row[0] == "summary" && row[14] == "true":
			sawSummary = true
		}
	}
	if !sawTerminal || !sawNotFound || !sawSummary {
		t.Fatalf("missing accepted, absent, or valid summary row: terminal=%t not_found=%t summary=%t output=%s",
			sawTerminal, sawNotFound, sawSummary, output.String())
	}
}

func TestBatchObserverUsesCompletionMarkerToClassifyUnacceptedIDs(t *testing.T) {
	var output bytes.Buffer
	writer := csv.NewWriter(&output)
	polls := 0
	submissionsDone := false
	query := func(_ context.Context, ids []string) (map[string]workflowSnapshot, error) {
		polls++
		if polls == 1 {
			return map[string]workflowSnapshot{ids[0]: {ID: ids[0], State: "RUNNABLE"}}, nil
		}
		submissionsDone = true
		return map[string]workflowSnapshot{ids[0]: {ID: ids[0], State: "SUCCEEDED"}}, nil
	}
	completionDone := func() (bool, error) { return submissionsDone, nil }
	err := observeUntil(context.Background(), writer, time.Millisecond, 100*time.Millisecond,
		[]string{"accepted-workflow", "rejected-workflow"}, completionDone, query)
	if err != nil {
		t.Fatalf("batch observer with completion marker: %v", err)
	}
	if polls != 3 {
		t.Fatalf("batch observer polls=%d; want one post-marker query before classifying absent IDs", polls)
	}
	rows, err := csv.NewReader(strings.NewReader(output.String())).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	var sawTerminal, sawNotFound, sawSummary bool
	for _, row := range rows {
		if len(row) < len(columns) {
			continue
		}
		switch {
		case row[0] == "first_terminal_observation" && row[2] == "accepted-workflow" && row[5] == "SUCCEEDED":
			sawTerminal = true
		case row[0] == "snapshot" && row[2] == "rejected-workflow" && row[5] == "NOT_FOUND":
			sawNotFound = true
		case row[0] == "summary" && row[14] == "true":
			sawSummary = true
		}
	}
	if !sawTerminal || !sawNotFound || !sawSummary {
		t.Fatalf("missing accepted, absent, or valid summary row: terminal=%t not_found=%t summary=%t output=%s",
			sawTerminal, sawNotFound, sawSummary, output.String())
	}
}

func TestBatchObserverPollsAgainWhenMarkerAppearsDuringQuery(t *testing.T) {
	var output bytes.Buffer
	writer := csv.NewWriter(&output)
	markerSeen := false
	committed := false
	polls := 0
	completionDone := func() (bool, error) { return markerSeen, nil }
	query := func(_ context.Context, ids []string) (map[string]workflowSnapshot, error) {
		polls++
		if polls == 1 {
			// Model the database snapshot being taken before the accepted insert
			// commits, then the commit and marker creation during this query.
			snapshotBeforeCommit := map[string]workflowSnapshot{}
			committed = true
			markerSeen = true
			return snapshotBeforeCommit, nil
		}
		if !committed {
			t.Fatal("second poll ran before the simulated submission committed")
		}
		terminalAt := time.Now()
		return map[string]workflowSnapshot{
			ids[0]: {ID: ids[0], State: "SUCCEEDED", CreatedAt: terminalAt, UpdatedAt: terminalAt, TerminalAt: &terminalAt},
		}, nil
	}
	err := observeUntil(context.Background(), writer, time.Millisecond, 100*time.Millisecond,
		[]string{"late-accepted-workflow"}, completionDone, query)
	if err != nil {
		t.Fatalf("observer returned error after marker/query race: %v", err)
	}
	if polls != 2 {
		t.Fatalf("poll count=%d, want a post-marker query after the stale snapshot", polls)
	}
	rows, err := csv.NewReader(strings.NewReader(output.String())).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	var pendingBeforeMarker, notFound, terminalAfterMarker bool
	for _, row := range rows {
		if len(row) < len(columns) {
			continue
		}
		if row[0] == "snapshot" && row[2] == "late-accepted-workflow" {
			if row[1] == "1" && row[5] == "SUBMISSION_PENDING" {
				pendingBeforeMarker = true
			}
			if row[5] == "NOT_FOUND" {
				notFound = true
			}
		}
		if row[0] == "first_terminal_observation" && row[2] == "late-accepted-workflow" && row[1] == "2" && row[5] == "SUCCEEDED" {
			terminalAfterMarker = true
		}
	}
	if !pendingBeforeMarker || notFound || !terminalAfterMarker {
		t.Fatalf("marker/query race was misclassified: pending=%t not_found=%t terminal=%t rows=%s",
			pendingBeforeMarker, notFound, terminalAfterMarker, output.String())
	}
}

func TestBatchObserverLatchesCompletionMarkerOnceSeen(t *testing.T) {
	var output bytes.Buffer
	writer := csv.NewWriter(&output)
	markerReads := 0
	polls := 0
	completionDone := func() (bool, error) {
		markerReads++
		return markerReads == 2, nil
	}
	query := func(_ context.Context, ids []string) (map[string]workflowSnapshot, error) {
		polls++
		if polls < 3 {
			return map[string]workflowSnapshot{ids[0]: {ID: ids[0], State: "RUNNABLE"}}, nil
		}
		terminalAt := time.Now()
		return map[string]workflowSnapshot{
			ids[0]: {ID: ids[0], State: "SUCCEEDED", TerminalAt: &terminalAt},
		}, nil
	}
	err := observeUntil(context.Background(), writer, time.Millisecond, 100*time.Millisecond,
		[]string{"accepted-workflow", "absent-workflow"}, completionDone, query)
	if err != nil {
		t.Fatalf("observer returned error after latching the marker: %v", err)
	}
	if polls != 3 || markerReads != 2 {
		t.Fatalf("polls=%d marker reads=%d, want 3 polls and no marker reads after latching", polls, markerReads)
	}
	rows, err := csv.NewReader(strings.NewReader(output.String())).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	var sawAbsentAfterLatch bool
	for _, row := range rows {
		if len(row) >= len(columns) && row[0] == "snapshot" && row[2] == "absent-workflow" &&
			row[1] == "3" && row[5] == "NOT_FOUND" && row[18] == "true" {
			sawAbsentAfterLatch = true
		}
	}
	if !sawAbsentAfterLatch {
		t.Fatalf("absent ID was not classified after the latched marker: %s", output.String())
	}
}

func TestReadSubmissionRowsRetainsAcceptedAndUncertainIDs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "submissions.csv")
	contents := "record_type,workflow_id,outcome,http_status\n" +
		"submission,accepted-id,accepted,201\n" +
		"submission,uncertain-id,ambiguous,\n" +
		"submission,rejected-id,rejected,503\n"
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	rows, err := readSubmissionRows(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 || rows[0].WorkflowID != "accepted-id" || rows[1].Outcome != "ambiguous" || rows[2].HTTPStatus != "503" {
		t.Fatalf("submission reconciliation rows = %#v", rows)
	}
}
