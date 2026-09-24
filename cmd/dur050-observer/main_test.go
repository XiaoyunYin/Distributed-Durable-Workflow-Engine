package main

import (
	"bytes"
	"context"
	"encoding/csv"
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
