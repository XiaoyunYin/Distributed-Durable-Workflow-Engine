// Command j9a-stage-histograms renders the checked J2 phase-rows.json array
// on stdin as Prometheus text. Run the J2 replay before this projection.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"durable-agent-execution-engine/internal/telemetry"
)

type phaseRow struct {
	WorkflowID  string   `json:"workflow_id"`
	ScheduledNS int64    `json:"scheduled_ns"`
	TerminalNS  int64    `json:"terminal_ns"`
	PhaseNS     []*int64 `json:"phase_ns"`
}

func renderCheckedPhases(input io.Reader, output io.Writer) error {
	decoder := json.NewDecoder(input)
	if token, err := decoder.Token(); err != nil || token != json.Delim('[') {
		return errors.New("expected the checked J2 phase row array")
	}
	metrics := telemetry.NewWithStageTimings("j9a-offline", true)
	stages := [...]telemetry.Stage{telemetry.StageScheduledToAttempt, telemetry.StageAttemptToWorkerStart,
		telemetry.StageWorkerStartToResult, telemetry.StageResultToSucceeded}
	previousID := ""
	for decoder.More() {
		var row phaseRow
		if err := decoder.Decode(&row); err != nil {
			return err
		}
		if row.WorkflowID <= previousID || row.ScheduledNS <= 0 || row.TerminalNS < row.ScheduledNS ||
			len(row.PhaseNS) != len(stages) {
			return fmt.Errorf("invalid, duplicate or unordered phase row %q", row.WorkflowID)
		}
		previousID = row.WorkflowID
		total := row.TerminalNS - row.ScheduledNS
		remaining := total
		for _, value := range row.PhaseNS {
			if value == nil || *value < 0 || *value > remaining {
				return fmt.Errorf("invalid phase duration for %q", row.WorkflowID)
			}
			remaining -= *value
		}
		if remaining != 0 {
			return fmt.Errorf("phase durations do not span boundaries for %q", row.WorkflowID)
		}
		for index, value := range row.PhaseNS {
			metrics.RecordStage(stages[index], time.Duration(*value))
		}
		metrics.RecordStage(telemetry.StageScheduledToSucceeded, time.Duration(total))
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim(']') {
		return errors.New("unterminated checked J2 phase row array")
	}
	if _, err := decoder.Token(); err != io.EOF {
		return errors.New("trailing bytes after the checked J2 phase row array")
	}
	_, err := io.WriteString(output, metrics.RenderWorkflowStages())
	return err
}

func main() {
	if err := renderCheckedPhases(os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
