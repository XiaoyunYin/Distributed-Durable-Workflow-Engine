package telemetry

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Stage identifies a timing boundary from the fixed J9a registry.
type Stage uint8

const (
	StageRelayClaim Stage = iota
	StageRelayCaptureStart
	StageRelayBrokerPublish
	StageRelayFinalize
	StageRelayStartup
	StageRelayWaitPoll
	StageRelayWaitNotify
	StageScheduledToAttempt
	StageAttemptToWorkerStart
	StageWorkerStartToResult
	StageResultToSucceeded
	StageScheduledToSucceeded
	stageCount
)

var stageLabels = [stageCount]string{
	StageRelayClaim:           "claim",
	StageRelayCaptureStart:    "capture_publication_start",
	StageRelayBrokerPublish:   "broker_publish",
	StageRelayFinalize:        "finalize",
	StageRelayStartup:         "wakeup_startup",
	StageRelayWaitPoll:        "wakeup_wait_poll",
	StageRelayWaitNotify:      "wakeup_wait_notify",
	StageScheduledToAttempt:   "scheduled_to_attempt_created",
	StageAttemptToWorkerStart: "attempt_created_to_worker_start",
	StageWorkerStartToResult:  "worker_start_to_result_recorded",
	StageResultToSucceeded:    "result_recorded_to_succeeded",
	StageScheduledToSucceeded: "scheduled_to_succeeded",
}

var stageBuckets = [...]time.Duration{
	100 * time.Microsecond, 500 * time.Microsecond,
	time.Millisecond, 5 * time.Millisecond, 10 * time.Millisecond, 25 * time.Millisecond,
	50 * time.Millisecond, 100 * time.Millisecond, 250 * time.Millisecond, 500 * time.Millisecond,
	time.Second, 2 * time.Second, 5 * time.Second, 10 * time.Second,
	30 * time.Second, time.Minute, 2 * time.Minute, 5 * time.Minute, 10 * time.Minute,
}

type stageHistogram struct {
	buckets [len(stageBuckets) + 1]uint64
	sum     time.Duration
}

type stageTimings struct {
	mu     sync.Mutex
	stages [stageCount]stageHistogram
}

type stageFamily struct {
	name, help   string
	first, limit Stage
}

var relayStageFamily = stageFamily{
	"durable_j9a_relay_stage_seconds", "Wall-clock durations of relay stages.",
	StageRelayClaim, StageScheduledToAttempt,
}

var workflowStageFamily = stageFamily{
	"durable_j9a_workflow_stage_seconds", "Raw durations from checked J2 workflow phase boundaries.",
	StageScheduledToAttempt, stageCount,
}

var stageLabelEscaper = strings.NewReplacer("\\", "\\\\", "\"", "\\\"", "\n", "\\n")

// StartStage skips reading the clock when stage timing is disabled.
func (m *Metrics) StartStage() time.Time {
	if m == nil || m.stageTimings == nil {
		return time.Time{}
	}
	return time.Now()
}

func (m *Metrics) RecordStageSince(stage Stage, start time.Time) {
	if !start.IsZero() {
		m.RecordStage(stage, time.Since(start))
	}
}

func (m *Metrics) RecordStage(stage Stage, elapsed time.Duration) {
	if m == nil || m.stageTimings == nil || stage >= stageCount || elapsed < 0 {
		return
	}
	index := sort.Search(len(stageBuckets), func(i int) bool { return elapsed <= stageBuckets[i] })
	timings := m.stageTimings
	timings.mu.Lock()
	h := &timings.stages[stage]
	h.buckets[index]++
	h.sum += elapsed
	timings.mu.Unlock()
}

// RenderWorkflowStages emits the offline family using the same classic text
// renderer as the runtime relay family. Its inputs have already been joined.
func (m *Metrics) RenderWorkflowStages() string {
	if m == nil || m.stageTimings == nil {
		return ""
	}
	var b strings.Builder
	m.stageTimings.render(&b, m.roleLabel(), workflowStageFamily)
	return b.String()
}

func (t *stageTimings) render(b *strings.Builder, role string, family stageFamily) {
	t.mu.Lock()
	snapshot := t.stages
	t.mu.Unlock()
	roleLabel := `role="` + stageLabelEscaper.Replace(role) + `"`
	fmt.Fprintf(b, "# HELP %s %s\n# TYPE %s histogram\n", family.name, family.help, family.name)
	for stage := family.first; stage < family.limit; stage++ {
		label := roleLabel + ",stage=" + strconv.Quote(stageLabels[stage])
		h := snapshot[stage]
		var cumulative uint64
		for i, upper := range stageBuckets {
			cumulative += h.buckets[i]
			bound := strconv.FormatFloat(upper.Seconds(), 'f', -1, 64)
			fmt.Fprintf(b, "%s_bucket{%s,le=%q} %d\n", family.name, label, bound, cumulative)
		}
		cumulative += h.buckets[len(stageBuckets)]
		fmt.Fprintf(b, "%s_bucket{%s,le=\"+Inf\"} %d\n", family.name, label, cumulative)
		fmt.Fprintf(b, "%s_sum{%s} %s\n", family.name, label, strconv.FormatFloat(h.sum.Seconds(), 'f', -1, 64))
		fmt.Fprintf(b, "%s_count{%s} %d\n", family.name, label, cumulative)
	}
}
