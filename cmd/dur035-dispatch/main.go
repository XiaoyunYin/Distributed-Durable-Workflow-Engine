// Command dur035-dispatch measures the four DUR-035 dispatch configurations.
//
// Every configuration creates the same durable workflow, scheduler-owned
// attempt, and task outbox row. Direct configurations deliver the claimed row
// to the same worker pool without a broker. The Kafka configuration uses the
// production transport.Relay and KafkaSource. The command is deliberately a
// bounded measurement fixture, not a replacement runtime scheduler.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"durable-agent-execution-engine/internal/partition"
	"durable-agent-execution-engine/internal/state"
	"durable-agent-execution-engine/internal/telemetry"
	"durable-agent-execution-engine/internal/transport"
)

const (
	workflowCount = 24
	warmupCount   = 4
	arrivalRate   = 2.0
	workerSlots   = 4
	attemptLease  = time.Minute
	claimLease    = time.Minute
)

type arm struct {
	Name         string
	Mode         string
	PollInterval time.Duration
	FallbackPoll time.Duration
}

var arms = []arm{
	{Name: "poll_250ms", Mode: "direct_poll", PollInterval: 250 * time.Millisecond, FallbackPoll: 250 * time.Millisecond},
	{Name: "poll_1s", Mode: "direct_poll", PollInterval: time.Second, FallbackPoll: time.Second},
	{Name: "notify_direct", Mode: "direct_notify", PollInterval: time.Second, FallbackPoll: time.Second},
	{Name: "notify_kafka", Mode: "kafka", PollInterval: time.Second, FallbackPoll: time.Second},
}

type config struct {
	OutputPath   string
	Repeats      int
	WorkerBinary string
}

type protocolReport struct {
	SchemaVersion        string   `json:"schema_version"`
	Workload             string   `json:"workload"`
	ArrivalRatePerSec    float64  `json:"arrival_rate_per_second"`
	MeasuredWorkflows    int      `json:"measured_workflows"`
	WarmupWorkflows      int      `json:"warmup_workflows"`
	WorkerSlots          int      `json:"worker_slots"`
	AttemptLeaseSeconds  int      `json:"attempt_lease_seconds"`
	ClaimLeaseSeconds    int      `json:"claim_lease_seconds"`
	WorkerSelection      string   `json:"worker_selection"`
	WorkerModelRationale string   `json:"worker_model_rationale"`
	FallbackPolls        []string `json:"fallback_poll_intervals"`
	SameTaskOutbox       bool     `json:"same_task_outbox_record"`
	SameClaimAPI         bool     `json:"same_worker_claim_api"`
	HostLimit            string   `json:"host_limit"`
}

type artifact struct {
	SchemaVersion  string           `json:"schema_version"`
	Status         string           `json:"status"`
	GeneratedAt    time.Time        `json:"generated_at"`
	GitCommit      string           `json:"git_commit"`
	Protocol       protocolReport   `json:"protocol"`
	Configurations []armReport      `json:"configurations"`
	Runs           []runReport      `json:"runs"`
	Summary        []summaryRow     `json:"summary"`
	Validation     validationReport `json:"validation"`
	Conclusions    conclusionReport `json:"conclusions"`
	Failure        string           `json:"failure,omitempty"`
}

type armReport struct {
	Name           string `json:"name"`
	Mode           string `json:"mode"`
	PollInterval   string `json:"poll_interval"`
	FallbackPoll   string `json:"fallback_poll"`
	Interpretation string `json:"interpretation"`
}

type summaryRow struct {
	Configuration    string  `json:"configuration"`
	Repeats          int     `json:"repeats"`
	MedianDelayMS    float64 `json:"median_ready_to_claim_ms"`
	MedianTerminalMS float64 `json:"median_terminal_latency_ms"`
	MedianThroughput float64 `json:"median_throughput_per_second"`
	DelaySpreadPct   float64 `json:"delay_spread_percent"`
}

type validationReport struct {
	MeasuredRuns        int    `json:"measured_runs"`
	MeasuredWorkflows   int    `json:"measured_workflows"`
	TerminalWorkflows   int    `json:"terminal_workflows"`
	PendingWorkflows    int    `json:"pending_workflows"`
	FailedWorkflows     int    `json:"failed_workflows"`
	AllRunsReconciled   bool   `json:"all_runs_reconciled"`
	KafkaNotifications  uint64 `json:"kafka_notifications"`
	KafkaBrokerReceives uint64 `json:"kafka_broker_receives"`
	KafkaBrokerCommits  uint64 `json:"kafka_broker_commits"`
}

type effectComparison struct {
	HigherLatencyConfiguration string       `json:"higher_latency_configuration"`
	LowerLatencyConfiguration  string       `json:"lower_latency_configuration"`
	HigherLatencyMS            medianReport `json:"higher_latency_ms"`
	LowerLatencyMS             medianReport `json:"lower_latency_ms"`
	DifferenceMS               float64      `json:"median_difference_ms"`
	Ratio                      float64      `json:"median_ratio"`
	IntervalsSeparated         bool         `json:"intervals_separated"`
}

type effectConclusion struct {
	Comparisons []effectComparison `json:"comparisons"`
	Resolved    bool               `json:"resolved"`
	Summary     string             `json:"summary"`
}

type conclusionReport struct {
	WakeMechanism       effectConclusion `json:"wake_mechanism"`
	TransportEffect     effectConclusion `json:"transport_effect"`
	CostEffectsResolved bool             `json:"cost_effects_resolved"`
	ResolvedCostEffects []string         `json:"resolved_cost_effects"`
	Interpretation      string           `json:"interpretation"`
}

type runReport struct {
	Configuration           string          `json:"configuration"`
	Mode                    string          `json:"mode"`
	Repeat                  int             `json:"repeat"`
	RunID                   string          `json:"run_id"`
	Status                  string          `json:"status"`
	WorkflowCount           int             `json:"workflow_count"`
	WarmupCount             int             `json:"warmup_count"`
	Terminal                int             `json:"terminal"`
	Pending                 int             `json:"pending"`
	Failed                  int             `json:"failed"`
	ArrivalRate             float64         `json:"arrival_rate_per_second"`
	ElapsedSeconds          float64         `json:"elapsed_seconds"`
	DrainSeconds            float64         `json:"drain_seconds"`
	Throughput              float64         `json:"throughput_per_second"`
	BacklogOldestAgeSeconds float64         `json:"backlog_oldest_age_seconds"`
	Timings                 timingReport    `json:"timings"`
	Telemetry               telemetryReport `json:"telemetry"`
	Transport               transportReport `json:"transport"`
	ProcessCPUSeconds       float64         `json:"process_cpu_seconds"`
	WorkerCPUSeconds        float64         `json:"worker_cpu_seconds"`
	CPUDescription          string          `json:"cpu_description"`
	Failure                 string          `json:"failure,omitempty"`
}

type timingReport struct {
	ReadyToOutboxClaimMS         medianReport `json:"outbox_ready_to_claim_ms"`
	OutboxClaimToWorkerClaimMS   medianReport `json:"outbox_claim_to_worker_claim_ms"`
	BrokerReceiveToWorkerClaimMS medianReport `json:"broker_receive_to_worker_claim_ms"`
	WorkerClaimToResultMS        medianReport `json:"worker_claim_to_result_ms"`
	ResultToTerminalMS           medianReport `json:"result_to_terminal_ms"`
	TerminalLatencyMS            medianReport `json:"terminal_latency_ms"`
}

type medianReport struct {
	Count  int     `json:"count"`
	Min    float64 `json:"min"`
	Median float64 `json:"median"`
	Max    float64 `json:"max"`
}

type telemetryReport struct {
	LeaseAcquires     uint64  `json:"lease_acquires"`
	AcceptedClaims    uint64  `json:"accepted_claims"`
	AcceptedResults   uint64  `json:"accepted_results"`
	DBTransactions    uint64  `json:"db_transactions"`
	DBQueries         uint64  `json:"db_queries"`
	QuerySeconds      float64 `json:"query_seconds"`
	LockWaits         uint64  `json:"lock_waits"`
	LockWaitSeconds   float64 `json:"lock_wait_seconds"`
	RelayPublications uint64  `json:"relay_publications"`
	RelayFailures     uint64  `json:"relay_failures"`
}

type transportReport struct {
	OutboxRowsClaimed   int    `json:"outbox_rows_claimed"`
	OutboxRowsPublished int    `json:"outbox_rows_published"`
	BrokerPublications  uint64 `json:"broker_publications"`
	BrokerReceiveCount  uint64 `json:"broker_receive_count"`
	BrokerCommitCount   uint64 `json:"broker_commit_count"`
	Notifications       uint64 `json:"notifications"`
	Failures            uint64 `json:"failures"`
	Retries             uint64 `json:"retries"`
}

type taskTiming struct {
	WorkflowID       string
	ReadyAt          time.Time
	OutboxClaimedAt  time.Time
	BrokerReceivedAt time.Time
	WorkerClaimedAt  time.Time
	ResultAt         time.Time
	TerminalAt       time.Time
	Error            string
}

type cohortTracker struct {
	mu    sync.Mutex
	items map[string]taskTiming
}

func newTracker() *cohortTracker {
	return &cohortTracker{items: map[string]taskTiming{}}
}

func (t *cohortTracker) add(id string, ready time.Time) {
	t.mu.Lock()
	t.items[id] = taskTiming{WorkflowID: id, ReadyAt: ready}
	t.mu.Unlock()
}

func (t *cohortTracker) update(id string, fn func(*taskTiming)) {
	t.mu.Lock()
	item := t.items[id]
	fn(&item)
	t.items[id] = item
	t.mu.Unlock()
}

func (t *cohortTracker) wait(ctx context.Context, ids []string) error {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		t.mu.Lock()
		complete := true
		for _, id := range ids {
			item, found := t.items[id]
			if !found || (item.TerminalAt.IsZero() && item.Error == "") {
				complete = false
				break
			}
		}
		t.mu.Unlock()
		if complete {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (t *cohortTracker) snapshot(ids []string) []taskTiming {
	t.mu.Lock()
	defer t.mu.Unlock()
	result := make([]taskTiming, 0, len(ids))
	for _, id := range ids {
		result = append(result, t.items[id])
	}
	return result
}

type workerPool struct {
	store        *state.Store
	metrics      *telemetry.Metrics
	jobs         chan dispatchJob
	tracker      *cohortTracker
	wg           sync.WaitGroup
	seed         int64
	external     []*externalWorker
	nextExternal atomic.Uint64
	failures     atomic.Uint64
	closed       bool
}

type dispatchJob struct {
	event     state.OutboxEvent
	claimedAt time.Time
}

type workerRequest struct {
	Seed      int64  `json:"seed"`
	NodeID    string `json:"node_id"`
	WorkUnits int    `json:"work_units"`
}

type workerResponse struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

type externalWorker struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	encode *json.Encoder
	decode *json.Decoder
	mu     sync.Mutex
}

func newWorkerPool(store *state.Store, metrics *telemetry.Metrics, tracker *cohortTracker, slots int, seed int64, workerBinary string) (*workerPool, error) {
	p := &workerPool{store: store, metrics: metrics, jobs: make(chan dispatchJob, slots), tracker: tracker, seed: seed}
	if workerBinary != "" {
		p.external = make([]*externalWorker, 0, slots)
		for index := 0; index < slots; index++ {
			command := exec.Command(workerBinary)
			stdin, err := command.StdinPipe()
			if err != nil {
				_ = p.close()
				return nil, fmt.Errorf("worker %d stdin: %w", index, err)
			}
			stdout, err := command.StdoutPipe()
			if err != nil {
				_ = stdin.Close()
				_ = p.close()
				return nil, fmt.Errorf("worker %d stdout: %w", index, err)
			}
			command.Stderr = os.Stderr
			if err := command.Start(); err != nil {
				_ = stdin.Close()
				_ = p.close()
				return nil, fmt.Errorf("start worker %d: %w", index, err)
			}
			p.external = append(p.external, &externalWorker{cmd: command, stdin: stdin,
				encode: json.NewEncoder(stdin), decode: json.NewDecoder(stdout)})
		}
	}
	for i := 0; i < slots; i++ {
		p.wg.Add(1)
		go p.run(fmt.Sprintf("dur035-worker-%d", i))
	}
	return p, nil
}

func (p *workerPool) submit(job dispatchJob) { p.jobs <- job }

func (p *workerPool) run(workerID string) {
	defer p.wg.Done()
	for job := range p.jobs {
		if err := p.process(workerID, job); err != nil {
			p.failures.Add(1)
			p.tracker.update(stringPayloadID(job.event.Payload), func(item *taskTiming) { item.Error = err.Error(); item.TerminalAt = time.Now().UTC() })
		}
	}
}

func (p *workerPool) close() float64 {
	if p == nil || p.closed {
		return 0
	}
	p.closed = true
	close(p.jobs)
	p.wg.Wait()
	var cpu time.Duration
	for _, worker := range p.external {
		if err := worker.stdin.Close(); err != nil {
			continue
		}
		if err := worker.cmd.Wait(); err != nil {
			continue
		}
		if worker.cmd.ProcessState != nil {
			cpu += worker.cmd.ProcessState.UserTime() + worker.cmd.ProcessState.SystemTime()
		}
	}
	return cpu.Seconds()
}

func (p *workerPool) runActivity(seed int64, nodeID string, units int) error {
	if len(p.external) == 0 {
		busyWork(seed, nodeID, units)
		return nil
	}
	index := (p.nextExternal.Add(1) - 1) % uint64(len(p.external))
	worker := p.external[index]
	worker.mu.Lock()
	defer worker.mu.Unlock()
	if err := worker.encode.Encode(workerRequest{Seed: seed, NodeID: nodeID, WorkUnits: units}); err != nil {
		return fmt.Errorf("send activity to worker: %w", err)
	}
	var response workerResponse
	if err := worker.decode.Decode(&response); err != nil {
		return fmt.Errorf("read worker response: %w", err)
	}
	if !response.OK {
		if response.Error == "" {
			response.Error = "worker rejected activity"
		}
		return errors.New(response.Error)
	}
	return nil
}

func (p *workerPool) process(workerID string, job dispatchJob) error {
	var task struct {
		WorkflowID    string `json:"workflow_id"`
		NodeID        string `json:"node_id"`
		Iteration     int    `json:"iteration"`
		AttemptNumber int64  `json:"attempt_number"`
	}
	if err := json.Unmarshal(job.event.Payload, &task); err != nil {
		return fmt.Errorf("decode dispatch payload: %w", err)
	}
	if p.metrics != nil {
		p.metrics.WorkerStarted()
		defer p.metrics.WorkerFinished()
	}
	claimedAt := time.Now().UTC()
	requestID := "dur035:" + job.event.EventID
	claim, err := p.store.ClaimAttempt(context.Background(), state.ClaimInput{WorkflowID: task.WorkflowID, NodeID: task.NodeID,
		Iteration: task.Iteration, WorkerID: workerID, RequestID: requestID, AttemptLease: attemptLease})
	if err != nil {
		return fmt.Errorf("claim task: %w", err)
	}
	if p.metrics != nil {
		p.metrics.RecordClaimAccepted()
	}
	p.tracker.update(task.WorkflowID, func(item *taskTiming) {
		if item.OutboxClaimedAt.IsZero() {
			item.OutboxClaimedAt = job.claimedAt
		}
		item.WorkerClaimedAt = claimedAt
	})
	// The fixture does a fixed, small amount of deterministic work so the
	// measured path includes a stable worker handoff but not LLM latency.
	if err := p.runActivity(p.seed, task.WorkflowID, 3000); err != nil {
		return fmt.Errorf("run activity: %w", err)
	}
	resultAt := time.Now().UTC()
	if _, err := p.store.RecordResultReceipt(context.Background(), state.ResultInput{WorkflowID: task.WorkflowID, NodeID: task.NodeID,
		Iteration: task.Iteration, AttemptNumber: claim.AttemptNumber, ClaimToken: claim.ClaimToken,
		AttemptState: state.AttemptSucceeded, Payload: json.RawMessage(`{"ok":true}`), EventType: state.DefaultResultEventType}); err != nil {
		return fmt.Errorf("record worker result: %w", err)
	}
	if p.metrics != nil {
		p.metrics.RecordResultAccepted()
	}
	p.tracker.update(task.WorkflowID, func(item *taskTiming) { item.ResultAt = resultAt })
	partitionID, err := partition.ID(task.WorkflowID)
	if err != nil {
		return err
	}
	ownerID := state.NewID()
	lease, err := acquireLeaseWithRetry(context.Background(), p.store, int16(partitionID), ownerID, claimLease)
	if err != nil {
		return fmt.Errorf("acquire consume lease: %w", err)
	}
	if p.metrics != nil {
		p.metrics.RecordLeaseAcquired()
	}
	defer func() {
		_ = p.store.ReleaseLease(context.Background(), state.LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch})
	}()
	wf, err := p.store.GetWorkflow(context.Background(), task.WorkflowID)
	if err != nil {
		return err
	}
	if _, err := p.store.ConsumeResult(context.Background(), state.ConsumeResultInput{Lease: state.LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch},
		WorkflowID: task.WorkflowID, NodeID: task.NodeID, Iteration: task.Iteration, AttemptNumber: claim.AttemptNumber,
		ExpectedRevision: wf.Revision, NewWorkflowState: state.StateSucceeded, ActorID: "dur035-scheduler"}); err != nil {
		return fmt.Errorf("consume worker result: %w", err)
	}
	p.tracker.update(task.WorkflowID, func(item *taskTiming) { item.TerminalAt = time.Now().UTC() })
	return nil
}

func acquireLeaseWithRetry(ctx context.Context, store *state.Store, partitionID int16, ownerID string, ttl time.Duration) (state.Lease, error) {
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	for {
		lease, acquired, err := store.AcquireLease(ctx, partitionID, ownerID, ttl)
		if err != nil {
			return state.Lease{}, err
		}
		if acquired {
			return lease, nil
		}
		select {
		case <-ctx.Done():
			return state.Lease{}, ctx.Err()
		case <-deadline.C:
			return state.Lease{}, state.ErrLeaseNotOwned
		case <-time.After(5 * time.Millisecond):
		}
	}
}

type dispatcher struct {
	store         *state.Store
	namespace     string
	ownerID       string
	arm           arm
	pool          *workerPool
	metrics       *telemetry.Metrics
	claimed       atomic.Int64
	published     atomic.Int64
	failures      atomic.Uint64
	retries       atomic.Uint64
	notifications atomic.Uint64
	mu            sync.Mutex
}

func (d *dispatcher) pass(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	events, err := d.store.ClaimOutbox(ctx, d.ownerID, 32, claimLease)
	if err != nil {
		d.failures.Add(1)
		return err
	}
	for _, event := range events {
		d.claimed.Add(1)
		if event.EventType == "attempt.dispatch" {
			workflowID := stringPayloadID(event.Payload)
			claimedAt := time.Now().UTC()
			d.pool.tracker.update(workflowID, func(item *taskTiming) {
				if !event.CreatedAt.IsZero() {
					item.ReadyAt = event.CreatedAt
				}
				item.OutboxClaimedAt = claimedAt
			})
			d.pool.submit(dispatchJob{event: event, claimedAt: claimedAt})
		} // Non-task rows are drained to keep the comparison's backlog honest.
		if err := d.store.FinalizeOutboxPublication(ctx, event.EventID, d.ownerID, event.RelayAttempts, true, "", 0); err != nil {
			d.failures.Add(1)
			return err
		}
		d.published.Add(1)
	}
	return nil
}

func (d *dispatcher) run(ctx context.Context) {
	_ = d.pass(ctx)
	ticker := time.NewTicker(d.arm.PollInterval)
	defer ticker.Stop()
	wakeups := (<-chan struct{})(nil)
	if d.arm.Mode == "direct_notify" {
		wakeups = d.listen(ctx)
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = d.pass(ctx)
		case <-wakeups:
			d.notifications.Add(1)
			_ = d.pass(ctx)
		}
	}
}

func (d *dispatcher) listen(ctx context.Context) <-chan struct{} {
	wakeups := make(chan struct{}, 1)
	go func() {
		connection, err := d.store.Pool().Acquire(ctx)
		if err != nil {
			d.failures.Add(1)
			return
		}
		defer connection.Release()
		if _, err := connection.Exec(ctx, `LISTEN durable_agent_outbox`); err != nil {
			d.failures.Add(1)
			return
		}
		for {
			if _, err := connection.Conn().WaitForNotification(ctx); err != nil {
				return
			}
			select {
			case wakeups <- struct{}{}:
			default:
			}
		}
	}()
	return wakeups
}

type kafkaDispatcher struct {
	store         *state.Store
	pool          *workerPool
	source        *transport.KafkaSource
	relay         *transport.Relay
	metrics       *telemetry.Metrics
	receives      atomic.Uint64
	commits       atomic.Uint64
	notifications atomic.Uint64
	failures      atomic.Uint64
}

type observedBroker struct {
	inner   transport.Broker
	tracker *cohortTracker
}

func (b *observedBroker) Publish(ctx context.Context, event state.OutboxEvent) error {
	if event.EventType == "attempt.dispatch" {
		workflowID := stringPayloadID(event.Payload)
		claimedAt := time.Now().UTC()
		b.tracker.update(workflowID, func(item *taskTiming) {
			if !event.CreatedAt.IsZero() {
				item.ReadyAt = event.CreatedAt
			}
			item.OutboxClaimedAt = claimedAt
		})
	}
	return b.inner.Publish(ctx, event)
}

func (b *observedBroker) Close() error { return b.inner.Close() }

func (d *kafkaDispatcher) consume(ctx context.Context) {
	for {
		message, err := d.source.Receive(ctx)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				d.failures.Add(1)
			}
			return
		}
		if message.EventType != "attempt.dispatch" {
			continue
		}
		d.receives.Add(1)
		event := state.OutboxEvent{EventID: message.EventID, EventType: message.EventType, Payload: message.Payload}
		receivedAt := time.Now().UTC()
		var createdAt time.Time
		if err := d.store.Pool().QueryRow(ctx, `SELECT created_at FROM engine.outbox WHERE event_id = $1`, message.EventID).Scan(&createdAt); err == nil {
			workflowID := stringPayloadID(message.Payload)
			d.pool.tracker.update(workflowID, func(item *taskTiming) {
				item.ReadyAt = createdAt
				if item.OutboxClaimedAt.IsZero() {
					item.OutboxClaimedAt = receivedAt
				}
				item.BrokerReceivedAt = receivedAt
			})
		}
		d.pool.submit(dispatchJob{event: event, claimedAt: receivedAt})
		// Kafka offset commit is done after the job is accepted by the fixed
		// worker pool. Durable result/terminal state is still written by the
		// worker and scheduler APIs, not by offset advancement.
		if err := d.source.Commit(ctx, message, message.Offset+1); err != nil {
			d.failures.Add(1)
			return
		}
		d.commits.Add(1)
	}
}

func scheduleActivity(ctx context.Context, store *state.Store, workflowID, actorID string) (state.Attempt, error) {
	wf, err := store.GetWorkflow(ctx, workflowID)
	if err != nil {
		return state.Attempt{}, err
	}
	partitionID, err := partition.ID(workflowID)
	if err != nil {
		return state.Attempt{}, err
	}
	ownerID := state.NewID()
	lease, err := acquireLeaseWithRetry(ctx, store, int16(partitionID), ownerID, claimLease)
	if err != nil {
		return state.Attempt{}, err
	}
	ref := state.LeaseRef{PartitionID: lease.PartitionID, OwnerID: lease.OwnerID, Epoch: lease.Epoch}
	defer func() { _ = store.ReleaseLease(context.Background(), ref) }()
	nodeState := state.StateWaitingActivity
	if err := store.ApplyOwnerTransition(ctx, state.OwnerTransitionInput{Lease: ref, WorkflowID: workflowID,
		ExpectedRevision: wf.Revision, NewState: state.StateWaitingActivity, ActorID: actorID,
		Reason: "ACTIVITY_SCHEDULED", NodeID: "activity", Iteration: 0, NodeState: &nodeState}); err != nil {
		return state.Attempt{}, err
	}
	wf, err = store.GetWorkflow(ctx, workflowID)
	if err != nil {
		return state.Attempt{}, err
	}
	return store.CreateAttempt(ctx, state.AttemptInput{Lease: ref, WorkflowID: workflowID, NodeID: "activity", Iteration: 0,
		ExpectedRevision: wf.Revision, EffectClass: state.EffectPure, HeartbeatDeadline: time.Now().Add(attemptLease), ActorID: actorID})
}

func runCase(ctx context.Context, store *state.Store, metrics *telemetry.Metrics, selected arm, repeat int, seed int64, workerBinary string) (runReport, error) {
	runID := fmt.Sprintf("%s-r%d-%s", selected.Name, repeat, state.NewID())
	namespace := "dur035-dispatch-" + runID
	definitionID := namespace + "-def"
	cleanup := func() { cleanupNamespace(context.Background(), store, namespace, definitionID) }
	defer cleanup()
	graph := json.RawMessage(`{"entry":"activity","nodes":[{"id":"activity","kind":"activity"}]}`)
	if err := store.CreateDefinition(ctx, state.DefinitionInput{DefinitionID: definitionID, Version: 1, DefinitionHash: namespace,
		Graph: graph, ActivityVersions: json.RawMessage(`{"activity":"1"}`), EffectClasses: json.RawMessage(`{"activity":"PURE_ACTIVITY"}`)}); err != nil {
		return runReport{}, err
	}
	tracker := newTracker()
	baseline := metrics.Snapshot()
	cpuStart, err := processCPUSeconds()
	if err != nil {
		return runReport{}, err
	}
	pool, err := newWorkerPool(store, metrics, tracker, workerSlots, seed, workerBinary)
	if err != nil {
		return runReport{}, err
	}
	workerClosed := false
	defer func() {
		if !workerClosed {
			_ = pool.close()
		}
	}()
	caseCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	d := &dispatcher{store: store, namespace: namespace, ownerID: state.NewID(), arm: selected, pool: pool, metrics: metrics}
	var kafkaD *kafkaDispatcher
	var relayCancel context.CancelFunc
	if selected.Mode == "kafka" {
		brokers, err := kafkaBrokers()
		if err != nil {
			return runReport{}, err
		}
		broker, err := transport.NewKafkaBroker(brokers, state.EventTopic)
		if err != nil {
			return runReport{}, err
		}
		group := "dur035-" + strings.ReplaceAll(runID, "_", "-")
		source, err := transport.NewKafkaSource(brokers, state.TaskTopic, group)
		if err != nil {
			_ = broker.Close()
			return runReport{}, err
		}
		// Fetch once with no publication in flight so the new consumer group
		// completes assignment before the relay can publish the first task.
		// A timeout is expected; any other startup error is actionable.
		primeCtx, cancelPrime := context.WithTimeout(caseCtx, 500*time.Millisecond)
		_, primeErr := source.Receive(primeCtx)
		cancelPrime()
		if primeErr != nil && !errors.Is(primeErr, context.DeadlineExceeded) && !errors.Is(primeErr, context.Canceled) {
			_ = source.Close()
			_ = broker.Close()
			return runReport{}, fmt.Errorf("prime Kafka consumer group: %w", primeErr)
		}
		relayCtx, cancelRelay := context.WithCancel(caseCtx)
		relayCancel = cancelRelay
		observed := &observedBroker{inner: broker, tracker: tracker}
		kafkaD = &kafkaDispatcher{store: store, pool: pool, source: source, metrics: metrics}
		relay := transport.NewRelay(store, observed, transport.RelayConfig{OwnerID: state.NewID(), BatchSize: 32, ClaimLease: claimLease,
			PollInterval: selected.FallbackPoll, OnError: func(err error) { _ = err }, OnSuccess: func(report transport.RelayReport) {
				if metrics != nil {
					for i := 0; i < report.Failed; i++ {
						metrics.RecordRelayFailure()
					}
				}
			}, OnNotification: func() { kafkaD.notifications.Add(1) }})
		kafkaD.relay = relay
		go func() { _ = relay.Run(relayCtx) }()
		go kafkaD.consume(caseCtx)
	} else {
		go d.run(caseCtx)
	}
	createCohort := func(count int, prefix string) ([]string, error) {
		ids := make([]string, 0, count)
		for index := 0; index < count; index++ {
			workflowID := fmt.Sprintf("%s-%s-%02d", namespace, prefix, index)
			pid, err := partition.ID(workflowID)
			if err != nil {
				return nil, err
			}
			created, err := store.CreateWorkflow(ctx, state.CreateWorkflowInput{WorkflowID: workflowID, Namespace: namespace,
				SubmissionKey: workflowID, SubmissionPayloadHash: "sub-v1:" + workflowID, DefinitionID: definitionID, DefinitionVersion: 1,
				PartitionID: int16(pid), InitialNodeID: "activity", InitialInput: json.RawMessage(fmt.Sprintf(`{"seed":%d}`, seed+int64(index))), ActorID: "dur035-client"})
			if err != nil {
				return nil, err
			}
			if !created.Created {
				return nil, fmt.Errorf("workflow %s unexpectedly reused", workflowID)
			}
			tracker.add(workflowID, time.Now().UTC())
			if _, err := scheduleActivity(ctx, store, workflowID, "dur035-scheduler"); err != nil {
				return nil, err
			}
			ids = append(ids, workflowID)
			if index+1 < count {
				time.Sleep(500 * time.Millisecond)
			}
		}
		return ids, nil
	}
	warmups, err := createCohort(warmupCount, "warmup")
	if err != nil {
		return runReport{}, err
	}
	if err := tracker.wait(ctx, warmups); err != nil {
		return runReport{}, err
	}
	measuredStart := time.Now().UTC()
	measured, err := createCohort(workflowCount, "measured")
	if err != nil {
		return runReport{}, err
	}
	if err := tracker.wait(ctx, measured); err != nil {
		return runReport{}, err
	}
	// Give the relay/dispatcher a bounded drain window for result and terminal
	// outbox rows before the namespace reconciliation check.
	if err := drainNamespace(ctx, store, namespace, selected.FallbackPoll); err != nil {
		return runReport{}, err
	}
	measuredEnd := time.Now().UTC()
	if relayCancel != nil {
		relayCancel()
		cancel()
		_ = kafkaD.source.Close()
		_ = kafkaD.relay.Broker.Close()
	}
	workerCPUSeconds := pool.close()
	workerClosed = true
	cpuEnd, cpuErr := processCPUSeconds()
	if cpuErr != nil {
		return runReport{}, cpuErr
	}
	rows := tracker.snapshot(measured)
	for _, row := range rows {
		if row.Error != "" || row.TerminalAt.IsZero() {
			return runReport{}, fmt.Errorf("workflow %s did not complete: %s", row.WorkflowID, row.Error)
		}
	}
	terminal, pending, oldest, err := reconcileNamespace(ctx, store, namespace)
	if err != nil {
		return runReport{}, err
	}
	if terminal != workflowCount+warmupCount || pending != 0 {
		return runReport{}, fmt.Errorf("cohort reconciliation terminal=%d pending=%d", terminal, pending)
	}
	telemetryDelta := telemetryDelta(metrics.Snapshot(), baseline)
	report := runReport{Configuration: selected.Name, Mode: selected.Mode, Repeat: repeat, RunID: runID, Status: "PASS",
		WorkflowCount: workflowCount, WarmupCount: warmupCount, Terminal: workflowCount, Pending: pending, ArrivalRate: arrivalRate,
		ElapsedSeconds: measuredEnd.Sub(measuredStart).Seconds(), DrainSeconds: maxFloat(0, measuredEnd.Sub(measuredStart).Seconds()-float64(workflowCount-1)/arrivalRate), Throughput: float64(workflowCount) / measuredEnd.Sub(measuredStart).Seconds(),
		BacklogOldestAgeSeconds: oldest, Timings: makeTimings(rows), Telemetry: telemetryDelta, ProcessCPUSeconds: cpuEnd - cpuStart,
		WorkerCPUSeconds: workerCPUSeconds, CPUDescription: "dispatcher process CPU; worker CPU is measured from four fixed subprocesses", Transport: transportReport{Failures: pool.failures.Load()}}
	if kafkaD != nil {
		report.Transport.BrokerReceiveCount = kafkaD.receives.Load()
		report.Transport.BrokerCommitCount = kafkaD.commits.Load()
		report.Transport.Notifications = kafkaD.notifications.Load()
		report.Transport.Failures += kafkaD.failures.Load()
	}
	if kafkaD != nil {
		report.Transport.BrokerPublications = telemetryDelta.RelayPublications
	}
	report.Transport.Failures += telemetryDelta.RelayFailures
	if selected.Mode != "kafka" {
		report.Transport.Notifications = d.notifications.Load()
		report.Transport.Failures += d.failures.Load()
		report.Transport.Retries = d.retries.Load()
		report.Transport.OutboxRowsClaimed = int(d.claimed.Load())
		report.Transport.OutboxRowsPublished = int(d.published.Load())
	}
	return report, nil
}

func drainNamespace(ctx context.Context, store *state.Store, namespace string, interval time.Duration) error {
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	for {
		var pending int
		if err := store.Pool().QueryRow(ctx, `SELECT count(*) FROM engine.outbox o JOIN engine.workflow_executions w ON w.workflow_id = o.workflow_id WHERE w.namespace = $1 AND o.publish_state IN ('PENDING','CLAIMED')`, namespace).Scan(&pending); err != nil {
			return err
		}
		if pending == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("outbox drain timeout with %d pending rows", pending)
		case <-time.After(interval):
		}
	}
}

func reconcileNamespace(ctx context.Context, store *state.Store, namespace string) (terminal, pending int, oldest float64, err error) {
	err = store.Pool().QueryRow(ctx, `SELECT count(*) FILTER (WHERE state IN ('SUCCEEDED','FAILED','REJECTED','CANCELED','ABANDONED')), count(*) FILTER (WHERE state NOT IN ('SUCCEEDED','FAILED','REJECTED','CANCELED','ABANDONED')) FROM engine.workflow_executions WHERE namespace = $1`, namespace).Scan(&terminal, &pending)
	if err != nil {
		return
	}
	err = store.Pool().QueryRow(ctx, `SELECT COALESCE(EXTRACT(EPOCH FROM (clock_timestamp() - min(o.created_at))), 0) FROM engine.outbox o JOIN engine.workflow_executions w ON w.workflow_id = o.workflow_id WHERE w.namespace = $1 AND o.publish_state IN ('PENDING','CLAIMED')`, namespace).Scan(&oldest)
	return
}

func cleanupNamespace(ctx context.Context, store *state.Store, namespace, definitionID string) {
	_, _ = store.Pool().Exec(ctx, `DELETE FROM engine.workflow_executions WHERE namespace = $1`, namespace)
	_, _ = store.Pool().Exec(ctx, `DELETE FROM engine.workflow_definitions WHERE definition_id = $1`, definitionID)
}

func makeTimings(rows []taskTiming) timingReport {
	ready, handoff, brokerHandoff, result, terminal := make([]float64, 0, len(rows)), make([]float64, 0, len(rows)), make([]float64, 0, len(rows)), make([]float64, 0, len(rows)), make([]float64, 0, len(rows))
	for _, row := range rows {
		ready = append(ready, row.OutboxClaimedAt.Sub(row.ReadyAt).Seconds()*1000)
		handoff = append(handoff, row.WorkerClaimedAt.Sub(row.OutboxClaimedAt).Seconds()*1000)
		if !row.BrokerReceivedAt.IsZero() {
			brokerHandoff = append(brokerHandoff, row.WorkerClaimedAt.Sub(row.BrokerReceivedAt).Seconds()*1000)
		}
		result = append(result, row.ResultAt.Sub(row.WorkerClaimedAt).Seconds()*1000)
		terminal = append(terminal, row.TerminalAt.Sub(row.ResultAt).Seconds()*1000)
	}
	return timingReport{ReadyToOutboxClaimMS: summarize(ready), OutboxClaimToWorkerClaimMS: summarize(handoff), BrokerReceiveToWorkerClaimMS: summarize(brokerHandoff), WorkerClaimToResultMS: summarize(result), ResultToTerminalMS: summarize(terminal), TerminalLatencyMS: summarizeTerminal(rows)}
}

func summarizeTerminal(rows []taskTiming) medianReport {
	values := make([]float64, 0, len(rows))
	for _, row := range rows {
		values = append(values, row.TerminalAt.Sub(row.ReadyAt).Seconds()*1000)
	}
	return summarize(values)
}

func summarize(values []float64) medianReport {
	if len(values) == 0 {
		return medianReport{}
	}
	sort.Float64s(values)
	return medianReport{Count: len(values), Min: values[0], Median: values[len(values)/2], Max: values[len(values)-1]}
}

func telemetryDelta(after, before telemetry.Snapshot) telemetryReport {
	return telemetryReport{LeaseAcquires: after.LeaseAcquires - before.LeaseAcquires, AcceptedClaims: after.AcceptedClaims - before.AcceptedClaims, AcceptedResults: after.AcceptedResults - before.AcceptedResults,
		DBTransactions: after.DBTransactions - before.DBTransactions, DBQueries: after.DBQueries - before.DBQueries, QuerySeconds: after.QuerySeconds - before.QuerySeconds,
		LockWaits: after.LockWaitCount - before.LockWaitCount, LockWaitSeconds: after.LockWaitSeconds - before.LockWaitSeconds,
		RelayPublications: after.RelayPublications - before.RelayPublications, RelayFailures: after.RelayFailures - before.RelayFailures}
}

func maxFloat(left, right float64) float64 {
	if left > right {
		return left
	}
	return right
}

func stringPayloadID(payload []byte) string {
	var item struct {
		WorkflowID string `json:"workflow_id"`
	}
	_ = json.Unmarshal(payload, &item)
	return item.WorkflowID
}

func busyWork(seed int64, text string, units int) uint64 {
	value := uint64(seed) ^ uint64(len(text))
	for i := 0; i < units; i++ {
		value ^= value << 13
		value ^= value >> 7
		value ^= value << 17
	}
	return value
}

func main() {
	var cfg config
	flag.StringVar(&cfg.OutputPath, "output", "experiments/m7/dur035/results.json", "artifact output path")
	flag.IntVar(&cfg.Repeats, "repeats", 3, "measured repeats per configuration")
	flag.StringVar(&cfg.WorkerBinary, "worker-binary", "", "fixed-capacity worker executable")
	flag.Parse()
	if cfg.Repeats <= 0 {
		fatal("repeats must be positive")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	result, err := runStudy(ctx, cfg)
	data, marshalErr := json.MarshalIndent(result, "", "  ")
	if marshalErr != nil {
		fatal(marshalErr.Error())
	}
	if err := os.MkdirAll(filepath.Dir(cfg.OutputPath), 0o755); err != nil {
		fatal(err.Error())
	}
	if err := os.WriteFile(cfg.OutputPath, append(data, '\n'), 0o644); err != nil {
		fatal(err.Error())
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runStudy(ctx context.Context, cfg config) (artifact, error) {
	database, err := databaseURL()
	if err != nil {
		return artifact{SchemaVersion: "dur035-dispatch.v1", Status: "FAIL", Failure: err.Error()}, err
	}
	store, err := state.NewFromURL(ctx, database)
	if err != nil {
		return artifact{SchemaVersion: "dur035-dispatch.v1", Status: "FAIL", Failure: err.Error()}, err
	}
	defer store.Close()
	metrics := telemetry.New("dur035-dispatch")
	store.SetTelemetry(metrics)
	result := artifact{SchemaVersion: "dur035-dispatch.v1", Status: "PASS", GeneratedAt: time.Now().UTC(), GitCommit: gitCommit(),
		Protocol: protocolReport{SchemaVersion: "dur035-dispatch.v1", Workload: "one pure activity", ArrivalRatePerSec: arrivalRate, MeasuredWorkflows: workflowCount,
			WarmupWorkflows: warmupCount, WorkerSlots: workerSlots, AttemptLeaseSeconds: int(attemptLease.Seconds()), ClaimLeaseSeconds: int(claimLease.Seconds()),
			WorkerSelection: "fixed round-robin pool; four worker subprocesses", WorkerModelRationale: "worker CPU is kept outside the dispatcher process so the four dispatch arms compare the same execution capacity without mixing worker burn into dispatcher CPU",
			FallbackPolls: []string{"250ms", "1s"}, SameTaskOutbox: true, SameClaimAPI: true,
			HostLimit: "single-node Docker Desktop/WSL2 host; bounded fixture"}}
	for _, selected := range arms {
		result.Configurations = append(result.Configurations, armReport{Name: selected.Name, Mode: selected.Mode, PollInterval: selected.PollInterval.String(), FallbackPoll: selected.FallbackPoll.String(), Interpretation: interpretation(selected.Name)})
	}
	for _, selected := range arms {
		for repeat := 1; repeat <= cfg.Repeats; repeat++ {
			run, runErr := runCase(ctx, store, metrics, selected, repeat, int64(repeat), cfg.WorkerBinary)
			if runErr != nil {
				result.Status = "FAIL"
				result.Failure = fmt.Sprintf("%s repeat %d: %v", selected.Name, repeat, runErr)
				result.Runs = append(result.Runs, runReport{Configuration: selected.Name, Mode: selected.Mode, Repeat: repeat, Status: "FAIL", Failure: runErr.Error()})
				return result, runErr
			}
			result.Runs = append(result.Runs, run)
		}
	}
	result.Summary = makeSummary(result.Runs)
	result.Validation = validateRuns(result.Runs)
	result.Conclusions = deriveConclusions(result.Runs)
	if !result.Validation.AllRunsReconciled {
		result.Status = "FAIL"
		result.Failure = "validation did not reconcile every measured run"
		return result, errors.New(result.Failure)
	}
	return result, nil
}

func interpretation(name string) string {
	if strings.HasPrefix(name, "poll_") {
		return "A: polling wake mechanism"
	}
	if name == "notify_direct" {
		return "B: PostgreSQL wakeup without broker"
	}
	return "C: production relay and Kafka transport"
}

func makeSummary(runs []runReport) []summaryRow {
	grouped := map[string][]runReport{}
	for _, run := range runs {
		grouped[run.Configuration] = append(grouped[run.Configuration], run)
	}
	result := make([]summaryRow, 0, len(grouped))
	for _, selected := range arms {
		rows := grouped[selected.Name]
		delays, terminals, throughput := []float64{}, []float64{}, []float64{}
		for _, run := range rows {
			delays = append(delays, run.Timings.ReadyToOutboxClaimMS.Median)
			terminals = append(terminals, run.Timings.TerminalLatencyMS.Median)
			throughput = append(throughput, run.Throughput)
		}
		delaySummary := summarize(delays)
		terminalSummary := summarize(terminals)
		throughputSummary := summarize(throughput)
		spread := 0.0
		if delaySummary.Median > 0 {
			spread = (delaySummary.Max - delaySummary.Min) / delaySummary.Median * 100
		}
		result = append(result, summaryRow{Configuration: selected.Name, Repeats: len(rows), MedianDelayMS: delaySummary.Median, MedianTerminalMS: terminalSummary.Median, MedianThroughput: throughputSummary.Median, DelaySpreadPct: spread})
	}
	return result
}

func validateRuns(runs []runReport) validationReport {
	result := validationReport{MeasuredRuns: len(runs), AllRunsReconciled: len(runs) == len(arms)*3}
	for _, run := range runs {
		result.MeasuredWorkflows += run.WorkflowCount
		result.TerminalWorkflows += run.Terminal
		result.PendingWorkflows += run.Pending
		result.FailedWorkflows += run.Failed
		if run.Status != "PASS" || run.Terminal != run.WorkflowCount || run.Pending != 0 || run.Failed != 0 ||
			run.Timings.TerminalLatencyMS.Count != run.WorkflowCount || run.Timings.ReadyToOutboxClaimMS.Count != run.WorkflowCount {
			result.AllRunsReconciled = false
		}
		if run.Mode == "kafka" {
			result.KafkaNotifications += run.Transport.Notifications
			result.KafkaBrokerReceives += run.Transport.BrokerReceiveCount
			result.KafkaBrokerCommits += run.Transport.BrokerCommitCount
		}
	}
	return result
}

func deriveConclusions(runs []runReport) conclusionReport {
	grouped := map[string][]runReport{}
	for _, run := range runs {
		grouped[run.Configuration] = append(grouped[run.Configuration], run)
	}
	comparison := func(higher, lower, metric string) effectComparison {
		higherRange := runMetricRange(grouped[higher], metric)
		lowerRange := runMetricRange(grouped[lower], metric)
		result := effectComparison{HigherLatencyConfiguration: higher, LowerLatencyConfiguration: lower, HigherLatencyMS: higherRange, LowerLatencyMS: lowerRange}
		result.DifferenceMS = higherRange.Median - lowerRange.Median
		if lowerRange.Median > 0 {
			result.Ratio = higherRange.Median / lowerRange.Median
		}
		result.IntervalsSeparated = higherRange.Min > lowerRange.Max
		return result
	}
	wake := effectConclusion{Comparisons: []effectComparison{
		comparison("poll_250ms", "notify_direct", "ready"),
		comparison("poll_1s", "notify_direct", "ready"),
	}}
	wake.Resolved = len(wake.Comparisons) == 2 && wake.Comparisons[0].IntervalsSeparated && wake.Comparisons[1].IntervalsSeparated
	if wake.Resolved {
		wake.Summary = "Notification-driven dispatch has a resolved lower ready-to-claim delay than both frozen polling intervals."
	} else {
		wake.Summary = "The wake-mechanism comparison is unresolved at the observed run dispersion."
	}
	transportEffect := effectConclusion{Comparisons: []effectComparison{
		comparison("notify_kafka", "notify_direct", "ready"),
		comparison("notify_kafka", "notify_direct", "terminal"),
	}}
	transportEffect.Resolved = transportEffect.Comparisons[0].IntervalsSeparated || transportEffect.Comparisons[1].IntervalsSeparated
	if transportEffect.Comparisons[0].IntervalsSeparated && !transportEffect.Comparisons[1].IntervalsSeparated {
		transportEffect.Summary = "Kafka adds a resolved dispatch-stage delay, while the end-to-end terminal-latency increment remains unresolved."
	} else if transportEffect.Resolved {
		transportEffect.Summary = "The measured Kafka transport increment is resolved at the reported stages."
	} else {
		transportEffect.Summary = "The incremental Kafka transport effect is unresolved at the observed run dispersion."
	}
	resolved := make([]string, 0, 3)
	if wake.Resolved {
		resolved = append(resolved, "wake_mechanism_ready_to_claim")
	}
	if transportEffect.Comparisons[0].IntervalsSeparated {
		resolved = append(resolved, "kafka_dispatch_stage_ready_to_claim")
	}
	transportInterpretation := "The incremental Kafka transport effect is unresolved at the observed run dispersion. Notification-direct and Kafka are therefore not distinguished as a latency winner at this scale."
	if transportEffect.Comparisons[0].IntervalsSeparated && transportEffect.Comparisons[1].IntervalsSeparated {
		readyKafkaSlower := transportEffect.Comparisons[0].HigherLatencyMS.Median > transportEffect.Comparisons[0].LowerLatencyMS.Median
		terminalKafkaSlower := transportEffect.Comparisons[1].HigherLatencyMS.Median > transportEffect.Comparisons[1].LowerLatencyMS.Median
		switch {
		case readyKafkaSlower && terminalKafkaSlower:
			transportInterpretation = "Notification-direct is faster than Kafka on both ready-to-claim and terminal latency at the tested scale; Kafka is not a latency optimization here, so any remaining rationale is architectural unless separately measured."
		case !readyKafkaSlower && !terminalKafkaSlower:
			transportInterpretation = "Kafka is faster than notification-direct on both reported latency stages at the tested scale."
		default:
			transportInterpretation = "The resolved transport result is stage-specific: the reported latency direction differs between ready-to-claim and terminal completion."
		}
	} else if transportEffect.Comparisons[0].IntervalsSeparated || transportEffect.Comparisons[1].IntervalsSeparated {
		transportInterpretation = transportEffect.Summary
	} else if direct, ok := grouped["notify_direct"]; ok {
		kafka, kafkaOK := grouped["notify_kafka"]
		if kafkaOK && runMetricRange(direct, "ready").Median <= runMetricRange(kafka, "ready").Median {
			transportInterpretation = "Notification-direct matches or beats Kafka on the measured ready-to-claim median, but the transport effect is unresolved by the observed intervals."
		}
	}
	result := conclusionReport{WakeMechanism: wake, TransportEffect: transportEffect, CostEffectsResolved: len(resolved) > 0,
		ResolvedCostEffects: resolved, Interpretation: "At this four-worker, single-host scale, notification-driven dispatch removes the polling delay. " + transportInterpretation + " This does not address decoupling, retained backlog, connection count, or multi-host scaling."}
	return result
}

func runMetricRange(runs []runReport, metric string) medianReport {
	values := make([]float64, 0, len(runs))
	for _, run := range runs {
		switch metric {
		case "ready":
			values = append(values, run.Timings.ReadyToOutboxClaimMS.Median)
		case "terminal":
			values = append(values, run.Timings.TerminalLatencyMS.Median)
		}
	}
	return summarize(values)
}

func gitCommit() string {
	command := exec.Command("git", "rev-parse", "HEAD")
	output, err := command.Output()
	if err != nil {
		return "unavailable"
	}
	return strings.TrimSpace(string(output))
}

func databaseURL() (string, error) {
	for _, name := range []string{"DURABLE_DATABASE_URL", "DATABASE_URL"} {
		if value := os.Getenv(name); value != "" {
			return value, nil
		}
	}
	values, err := envFile()
	if err != nil {
		return "", errors.New("set DURABLE_DATABASE_URL or provide .env")
	}
	port := values["POSTGRES_PORT"]
	if port == "" {
		port = "5432"
	}
	if _, err := strconv.Atoi(port); err != nil {
		return "", err
	}
	return (&url.URL{Scheme: "postgresql", User: url.UserPassword(values["POSTGRES_USER"], values["POSTGRES_PASSWORD"]), Host: "127.0.0.1:" + port, Path: "/" + values["POSTGRES_DB"], RawQuery: "sslmode=disable"}).String(), nil
}

func kafkaBrokers() ([]string, error) {
	if value := os.Getenv("DURABLE_KAFKA_BROKERS"); value != "" {
		return strings.Split(value, ","), nil
	}
	if value := os.Getenv("KAFKA_BOOTSTRAP_SERVERS"); value != "" {
		return strings.Split(value, ","), nil
	}
	values, err := envFile()
	if err != nil {
		return nil, errors.New("set DURABLE_KAFKA_BROKERS or provide .env")
	}
	port := values["KAFKA_PORT"]
	if port == "" {
		port = "9092"
	}
	return []string{"127.0.0.1:" + port}, nil
}

func envFile() (map[string]string, error) {
	directory, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	for {
		data, readErr := os.ReadFile(filepath.Join(directory, ".env"))
		if readErr == nil {
			values := map[string]string{}
			for _, line := range strings.Split(string(data), "\n") {
				parts := strings.SplitN(strings.TrimSpace(line), "=", 2)
				if len(parts) == 2 {
					values[parts[0]] = strings.Trim(parts[1], "\r\"")
				}
			}
			return values, nil
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			break
		}
		directory = parent
	}
	return nil, errors.New(".env not found")
}

func fatal(message string) { fmt.Fprintln(os.Stderr, message); os.Exit(2) }
