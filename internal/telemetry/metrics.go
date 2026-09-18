// Package telemetry exposes the bounded runtime metrics required by the
// engine correctness and measurement gates.  It intentionally has no
// exporter dependency: the runtime renders the small fixed registry in the
// Prometheus text format, while durable evidence remains authoritative for
// workflow state.
package telemetry

import (
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// Metrics is a bounded-cardinality process registry.  Labels are fixed to the
// process role; incident IDs, workflow IDs, prompts, evidence text, and other
// unbounded values are deliberately not accepted.
type Metrics struct {
	role string

	startedUnix            atomic.Int64
	durableReadyUnix       atomic.Int64
	lastAcceptedClaimUnix  atomic.Int64
	schedulerLeaseAcquires atomic.Uint64
	schedulerLeaseRenews   atomic.Uint64
	schedulerFenced        atomic.Uint64
	acceptedClaims         atomic.Uint64
	acceptedResults        atomic.Uint64
	reconciliationItems    atomic.Uint64
	relayPublications      atomic.Uint64
	relayFailures          atomic.Uint64
	dbTransactions         atomic.Uint64
	dbQueries              atomic.Uint64
	workerActive           atomic.Int64
	workerCompleted        atomic.Uint64
	queryCount             atomic.Uint64
	querySecondsBits       atomic.Uint64
	lockWaitCount          atomic.Uint64
	lockWaitSecondsBits    atomic.Uint64
	backlogAgeSecondsBits  atomic.Uint64
}

// New returns a registry whose only label is the bounded runtime role.
func New(role string) *Metrics {
	if role == "" {
		role = "runtime"
	}
	metrics := &Metrics{role: role}
	metrics.startedUnix.Store(time.Now().Unix())
	return metrics
}

func (m *Metrics) roleLabel() string {
	if m == nil || m.role == "" {
		return "runtime"
	}
	return m.role
}

func (m *Metrics) MarkDurableReady() {
	if m != nil {
		m.durableReadyUnix.Store(time.Now().Unix())
	}
}

func (m *Metrics) RecordLeaseAcquired() {
	if m != nil {
		m.schedulerLeaseAcquires.Add(1)
	}
}
func (m *Metrics) RecordLeaseRenewed() {
	if m != nil {
		m.schedulerLeaseRenews.Add(1)
	}
}
func (m *Metrics) RecordFencedWrite() {
	if m != nil {
		m.schedulerFenced.Add(1)
	}
}

func (m *Metrics) RecordClaimAccepted() {
	if m != nil {
		m.acceptedClaims.Add(1)
		m.lastAcceptedClaimUnix.Store(time.Now().Unix())
	}
}

func (m *Metrics) RecordResultAccepted() {
	if m != nil {
		m.acceptedResults.Add(1)
	}
}
func (m *Metrics) RecordReconciliationItem() {
	if m != nil {
		m.reconciliationItems.Add(1)
	}
}
func (m *Metrics) RecordRelayPublication() {
	if m != nil {
		m.relayPublications.Add(1)
	}
}
func (m *Metrics) RecordRelayFailure() {
	if m != nil {
		m.relayFailures.Add(1)
	}
}
func (m *Metrics) RecordDBTransaction() {
	if m != nil {
		m.dbTransactions.Add(1)
	}
}

func (m *Metrics) RecordDBQuery(duration time.Duration) {
	if m == nil {
		return
	}
	m.dbQueries.Add(1)
	m.queryCount.Add(1)
	addFloat(&m.querySecondsBits, duration.Seconds())
}

func (m *Metrics) RecordLockWait(duration time.Duration) {
	if m == nil {
		return
	}
	m.lockWaitCount.Add(1)
	addFloat(&m.lockWaitSecondsBits, duration.Seconds())
}

func (m *Metrics) SetBacklogAge(age time.Duration) {
	if m != nil {
		storeFloat(&m.backlogAgeSecondsBits, age.Seconds())
	}
}

func (m *Metrics) WorkerStarted() {
	if m != nil {
		m.workerActive.Add(1)
	}
}
func (m *Metrics) WorkerFinished() {
	if m != nil {
		m.workerActive.Add(-1)
		m.workerCompleted.Add(1)
	}
}

func addFloat(target *atomic.Uint64, value float64) {
	for {
		old := target.Load()
		updated := math.Float64bits(math.Float64frombits(old) + value)
		if target.CompareAndSwap(old, updated) {
			return
		}
	}
}

func storeFloat(target *atomic.Uint64, value float64) { target.Store(math.Float64bits(value)) }
func loadFloat(target *atomic.Uint64) float64         { return math.Float64frombits(target.Load()) }

// Snapshot is useful for tests and for a durable evidence join performed by a
// campaign runner.  It contains only bounded numeric fields.
type Snapshot struct {
	Role                  string
	StartedUnix           int64
	DurableReadyUnix      int64
	LastAcceptedClaimUnix int64
	LeaseAcquires         uint64
	LeaseRenews           uint64
	FencedWrites          uint64
	AcceptedClaims        uint64
	AcceptedResults       uint64
	ReconciliationItems   uint64
	RelayPublications     uint64
	RelayFailures         uint64
	DBTransactions        uint64
	DBQueries             uint64
	WorkerActive          int64
	WorkerCompleted       uint64
	QueryCount            uint64
	QuerySeconds          float64
	LockWaitCount         uint64
	LockWaitSeconds       float64
	BacklogAgeSeconds     float64
}

func (m *Metrics) Snapshot() Snapshot {
	if m == nil {
		return Snapshot{}
	}
	return Snapshot{Role: m.roleLabel(), StartedUnix: m.startedUnix.Load(),
		DurableReadyUnix: m.durableReadyUnix.Load(), LastAcceptedClaimUnix: m.lastAcceptedClaimUnix.Load(),
		LeaseAcquires: m.schedulerLeaseAcquires.Load(), LeaseRenews: m.schedulerLeaseRenews.Load(),
		FencedWrites: m.schedulerFenced.Load(), AcceptedClaims: m.acceptedClaims.Load(),
		AcceptedResults: m.acceptedResults.Load(), ReconciliationItems: m.reconciliationItems.Load(),
		RelayPublications: m.relayPublications.Load(), RelayFailures: m.relayFailures.Load(),
		DBTransactions: m.dbTransactions.Load(), DBQueries: m.dbQueries.Load(),
		WorkerActive: m.workerActive.Load(), WorkerCompleted: m.workerCompleted.Load(),
		QueryCount: m.queryCount.Load(), QuerySeconds: loadFloat(&m.querySecondsBits),
		LockWaitCount: m.lockWaitCount.Load(), LockWaitSeconds: loadFloat(&m.lockWaitSecondsBits),
		BacklogAgeSeconds: loadFloat(&m.backlogAgeSecondsBits)}
}

func (m *Metrics) Render() string {
	s := m.Snapshot()
	label := `role="` + strings.ReplaceAll(s.Role, `"`, `\"`) + `"`
	var b strings.Builder
	writeGauge := func(name, help string, value any) {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s gauge\n%s{%s} %v\n", name, help, name, name, label, value)
	}
	writeCounter := func(name, help string, value any) {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s counter\n%s{%s} %v\n", name, help, name, name, label, value)
	}
	writeGauge("durable_runtime_started_timestamp_seconds", "Unix time when the runtime started.", s.StartedUnix)
	writeGauge("durable_runtime_durable_ready_timestamp_seconds", "Unix time when durable dependencies became ready.", s.DurableReadyUnix)
	writeGauge("durable_runtime_last_accepted_claim_timestamp_seconds", "Unix time of the last accepted worker claim.", s.LastAcceptedClaimUnix)
	writeCounter("durable_scheduler_lease_acquisitions_total", "Accepted scheduler lease acquisitions.", s.LeaseAcquires)
	writeCounter("durable_scheduler_lease_renewals_total", "Scheduler lease renewals.", s.LeaseRenews)
	writeCounter("durable_scheduler_fenced_writes_total", "Writes rejected by ownership fencing.", s.FencedWrites)
	writeCounter("durable_worker_claims_accepted_total", "Accepted worker claims.", s.AcceptedClaims)
	writeCounter("durable_worker_results_accepted_total", "Accepted worker results.", s.AcceptedResults)
	writeCounter("durable_reconciliation_items_total", "Reconciliation obligations observed.", s.ReconciliationItems)
	writeCounter("durable_outbox_publications_total", "Outbox publications observed.", s.RelayPublications)
	writeCounter("durable_outbox_relay_failures_total", "Outbox relay failures observed.", s.RelayFailures)
	writeCounter("durable_db_transactions_total", "Database transactions observed.", s.DBTransactions)
	writeCounter("durable_db_queries_total", "Database queries observed.", s.DBQueries)
	writeGauge("durable_worker_active", "Active worker executions.", s.WorkerActive)
	writeCounter("durable_worker_completed_total", "Completed worker executions.", s.WorkerCompleted)
	writeCounter("durable_db_query_seconds_total", "Cumulative database query seconds.", s.QuerySeconds)
	writeCounter("durable_db_lock_wait_seconds_total", "Cumulative database lock-wait seconds.", s.LockWaitSeconds)
	writeCounter("durable_db_lock_waits_total", "Database lock waits observed.", s.LockWaitCount)
	writeGauge("durable_reconciliation_oldest_age_seconds", "Age of the oldest reconciliation obligation.", s.BacklogAgeSeconds)
	return b.String()
}

func (m *Metrics) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = w.Write([]byte(m.Render()))
}

// Handler returns an HTTP handler for the fixed registry.
func (m *Metrics) Handler() http.Handler { return http.HandlerFunc(m.ServeHTTP) }

// Role returns the fixed role label without exposing mutable registry state.
func (m *Metrics) Role() string { return strconv.Quote(m.roleLabel()) }
