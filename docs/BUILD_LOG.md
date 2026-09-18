# Build log

## 2026-09-17 - M4 start

- M3 closeout/base: `8fb2f75`; M4 status: IN_PROGRESS.
- DUR-015 status: IN_PROGRESS. DUR-016, DUR-017, DUR-018, and DUR-023A-M4
  remain TODO with start records in PLAN.md.
- DUR-015 scope is limited to durable retry classification/budgets,
  backoff/timer enforcement, compatible pure-work checkpoints, stale-update
  rejection, and measured repeated work. Effect ledgers, ambiguous-effect
  remediation, approvals, and their checker extension remain later M4 tasks.
- No M4 implementation code has been added yet. The next implementation
  review must use `8fb2f75` as its exact base.

Interview explanation: M4 separates pure-work recovery from external-effect
recovery. Retry budgets and checkpoints can be measured directly, while
cooperating and non-cooperating effects require distinct receipt and
reconciliation rules before any approval-controlled remediation is dispatched.

## 2026-09-17 - M3 closeout after round-18 review

- M3 implementation target: `b1e11bb`; review base: `9412f3e`.
- Task status: DUR-011, DUR-012, DUR-013, DUR-014, and DUR-023A-M3 are DONE.
- Claude's committed round-18 review returned `NO_BLOCKING_FINDINGS`; R045-R048
  are VERIFIED. The residual manual DB-enabled parallel-run hazard and the
  owner-unscoped test cleanup remain nonblocking follow-ups before M4 adds
  more database-backed suites.
- No runtime behavior or protected M3 scope changed at closeout. The next
  milestone is M4: retry/checkpoint recovery, ambiguous outcomes and effect
  handling, approvals, and the corresponding independent checker extension.

Interview explanation: M3 closes the transport/reconciliation boundary with
PostgreSQL as the durable source of obligations and Kafka as a recoverable,
duplicate-tolerant delivery path. The service CI serializes shared database
fixtures until stronger per-package isolation is added.

## 2026-09-17 - M3 round-18 R048 test isolation fix and review handoff

- Review base: `9412f3e` (M2 closeout and original M3 review base).
- Committed test-infrastructure target: `b1e11bb`.
- Task status: DUR-011, DUR-012, DUR-013, DUR-014, and DUR-023A-M3 remain
  READY_FOR_REVIEW pending Claude verification of R048.
- Fixed the shared-database test race at both Go test entry points used by
  service CI: `ci.ps1 -WithServices` passes `-SerialPackages` to `check.ps1`,
  and its race phase runs `go test -race -p 1 ./...`. Non-service checks keep
  their existing parallel execution. This prevents package test binaries from
  competing for the same PostgreSQL partition leases and fixture rows.
- Validation passed against the local services: serial
  `DURABLE_REQUIRE_DATABASE=1 go test -race -p 1 ./... -count=1`, followed by
  `powershell.exe -NoProfile -ExecutionPolicy Bypass -File
  scripts/ci.ps1 -WithRace -WithServices` after the fix. The latter passed
  shared checks, 19 Python tests, all Go race packages, PostgreSQL integration,
  real Kafka task/event round trips, and smoke checks. `git diff --check` is
  clean.
- Direct database-enabled `go test -race ./...` remains intentionally outside
  the supported service validation path because it bypasses the isolation
  switch; use the documented `ci.ps1 -WithRace -WithServices` command instead.
  No product guarantees, protected scope, budgets, or runtime code changed.

Interview explanation: the shared PostgreSQL lease table makes package-level
parallelism an invalid fixture assumption. Serializing only the service-mode
package processes preserves fast parallel unit checks while making the
database-backed acceptance command deterministic.

## 2026-09-17 - M3 round-17 fixes and review handoff

- Review base: `9412f3e` (M2 closeout and original M3 review base).
- Committed implementation target: `fb70d41`.
- Task status: DUR-011, DUR-012, DUR-013, DUR-014, and DUR-023A-M3 remain
  READY_FOR_REVIEW pending Claude verification of R045-R047.
- Fixed the optional worker-result event path by normalizing an omitted event
  type to `activity.result`, rejecting unsupported explicit types, and using a
  single explicit event-to-topic registry in the producer, relay, and checker.
- Made both quarantine paths observable: workflow-linked relay poison and
  broker poison now create `POISON_RECORD` reconciliation obligations; poison
  with no trustworthy workflow identity is a global operator obligation.
  Backlog reporting now includes quarantined-outbox count, poison count, and
  oldest-obligation age, with an age backpressure limit. Relay pass errors have
  a counter and optional structured logging hook.
- Migration `000009_m3_poison_reconciliation.up.sql` applied successfully and
  was skipped safely on the subsequent migration run. The focused M3 state,
  transport, reconciliation, and invariant suites passed with race detection.
- Validation passed: `powershell.exe -NoProfile -ExecutionPolicy Bypass -File
  scripts/ci.ps1 -WithRace -WithServices` (19 Python tests, all Go race
  packages, PostgreSQL/M3 integration, real Kafka task/event round trip, and
  smoke), `go vet ./...`, `gofmt`, and `git diff --check`.
- Remaining gaps: sustained load, database outage/lock-timeout campaigns,
  hard-kill durability, consumer rebalance, clean bootstrap/restart-smoke
  reruns, and remote CI. `go mod tidy` remains blocked by the pre-existing
  permission-locked module-cache test files.

Interview explanation: one explicit event registry prevents producer, relay,
and checker drift. Quarantine is a durable obligation rather than a terminal
discard, and a global poison record is kept operator-visible when no safe
workflow link exists.

## 2026-09-17 - M3 implementation handoff

- Review base: `9412f3e` (M2 closeout).
- Committed implementation target: `bc1b68b` (M3 implementation plus the
  parallel-fixture isolation correction).
- Task status: DUR-011, DUR-012, DUR-013, DUR-014, and DUR-023A-M3 are
  READY_FOR_REVIEW; M3 is not marked DONE before Claude's review.
- Built the PostgreSQL transactional outbox and stable task/event topics,
  relay claims and publication evidence, Kafka and deterministic consumer
  adapters, contiguous inbox offsets, lease-fenced scheduler wake-ups,
  durable poison quarantine, partition-owner reconciliation/backpressure, and
  independent transport/reconciliation invariants. Runtime replicas start a
  relay when their existing `DATABASE_URL` and `KAFKA_BOOTSTRAP_SERVERS` are
  configured.
- Migrations `000006` through `000008` add transport metadata/publication
  evidence, wake-ups/reconciliation, inbox cleanup, and raw poison-record
  retention. The migration runner applied them successfully and skipped them
  safely on the final rerun.
- Validation passed: `powershell.exe -NoProfile -ExecutionPolicy Bypass -File
  scripts/ci.ps1 -WithRace -WithServices` (19 Python tests, Go race suite,
  PostgreSQL state/reconciliation tests, real Kafka task/event round trip,
  and service smoke); `go vet ./...`; `gofmt`; `git diff --check`; and the
  runtime-a/runtime-b Docker Compose build.
- Focused fault evidence includes relay crash after broker acknowledgment,
  expired-claim recovery, duplicate delivery at a new Kafka offset, missing
  `LISTEN/NOTIFY` fallback polling, contiguous offset gaps, poison
  quarantine, and expired-attempt reconciliation/redispatch. M3 fixtures are
  partition/prefix isolated so parallel package integration tests do not
  borrow each other's leases or outbox rows.
- An attempted `go mod tidy` could not acquire permission-locked module-cache
  test-download files. The verified dependency graph already built and passed;
  the Kafka module was declared directly in `go.mod` without claiming a tidy
  run.
- Remaining gaps: multi-host deployment/rebalance, sustained load, database
  outage and lock-timeout campaigns, hard-kill durability, clean bootstrap and
  restart-smoke reruns, and remote CI (none is configured). The external-effect
  ledger and approval invariants remain later M4 work.

Interview explanation: PostgreSQL commits the workflow transition and its
publication obligation together, while Kafka is allowed to duplicate records.
The stable event ID, inbox disposition, contiguous offset watermark, and
reconciliation scans make the broker crash windows recoverable without giving
Kafka authority over workflow state.

## 2026-09-17 - DUR-011 start

- Base commit: `9412f3e` (M2 closeout after Claude's `NO_BLOCKING_FINDINGS`
  review of `0d663c3` against `600726f`).
- Task status: IN_PROGRESS; no DUR-011 implementation commit yet.
- Scope: transactional outbox and Kafka relay claim/retry/recovery, including
  stable event identities, duplicate publication after broker acknowledgement,
  crash-window fault injection, and `LISTEN/NOTIFY` fallback polling. Consumer
  offsets, inbox disposition, scheduler wake-up ownership, reconciliation
  scans, and backpressure remain DUR-012 through DUR-014.

DUR-011 is started from the M2 closeout without changing the reviewed M2
implementation. The implementation must keep PostgreSQL authoritative: state
transitions and their outbox obligations commit together, relay claims are
recoverable, and a broker acknowledgement followed by a process crash must
produce a safe, visible duplicate rather than an ambiguous new event.

Planned evidence is in the outbox/relay code, migrations, PostgreSQL/Kafka
integration tests, crash/fault tests, `docs/CONTRACTS.md`, and the committed
review handoff. Planned validation is the race-enabled service CI, focused
PostgreSQL/Kafka and fallback-poll tests, `go vet`, `gofmt`, the runtime image
build, and `git diff --check`. No implementation or correctness result is
claimed at this start record.

Interview explanation: the outbox turns a workflow transition into a durable
publication obligation. The relay may publish more than once, but stable event
identity lets the later inbox make duplicates harmless while PostgreSQL keeps
unfinished work discoverable after a missed notification or relay crash.

## 2026-09-17 - M2 closeout

- Review base: `600726f` (M1 closeout).
- Reviewed code target: `0d663c3`.
- Task status: DONE for DUR-008, DUR-009, DUR-010, and DUR-023A-M2.
- Review: Claude's committed, non-provisional round-15 verdict is
  `NO_BLOCKING_FINDINGS`; R040-R044 are VERIFIED. The historical R019 claim
  retry-after-replacement test gap remains nonblocking.

The M2 implementation is accepted without changing the reviewed code target.
It provides lease fencing and deterministic takeover ordering, atomic worker
claim/heartbeat/result controls, a version-aware bounded Python runner, and an
independent ownership/attempt checker. The worker control seam remains
development-only and unauthenticated; authentication and worker identity
binding are recorded as a prerequisite for later M4 approval/effect work.

Claude's validation, recorded in REVIEW.md, includes the full Go race suite,
`go vet`, `gofmt`, the runtime build, Ruff, mypy, 19 Python tests, PostgreSQL
lease contention and worker-API integration probes, and checker validation on
live persisted data. The earlier M2 review also covered fencing, claim/result
idempotency, stale-result rejection, and seeded checker violations.

Remaining M2 limits are Kafka transport and duplicate dispatch, multi-host
deployment, database-outage and lock-timeout campaigns, sustained load,
hard-kill durability, clean bootstrap/restart smoke, and remote CI. These are
not M2 acceptance failures and remain visible in REVIEW.md.

Interview explanation: M2 separates scheduler authority from worker progress.
Leases and epochs fence owners, attempt tokens fence workers, receipts make
retries safe, and the independent checker tests the persisted evidence rather
than reusing transition code.

## 2026-09-17 - M2 round-15 corrections

- Base commit: `91e4b14` (M2 implementation handoff).
- Fix commits: `8e0a1a3`, `0d663c3`.
- Task status: READY_FOR_REVIEW; R040-R044 are addressed and remain pending
  Claude verification. M2 is not marked DONE.

The worker control API now returns typed not-found and invalid-attempt-state
errors. The runner resolves versions before claiming, retries transient
heartbeat failures with bounded backoff, submits completed results even after
a temporary heartbeat outage, and preserves definitive stale-claim signals
without converting shutdown exceptions into activity failures. PostgreSQL
integration tests now exercise concurrent expired-lease ownership, a lock-held
takeover ordering, and the real HTTP claim/result path through retries and
stale completion. Worker endpoint trust assumptions and the M4 authentication
prerequisite are documented.

The first service-mode rerun exposed a nondeterministic lock-wait fixture: the
takeover waiter could retain the pre-expiry snapshot after waiting on the
seeded lease-row insert. `AcquireLease` now relies on the migration-seeded
partition rows and locks the lease row directly; five repeated race runs of
the takeover tests pass.

Validation completed for this correction:

- `go test -race ./...`, `go vet ./...`, `go build ./cmd/runtime`, `gofmt -l
  cmd internal`, and `git diff --check`: PASS.
- `DURABLE_REQUIRE_DATABASE=1 go test -race ./internal/state ./internal/api
  -run 'TestM2...' -count=1`: PASS.
- `uv run ruff check python tests`, `uv run ruff format --check python tests`,
  `uv run mypy`, and `uv run pytest`: PASS; 19 Python tests.

The correction does not add authentication, Kafka transport, multi-host
deployment, sustained-load measurements, hard-kill durability, clean bootstrap
or restart smoke, or remote CI.

After the lease-ordering follow-up, the full
`scripts/ci.ps1 -WithRace -WithServices` command passed: migrations were
already applied, Go race and service-mode PostgreSQL suites passed, all 19
Python tests passed, and PostgreSQL/Kafka/two runtime/two worker/telemetry
health checks were healthy.

## 2026-09-17 - M2 implementation handoff

- Base commit: `600726f` (Claude-reviewed M1 closeout).
- Implementation target: `91e4b14`.
- Task status: READY_FOR_REVIEW; DUR-008, DUR-009, DUR-010, and
  DUR-023A-M2 are implemented and awaiting Claude's committed review. M2 is
  not marked DONE.

M2 adds explicit same-epoch lease renewal and takeover fencing, keeps the
interpreter renewing and releasing only its own lease, exposes the worker
claim/heartbeat/result control seam, and extends the independent checker with
persisted scheduler-epoch and attempt-generation evidence. The Python worker
runner has a versioned pure-activity registry, bounded concurrency, heartbeat
renewal, and documented lost-response behavior. Kafka transport and production
effect isolation remain later scope.

Validation completed:

- `go test -race ./...`: PASS.
- `DURABLE_REQUIRE_DATABASE=1 go test -race ./internal/state
  -run TestM2LeaseRenewalAndTakeoverFence -count=1`: PASS against PostgreSQL;
  owner A renewed, owner B took over after expiry, and stale owner operations
  were rejected.
- `go vet ./...`, `go build ./cmd/runtime`, `gofmt -l cmd internal`, and
  `git diff --check`: PASS.
- `uv run ruff check python tests`, `uv run ruff format --check python tests`,
  `uv run mypy`, and `uv run pytest`: PASS; 16 Python tests.
- `scripts/ci.ps1 -WithRace -WithServices`: PASS on the second run, including
  migrations 000001-000005, all DUR-005 through DUR-007 PostgreSQL scenarios,
  live PostgreSQL/Kafka/runtime/worker/telemetry health, and the service-mode
  Python suite. The first run hit the existing probabilistic M1 fixture's
  100-attempt partition-ID generation miss; the unchanged rerun passed.

The full service-mode command and Claude's independent probes remain part of
review validation. No M2 correctness, multi-host, Kafka, sustained-load,
hard-kill, clean-bootstrap, or remote-CI claim is made here.

## 2026-09-17 - DUR-007/DUR-023A round-13 corrections

- Base commit: `6bc0e2f` (verified DUR-006 closeout).
- Fix commits: `8cac005`, `b41deb6`, `600726f`.
- Status: DONE after Claude's committed round-13 review found
  `NO_BLOCKING_FINDINGS` for target `600726f` against base `6bc0e2f`.

Round-13 corrections make the interpreter recover durable `WAITING_ACTIVITY`
and `SUCCEEDED` phases after an intra-node crash, fence `Run` on the lease
acquisition result, and renew/release only the engine's own lease. Repository
graph advancement now validates the stored immutable graph, accepted activity
results, declared successors, join dependencies, graph entry, and terminal
sibling convergence. Explicit graph timers use a new persisted
`timer_fired` marker. The independent checker now loads real PostgreSQL
history, submissions, and accepted node results; its seeded tests assert each
violation independently.

Validation recorded for this correction:

- `scripts/migrate.ps1`: PASS; migration 000005 applied after 000001-000004.
- `scripts/ci.ps1 -WithRace -WithServices`: PASS; shared Go/Python checks,
  migrations 000001-000005, DUR-005 through DUR-007 PostgreSQL tests, and
  live dependency smoke checks all passed.
- `DURABLE_REQUIRE_DATABASE=1 go test -race ./...`: PASS, including crash
  recovery, lease fencing, overlapping branch results, cancellation races, and
  checker loading from persisted rows.
- `go vet ./...`, `go build ./cmd/runtime`, `gofmt`, and `git diff --check`:
  PASS.
- `docker build -f deploy/local/Dockerfile.runtime -t
  durable-agent-runtime:dur007-r13-check .`: PASS.
- After the final lease-cleanup correction, focused `go test -race
  ./internal/engine ./internal/state`, `go vet ./...`, `go build ./cmd/runtime`,
  and `git diff --check`: PASS.
- Remaining review-only gaps: clean bootstrap/restart smoke, hard-kill
  durability, sustained-load/timeout studies, and remote CI were not claimed
  by this correction.

Claude's independent trigger-based crash probes verified four recovery points,
including the declared at-least-once case where a lost activity result can be
re-executed after the claim lease expires. M1 reports this as at-least-once
activity execution, not exactly-once execution. R034-R039 and the DUR-005 R028
fan-out follow-up are verified; the nonblocking R019 claim-retry test gap
remains open. M1 is closed, and DUR-008 is the next task.

## 2026-09-17 - DUR-008 start

- Base commit: `600726f` (M1 closeout with Claude `NO_BLOCKING_FINDINGS`).
- Task status: IN_PROGRESS; no DUR-008 implementation commit yet.

Opened the M2 partition-lease/fencing task. PLAN.md now records the scoped
acceptance and validation before implementation: two owners, renewal,
takeover, epoch-fenced writes, release safety after takeover, and lock/expiry
ordering. The frozen partition map and later worker/Kafka tasks remain in
scope only at their existing M2/M3 boundaries.

## 2026-09-17 - DUR-007 and DUR-023A implementation

- Base commit: `6bc0e2f` (verified DUR-006 closeout).
- Implementation commit: `de0c5f2`.
- Implementation status: complete; the review handoff is recorded in `REVIEW.md`.

Implemented the M1 durable graph core. `internal/engine` parses versioned
activity, timer, fan-out, join, success, and failure nodes and drives only the
lease-fenced repository transitions. `internal/state` now reads node/definition
state, persists explicit graph timers, advances downstream nodes idempotently,
and cancels every non-terminal branch while preserving unknown claimed effects
as evidence. Migration 000004 adds the `WORKFLOW_TIMER` purpose without
editing an applied migration. `internal/invariants` provides an independent
evidence checker for revision continuity, terminal monotonicity, accepted
result uniqueness, and submission identity.

Validation completed before handoff:

- `powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/check.ps1`:
  PASS with task-local Go/uv/Pytest cache paths; Go formatting/vet/tests/build,
  Ruff, mypy, and 12 Python tests passed.
- `powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/ci.ps1 -WithRace`:
  PASS; all Go race tests and Python tests passed.
- `powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/ci.ps1 -WithRace -WithServices`:
  PASS; migration 000004 applied and then skipped on rerun, all DUR-005 and
  DUR-006 service tests, four M1 PostgreSQL scenarios, and live service smoke
  checks passed.
- `docker build -f deploy/local/Dockerfile.runtime -t durable-agent-runtime:dur007-check .`:
  PASS. `git diff --check` and `gofmt -l cmd internal` are clean.

The M1 integration scenarios cover restart between durable retry/graph timers,
full fan-out/join execution, concurrent branch completion with one join row,
and cancellation of multiple claimed effects with evidence-only late reports.
The activity driver remains a test fixture; Kafka relay, production effect
services, hard-kill durability, clean bootstrap/restart smoke, sustained-load
testing, and remote CI remain outside this task or untested.

## 2026-09-17 - DUR-007 start

- Base commit: `6bc0e2f` (DUR-006 closeout).
- Task status: IN_PROGRESS; no DUR-007 implementation commit yet.

Opened the interpreter/timer/fan-out/join task against the verified DUR-006
closeout. PLAN.md records the existing M1 scope, acceptance scenarios,
validation commands, evidence paths, and limitations before implementation.
The DUR-005 R028 cancellation follow-up is included explicitly: every active
fan-out branch must settle on cancellation, claimed effects retain
`OUTCOME_UNKNOWN`, late reports remain evidence-only, and no branch may advance
after the terminal decision.

No code or contract implementation changed in this start record. The review
for DUR-007 must use `6bc0e2f` as its exact base.

## 2026-09-17 - DUR-006 closeout

- Review base: `adf5934`; final code target: `a37661d`.
- Task status: DONE after Claude's committed round-11 review found no blocking
  findings. R029-R033 are VERIFIED; R028 remains an explicitly recorded
  DUR-007 fan-out follow-up.

Claude verified 480 concurrent identical fixed-ID retries (40 rounds of twelve)
with exactly one `201` and eleven `200` responses per round, correctly fenced
different-key reuse of the same workflow ID, and the sequential payload/ID
conflict cases. Claude also verified 503 responses after pooled connection loss
and database removal, top-level duplicate-key rejection, and the `sub-v1:`
stored hash prefix. The final code target remains `a37661d`; only the review
and status records changed for this closeout.

Remaining nonblocking limitations are recorded in REVIEW.md: development-only
unauthenticated API, no automatic history pruning, no clean bootstrap or
restart-smoke rerun, no hard-kill durability check, no sustained-load or
timeout study, and no remote CI. The next task is DUR-007, whose review base
will be this DUR-006 closeout commit.

## 2026-09-17 - DUR-006 round-11 corrections

- Base commit: `adf5934`; implementation target: `a37661d`.
- Task status: READY_FOR_REVIEW; R032 is addressed, and the R033 items are
  addressed in the implementation and documentation.

The create transaction now uses an unqualified `ON CONFLICT DO NOTHING` and
resolves the conflicting durable row afterward. It first checks the
namespace/submission key and then the workflow ID, so a concurrent identical
retry with a client-supplied workflow ID returns the committed workflow rather
than a false `WORKFLOW_ID_CONFLICT`; a genuinely different key still receives
the conflict. The integration test runs five rounds of twelve simultaneous
identical fixed-ID requests and requires exactly one `201` and eleven `200`
responses in every round.

The request decoder now rejects duplicate keys at the top level as well as in
`payload` and `initial_input`. Database-unavailable classification covers
safe-to-retry pgx errors, connection/network failures, PostgreSQL connection
and shutdown SQLSTATEs, and closed/EOF connections. Submission hashes carry a
`sub-v1:` prefix, reserving a version boundary for future canonicalization
changes. The API documentation states that a 503 outcome is ambiguous and
must be retried with the same execution meaning and idempotency key.

Validation:

- `go test -race ./...`: PASS.
- `go test -race ./internal/api -run
  '^TestWorkflowAPIResponseLossHistoryAndRetention$' -count=1 -v`: PASS
  against the configured development PostgreSQL service, including five
  rounds of concurrent fixed-ID retries.
- `scripts/ci.ps1 -WithRace -WithServices`: PASS; formatting, vet, Go tests
  and race tests, Ruff, strict mypy, 12 Python tests, all 9 DUR-005 database
  subtests, the DUR-006 PostgreSQL integration suite, and service smoke checks
  passed.
- `docker build -f deploy/local/Dockerfile.runtime -t
  durable-agent-runtime:dur006-r11-check .`: PASS.
- `docker compose --env-file .env -f deploy/local/compose.yaml config
  --quiet`: PASS.
- `git diff --check`: PASS before the handoff documentation edit.

Not rerun: clean bootstrap, restart-smoke, hard-kill durability, remote CI,
and a live stopped-database probe; helper tests cover connection, SQLSTATE,
network, and safe-to-retry classifications. No migrations changed.

Interview explanation: uniqueness constraints can race on more than one key.
The repository treats a no-op insert as an identity-resolution event and reads
the durable winner before deciding whether the caller is retrying or actually
conflicting.

## 2026-09-17 - DUR-006 round-10 corrections

- Base commit: `adf5934`; implementation target: `b252e36`.
- Task status: READY_FOR_REVIEW; R029 and R030 are addressed, and R031 is
  addressed in the implementation and documentation.

The API now hashes the complete execution meaning of a submission:
`definition_id`, `definition_version`, `initial_node_id`, canonical
`initial_input`, and canonical `payload`. Transport-only `workflow_id` and
`actor_id` are excluded. Duplicate JSON object keys are rejected, object keys
are sorted for canonicalization, and number spellings remain distinct because
the decoder preserves them. The repository validates the definition and
initial node inside the create transaction, maps unknown definitions, unknown
nodes, workflow-ID reuse, and database unavailability to typed API responses,
and resolves an existing idempotency record before validating a newly supplied
workflow ID's partition.

The standalone runtime now binds to localhost by default. Local Compose still
binds inside each container for service networking, but publishes both runtime
ports only on `127.0.0.1`. Unknown `/v1/` routes use the JSON error envelope.
The integration test now uses a real HTTP server with a transport that closes
the response and reports a client-side loss, then verifies the exact retry;
the same test covers changed execution meaning, definition/node/ID errors,
duplicate keys, retention, history, and no duplicate outbox/history rows.

Validation:

- `go test -race ./...`: PASS.
- `scripts/ci.ps1 -WithRace -WithServices`: PASS; formatting, vet, Go tests
  and race tests, Ruff, strict mypy, 12 Python tests, all 9 DUR-005 database
  subtests, the DUR-006 PostgreSQL integration suite, and service smoke checks
  passed.
- `docker build -f deploy/local/Dockerfile.runtime -t
  durable-agent-runtime:dur006-r10-check .`: PASS.
- `docker compose --env-file .env -f deploy/local/compose.yaml config
  --quiet`: PASS.
- `git diff --check`: PASS before the handoff documentation edit.

The first full race run exposed a regression in the pre-existing state test:
an idempotent retry with the same key but a different client-supplied workflow
ID was rejected by the new partition check. The repository now resolves the
existing key first and only enforces the new workflow's partition on the
creation path; the full suite was rerun successfully. The service checks used
the configured development PostgreSQL database, which the integration cleanup
left without DUR-006 test rows. Claude's earlier round-9 verification used a
separate throwaway database; the evidence is no longer described as if Codex's
run used one.

Not rerun: clean bootstrap, restart-smoke, hard-kill durability, remote CI,
database-down behavior against a live stopped service, and production
retention/failover. No migrations changed.

Interview explanation: idempotency must protect the request's execution
meaning, not a decorative field. The API computes that identity at the edge,
while the repository independently validates definitions and durable identity
inside the transaction so client mistakes cannot become server-looking 500s.

## 2026-09-14 - DUR-001 repository and reproducible toolchain

- Base commit: `d722cf7` (planning baseline)
- Target commit: `87e123a`
- Task status: READY_FOR_REVIEW

Built the initial Go/Python repository layout, project-level toolchain pins,
Python lock configuration, version-pinned Docker Compose topology, local secret
generation, migration entry point, health endpoints, validation scripts, and
operations documentation. This foundation exists so later durability work can
be tested against real PostgreSQL and Kafka from the first implementation task.

Selected a single-node local PostgreSQL/Kafka topology to keep development
reproduction bounded. Mocks were rejected because they cannot satisfy DUR-001's
real-dependency health and restart checks. Host-wide Go replacement was avoided;
the module's `toolchain` directive obtains the exact Go version per project.

Validation run on a Windows host with Docker Desktop's Linux engine:

- `scripts/bootstrap.ps1`: PASS. Downloaded project Go 1.27.1, created the
  CPython 3.12.7 `.venv`, and installed the exact `uv.lock` set (including
  Ruff 0.16.7, mypy 2.3.1, and pytest 9.1.1).
- `scripts/check.ps1`: PASS. Go format/vet/test/build, Ruff lint/format,
  strict mypy, and pytest all passed; Go had one package test and pytest had
  one import test.
- `scripts/smoke.ps1`: PASS. PostgreSQL 18.6 and Kafka 4.3.1 responded,
  required topics existed, two runtimes/two workers/collector/Prometheus were
  healthy, both runtimes were scraped, and PostgreSQL reported data checksums,
  `fsync`, `synchronous_commit`, and `full_page_writes` on.
- `scripts/restart-smoke.ps1`: PASS. A PostgreSQL marker row and Kafka marker
  topic survived forced container recreation with named volumes; temporary
  evidence was removed after verification.
- `docker compose ... logs kafka | Select-String /var/lib/kafka/data`: PASS.
  Broker recovery logs identified the named-volume path.

The first service smoke attempt timed out immediately after the initial image
pull despite Docker health having just succeeded; every endpoint returned HTTP
200 on inspection. The script now retries each named URL and reports the URL
and final error. A more important inspection found Kafka's initial logs under
`/tmp/kafka-logs`; setting `KAFKA_LOG_DIRS=/var/lib/kafka/data` corrected
the persistence boundary before the passing container-recreation test.

Remaining gap: the clean-checkout bootstrap will be rerun from the committed
target before review handoff. No durable-execution correctness or performance
result is claimed.

Interview explanation: this task separates repeatable infrastructure evidence
from future engine claims. Pinning versions and testing retained volumes makes
later crash/recovery failures attributable to code and protocol changes instead
of an undocumented local setup.

## 2026-09-16 - M0 foundation completion pass

- Base commit: `198fd4b` (DUR-001 scaffold); target implementation commit: `87e123a`.
- Task status: READY_FOR_REVIEW after the implementation commit.

Completed the remaining M0 foundation work. `docs/CONTRACTS.md` freezes actor
authority, identity separation, transaction boundaries, transition permissions,
the required race traces, and the declared failure-model limits. The stable
workflow partition map is versioned as `sha256-u64-be-v1` with 16 partitions;
Go and Python independently implement and test the same canonical vectors.

Added an event-driven DUR-003 fault fixture. A target reports a named boundary
and waits for an explicit release command; the controller can release or kill
only after that event, writes `fault-trace.v1` JSONL evidence, and records a
bounded timeout when the boundary is not reported. Tests cover release, kill,
timeout, and same-seed trace stability.

Added `scripts/ci.ps1` as the shared validation entry point. Go race checks and
real PostgreSQL/Kafka smoke checks are explicit opt-ins; model and paid-provider
checks are not invoked. This keeps unavailable or intentionally out-of-scope
checks visible instead of treating them as passes.

Validation on 2026-09-16:

- `powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/bootstrap.ps1 -StartServices`: PASS. Go 1.27.1, locked uv environment, Docker image builds, migration, and service smoke checks completed.
- `scripts/check.ps1` with repository-local `GOCACHE`, `UV_CACHE_DIR`, `TEMP`, `TMP`, and `PYTEST_ADDOPTS`: PASS. Go vet/test/build, Ruff, strict mypy, and 7 pytest tests passed.
- `scripts/restart-smoke.ps1`: PASS. PostgreSQL marker row and Kafka marker topic survived forced container recreation.
- `python -m faults.control --seed 23 --boundary after-effect --action release`: PASS.
- Timeout CLI run with `--skip-boundary --timeout-seconds 0.1`: expected non-zero timeout result and trace recorded.

The first post-change check attempts exposed pre-existing host cache/temporary
directory collisions under the user's profile; those runs were not counted as
project failures. Validation was repeated with task-local cache locations. The
pytest cache warning for the existing `.pytest_cache` ACL remained non-fatal.
No durable-execution correctness or performance result is claimed by M0.

Interview explanation: freeze the protocol before adding concurrency, then
make crash tests wait on named observed boundaries. The controller proves that
the fault was injected at the intended point; the later engine checker can
consume the same versioned trace without guessing from elapsed time.

## 2026-09-16 - M0 round-2 feedback fixes

- Base commit: `d722cf7` (last reviewed planning baseline); target implementation commit: `ed62eca`.
- Task status: READY_FOR_REVIEW; R001 scope authorization remains open.

Addressed R002-R009 from the round-1 review. The DUR-002 contract is now
`dur-002.v2` with complete workflow/attempt states, cooperating versus
non-cooperating effect recovery, approval/version-paused paths, and numbered
race traces with transaction/store boundaries. DUR-003 now has reusable Go and
Python failpoint clients, a loopback-channel controller for arbitrary commands,
seeded fixtures/fake activities, repeated kill validation, dedicated protocol
messages, early-exit/error handling, stderr draining, release acknowledgements,
and append-flushed traces. Partition vectors are shared in one JSON file with
non-ASCII and invalid-UTF-8 coverage. The runtime image copies `internal/`, the
migration script is ledger-aware, and service-only CI explicitly reports that
M0 has no integration tests yet.

Validation after the fixes:

- `scripts/check.ps1` with task-local caches/temp paths: PASS; Go build/vet/test, Ruff, strict mypy, and 11 pytest tests passed.
- `scripts/ci.ps1 -WithRace`: PASS; Go race tests and the full Python suite passed.
- `scripts/bootstrap.ps1 -StartServices`: PASS; runtime images rebuilt with `internal/`, migration ledger skip worked, and all services were healthy.
- `scripts/restart-smoke.ps1`: PASS; PostgreSQL and Kafka markers survived forced container recreation.
- `scripts/ci.ps1 -WithRace -WithServices`: PASS; Go race/Python tests, live service smoke, durability settings, and Prometheus target checks passed. The command explicitly reported that M0 integration tests are not implemented.
- Targeted fault validation: 7/7 fault tests passed, including Python/Go targets, repeated same-seed kills, different-seed fixture fields, early exit, non-protocol stdout, heavy stderr, release acknowledgement, and JSONL sequence persistence.

The remaining review blocker is R001: RQ7/RQ8 and the retrieval/adversarial
scope expansion were already present before this M0 implementation pass. They
were preserved in the target commit, but no authorship/authorization decision
is recorded yet. No paid cap is invented; PLAN.md still requires explicit
approval before any paid/model run.

Interview explanation: review feedback turned the first harness from a toy
process demo into a reusable protocol boundary. The important distinction is
that ordinary logs cannot be allowed to corrupt fault-control evidence, and a
process exit must be reported as an exit rather than guessed to be a timeout.

## 2026-09-16 - M0 scope authorization follow-up

- Base commit: `ed62eca`; target commit: `c78803e`.
- Task status: READY_FOR_REVIEW; R001 is addressed and M0 remains pending Claude round-2 verification.

The user confirmed authorship and authorization of the protected PLAN.md scope
changes that predated the M0 implementation pass. Added D005 to
`docs/DECISIONS.md` covering RQ7/RQ8, retrieval/MCP/pgvector/full-text/Grafana
scope, the revised release criteria, the 40-development/120-held-out
retrieval benchmark, 60 live retrieval-arm executions, and 120 adversarial
executions. D005 records that paid/model execution and its spending cap need
separate approval before DUR-029; it does not claim that those experiments
have run.

Validation: `git diff --check` passed, and the decision was reviewed against
the protected-scope locations listed in R001. No runtime checks were rerun
because this was a documentation-only authorization change; the full M0
validation remains recorded in the preceding round-2 entry.

Interview explanation: protected planning changes are evidence too. D005
separates authorization of the portfolio question from authorization to spend
money or claim that the future model experiments have produced results.

## 2026-09-16 - M0 round-3 contract and trace fixes

- Base commit: `c78803e`; target implementation commit: `eb32162`.
- Task status: READY_FOR_REVIEW; R010-R012 are addressed and M0 remains pending Claude round-3 verification.

Addressed the new round-2 findings before DUR-005. The contract is now
`dur-002.v3`: worker result receipts and client/approver intent records do not
apply workflow transitions; lease-owning scheduler transactions apply those
transitions with lease-first locking and validation. Timeout and reconciliation
walkthroughs now include the lease lock, result advancement is a separate
owner transaction, and approval/cancellation races distinguish intent from
owner application. The state table adds `REJECTED` no-action, complete
cancellation exits, complete attempt outcomes, and retry/backoff-only
`WAITING_TIMER` semantics. Fault traces now have per-run IDs and exclusive
paths, with regression coverage for path reuse.

Validation:

- `scripts/check.ps1` with task-local cache/temp paths: PASS; Go tests/build/vet, Ruff, strict mypy, and 12 Python tests passed.
- `scripts/ci.ps1 -WithRace`: PASS; Go race tests and 12 Python tests passed.
- Targeted fault suite: PASS, 8/8 tests, including exclusive trace paths and run IDs.
- `git diff --check`: PASS. Host pytest cache ACL warnings persisted but were non-fatal; task-local roots were used and removed afterward.

Remaining gaps: clean bootstrap and restart smoke were not rerun in this pass
because Codex did not force-recreate the user's shared running containers; no runtime
engine or paid/model behavior is claimed by M0. Claude round-3 verification is
pending.

Interview explanation: the contract now separates evidence receipt from
authority to advance durable workflow state. That makes fencing, lock order,
and cancellation/approval races explicit before the first engine schema is
implemented.

## 2026-09-16 - M0 round-4 timeout-contract fixes

- Base commit: `eb32162`; target implementation commit: `b993d71`.
- Task status: READY_FOR_REVIEW; R013-R015 are addressed and M0 remains pending Claude round-4 verification.

Advanced the contract to `dur-002.v4` to make timeout handling depend on both
claim state and the immutable activity effect class. Unclaimed dispatchable
work is redispatched with the same attempt identity. Claimed pure activity may
be replaced; claimed cooperating effects may be replaced only with the same
effect key and grant scope; claimed non-cooperating effects become
outcome-unknown and enter `RECONCILIATION_REQUIRED` without an automatic
replacement. The permission table now separates approver/client intent from
lease-owner application, and the worker revision wording no longer rejects a
valid result solely because of an unrelated revision change. The handoff now
reports 12 Python tests and round-3 verification status.

Validation:

- `scripts/ci.ps1 -WithRace`: PASS; formatting, lint, strict mypy, 12 Python tests, and Go race tests passed.
- `git diff --check`: PASS.
- Known host pytest cache ACL warning remained non-fatal; task-local roots were used and removed afterward.

Remaining gaps: clean bootstrap and restart smoke were not rerun in this pass
because Codex did not force-recreate the user's shared running containers. M0 makes no
runtime-engine or paid/model evidence claim. Claude round-4 verification is
pending.

Interview explanation: timeout is not evidence that an external action did not
happen. The v4 contract makes retry safety an explicit property of both claim
state and effect cooperation, preventing an uncertain irreversible action from
being automatically repeated.

## 2026-09-16 - DUR-005 durable state repository

- Base commit: `79ba118`; implementation commit: `1698747`; final handoff
  commit is the documentation commit containing the fixed review target.
- Task status: READY_FOR_REVIEW; DUR-005 is handed off for Claude review.

Implemented the first PostgreSQL-backed durable state repository under the
frozen `dur-002.v4` contract. Migration `000002` adds workflow definitions and
executions, node instances, attempts with immutable effect classes, leases,
timers, checkpoints, transition history, outbox/inbox, approval/cancellation
intents, effect records, and late-result evidence. The Go repository enforces
lease-first scheduler locking, revision checks, transition legality, claim
tokens, idempotent submission, rollback on rejected transitions, and the four
effect-class-aware timeout outcomes. R016 is closed in the first contract
touch: late reports from timed-out non-cooperating attempts are retained as
evidence without changing durable workflow state.

Validation:

- `scripts/ci.ps1 -WithRace -WithServices`: PASS with task-local Go/UV caches
  and an explicit pytest basetemp; Go formatting/vet/tests/build, Ruff,
  strict mypy, 12 Python tests, Go race tests, migration-ledger skip checks,
  three PostgreSQL integration subtests, and PostgreSQL/Kafka/telemetry smoke
  checks passed.
- `docker build -f deploy/local/Dockerfile.runtime .`: PASS; the runtime image
  includes `go.sum` and `internal/` and builds successfully.
- `git diff --check`: PASS before the final handoff-only documentation edit.

Remaining gaps: clean bootstrap and restart smoke were not rerun because they
recreate the user’s running containers; no remote CI exists. DUR-005 does not
implement the complete scheduler, interpreter, Kafka relay, effect service, or
end-to-end workflow engine, and makes no exactly-once, failover, hard-kill, or
paid/model claim.

Interview explanation: the repository establishes the database boundary before
the interpreter exists. The key design choice is that owner-authorized writes
lock and fence the lease before workflow/node/attempt rows, while uncertain
non-cooperating effects stop in reconciliation and preserve late evidence.

## 2026-09-17 - DUR-005 review-fix pass

- Base commit: `79ba118`; implementation commits: `f5fe570`, `49e4844`.
- Task status: READY_FOR_REVIEW; DUR-005 is handed off to Claude at `49e4844`.

Addressed round-1 DUR-005 findings R017-R023. Claim, redispatch, and replacement
deadlines are re-armed; result and claim retries are durable and scoped; the
lease owner consumes results before advancing nodes or creating retry timers;
partition and effect-class metadata are validated at the repository boundary;
service-mode database checks fail loudly when PostgreSQL is unavailable; and
test cleanup is compatible with retained late evidence. Workflow creation and
worker result history/outbox records are now durable, same-owner lease renewal
preserves its epoch, and late evidence retries are idempotent. The integration
suite also injects outbox failures to prove `CreateAttempt` and `TimeoutAttempt`
roll back all intermediate writes.

Validation:

- `scripts/ci.ps1 -WithRace -WithServices`: PASS; formatting, vet, Go tests and
  race tests, build, Ruff, strict mypy, 12 Python tests, numbered migration
  checks, eight PostgreSQL integration subtests, and PostgreSQL/Kafka/telemetry
  smoke checks passed. The integration suite includes 20 rounds each of
  concurrent claims and result-versus-timeout races.
- Required-database mode with a wrong PostgreSQL password failed instead of
  skipping. The default non-service integration invocation explicitly skipped.
- A post-run query found zero `dur005-*` workflows, definitions, or evidence
  rows; the live evidence foreign key is `ON DELETE CASCADE`.
- `docker build -f deploy/local/Dockerfile.runtime -t durable-agent-runtime:dur005-check .`: PASS.
- `git diff --check`: PASS before the handoff-only documentation edit.

Remaining gaps: clean bootstrap and restart smoke were not rerun because they
recreate the user's running containers; no remote CI exists; and DUR-005 still
does not implement the complete scheduler, interpreter, Kafka relay, effect
service, or hard-kill durability study. The database tests use generated local
identities and do not claim production retention, failover, or exactly-once
behavior.

Interview explanation: the repository now makes the safety boundary explicit
at both sides of an uncertain activity. The worker can record exactly one
durable receipt, while only the lease owner can consume it and advance the
workflow; timeout branches are selected from immutable effect metadata, so a
non-cooperating unknown effect cannot be automatically repeated.

## 2026-09-17 - DUR-005 round-6 fixes

- Base commit: `79ba118`; implementation target: `dce5433`.
- Task status: READY_FOR_REVIEW; round-6 findings R024-R026 are addressed and
  handed back to Claude for verification.

Addressed the cancellation and retry-integrity findings. Owner cancellation
now settles the current attempt and clears the node pointer in the same locked
transaction. Terminal workflows reject timeout replacement and new late
results, while identical durable-result retries remain idempotent. Cooperating
retries inherit the first attempt's effect key and grant scope, or fail without
partial state when a caller supplies a mismatch. Retry timers must be due and
are consumed atomically when the owner releases `WAITING_TIMER`.

Migration hygiene was corrected by restoring the already-applied `000002`
schema and keeping the change in `000003`; the new constraint creation is
guarded so the migration can be re-run manually. CI script indentation was
normalized.

Validation:

- `scripts/ci.ps1 -WithRace -WithServices`: PASS; Go format/vet/tests/build,
  Go race tests, Ruff, strict mypy, 12 Python tests, migration checks, nine
  PostgreSQL integration subtests, 20-round claim/result races, rollback
  injection, cancellation fencing, timer enforcement, retry identity, and
  service smoke checks passed.
- `000003_dur005_integrity.up.sql` executed twice manually: PASS; the schema
  ledger retained one version-3 row and no duplicate constraint was created.
- `docker build -f deploy/local/Dockerfile.runtime -t
  durable-agent-runtime:dur005-r7-check .`: PASS.
- Post-run database hygiene query: zero `dur005-*` workflows, definitions, or
  evidence rows. `git diff --check` passed before the handoff documentation
  edit.

Remaining gaps: clean bootstrap and restart smoke were not rerun because they
recreate the user's running containers; no remote CI exists; and DUR-005 does
not implement the complete scheduler/interpreter, Kafka relay, hard-kill
durability study, or production exactly-once behavior. Timer scheduling beyond
the repository's due/consume guard remains a DUR-007 concern.

Interview explanation: cancellation is a state-and-attempt settlement, not
just a workflow flag. The repository fences every later timeout, heartbeat,
and result against that terminal decision, while retry identity is derived
from the first cooperating attempt so a scheduler bug cannot create a second
logical sink operation.

## 2026-09-17 - DUR-005 round-7 cancellation-evidence fix

- Base commit: `79ba118`; implementation target: `333a555`.
- Task status: READY_FOR_REVIEW; R027 is addressed and R028's node-revision and
  non-cooperating grant-scope gaps are addressed. Fan-out cancellation remains
  a DUR-007 follow-up because DUR-005 has one current node per workflow.

Addressed the cancellation evidence boundary. A claimed cooperating or
non-cooperating effect canceled in flight is stored as `CANCELED` with
`OUTCOME_UNKNOWN`; pure and unclaimed attempts remain `NONE`. A later worker
result or explicit late-evidence report is retained idempotently as evidence,
including after terminal cancellation, without changing the workflow state,
revision, attempt state, or outbox. Cancellation now also increments the node
revision. Non-cooperating retries inherit the first grant scope or reject a
mismatch, matching the existing cooperating-effect identity rule. The v4
contract now describes the evidence-only exception.

Validation:

- `scripts/ci.ps1 -WithRace -WithServices`: PASS; Go format/vet/tests/build,
  Go race tests, Ruff, strict mypy, 12 Python tests, migration checks, all nine
  PostgreSQL integration subtests, cancellation evidence fencing, audited
  reconciliation cancellation, non-cooperating grant identity, rollback
  injection, and 20-round claim/result races passed; service smoke and
  durability checks passed.
- Focused `TestPostgresStateRepository`: PASS with all 9 subtests.
- `git diff --check`: PASS before handoff documentation changes.

The runtime Docker build was not rerun in this pass because no Dockerfile or
runtime packaging changed; the prior successful image build remains applicable.
Clean bootstrap/restart smoke still was not rerun because those workflows
recreate the user's running containers. No remote CI exists. DUR-005 still does
not claim fan-out scheduling, hard-kill durability, failover, or exactly-once
execution.

Interview explanation: cancellation cannot be allowed to turn an issued effect
into a false "nothing happened" record. The repository makes uncertainty
durable, accepts late reports only as evidence, and keeps the terminal decision
fenced from progress.

## 2026-09-17 - DUR-005 closeout

- Base commit: `79ba118`; final code target: `333a555`; handoff commit:
  `614450c`; closeout commit: `adf5934`.
- Task status: DONE after Claude's committed, non-provisional round-8 review;
  verdict `NO_BLOCKING_FINDINGS`. R001-R027 are VERIFIED. R028 remains a
  non-blocking P3 fan-out follow-up and is now recorded in the DUR-007 scope in
  PLAN.md.

The final DUR-005 repository implementation is accepted under the frozen v4
contract. Claude verified cancellation evidence for claimed effect attempts,
idempotent/conflicting late reports, wrong-token fencing, operator cancellation
from reconciliation, and three consecutive race-suite runs. The remaining
untested areas are explicitly preserved in REVIEW.md: PostgreSQL unavailability,
lock/statement timeouts, clean bootstrap/restart smoke, hard-kill durability,
and remote CI. DUR-005 makes no claim that a complete engine or scheduler
exists.

The contract attempt diagram indentation was corrected as part of closeout.
The next task is DUR-006, whose review must use this closeout commit as its
base and whose scope/acceptance record is now present in PLAN.md.

Interview explanation: a milestone closes only when the fixed code target,
acceptance evidence, and independent committed review agree. The repository's
durability boundary is now a reviewed foundation for the client-facing API
work, while fan-out cancellation remains explicitly owned by DUR-007.

## 2026-09-17 - DUR-006 start

- Base commit: `adf5934` (DUR-005 closeout).
- Task status: IN_PROGRESS; no DUR-006 implementation commit yet.

Opened the submission/query API task with its scope, acceptance scenarios,
validation commands, evidence paths, review base, and limitations recorded in
PLAN.md. The first implementation pass will expose the durable create/status/
history behavior already supported by the repository without expanding into
scheduler, fan-out, Kafka, or paid/model work.

Next implementation evidence must include a response-drop-after-commit retry,
same-key/same-payload idempotency, conflicting-payload rejection without
mutation, and ordered status/history reads against throwaway PostgreSQL.

## 2026-09-17 - DUR-006 implementation handoff

- Base commit: `adf5934` (DUR-005 closeout); implementation target: `d8083d2`.
- Task status: READY_FOR_REVIEW; DUR-006 is handed to Claude for review.

Implemented the first versioned client API. Submission canonicalizes JSON
payloads and computes the SHA-256 identity used by the repository; retries with
the same namespace, submission key, and payload return the existing workflow
after a response loss, while a changed payload returns a precise conflict.
Status and ordered transition-history queries are exposed with bounded cursors
and stale-revision checks. The runtime connects to PostgreSQL when
`DATABASE_URL` is configured and still supports the foundation health-only mode
without it. The API documents a no-automatic-pruning retention policy and the
exact-retry policy for uncertain client outcomes.

Validation:

- `scripts/ci.ps1 -WithRace -WithServices`: PASS; all shared Go/Python checks,
  Go race tests, 9 DUR-005 repository subtests, the DUR-006 API integration
  test, and service smoke checks passed.
- API integration: response discarded after commit then retried; one workflow,
  one creation history row, and one outbox row remained. Payload conflict,
  not-found, stale revision, ordered history, cursor, and retention behavior
  passed.
- `docker build -f deploy/local/Dockerfile.runtime -t
  durable-agent-runtime:dur006-check .`: PASS.
- `git diff --check`: PASS before the handoff-only documentation edit.

Remaining gaps: PostgreSQL-unavailable behavior, lock/statement timeouts,
clean bootstrap/restart smoke, hard-kill durability, remote CI, automatic
retention/deletion, and the complete engine/scheduler/Kafka path are not claimed
by DUR-006. Review should use `adf5934` as its base and `d8083d2` as its target.

Interview explanation: the API separates a lost client response from a lost
database commit. The database remains authoritative, and an exact retry either
finds the committed workflow or receives an explicit payload conflict; status
and history then let the client resolve the uncertain outcome without guessing.

## 2026-09-16 - M0 final acceptance

- Base commit: `d722cf7`; final code/contract target: `b993d71`.
- Task status: DONE for DUR-001 through DUR-004; M1/DUR-005 remains the next task.
- Review basis: COMMITTED, non-provisional Claude review; verdict `NO_BLOCKING_FINDINGS`.

Claude verified R001-R015 at the final target. The remaining R016 item is a
nonblocking P3 wording inconsistency about recording a late result from a
timed-out non-cooperating attempt; it is preserved in REVIEW.md for the first
contract touch in DUR-005. No engine implementation or runtime correctness
claim is made by M0.

The final acceptance evidence includes `ci.ps1 -WithRace` with 12 Python tests
and Go race tests, the earlier service health/durability/restart evidence, and
the committed contract, fault-control, partition, and setup records. Untested
items remain explicitly listed in REVIEW.md: Go failpoint release, a
non-idempotent migration, clean-machine bootstrap, hard-kill durability, and
remote CI.

Interview explanation: M0 is complete when the setup, contracts, deterministic
fault controls, and shared checks are reviewed as a fixed target. It does not
claim that the durable engine exists; DUR-005 begins that implementation under
the frozen v4 contract.

## 2026-09-17 - M4 implementation handoff

- Base commit: `8fb2f75` (M3 closeout); implementation commit: `92f3f14`.
- Task status: DUR-015, DUR-016, DUR-017, DUR-018, and DUR-023A-M4 are
  READY_FOR_REVIEW. M4 remains IN_PROGRESS pending Claude's committed review.

M4 adds durable retry policies and checkpoint progress, retry-budget and timer
enforcement, actor-audited unknown-effect resolution, a grant-bound
cooperating effect ledger with request-attempt evidence and resource-local
fencing, a deliberately non-cooperating endpoint fixture, exact approval
proposal/decision/grant handling, rejection/no-action application, and an
atomic cancellation-request owner path. The independent checker now loads and
validates checkpoint, effect, call-attempt, and approval evidence. Migrations
000010 and 000011 add the M4 tables and resolution audit.

Validation actually run:

- `powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/ci.ps1
  -WithRace -WithServices`: PASS. Migrations 000010 and 000011 were already
  applied idempotently, shared Go/Python checks passed, 19 Python tests passed,
  serial Go race packages passed, all DUR-005 through DUR-014 focused suites
  passed, all M4 state/engine/checker integration suites passed, and service
  smoke passed for PostgreSQL/Kafka/runtime/workers/telemetry.
- Focused race-enabled M4 suites: checkpoint crash resume, retry exhaustion,
  approval grant/rejection, cancellation-before-grant, cooperating dedupe and
  scoped fencing, non-cooperating timeout reconciliation, and loaded
  independent checker evidence all passed against local PostgreSQL.
- `go vet ./...`, `gofmt`, Ruff, mypy, and 19 Python tests passed through CI.
- `docker compose --env-file .env -f deploy/local/compose.yaml config --quiet`,
  `git diff --check`, and
  `docker build -f deploy/local/Dockerfile.runtime
  -t durable-agent-runtime:dur004-m4-check .`: PASS.

Known limitations: the approval/worker API remains localhost-bound and
development-only without production authentication; the non-cooperating
endpoint is a deterministic fixture; hard-kill durability, database outage and
lock-timeout campaigns, multi-host deployment, sustained load, clean
bootstrap/restart smoke, and remote CI remain untested or outside this
milestone. No exactly-once or production-remediation claim is made.

Interview explanation: a checkpoint preserves committed pure work but cannot
make an external call atomic. The effect service therefore needs a separate
stable-key receipt and resource fence, while an unobservable endpoint must stop
at reconciliation. Approval is a durable capability bound to the exact action,
and cancellation/grant races are serialized by the scheduler lease and
workflow row.

## 2026-09-17 - M4 final effect-boundary fix and handoff refresh

- Base commit: `8fb2f75`; final implementation target: `4d2aa81`.
- Task status: READY_FOR_REVIEW; DUR-015, DUR-016, DUR-017, DUR-018, and
  DUR-023A-M4 remain pending Claude's committed review.

The cooperating effect ledger now lives in the independently owned `effects`
schema created by migration `000012`; grant validation reads the engine grant
before the effect transaction, and the effect transaction writes only the
effect ledger. It no longer updates approval rows or uses foreign keys into
workflow state. The integration test asserts that the effect service row is
written while the legacy engine effect table remains untouched. A shared M1
fixture candidate cap was also increased to avoid a rare partition-mapping
false failure under service CI.

Validation:

- `scripts/migrate.ps1`: PASS; migration `000012` applied once and is now
  skipped idempotently.
- `scripts/ci.ps1 -WithRace -WithServices`: PASS; serial Go checks and race
  tests, 19 Python tests, DUR-005 through DUR-018 integration tests, M4
  checkpoint/effect/approval/checker suites, and live service smoke passed.
- Focused M4 race suites: PASS for state, engine checkpoint recovery, and
  invariant loading against PostgreSQL.
- `docker compose ... config --quiet`: PASS.
- `docker build -f deploy/local/Dockerfile.runtime -t
  durable-agent-runtime:dur004-m4-final .`: PASS.
- `git diff --check`: PASS before this handoff metadata update.

Skipped/remaining gaps are unchanged: hard-kill durability, PostgreSQL outage
and lock-timeout campaigns, sustained load, multi-host deployment or broker
rebalance, clean bootstrap/restart smoke, and remote CI. The APIs remain
localhost-bound development surfaces without production authentication; no
exactly-once or production-remediation claim is made.

Interview explanation: the engine authorizes an effect, but the sink owns its
own receipt and protected state. Separating the `effects` ledger transaction
prevents a workflow transaction from pretending it can make an external
mutation atomic, while stable keys and resource fences still make retries
safe for the cooperating contract.

## 2026-09-17 - M4 R049 approval-grant binding fix

- Base commit: `4d2aa81`; implementation target: `fcdbf09`.
- Task status: READY_FOR_REVIEW; R049 is ADDRESSED and pending Claude
  verification. M4 remains IN_PROGRESS.

Closed the approval authorization gap. Grant scope now includes the approved
resource, effect application recomputes the argument hash from submitted state
and compares both resource and arguments with the approved intent, and the
first successful application marks the grant `DISPATCHED`. Applied effect
receipts retain intent/resource identity. The independent checker validates
proposal hashes and reconciles applied effects to approved intent, resource,
arguments, and scope. Migration `000013` adds receipt authorization fields.

Validation:

- `scripts/migrate.ps1`: PASS; migration `000013` applied and subsequent runs
  skip it idempotently.
- `scripts/ci.ps1 -WithRace -WithServices`: PASS; serial Go checks and race
  tests, 19 Python tests, DUR-005 through DUR-018 PostgreSQL/Kafka integration,
  M4 state/engine/invariant suites, and service smoke all passed.
- Regression checks: mismatched resource and submitted state are rejected
  before mutation or a new effect row; duplicate same-key/same-state returns
  the original receipt; grant status becomes `DISPATCHED`; checker fixtures
  reject mismatched resource/arguments and missing approval linkage.
- `docker compose ... config --quiet`: PASS.
- `docker build -f deploy/local/Dockerfile.runtime -t
  durable-agent-runtime:dur004-m4-r049 .`: PASS.
- `git diff --check`: PASS before handoff metadata changes.

Interview explanation: approval is a capability over a complete action, not a
key-shaped permission. The sink independently recomputes the payload identity,
binds the resource and scope, and leaves durable evidence that the checker can
compare with the approval record.

## 2026-09-17 - M4 closeout

- Base commit: `8fb2f75`; reviewed implementation target: `fcdbf09`.
- Task status: M4 DONE. DUR-015, DUR-016, DUR-017, DUR-018, and DUR-023A-M4
  are DONE after Claude's committed round-20 `NO_BLOCKING_FINDINGS` review;
  R049 is VERIFIED.
- Closeout documentation is committed together with the preserved review
  record in this closeout change.

M4 acceptance is complete: retry/checkpoint recovery, cancellation and
ambiguous-outcome handling, cooperating and non-cooperating effect paths,
approval binding, and independent checkpoint/effect/approval invariants are
implemented and reviewed. Migration 000013 applied twice, seven consecutive
serial race-suite runs on a pristine database passed, and the full service
validation, vet, formatting, build, and 19 Python tests passed.

Remaining limitations are explicit: R019's committed-test gap, hard-kill and
outage campaigns, sustained load, multi-host deployment, clean bootstrap and
restart smoke, real production effect callers, and remote CI remain outside
the demonstrated M4 claim.

Interview explanation: M4 closes the authorization boundary by making an
approval a capability over the exact resource and canonical action, while the
effect ledger and independent checker preserve evidence across lost responses
and prevent an approval-shaped token from authorizing a different mutation.

## 2026-09-17 - M5 start

- Base commit: `612dde9` (M4 closeout).
- Task status: M5 IN_PROGRESS; DUR-022 is IN_PROGRESS. DUR-023B, DUR-024,
  DUR-025, and DUR-021A remain TODO.
- Scope: complete the fault controller's named boundaries, process
  kill/pause, finite network cuts, message manipulation, deterministic
  schedules, target acknowledgements, observed-outcome recording, and
  bounded cleanup. The full F01-F11 campaign remains DUR-024.
- No DUR-022 implementation commit exists yet. No M5 correctness or
  performance result is claimed.

The M5 start uses the M4 closeout as its exact base. The first implementation
pass will make requested-versus-observed fault outcomes durable and
distinguishable, including target crash, timeout, malformed output, and
post-boundary completion cases, before the correctness campaign consumes the
controller evidence.

Interview explanation: a fault request is not proof that the fault happened;
the controller must receive a named-boundary acknowledgement and persist the
observed result so a later checker can distinguish an unexecuted experiment
from a successful recovery claim.

## 2026-09-17 - M5 implementation and review handoff

- Base commit: `561b5a9` (M5 start record; M4 closeout ancestor `612dde9`).
- Implementation target: `c5cd2b8`.
- Task status: M5 READY_FOR_REVIEW; DUR-022, DUR-023B, DUR-024, DUR-025, and
  DUR-021A are READY_FOR_REVIEW. Claude review is pending.

M5 completed the named-boundary fault controller and local fault proxy, with
target acknowledgements, requested-versus-observed actions, malformed-output
and timeout classification, process pause/kill, message/network controls, and
bounded idempotent cleanup. The independent invariant checker now consumes
`fault-trace.v1` summaries without importing the controller or production
transition validators, and mutation tests cover missing cleanup/outcomes,
duplicate runs, identity errors, contradictory boundary evidence, and the
existing durable-state/effect/approval invariants. Runtime, worker, store,
relay, and database instrumentation exports bounded Prometheus metrics using
only the `role` label.

Validation and evidence:

- `scripts/ci.ps1 -WithRace -WithServices -WithM5`: PASS. This ran migrations
  000001 through 000013, Go format/vet/build/tests, 24 Python tests, serial Go
  race packages, DUR-005 through DUR-018 service suites, service smoke,
  `TestM5BoundedTwoSchedulerSmoke`, and the isolated F01-F11 campaign.
- `experiments/m5/f01-f11-results.json`: all F01-F11 cases PASS with package,
  test pattern, exit code, timestamp, and captured output. The campaign
  temporarily stops only runtime/worker relays so they cannot consume its
  outbox fixtures, then restores them in a bounded cleanup path.
- `TestM5BoundedTwoSchedulerSmoke`: four workflows completed concurrently on
  two scheduler owners in 137 ms. This is a preliminary smoke, not a final
  throughput result.
- Real local recovery checks: Kafka outage restored with runtime health 200;
  PostgreSQL outage returned runtime 503 and recovered; one worker stopped
  left runtime health 200 and recovered; runtime/worker process restart with
  retained volumes passed `scripts/smoke.ps1`; `scripts/restart-smoke.ps1`
  had already verified PostgreSQL/Kafka markers survive recreation.
- `docker compose --env-file .env -f deploy/local/compose.yaml config --quiet`:
  PASS. Fresh `durable-agent-runtime:m5-check` and
  `durable-agent-worker:m5-check` images built successfully.
- Live `/metrics` rendering and Prometheus scraping: PASS for both runtime
  replicas; telemetry unit tests passed with the full Go suite.
- `git diff --check`: PASS before this documentation handoff commit.

Failed approach and correction: the first complete service gate allowed live
runtime relays to consume database-test outbox fixtures, producing a
reproducible `TestM3TransportPersistenceAndFencing` fixture race. Service-mode
CI now stops only runtime/worker relays around shared database checks, restores
them before smoke, and restores them in `finally`; the M5 campaign has the
same explicit isolation switch. The corrected gate passed.

Skipped or untested: Kafka consumer rebalance, multi-host deployment,
hard-kill storage durability, lock/statement-timeout campaigns,
sustained-load/final performance studies, clean-machine bootstrap, and remote
CI. The local Docker topology is single-node development evidence. Control
and worker APIs remain unauthenticated and localhost-bound; no production
effect caller, exactly-once, or multi-host durability claim is made.

Interview explanation: M5 turns “we requested a crash” into an auditable
experiment by recording what the target acknowledged, what process outcome
was observed, and whether cleanup completed. The checker then joins that
evidence to durable engine history independently, while the service gate
isolates test fixtures from live relays so a green campaign is reproducible.

## 2026-09-18 - M5 round-22 corrections

- Review basis: Claude round 21 requested R050-R054 changes against `c5cd2b8`.
- Implementation commit: `6ab54ea`; campaign evidence guard and regenerated
  evidence: `69917db`. M5 remains READY_FOR_REVIEW; no task is DONE.

R050 is addressed by accepting the controller's array-valued `process_started`
command, validating `fault-trace.v1` schema/run/sequence data, and joining only
explicit durable identity fields to PostgreSQL-backed workflow, attempt, result,
outbox, and reconciliation evidence. The checker now rejects cleanup timeouts,
unbounded cleanup, and contradictory process-exit evidence. `cmd/fault-checker`
is the executable path used by the campaign, and 48 committed traces were
parsed by it.

R051 is addressed by `cmd/m5-fixture`, a real PostgreSQL target that pauses at
the named boundary and is killed by the Python controller. The campaign now
enumerates 16 atomic cases across F01-F11, runs seeds 11/23/47, records case
IDs/orderings/observations and trace paths, forces `DURABLE_REQUIRE_DATABASE=1`,
and fails if the selected Go test emits `--- SKIP`. The final artifact has
48/48 controller PASS, checker PASS, Go PASS, with zero skips.

R052 is addressed by distinguishing requested cleanup from observed exit:
cleanup timeout records `cleanup_timeout`/`cleanup_unbounded`, does not invent a
return code, and leaves `cleaned_up` false. The Python regression test and
independent Go checker mutation test cover this path.

R053 is addressed by wiring store, lease, outbox, reconciliation, backlog,
relay, and engine telemetry, moving readiness behind real dependency health and
re-marking it after recovery. The service-backed M5 smoke observes readiness,
lease, claim, result, and database-query signals. The runtime binary remains a
control-plane/relay foundation and does not yet construct a production
scheduler Engine; that limitation is now explicit rather than presented as
live engine telemetry.

R054 is addressed by `scripts/m5-outage-report.ps1` and
`experiments/m5/outage-recovery.json`. Four local episodes (Kafka, PostgreSQL,
worker, runtime) contain separate before/during/after health and unresolved-
work snapshots. A dependency-down database snapshot is recorded as unavailable,
not as zero outstanding obligations; all four after snapshots recovered.

Validation completed after the fixes:

- `scripts/check.ps1 -SerialPackages`: PASS; Go tests, vet/build, Ruff, mypy,
  and 25 Python tests.
- Relay-isolated `go test -race -p 1 ./... -count=1` with
  `DURABLE_REQUIRE_DATABASE=1`: PASS, including the M5 smoke.
- `scripts/m5-campaign.ps1 -StopRuntimeRelays`: PASS, 16 cases x 3 seeds,
  48/48 PASS and zero skips.
- `scripts/m5-outage-report.ps1`: PASS; artifact written and Compose services
  restored. `git diff --check`: clean before the documentation commit.

Failed approaches and lessons: an initial campaign invocation omitted the
database-required flag and produced green skipped tests; the campaign now
guards against both omission and skips. A Windows temp-path invocation also
created one malformed generated directory that confused Go's `./...` scanner;
the exact generated directory was removed, and the isolated service race gate
passed afterward. Earlier live-relay fixture interference remains handled by
the serial stop/restore isolation.

Interview explanation: the correction turns the M5 report from a collection of
green commands into an auditable chain: the controller creates the durable
prefix, records what it actually observed, the independent checker joins that
identity to persisted state, and the campaign refuses to accept a skipped
database case.

## 2026-09-18 - M5 round-23 corrections

- Review basis: Claude round 22 requested R055-R058 changes against `69917db`.
- Implementation commit: `acb28ba`. M5 remains READY_FOR_REVIEW; no task is
  DONE pending Claude verification.

R055 is addressed by making missed-boundary evidence sticky, requiring a
boundary acknowledgement for every campaign-scoped action, rejecting unknown
boundary names, and requiring the durable identity needed by each boundary
join. The checker now rejects a target that exits before its declared boundary
instead of treating missing durable fields as a valid empty join.

R056 is addressed by building `m5-fixture` before the campaign and killing the
actual target process tree (`taskkill /T /F` on Windows and a process group on
POSIX). The trace records the kill mechanism. This removes the `go run` wrapper
and its cold-start race from the boundary protocol.

R057 is addressed by making the fixture execute the substantive durable path
for each named boundary: effects, approval grants, timer consumption,
timeouts, cancellation, lease takeover, and result transitions now have
corresponding persisted rows. The checker has a join rule for every campaign
boundary and rejects unknown names. The timeout path also marks the terminal
non-cooperating attempt non-current and writes the reconciliation outbox event,
which the strict checker correctly requires.

R058 is addressed with per-campaign run IDs, seed/run identity in fixture input,
durable JSON snapshots beside all 48 traces, and the offline archive checker.
`scripts/m5-archive-check.ps1` revalidated all 48 committed traces after the
campaign completed. The campaign preserves the rows needed by those snapshots
and releases fixture lease ownership without deleting the lease table rows.

Validation:

- `scripts/check.ps1 -SerialPackages`: PASS; all Go packages, formatting,
  Ruff, mypy, and 25 Python tests.
- `scripts/m5-campaign.ps1 -StopRuntimeRelays`: PASS; 16 cases x seeds
  11/23/47, 48/48 controller/checker/Go PASS, zero skips.
- `scripts/m5-archive-check.ps1`: PASS; 48/48 archived traces validated
  offline.
- Focused invariant/checker and fixture packages, `gofmt`, and `git diff
  --check`: PASS.

Post-handoff validation note: a direct race run with live relays and the first
service-gate attempt reached the M3 transport phase but hit the known shared
fixture hazard after a stale partition-8 lease; that run is not counted as a
full-gate pass. After stopping runtime/worker relays, the isolated
`go test ./internal/transport -run '^TestM3' -count=1 -v` passed all five tests,
and the relays were restored. The database was checked afterward: all 16 lease
rows exist, no lease is active, and no `m5-fixture-*` workflows remain.

The first round-23 campaign cleanup attempt deleted partition-lease rows;
the cleanup was corrected to release fixture ownership while preserving the
fixed lease table, and the final campaign and archive validation passed. The
remaining evidence limits are unchanged: outage episodes were idle, the
readiness timestamp is refreshed periodically, Kafka rebalance, multi-host
deployment, hard-kill storage durability, lock/statement-timeout campaigns,
sustained load, clean bootstrap, and remote CI remain untested.

Interview explanation: M5 now treats a fault campaign as a durable proof
artifact. A boundary is valid only when the target acknowledges it and the
independent checker finds the corresponding persisted record; the archived
trace and durable snapshot can be replayed after the campaign rather than
depending on rows that cleanup removed.

## 2026-09-18 - M5 closeout

- Review: Claude round 23 reviewed `acb28ba` against `69917db` and returned
  `NO_BLOCKING_FINDINGS`.
- Task status: DUR-022, DUR-023B, DUR-024, DUR-025, and DUR-021A are DONE;
  M5 is DONE.
- Verified findings: R055, R056, and R058 are VERIFIED. R057 remains OPEN at
  P3 as a narrow evidence-labelling limitation: F01's declared
  `submission_committed` boundary can overshoot into a claimed attempt, and
  several early-prefix joins cannot detect that overshoot. The Go family oracle
  and engine correctness claims are unaffected.

Claude re-ran the previously failing probes for real process-tree crash
semantics, missed-boundary rejection, and offline trace validation. The full
serial race suite passed on a pristine database and the Python suite passed
with 25 tests. Remaining nonblocking evidence gaps are the re-marked readiness
timestamp, idle outage episodes with zero unresolved-work counters, and the
carried R019 test gap.

Interview explanation: M5 is closed because its fault evidence is now both
durable and independently checkable, and Claude's final review found no
blocking correctness or evidence defects. The remaining P3 boundary-label
limitation is explicitly recorded for future campaign refinement rather than
being presented as a stronger experiment than it is.

## 2026-09-18 - M6 implementation handoff

- Review basis: M6 implementation committed at `d8ec3d6`, based on the M5
  closeout `db8b462`; Claude review is pending and M6 is READY_FOR_REVIEW.
- Tasks: DUR-019, DUR-020, DUR-021B, and DUR-033.

M6 adds a deterministic local-first incident evidence layer. The fixture
generator creates 60 documents/300 chunks, 30 incident cases split 10/20, and
40 development plus 120 held-out queries with evaluator-only labels. Retrieval
uses one frozen deterministic local embedding adapter plus keyword and hybrid
OR-sufficiency paths. The generated benchmark records both pre-gate ranking
and delivered evidence, and the difficulty audit records that configuration
was frozen before held-out scoring.

The MCP-style surface is transport-neutral but schema-constrained, allowlisted,
bounded by calls and rows, redacts logs, and preserves stable evidence IDs in
serialized responses. The workflow adapter persists incident state, timeline
events, citations, approval decisions, idempotent sandbox receipts, and
interrupt/resume state in SQLite. Live mode is only an authorization boundary
with a positive budget and explicit environment approval; no provider call or
paid run is claimed. Metrics normalize event/arm labels to bounded values and
the dashboard manifest excludes workflow IDs, prompts, and evidence text.

Validation completed:

- `powershell.exe -NoProfile -ExecutionPolicy Bypass -File
  ./scripts/ci.ps1 -WithRace -WithM6`: PASS; Go race tests, vet/build, Ruff,
  mypy, 37 Python tests, and deterministic M6 artifact generation.
- `migrate.ps1`: PASS applying `000014_m6_incident_source_corpus.up.sql`; a
  second invocation skipped the applied version. The migration created the
  `source_corpus` schema, FTS indexes, fixture-case table, and optional
  pgvector branch where available.
- M6 artifacts: 20/20 interrupted/uninterrupted held-out continuity cases;
  120 adversarial executions, zero canary leaks, zero clean/injected proposal
  changes, and 24/24 approval-gate checks blocked before approval.
- `git diff --check`: PASS before the handoff documentation commit.

Skipped/untested: live model quality and paid-provider execution, production
MCP wire transport, PostgreSQL-backed workflow persistence/source seeding,
native pgvector execution when the extension is unavailable, clean bootstrap
and restart smoke, hard-kill storage durability, multi-host deployment,
sustained load, Kafka rebalance, and remote CI. The SQLite workflow and hash
embedding are reproducibility fixtures, not production deployment claims.

Failed approach and lesson: the first retrieval labels named one document even
though every family intentionally has twelve equivalent documents, depressing
measured recall. The labels now enumerate evaluator-relevant family chunks, a
development-tuned dense gate rejects the synthetic no-answer distractors, and
the held-out artifact is regenerated only after that configuration is frozen.

Interview explanation: M6 separates agent-specific claims into inspectable
properties. A reviewer can follow a stable evidence ID from a bounded MCP
call into the durable timeline, see that a human approval is required before
the sandbox receipt, resume the same SQLite state after an interruption, and
re-run redaction/citation/continuity checks without credentials or a live
model.

## 2026-09-18 - M6 evidence-quality correction

- Task status: READY_FOR_REVIEW; the round-24 P1/P2 findings are addressed in
  implementation targets `a11599b` and `a421d55`; Claude review is pending.
- Base and target: correction work starts from the reviewed M6 target `d8ec3d6`
  and ends at `a421d55`; M6 remains based on M5 closeout `db8b462`.

The correction makes the evidence falsifiable. Retrieval now uses distinct
development and held-out query strings, one-chunk labels, plausible no-answer
queries, real paired near-duplicate documents, derived configuration hashes,
and a random-retriever baseline. The published benchmark reports each split
separately instead of pooling tuning and evaluation rows.

The adversarial protocol seeds canaries in source chunks and logs, scans the
actual downstream surfaces, keeps redaction enabled in both measured profiles,
and varies the evidence-handling decision path. A separate redaction-off
negative control now leaks and changes the scripted proposal, proving that the
scanner and injection path can fail. Citation checking covers all 30 cases and
has an unsupported-citation negative control. Continuity uses file-backed
SQLite, persists MCP result checkpoints, and verifies that restart replays
stored results rather than issuing the calls again.

The local workflow now materializes and reads the declared `source_corpus`
tables through an attached SQLite adapter. Its approval grant/effect ledger
binds the resource, canonical proposal hash, workflow revision and dispatched
receipt, with a regression test for forged arguments. This is an explicit
portable correctness adapter, not a claim that the Python fixture is the
production PostgreSQL engine or Go effect service.

Validation: `powershell.exe -NoProfile -ExecutionPolicy Bypass -File
./scripts/ci.ps1 -WithRace -WithM6` PASS with 38 Python tests, Go race/vet/build,
Ruff, mypy and regenerated artifacts. The host's global pytest temp directory
was inaccessible, so the same CI command used a workspace-local `--basetemp`
for the successful run. The normal pytest cache still emitted one non-fatal
permission warning. No live-model, paid-provider or production transport run
was performed.

Interview explanation: a benchmark is evidence only when a random baseline,
held-out split, seeded canary, failing negative control, and persisted
checkpoint give the reviewer a way to distinguish a measured property from a
fixture that always returns the desired number.

## 2026-09-18 - M6 citation-enforcement follow-up

- Task status: READY_FOR_REVIEW; R064's plan-record gap and R065's citation
  enforcement are addressed at code target `1048ad0`; the review response and
  handoff will be committed separately.
- Base and target: correction starts from `a421d55` and ends at `1048ad0`;
  M6 remains based on M5 closeout `db8b462`.

R065 is closed in the implementation: the workflow now combines authorized
log, metric, and retrieved-document evidence IDs, rejects an empty citation
set before persisting a remediation proposal or approval request, and records
`missing_evidence` as a citation violation. Every answerable fixture now has a
document target whose detail token passes the frozen retrieval gate. The
citation artifact reports 30 cases, zero uncited proposals, and separate
negative controls for unauthorized and missing citations; both controls fire.

R064's remaining scope issue is now explicit in PLAN.md and D010. M6 remains a
portable local adapter that mirrors the M4 grant/effect boundary. Named
follow-up `DUR-033A` must route an incident remediation through the production
submission, scheduler approval/grant, `effects.Service`, and seeded/queryable
`source_corpus` path before any production or live-model incident claim.

Validation: `powershell.exe -NoProfile -ExecutionPolicy Bypass -File
./scripts/ci.ps1 -WithRace -WithM6` PASS with 38 Python tests, Go race/vet/build,
Ruff, mypy, and regenerated M6 artifacts. No live-model, paid-provider,
PostgreSQL-backed incident workflow, or production engine integration was run.

## 2026-09-18 - M6 closeout after Claude review

- Task status: DONE for M6 and DUR-019, DUR-020, DUR-021B, and DUR-033.
- Review: Claude's committed round-26 review returned `NO_BLOCKING_FINDINGS` for
  target `1048ad0` against milestone base `db8b462`; R064 and R065 are VERIFIED.
- The closeout records the reviewer's measured citation evidence: 0 uncited
  proposals across 30 cases, 24 answerable cases with eight citations each, six
  abstentions, both citation negative controls firing, and no retrieval
  regression after the case-query update. The review also verifies the DUR-033A
  plan record: production engine wiring remains an explicit TODO and is not part
  of M6 acceptance.
- Closeout changes are documentation/status metadata only; the reviewed code
  remains exactly `1048ad0`. REVIEW.md's round history is preserved.
- Validation evidence carried into closeout: 38 Python tests and the previously
  recorded race/vet/build, Ruff, mypy, and artifact-generation checks passed.
- Remaining nonblocking limitations: the zero-valued citation metric has no
  sample line, freeze derivation compares fingerprints in one process,
  continuity covers one interruption boundary, keyword retrieval remains
  saturated on held-out data, M5 R057 and the historical R019 test gap remain
  open, and DUR-033A is TODO. No live-model, paid-provider, or production
  engine integration claim is made.

## 2026-09-18 - M7 DUR-036 readiness implementation

- Task status: IN_PROGRESS; M7 starts with the DUR-036 readiness gate from
  M6 closeout `421b1cb`.
- The new `scripts/m7-readiness.ps1` declares Docker Desktop `desktop-linux`
  (WSL2) as the explicit Linux VM/volume boundary, records host/runtime,
  filesystem, resource, clock, PostgreSQL durability, and cost-control facts,
  and validates the real Compose topology plus one normal telemetry smoke and
  one crash-recovery/checker episode.
- `experiments/m7/README.md` defines the evidence boundary. No final
  throughput, safeguard, dispatch-path, lease, checkpoint, live-model, or
  paid-provider measurement is started by DUR-036.
- Validation is pending the clean-checkout run of
  `scripts/m7-readiness.ps1 -StartServices`; the generated artifact and exact
  command results will be added before the review handoff.

## 2026-09-18 - M7 DUR-036 readiness handoff

- Task status: READY_FOR_REVIEW; M7 remains IN_PROGRESS and DUR-036 is not DONE
  pending Claude's committed review.
- Base and implementation target: `421b1cb` (M6 closeout) to `efe1fe3`.
  The handoff commit changes only PLAN.md, README.md, REVIEW.md,
  docs/BUILD_LOG.md, docs/DECISIONS.md, and the generated M7 artifact.
- Readiness command: `powershell.exe -NoProfile -ExecutionPolicy Bypass -File
  ./scripts/m7-readiness.ps1 -StartServices` PASS from a clean checkout. The
  artifact is `experiments/m7/dur036-readiness.json` with schema
  `dur036-readiness.v1` and status `PASS`.
- Host evidence: Docker Desktop 4.86.0 / Engine 29.7.2 `desktop-linux`, Linux
  amd64, WSL2 kernel 6.6.87.2, overlayfs, cgroup v2, 32 CPUs, 8 GiB Docker
  memory, Docker local volumes, and PostgreSQL's ext4 data filesystem. The
  database reported checksums, fsync, synchronous_commit, and
  full_page_writes all enabled, with UTC timezone.
- Validation evidence: Compose dependency smoke passed; all eight required
  services were running/healthy where healthchecks exist; the focused
  `TestDUR036TelemetryReconstruction` passed with normal and crash/resume
  telemetry snapshots and independent invariant verdicts; the committed F07
  trace passed `go run ./cmd/fault-checker -offline`.
- Remaining limits: this is a single Docker Desktop/WSL2 development VM and
  local-volume readiness gate. No replicated storage, hard-kill durability,
  sustained load, final performance, live-model, or paid-provider claim is
  made. DUR-026, DUR-034, DUR-035, DUR-027, DUR-028, and DUR-029 remain gated
  on DUR-036 acceptance.

## 2026-09-18 - M7 DUR-036 R066-R068 response

- Task status: IN_PROGRESS; DUR-036 remains READY_FOR_REVIEW and is not DONE.
- Addressed Claude's round-27 findings without expanding the protected M7
  scope. The Compose runtime now has an explicit readiness profile in which
  `runtime-a` exposes a bounded internal workload that constructs the engine,
  runs a pure activity through PostgreSQL, and returns its durable result plus
  the process registry snapshot. The readiness script calls that deployed
  endpoint and fails unless lease, claim, result, and database-query metrics
  all increase in the scraped runtime endpoint.
- `MarkDurableReady` is now a one-time compare-and-swap transition. Recovery
  and relay success no longer overwrite the first-ready timestamp; liveness is
  left to health endpoints and other counters. The telemetry unit test covers
  timestamp stability.
- The artifact now calls the checked condition a clean worktree, explicitly
  records that the run is not a fresh clone, and records that healthy services
  and Docker-managed volumes may have been reused. Service uptimes and volume
  metadata remain the lifecycle evidence.
- Focused validation: `gofmt`; `go test ./cmd/runtime ./internal/telemetry
  ./internal/engine` passed. After the fix target was committed as `bfa99fe`,
  `scripts/m7-readiness.ps1 -StartServices` passed and regenerated the tracked
  artifact. The deployed readiness endpoint returned `SUCCEEDED`; the scraped
  scheduler-a deltas were lease acquisitions +1, accepted claims +1, accepted
  results +1, and database queries +41. The durable-ready timestamp remained
  `1789753318` before and after the workload. The artifact records the exact
  service lifecycle and explicitly does not claim a fresh clone or volume
  recreation.
- Alternatives considered: a separate scheduler container would have added a
  second production-like process and another ownership path; the bounded
  readiness profile keeps the measured runtime path explicit while remaining
  disabled by default. Destroying developer volumes would make the probe more
  destructive, so lifecycle reuse is disclosed rather than mislabeled as a
  fresh deployment.

## 2026-09-18 - M7 DUR-036 deployed fault-episode follow-up

- Task status: IN_PROGRESS; DUR-036 remains READY_FOR_REVIEW and is not DONE.
- Addressed the remaining R066 scope: the deployed readiness endpoint accepts
  an opt-in `mode=fault`, injects `AfterBoundary("result_recorded")` after the
  result transaction, then starts a fresh Engine in the same runtime process
  and resumes from PostgreSQL. The readiness artifact now treats this as the
  fault episode; the existing Go test is retained only as a local regression.
- Validation: `scripts/m7-readiness.ps1 -StartServices` passed at target
  `6325d1f`. The deployed response reported `crash_boundary=result_recorded`,
  `resumed=true`, `state=SUCCEEDED`, and the injected crash error. Fault-phase
  scheduler-a deltas were lease acquisitions +2, accepted claims +1,
  accepted results +1, and database queries +44. The durable-ready timestamp
  remained `1789754802` before and after the fault workload. The normal phase,
  committed F07 checker, local regression, and cleanup also passed.
- The artifact records `engine_mode: readiness`; the default Compose mode
  remains disabled. The endpoint is still localhost-bound and unauthenticated,
  so this remains a bounded development readiness profile rather than a
  production control surface.
- Repository validation after the follow-up: `ci.ps1 -WithRace` passed all Go
  race packages, Ruff, formatting, mypy, and 38 Python tests. The run used
  task-local Go/uv/Pytest cache paths because the host global cache locations
  are inaccessible or collide with files; one non-fatal pytest cache warning
  remains from that host restriction. PostgreSQL/Kafka service checks were
  already covered by the Docker-backed readiness run and were not duplicated
  by this non-service CI invocation.

## 2026-09-18 - M7 DUR-036 closeout

- Task status: DONE at reviewed implementation target `6325d1f`, based on
  `bfa99fe`. Claude's committed round-29 review returned
  `NO_BLOCKING_FINDINGS`; R066 is VERIFIED and DUR-026, DUR-027, DUR-028,
  DUR-034, and DUR-035 are now unblocked.
- The review identified R069 as a nonblocking P3 cleanup follow-up. The
  generated `dur036-runtime-*` namespace was swept from the dev database
  after review, including the stranded `RUNNABLE` fixture. Before the first
  measurement study, move readiness cleanup into the script's outer `finally`
  and record a pre-run sweep/count in the artifact.
- The accepted evidence remains bounded Docker Desktop/WSL2 development-host
  evidence: it does not claim multi-host durability, production scale, or
  equivalence to an OS-level process kill. The readiness endpoint is disabled
  by default and localhost-bound.

## 2026-09-18 - M7 R069 cleanup follow-up and DUR-026 start

- R069 follow-up commit `f45ba7f` moves readiness fixture deletion into the
  script's outer `finally`, sweeps only the reserved `dur036-runtime-*`
  namespace before a run, and records pre-run workflow/definition counts in
  `experiments/m7/dur036-readiness.json`. This prevents failed assertions or
  direct endpoint probes from contaminating the declared measurement database.
- Validation: `scripts/m7-readiness.ps1 -StartServices` passed from the clean
  committed worktree. The artifact records `PASS`, commit `f45ba7f`, and zero
  pre-run workflows and definitions. A follow-up database query found zero
  rows in both reserved prefixes after the run. PowerShell parse validation
  and `git diff --check` also passed.
- DUR-026 is now `IN_PROGRESS` from base `f45ba7f`. Its implementation record
  freezes the existing section-14 throughput protocol: 8 configurations and
  24 measured runs, with deterministic synthetic activities, fixed capacity,
  explicit reconciliation, and scheduler CPU-seconds per completed workflow.
  No final throughput run has started yet; the host and single-node scope
  limitations from DUR-036 remain.
