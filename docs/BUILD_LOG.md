# Build log

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
