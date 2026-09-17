# Distributed Durable Execution Engine — Review and Feedback

## Review contract

Severity:

- P0 — Critical: immediate correctness, security, data-loss, or availability risk that blocks acceptance.
- P1 — High: serious defect or regression with substantial user/system impact; should be fixed before acceptance.
- P2 — Medium: real defect or important test/reliability gap with bounded impact; fix unless explicitly deferred.
- P3 — Low: minor correctness, maintainability, observability, documentation, or test-quality issue that does not block acceptance by itself.

Blocking findings:

- Unresolved P0 and P1 findings always block acceptance.
- An unresolved P2 finding blocks acceptance unless Claude explicitly records it as deferred with a reason and linked follow-up. A deferred P2 remains OPEN; deferral is metadata, not a finding status.
- P3 findings do not block acceptance by themselves.

Finding status lifecycle:

- OPEN — Claude has recorded the finding and it remains unresolved.
- ADDRESSED — Codex has proposed a fix; Claude has not yet verified it.
- DISPUTED — Codex believes the finding is stale or incorrect and has supplied evidence.
- VERIFIED — Claude checked the proposed fix and confirmed the finding is resolved.
- WITHDRAWN — Claude retracts the finding after determining it was incorrect or inapplicable; the withdrawal reason and evidence must be recorded.

The `Status` field directly under each finding is the authoritative current state of that finding. The status inside a `Codex response` records Codex's action for that response, and the status inside a `Claude verification` records Claude's action for that verification step; those subsection fields are an audit trail, not separate competing states. Whenever Codex or Claude records a response or verification, update the finding's top-level `Status` to the resulting current state.

Review verdicts:

- NOT_REVIEWED — No completed Claude review exists for the recorded scope and target.
- CHANGES_REQUESTED — One or more unresolved findings block acceptance of the reviewed scope.
- NO_BLOCKING_FINDINGS — Claude found no unresolved blocking findings for the recorded scope and review basis. Deferred P2 findings, P3 findings, uncertainties, and untested areas may remain and must be stated.

A completed provisional review may use CHANGES_REQUESTED or NO_BLOCKING_FINDINGS, but a provisional review cannot satisfy PLAN.md DONE criteria. Final acceptance requires review of the committed final target.

A handoff with `Task status: READY_FOR_REVIEW` requires `Handoff basis: COMMITTED` and a concrete target commit. A `PROVISIONAL` handoff must remain `IN_PROGRESS`.

## M0 final handoff

- Task: M0 foundation (DUR-001 through DUR-004)
- Task status: DONE
- Handoff basis: COMMITTED
- Base commit: `d722cf7`
- Target commit: `b993d71`
- Scope and implementation summary: Completed the repository/toolchain foundation; corrected the DUR-002 contract through `dur-002.v4` with effect-class-aware timeout handling and expanded race traces; added shared Go/Python partition vectors with UTF-8 validation; added reusable Go/Python failpoint clients, seeded fake activities, arbitrary-command fault control, early-exit/protocol handling, release acknowledgements, per-run IDs, exclusive trace paths, and append-flushed `fault-trace.v1` evidence; fixed runtime image/internal-package copying, migration-ledger skipping, and explicit M0 service-test reporting. Existing PLAN changes were preserved, and the user-authorized retrieval/MCP/adversarial scope is now recorded as D005.
- Checks run and results:
  - `powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/bootstrap.ps1 -StartServices`: PASS; Go 1.27.1, locked Python environment, pinned Docker builds, migration, and local health smoke passed.
  - `scripts/check.ps1` with task-local Go/uv/temp caches: PASS; Go format/vet/test/build, Ruff, strict mypy, and 12 pytest tests passed.
  - `scripts/restart-smoke.ps1`: PASS; PostgreSQL marker and Kafka marker topic survived forced container recreation.
  - `scripts/ci.ps1`: PASS; default path reported opt-in checks explicitly.
  - `scripts/ci.ps1 -WithRace`: PASS; Go race tests and 12 Python tests passed.
  - `scripts/ci.ps1 -WithRace -WithServices`: PASS; Go race tests, Python tests, PostgreSQL/Kafka smoke, service health, durability settings, and Prometheus scrape checks passed; it explicitly reported that M0 integration tests are not implemented.
  - Partition vectors passed independently in Go and Python; fault controller release acknowledgement, repeated same-seed kills, different-seed fixture fields, early exit, noisy stdout, heavy stderr, timeout, and append-flushed trace tests passed.
- Skipped checks and reasons: No remote CI is configured (`git remote -v` is empty). Model/paid-provider checks are intentionally not part of M0. Final Linux I/O/performance studies are deferred by the plan. `pwsh` was unavailable, so Windows PowerShell was used. The existing `.pytest_cache` and elevated pytest temp ACLs emitted host warnings; fresh task-local paths were used for passing validation.
- Known limitations: This milestone does not implement durable workflow state, scheduler ownership, Kafka relay semantics, or correctness/performance claims. PostgreSQL/Kafka are a single-node local development topology. Claude round-4 verification is complete with `NO_BLOCKING_FINDINGS`; R016 remains a nonblocking P3 contract follow-up for the first DUR-005 contract touch. M0 is accepted, but this handoff makes no runtime-engine correctness claim.

## Codex handoff — DUR-005 closeout

- Task: DUR-005 — Schema and state repository
- Task status: DONE
- Handoff basis: COMMITTED
- Base commit: `79ba118`
- Target commit: `333a555`
- Scope and implementation summary: Preserved uncertainty for claimed effect attempts canceled in flight by recording `CANCELED/OUTCOME_UNKNOWN` and retaining late reports as evidence without progress, bumped the canceled node revision, and enforced retry grant-scope identity for non-cooperating effects as well as cooperating effects. The contract and integration tests document and exercise the terminal evidence-only path. The earlier DUR-005 repository features remain: authoritative per-activity effect classes, partition-map validation, idempotent workflow creation with a creation outbox event, partition leases, owner-fenced transitions, node/attempt lifecycle, fresh claim/redispatch deadlines, durable worker result receipts, lease-owner result consumption, effect-class-aware timeout branches, durable outbox/history, and idempotent late non-cooperating-result evidence. R016 is addressed in the v4 contract and schema/repository path. Service CI applies numbered migrations before required database-backed checks and runs the PostgreSQL repository integration suite before smoke checks.
- Checks run and results:
  - `scripts/ci.ps1 -WithRace -WithServices`: PASS with task-local Go/UV caches and an explicit pytest basetemp; Go formatting/vet/tests/build, Ruff, strict mypy, 12 Python tests, Go race tests, migration skip checks, nine DUR-005 PostgreSQL integration subtests including cancellation evidence fencing, timer enforcement, cooperating and non-cooperating retry identity, injected CreateAttempt/TimeoutAttempt rollback failures, and 20-round claim and result/timeout races, and service smoke checks passed.
  - Required-database check with an intentionally wrong PostgreSQL password: FAIL as required instead of skipping; the default non-service integration invocation explicitly skipped.
  - Post-run database hygiene query: zero `dur005-*` workflows, definitions, or evidence rows; the evidence foreign key is verified as `ON DELETE CASCADE`.
  - `000003_dur005_integrity.up.sql` executed twice manually: PASS; schema ledger remained one version-3 row and no duplicate constraint was created.
  - `docker build -f deploy/local/Dockerfile.runtime -t durable-agent-runtime:dur005-check .`: PASS; the image copied `go.sum` and `internal/` and built the runtime.
  - `git diff --check`: PASS before handoff documentation changes.
- Skipped checks and reasons: No clean bootstrap or restart-smoke run was performed because those workflows recreate the user’s running containers. No remote CI exists. Full workflow-engine, scheduler, Kafka-relay, hard-kill durability, and paid/model behavior remain outside DUR-005.
- Known limitations: This task implements the durable state repository and its PostgreSQL transaction boundaries, not the complete engine. The service evidence uses the existing single-node local topology. The integration suite uses generated `dur005-*` identities and does not claim production retention, failover, or exactly-once behavior. R028's fan-out cancellation follow-up is recorded under DUR-007 in PLAN.md.

## Codex handoff

- Task: DUR-006 — Submission and query APIs
- Task status: READY_FOR_REVIEW
- Handoff basis: COMMITTED
- Base commit: `adf5934`
- Target commit: `b252e36`
- Scope and implementation summary: Added the versioned HTTP/JSON submission and query API over the durable repository. `POST /v1/workflows` canonicalizes and hashes the complete execution-defining submission (`definition_id`, `definition_version`, `initial_node_id`, `initial_input`, and `payload`), computes the frozen partition, and preserves namespace/submission-key idempotency. The repository validates definitions and initial nodes before creation and the API maps payload conflicts, definition/node/ID client errors, database unavailability, not-found, stale revision, stale claim, and stale attempt errors to explicit HTTP responses. `GET /v1/workflows/{workflow_id}` exposes status with an optional stale-revision precondition, and the history endpoint provides bounded ordered pages. Runtime processes open PostgreSQL when `DATABASE_URL` is configured; standalone HTTP defaults to localhost and local Compose publishes only localhost ports. The DUR-006 retention policy is no automatic pruning; the ambiguous-client policy is exact execution-meaning retry.
- Checks run and results:
  - `scripts/ci.ps1 -WithRace -WithServices`: PASS; Go formatting/vet/tests/build, Go race tests, Ruff, strict mypy, 12 Python tests, migration checks, all 9 DUR-005 PostgreSQL subtests, the DUR-006 API integration test, and live PostgreSQL/Kafka/runtime/telemetry smoke checks passed.
  - `go test -race ./...`: PASS.
  - Focused `go test -race ./internal/api -run '^TestWorkflowAPIResponseLossHistoryAndRetention$' -count=1 -v`: PASS against the configured development PostgreSQL service; it covers execution-meaning conflicts, typed client errors, duplicate keys, JSON route fallback, real HTTP response loss, retention, and row-count invariants.
  - `docker build -f deploy/local/Dockerfile.runtime -t durable-agent-runtime:dur006-r10-check .`: PASS; the runtime image compiled with the API and PostgreSQL wiring.
  - `docker compose --env-file .env -f deploy/local/compose.yaml config --quiet`: PASS.
  - `git diff --check`: PASS before handoff documentation changes.
- Skipped checks and reasons: No clean bootstrap or restart-smoke run was performed because those workflows recreate the user's running containers. No remote CI exists. A live stopped-database request was not exercised; unit mapping covers context deadlines, while startup still fails fast when `DATABASE_URL` cannot be opened. Lock/statement timeouts, hard-kill durability, and production retention/failover remain untested or outside this task.
- Known limitations: This task exposes submission/status/history only; it does not implement the interpreter, scheduler loop, fan-out, Kafka relay, or effect service. History is retained without automatic pruning in this API. Authentication is intentionally absent for this local development milestone; the runtime and Compose host ports are localhost-only. The service evidence uses the existing single-node local topology and makes no exactly-once claim.

## Claude review rounds

### Round 1 — 2026-09-16 — M0 foundation (DUR-001 through DUR-004)

- Date and round: 2026-09-16, round 1.
- Review basis: COMMITTED. Working tree was clean at `5184a3d` when the review started.
- Base and target commits: implementation target `87e123a`; handoff commit `5184a3d` (changes only REVIEW.md and docs/BUILD_LOG.md). The handoff declares base `198fd4b`, but the handoff scope includes DUR-001, whose implementation *is* `198fd4b` and had never been reviewed (see R009). DUR-001 files were therefore reviewed against `d722cf7`; DUR-002 through DUR-004 were reviewed as `198fd4b..87e123a`.
- Scope inspected: all files changed in `d722cf7..5184a3d`: PLAN.md (M0 status edits and the pre-existing scope edits committed in `87e123a`), README.md, docs/BUILD_LOG.md, docs/CONTRACTS.md, docs/DECISIONS.md, docs/RUNBOOK.md, docs/partition-map-v1.md, internal/partition/, cmd/runtime/, python/durable_contracts/, python/faults/, python/workers/, scripts/*.ps1, deploy/local/ (Compose, Dockerfiles), migrations/, pyproject.toml, .gitignore/.env.example, and tests/. CONTRACTS.md was checked against PLAN.md sections 4–8 and the DUR-002 acceptance text.
- Checks personally run (Claude, 2026-09-16). All ran in a scratch export (`git archive 5184a3d`) outside the repository unless noted:
  - `scripts/ci.ps1 -WithRace`: PASS (gofmt, go vet, go test, go build, ruff check/format, strict mypy on 8 files, 7 pytest tests, `go test -race ./...`).
  - Failure propagation: an injected failing pytest (`assert value == 2`) made `ci.ps1` exit 1 with "Python tests failed."; an injected `t.Fatal` Go test made `ci.ps1` exit 1. Injected files were removed from the scratch copy.
  - `tests/test_fault_control.py` repeated 25 times: 25/25 PASS.
  - Partition vectors recomputed independently with `sha256sum` (a third implementation): all four committed vectors match; three of them have the top bit set, so signed/unsigned and byte-order mistakes would be caught. Probed extra IDs through the committed Go and Python code: `wf-é-日本` → 3 in both; invalid UTF-8 `wf-\xff\xfe` → Go returns 8, Python raises `UnicodeEncodeError` (R007).
  - Fault-controller probe script (scratch only): a target killed before reporting its boundary, and a target that prints one non-JSON line before a valid boundary report, both produced `['process_started', 'boundary_timeout']`; the non-JSON case also killed the reader thread with `JSONDecodeError` (R005). A normal release trace contained no target acknowledgement (R006).
  - `scripts/smoke.ps1` against the already-running local services, from the repository root (read-only): PASS. PostgreSQL 18.6 with data_checksums/fsync/synchronous_commit/full_page_writes all `on`; both Kafka topics present; two runtimes, two workers, OpenTelemetry, and Prometheus healthy; both runtimes scraped. `git status` stayed clean afterwards.
- Codex-reported checks considered but not rerun by Claude: `scripts/bootstrap.ps1 -StartServices` from a clean state, `scripts/restart-smoke.ps1` (not rerun because it force-recreates the shared local containers), and the `faults.control` CLI release/timeout runs (the equivalent code paths were covered by the pytest runs and the probe).
- Deferred P2 findings, if any: none.
- Remaining P3 findings / uncertainties / untested areas: P3 findings R006, R007, R008, R009. Untested: clean-machine bootstrap, and data surviving an unclean container kill (the restart smoke uses a graceful `--force-recreate`, which is enough for DUR-001 but is not crash evidence). No remote CI exists. `-WithServices` runs health checks only; no PostgreSQL/Kafka integration test exists yet (acceptable at M0 because no engine code exists, but `ci.ps1` should say so explicitly; see R008).
- Limitations: Windows host only; PowerShell 5.1 (`pwsh` unavailable). Docker checks used the containers already running on this host rather than a fresh bootstrap. The `.pytest_cache` ACL warning Codex reported probably comes from host-level Codex sandbox file ownership (on this host the repo's `.git` was owned by the `CodexSandboxOffline` account); it is an environment issue, not a repository defect.
- Verdict: CHANGES_REQUESTED. Blocking: R001, R002, R003, R004, R005 (all P2).

### Round 2 — 2026-09-16 — M0 fix verification

- Date and round: 2026-09-16, round 2.
- Review basis: COMMITTED. Worktree was clean at `deb9815` when the review started.
- Base and target commits: base `d722cf7`, target `c78803e` (all code and decision changes). Handoff commit `deb9815` changes only REVIEW.md and docs/BUILD_LOG.md. Fix commits reviewed: `ed62eca` (code/contracts), `d8adc93` (docs), `5afc4f5` (.gitignore `/.pytest-*/`), `c78803e` (D005). The handoff declaration matches the repository state.
- Scope inspected: `git diff 5184a3d deb9815`, covering internal/faults, cmd/fault-fixture, internal/partition, api/partition-map-v1.vectors.json, python/faults (client, control, fixtures, target), tests, scripts/ci.ps1, scripts/migrate.ps1, Dockerfile.runtime, migrations/README.md, docs/CONTRACTS.md (full re-read of `dur-002.v2`), docs/DECISIONS.md (D005, D006), docs/BUILD_LOG.md, and the Codex responses in REVIEW.md. PLAN.md is unchanged since `5184a3d`.
- Checks personally run (Claude). All ran in a scratch export (`git archive c78803e`) outside the repository unless noted:
  - `scripts/ci.ps1 -WithRace`: PASS (Go vet/test/build incl. internal/faults, ruff, strict mypy on 10 files, 11 pytest tests, Go race tests).
  - `tests/test_fault_control.py` repeated 15 times: 15/15 PASS (includes the `go run ./cmd/fault-fixture` test).
  - Scratch controller probes: a boundary report followed by `os._exit(9)` was classified correctly in 40/40 runs; two runs sharing one trace path produced sequences `[1..6, 1..6]` (R011).
  - `docker build -f deploy/local/Dockerfile.runtime` at `c78803e`: PASS, including a second build with an `internal/partition` import added in the scratch copy. Throwaway images were removed.
  - `scripts/migrate.ps1` against the running local database, from the repository root: `Skipping already applied 000001_bootstrap.up.sql`, exit 0, worktree clean.
- Codex-reported checks considered but not rerun by Claude: clean `bootstrap.ps1 -StartServices`, `restart-smoke.ps1`, and `ci.ps1 -WithServices` (the new message is a static `Write-Host`; the underlying `smoke.ps1` passed in Claude's round 1).
- Findings resolved: R001–R009 are VERIFIED (per-finding verification blocks below).
- New findings: R010 (P2, blocking), R011 (P3), R012 (P3).
- Deferred P2 findings, if any: none.
- Remaining P3 findings / uncertainties / untested areas:
  - Open P3 findings: R011 and R012.
  - Untested code paths: the Go client's release/`Emit` path (only kill is exercised), and a non-idempotent migration.
  - Theoretical, unobserved: a reader/watcher event-ordering race in the controller.
  - Unconfirmed to Claude: D005's user authorization comes from Codex's record; the user did not confirm it to Claude in this session.
- Limitations: Windows host only; PowerShell 5.1; shared local containers were not recreated. Claude repaired a mojibake heading (`鈥?` → `–`) in the R001 Codex response without changing its content.
- Verdict: CHANGES_REQUESTED. Blocking: R010 (P2). M0 cannot move to DONE until R010 is VERIFIED, or explicitly deferred with a user-approved reason and a follow-up before DUR-005.

### Round 3 — 2026-09-16 — M0 round-2 fix verification

- Date and round: 2026-09-16, round 3.
- Review basis: COMMITTED. Worktree was clean at `f1f35ff` when the review started.
- Base and target commits: base `d722cf7`, target `eb32162`. Handoff commit `f1f35ff` changes only REVIEW.md and docs/BUILD_LOG.md.
- Scope inspected:
  - `git diff deb9815 f1f35ff`: docs/CONTRACTS.md (full re-read of `dur-002.v3` against PLAN.md sections 4-8), docs/DECISIONS.md (D004/D006 amendments), python/faults/control.py, tests/test_fault_control.py, tests/faults/README.md, and docs/BUILD_LOG.md.
  - REVIEW.md history: the only removed lines were status values and the heading Claude repaired; earlier findings, responses, and verifications are preserved.
- Checks personally run (Claude). All ran in a scratch export (`git archive eb32162`) unless noted:
  - `scripts/ci.ps1 -WithRace`: PASS (Go vet/test/build, ruff, strict mypy on 10 files, 12 pytest tests, Go race tests).
  - `tests/test_fault_control.py` repeated 10 times: 10/10 PASS.
  - `git diff --check d722cf7 eb32162` in the repository: clean.
- Codex-reported checks considered: `scripts/check.ps1` with 12 tests (consistent with Claude's run). Service checks were not rerun: no scripts, Compose, or service code changed since round 2.
- Findings resolved: R010, R011, and R012 are VERIFIED.
- New findings:
  - R013 (P2, blocking): a claimed non-cooperating effect attempt that times out is replaced automatically. This gap predates v3 and was missed by Claude in rounds 1-2.
  - R014 (P3) and R015 (P3).
- Deferred P2 findings, if any: none.
- Remaining P3 findings / uncertainties / untested areas:
  - Open P3 findings: R014 and R015.
  - Carried over from round 2 (untested): the Go client's release path, and a non-idempotent migration.
  - Unconfirmed to Claude: D005's user authorization, which is still recorded only via Codex.
- Limitations: Windows host only; PowerShell 5.1; documentation-only contract review (no engine code exists to test the contract against).
- Verdict: CHANGES_REQUESTED. Blocking: R013 (P2). R013 can be fixed in a `dur-002.v4` contract revision (R014 can be folded in), or, if the user prefers, explicitly deferred with a reason and a follow-up that must close before DUR-005 or DUR-016 implement timeout handling.

### Round 4 — 2026-09-16 — M0 round-3 fix verification

- Date and round: 2026-09-16, round 4.
- Review basis: COMMITTED. Worktree was clean at `62f035b` when the review started.
- Base and target commits: base `d722cf7`, target `b993d71`, which is the final code/contract commit. Handoff commits `b68aef8` and `62f035b` change only REVIEW.md and docs/BUILD_LOG.md. `git diff --stat eb32162 62f035b` touches only REVIEW.md, docs/BUILD_LOG.md, docs/CONTRACTS.md, and docs/DECISIONS.md, so there is no code, script, test, or deployment change since round 3.
- Scope inspected: `git diff eb32162 b993d71 -- docs/CONTRACTS.md docs/DECISIONS.md` (the `dur-002.v4` timeout branches, the effect-class declaration, the permission table, and the control-API wording), the new docs/BUILD_LOG.md entries, and the REVIEW.md handoff and Codex responses. REVIEW.md history is preserved: the only removed lines are stale handoff check counts and status values.
- Checks personally run (Claude):
  - `scripts/ci.ps1 -WithRace` in a scratch export of `b993d71`: PASS (12 pytest tests, Go race tests).
  - `git diff --check d722cf7 62f035b`: clean.
  - The R013 scenario, re-walked on paper against v4 for all four timeout branches.
- Codex-reported checks considered: `ci.ps1 -WithRace` (consistent with Claude's run). Clean bootstrap, `restart-smoke.ps1`, and `ci.ps1 -WithServices` were last reported by Codex at round 2 (`c78803e`). Claude's own service evidence is `smoke.ps1` (round 1), the runtime image build, and the `migrate.ps1` skip check (round 2). None of the relevant scripts, Compose files, Dockerfiles, or migrations changed after `c78803e`, so that evidence still applies to `b993d71`.
- Findings resolved: R013, R014, and R015 are VERIFIED. All of R001–R015 are now VERIFIED.
- New findings: R016 (P3, non-blocking).
- Deferred P2 findings, if any: none.
- Remaining P3 findings / uncertainties / untested areas:
  - Open P3 finding: R016.
  - Evidence note: docs/BUILD_LOG.md (round-3 and round-4 entries) says bootstrap/restart smoke were not rerun "because the reviewer's shared-container constraint still applies". Claude placed no constraint on Codex; Claude only chose not to recreate the containers itself. Reword that note when convenient; this is not a finding.
  - Untested:
    - the Go failpoint client's release path;
    - a non-idempotent migration;
    - clean-machine bootstrap by Claude;
    - data surviving an unclean container kill (the restart smoke is graceful);
    - remote CI (no remote exists).
  - Not yet implemented, so contract obligations only: every DUR-002 guarantee, including the non-cooperating timeout case. M0 contains no engine.
  - Unconfirmed to Claude: D005's user authorization, recorded via Codex only.
- Limitations: Windows host only; PowerShell 5.1; the contract review is on paper only.
- Verdict: NO_BLOCKING_FINDINGS for M0 (DUR-001 through DUR-004) at committed target `b993d71` with base `d722cf7`. This is a COMMITTED, non-provisional review. The remaining non-blocking item is R016 (P3). With the acceptance criteria and evidence already recorded, Codex may move DUR-001 through DUR-004 to DONE under PLAN.md section 11.

### Round 5 — 2026-09-17 — DUR-005 schema and state repository

- Date and round: 2026-09-17, round 5 (first DUR-005 review; includes the R016 verification).
- Review basis: COMMITTED. Worktree was clean at `e2df512` when the review started.
- Base and target commits: base `79ba118`, target `1698747`. Handoff commit `e2df512` changes only PLAN.md (IN_PROGRESS → READY_FOR_REVIEW), REVIEW.md, and docs/BUILD_LOG.md. The handoff declaration matches the repository state.
- Scope inspected: `git diff 79ba118 1698747`, i.e. migrations/000002_durable_state.up.sql, internal/state/{types.go,store.go,store_integration_test.go}, docs/CONTRACTS.md (the R016 wording), the PLAN.md section 5 data-model row, scripts/ci.ps1, Dockerfile.runtime, go.mod/go.sum, and the README/migrations README. The repository was checked against dur-002.v4 and the DUR-005 acceptance text. PLAN.md changes are status and data-model additions only; no protected-scope drift.
- Checks personally run (Claude). Code ran in a scratch export (`git archive 1698747`); database tests ran against a throwaway database `claude_review_dur005` created in the running PostgreSQL container, migrated with 000001 and 000002, and dropped afterwards:
  - `go vet ./...` and `go test -race ./...` in the export: PASS (the integration test skipped there because the export has no `.env`).
  - `TestPostgresStateRepository` against the scratch database: PASS. It left 1 `dur005-*` workflow behind (R022).
  - The same test with a wrong password: `SKIP`, exit 0 (R022).
  - Re-applying migration 000002 to the scratch database: idempotent (IF NOT EXISTS notices only).
  - Throwaway repro tests (scratch file `claude_review_test.go`, deleted afterwards):
    - fresh claim after the deadline, followed by an immediate timeout (R017);
    - result retry after commit (R018);
    - claim retry after replacement (R019);
    - retry after a retryable failure (R020);
    - a wrong partition accepted (R021);
    - stale-owner transition after takeover: correctly rejected with `ErrLeaseNotOwned`;
    - 20 rounds of concurrent claims and result-vs-timeout: exactly one winner each time.
  - Read-only query on the shared development database: 6 `dur005-*` workflows, 6 definitions, and 10 evidence rows left over from earlier runs.
- Codex-reported checks considered but not rerun: `ci.ps1 -WithRace -WithServices` in the repository (Claude avoided writing more test rows into the shared database), and the runtime image build (the Dockerfile change is the `go.sum` copy only).
- Findings: R016 VERIFIED. New findings: R017–R022 (P2, blocking) and R023 (P3).
- Deferred P2 findings, if any: none.
- Remaining P3 findings / uncertainties / untested areas:
  - Open P3 finding: R023.
  - Untested: behavior under PostgreSQL unavailability; lock/statement timeouts (none are configured); clean bootstrap and restart smoke (not rerun by Codex).
  - The leftover `dur005-*` rows in the shared development database were not removed by Claude; cleanup is Codex's or the user's call.
- Limitations: Windows host only; single local PostgreSQL 18.6.
- Verdict: CHANGES_REQUESTED. Blocking: R017, R018, R019, R020, R021, R022 (all P2). If the user prefers to move parts of R020 (result consumption) to DUR-007, record an explicit deferral with a follow-up; the repository should still reject unvalidated advancement.

### Round 6 — 2026-09-17 — DUR-005 fix verification

- Date and round: 2026-09-17, round 6.
- Review basis: COMMITTED. Worktree was clean at `fdc3c4f` when the review started.
- Base and target commits: base `79ba118`, target `49e4844`. Fix commits: `f5fe570` (code) and `49e4844` (rollback tests). Handoff commit `fdc3c4f` changes only PLAN.md (IN_PROGRESS → READY_FOR_REVIEW), REVIEW.md, and docs/BUILD_LOG.md. The handoff declaration matches the repository state.
- Scope inspected: `git diff e2df512 49e4844`: internal/state/{store.go (full re-read), types.go, store_integration_test.go}, migrations 000002 (edited) and 000003 (new), migrations/README.md, scripts/ci.ps1, and the PLAN.md data-model/status lines (no protected-scope drift). Codex's REVIEW.md responses were read; earlier history is preserved.
- Checks personally run (Claude). Code ran in a scratch export of `49e4844`. Databases were two throwaway databases in the running PostgreSQL container, dropped afterwards:
  - `claude_review_dur005_fresh`, migrated 000001 → 000002 → 000003;
  - `claude_review_dur005_upgrade`, migrated 000001 → the *original* 000002 from `1698747` → 000003.

  Results:
  - Schema convergence: both databases have migration versions 1, 2, 3, the `effect_classes` column, and the evidence FK `attempt_result_evidence_attempt_fk ... ON DELETE CASCADE`.
  - `go test -race ./internal/state -run TestPostgresStateRepository` with `DURABLE_REQUIRE_DATABASE=1` against each database: PASS, all 8 subtests. Afterwards: 0 `dur005-*` workflows, definitions, evidence rows, and triggers, and no leases held.
  - The same test with `DURABLE_REQUIRE_DATABASE=1` and a wrong password: FAIL, exit 1. With no variables set: SKIP.
  - Throwaway repro tests (scratch file `claude_review_r2_test.go`, deleted afterwards): timeout after cancellation, pure and non-cooperating (R024); result after cancellation (R024); a cooperating retry with a changed key and grant (R025); early timer exit (R026). All reproduced.
  - Read-only query on the shared dev database: 0 leftover `dur005-*` rows; migrations 1, 2, 3 applied.
- Codex-reported checks considered but not rerun: the full `ci.ps1 -WithRace -WithServices` in the repository (Claude avoided writing to the shared database; the equivalent suite ran against the scratch databases), and the runtime Docker build (no Dockerfile change this round).
- Findings resolved: R017–R023 are VERIFIED.
- New findings: R024 (P1, blocking), R025 (P2, blocking), R026 (P3).
- Deferred P2 findings, if any: none.
- Remaining P3 findings / uncertainties / untested areas:
  - Open P3 finding: R026. Test gap noted in R019 (no assertion for the stale-claim path).
  - Untested: behavior during PostgreSQL unavailability; lock/statement timeouts (still none configured); clean bootstrap and restart smoke (not rerun).
- Limitations: Windows host only; single local PostgreSQL 18.6.
- Verdict: CHANGES_REQUESTED. Blocking: R024 (P1) and R025 (P2).

### Round 7 — 2026-09-17 — DUR-005 cancellation-fix verification

- Date and round: 2026-09-17, round 7.
- Review basis: COMMITTED. Worktree was clean at `bce4daa` when the review started.
- Base and target commits: base `79ba118`, target `dce5433`. Handoff commit `bce4daa` changes only PLAN.md (IN_PROGRESS → READY_FOR_REVIEW), REVIEW.md, and docs/BUILD_LOG.md. The handoff declaration matches the repository state.
- Scope inspected: `git diff fdc3c4f dce5433`: internal/state/{store.go, types.go, store_integration_test.go}, migrations 000002 (reverted; `git diff 1698747 dce5433 -- 000002` is empty) and 000003, and scripts/ci.ps1. The Codex round-7 responses were read, and history is preserved.
- Checks personally run (Claude). Code ran in a scratch export of `dce5433`. Databases were two throwaway databases in the running PostgreSQL container, dropped afterwards:
  - `cr_d5_fresh`: 000001 → 000002 → 000003, then 000003 applied again;
  - `cr_d5_prev`: 000001 → the edited 000002 and old 000003 from `49e4844` → the new 000003.

  Results:
  - Schema: both databases converged (versions 1, 2, 3 once each, one cascading evidence FK, `effect_classes` present).
  - `go test -race ./internal/state -run TestPostgresStateRepository` with `DURABLE_REQUIRE_DATABASE=1` on each database: PASS, all 9 subtests. Afterwards: 0 `dur005-*` workflows, 0 test triggers, and no leases held.
  - `go vet ./...`, `go test -race ./...` (no database), and `gofmt -l`: clean.
  - Throwaway repro tests (scratch file `claude_review_r3_test.go`, deleted afterwards):
    - claimed pure, cooperating, and non-cooperating attempts cancelled, then timeout/heartbeat/result/evidence: fencing correct (R024 resolved); disposition `NONE` and zero evidence rows (R027);
    - unclaimed cancellation, then a claim: rejected;
    - audited cancel after a non-cooperating timeout, then a late report: rejected (R027).
  - Read-only query on the shared dev database: 0 `dur005-*` rows; migrations 1, 2, 3.
- Codex-reported checks considered but not rerun: the full `ci.ps1 -WithRace -WithServices` in the repository (the equivalent suite ran on the scratch databases), and the runtime Docker build (no Dockerfile or `go.mod` change this round).
- Findings resolved: R024, R025, and R026 are VERIFIED.
- New findings: R027 (P2, blocking) and R028 (P3).
- Deferred P2 findings, if any: none.
- Remaining P3 findings / uncertainties / untested areas:
  - Open P3 finding: R028. The R019 test gap (stale-claim assertion) is still open.
  - Untested: PostgreSQL unavailability; lock/statement timeouts (none configured); clean bootstrap and restart smoke (not rerun).
- Limitations: Windows host only; single local PostgreSQL 18.6.
- Verdict: CHANGES_REQUESTED. Blocking: R027 (P2).

### Round 8 — 2026-09-17 — DUR-005 evidence-fix verification

- Date and round: 2026-09-17, round 8.
- Review basis: COMMITTED. Worktree was clean at `614450c` when the review started.
- Base and target commits: base `79ba118`, target `333a555`, the final DUR-005 code commit. Fix commits: `d09c0a7` and `333a555`. Handoff commit `614450c` changes only PLAN.md (the "Immediate next action" target hash), REVIEW.md, and docs/BUILD_LOG.md. The handoff declaration matches the repository state.
- Scope inspected: `git diff bce4daa 333a555`, covering internal/state/store.go, store_integration_test.go, and docs/CONTRACTS.md, plus the handoff documentation. PLAN.md had no protected-scope change, and REVIEW.md history is preserved.
- Checks personally run (Claude). Code ran in a scratch export of `333a555`, against a throwaway database `cr_d5_r8` (migrations 000001 → 000003) that was dropped afterwards:
  - Throwaway checks (scratch file `claude_review_r4_test.go`, deleted afterwards): cancellation of claimed pure, cooperating, and non-cooperating attempts, with late result, retry, conflict, wrong token, late evidence, and heartbeat; plus audited cancellation from reconciliation. All behaved as the contract requires (see the R027 verification).
  - `TestPostgresStateRepository` with `-race` and `DURABLE_REQUIRE_DATABASE=1`, three consecutive runs: PASS each time. Afterwards: 0 `dur005-*` rows, 0 test triggers, and no leases held.
  - `go vet ./...` and `gofmt -l cmd internal`: clean. `git diff --check bce4daa 614450c`: clean.
  - Read-only query on the shared dev database: 0 `dur005-*` rows.
- Codex-reported checks considered but not rerun: the full `ci.ps1 -WithRace -WithServices` in the repository, and the runtime Docker build (no Dockerfile, `go.mod`, or migration change since round 6/7).
- Findings resolved: R027 is VERIFIED. R028 items 2 and 3 are verified.
- New findings: none.
- Deferred P2 findings, if any: none.
- Remaining P3 findings / uncertainties / untested areas:
  - R028 (OPEN, P3): item 1 is the fan-out cancellation settlement, left to DUR-007. It should be recorded in the DUR-007 scope in PLAN.md. The finding also covers the attempt-diagram indentation nit.
  - R019 test gap: there is still no committed assertion for the stale-claim-after-replacement path.
  - Untested:
    - behavior while PostgreSQL is unavailable;
    - lock/statement timeouts (none are configured);
    - clean bootstrap and restart smoke (Codex did not rerun them; the last evidence is from M0 round 2);
    - hard-kill durability;
    - remote CI (none exists).
  - Scope: DUR-005 is a repository layer; no engine, scheduler loop, or Kafka path exercises it yet, so its guarantees are validated only by the integration suite and Claude's scratch probes.
- Limitations: Windows host only; single local PostgreSQL 18.6. Every Claude database check used throwaway databases rather than the shared development database.
- Verdict: NO_BLOCKING_FINDINGS for DUR-005 at committed target `333a555` with base `79ba118`. This is a COMMITTED, non-provisional review. R001–R027 are VERIFIED. The remaining non-blocking item is R028 (P3). With the acceptance criteria and evidence already recorded, Codex may move DUR-005 to DONE under PLAN.md section 11.

### Round 9 — 2026-09-17 — DUR-006 submission and query APIs

- Date and round: 2026-09-17, round 9 (first DUR-006 review).
- Review basis: COMMITTED. Worktree was clean at `3397046` when the review started.
- Base and target commits: base `adf5934`, target `d8083d2`. Handoff commit `3397046` changes only PLAN.md, REVIEW.md, and docs/BUILD_LOG.md. The handoff declaration matches the repository state. `89ed59f` (between base and target) is documentation-only.
- Scope inspected: `git diff adf5934 d8083d2`, i.e. internal/api/{server.go, server_test.go, server_integration_test.go}, the internal/state additions (`ListHistory`, `ErrWorkflowNotFound`, history node/iteration/attempt fields), cmd/runtime/main.go wiring, api/README.md, scripts/ci.ps1, and the PLAN.md DUR-006 section. There are no migration changes, and no protected-scope drift (DUR-006 adds no scheduler, fan-out, Kafka, or paid scope).
- Checks personally run (Claude). Code ran in a scratch export of `d8083d2`, against a throwaway database `cr_d6` (migrations 000001–000003) that was dropped afterwards:
  - Committed `TestPostgresStateRepository` and `TestWorkflowAPIResponseLossHistoryAndRetention` with `-race` and `DURABLE_REQUIRE_DATABASE=1`: PASS.
  - `go vet ./...`, `gofmt -l`, and `go test -race ./...`: clean.
  - Throwaway probes (scratch file deleted):
    - submission-hash coverage (R029);
    - error mapping and unrunnable nodes (R030);
    - duplicate JSON keys and numeric forms (R031);
    - 30 concurrent identical POSTs → one `201`, 29 `200`, one workflow;
    - 40 real TCP-level disconnects mid-request followed by a retry → always exactly one workflow (33 had not committed before the retry, 7 had).
  - Built `cmd/runtime` and ran it with `DATABASE_URL` pointed at the scratch database: unknown workflow → 404 JSON; wrong method → 405; unknown definition → 500 (R030); a wrong database password makes the runtime exit 1 at startup.
  - Read-only query on the shared dev database: 0 leftover `dur00*` rows.
- Codex-reported checks considered but not rerun: the full `ci.ps1 -WithRace -WithServices` in the repository, and the runtime Docker build (the Dockerfile already copies `internal/` and `go.sum`; Claude built the binary natively).
- Findings: new R029 (P2), R030 (P2), R031 (P3). R028 remains OPEN (P3, DUR-007 follow-up).
- Deferred P2 findings, if any: none.
- Remaining P3 findings / uncertainties / untested areas:
  - Open P3 findings: R028 and R031. The R019 test gap is still open.
  - Untested: API behavior during PostgreSQL unavailability after startup; request/DB timeouts under load; clean bootstrap and restart smoke; remote CI.
- Limitations: Windows host only; single local PostgreSQL 18.6; HTTP checks on loopback only.
- Verdict: CHANGES_REQUESTED. Blocking: R029 and R030 (both P2).

For each round, record:

- Date and round:
- Review basis: <PROVISIONAL | COMMITTED>
- Base and target commits: <commits | unavailable>
- Scope inspected:
- Checks personally run:
- Codex-reported checks considered:
- Deferred P2 findings, if any:
- Remaining P3 findings / uncertainties / untested areas:
- Limitations:
- Verdict: <CHANGES_REQUESTED | NO_BLOCKING_FINDINGS>

## Findings

### R001 — Protected-scope PLAN.md changes were committed without a recorded user decision

- Severity: P2
- Status: VERIFIED
- Deferred: no
- Reviewed commit: `87e123a`
- Location: PLAN.md:3, PLAN.md:49-50 (new RQ7/RQ8), section 10 release definitions (Portfolio MVP / Full v1), PLAN.md:615 (DUR-029 rescoped), PLAN.md:763 onward (live-model and adversarial studies); docs/DECISIONS.md (only D001–D004 exist).
- Failure scenario and impact: PLAN.md section 1 and CLAUDE.md forbid changing guarantees, release criteria, experiment families, paid budgets, or deferred scope without an explicit user decision recorded in `docs/DECISIONS.md`. `87e123a` commits a large scope change with no such record:
  - new research questions RQ7 (retrieval strategy) and RQ8 (defense profile / injection);
  - new MCP, pgvector, full-text-search, and Grafana stack items;
  - a separate retrieval benchmark (≥40 development + ≥120 held-out queries);
  - Portfolio MVP and Full v1 release criteria that now require these;
  - live-model volume raised from "20 × 2 = 40 executions" to a 60-execution retrieval-arm study plus a 120-execution adversarial ablation, which raises the planned paid model spend while the cap is still unset (PLAN.md:417).

  Git status at the start of this session already showed these PLAN.md edits uncommitted on top of `198fd4b`, and the handoff says "Existing PLAN changes were preserved", so they are most likely user-authored. Even so, nothing traceable shows the change was authorized. Future reviews cannot tell authorized scope from drift, and the expanded paid volume has no recorded basis.
- Evidence: `git diff 198fd4b 87e123a -- PLAN.md` (92+/32− in the scope-bearing sections beyond the M0 status edits); `docs/DECISIONS.md` at `87e123a` contains only D001–D004, none of which covers the scope change.
- Suggested correction: The user confirms authorship of the scope change. Codex then records it as a decision (e.g. `D005 — Retrieval/MCP/adversarial scope expansion`) in `docs/DECISIONS.md`, listing:
  - the added research questions, experiment families, and release-criteria changes;
  - the revised live-model execution counts;
  - a statement that paid caps remain unset and must be approved before DUR-029;
  - the alternatives considered.

  If the user did not author any part of the change, revert that part instead.
- Suggested validation: Claude checks that D005 covers every protected-scope change in `git diff 198fd4b 87e123a -- PLAN.md` and that the user's authorization is stated.

#### Codex response – round 2

- Change made or reason for disagreement: The user confirmed authorship and authorization of the pre-existing PLAN.md changes. Added D005 to `docs/DECISIONS.md`, covering RQ7/RQ8, the retrieval/MCP/pgvector/full-text/Grafana scope, the revised release criteria, the 40-development/120-held-out retrieval benchmark, the 60 live retrieval-arm executions, and the 120-execution adversarial study. D005 explicitly keeps paid execution and its spending cap subject to separate approval before DUR-029 and records the deferred-scope alternatives considered.
- Fix commit: `c78803e`
- Tests and results: Reviewed D005 against the protected-scope locations listed in R001. No model or paid-provider execution was performed or authorized by D005.
- Status: ADDRESSED

#### Claude verification – round 2

- Verification commit: `deb9815` (target `c78803e`)
- Evidence and remaining concerns: D005 (docs/DECISIONS.md) covers every protected-scope change listed in R001: RQ7/RQ8; MCP, full-text search, pgvector, and Grafana scope; the ≥40/≥120 retrieval benchmark; the Portfolio MVP/Full v1 release criteria; and the revised live-model volume (60 retrieval-arm executions plus 120 adversarial executions, i.e. 20 × 2 × 3). This matches PLAN.md:49-50, 615 and 763 onward. It keeps paid execution and the spending cap subject to separate approval before DUR-029. `git diff 5184a3d c78803e -- PLAN.md` is empty, so no further PLAN.md scope drift was introduced. Remaining note: the user's authorization is recorded on Codex's word. The user did not confirm it to Claude directly in this session and should correct D005 if it is inaccurate.
- Status: VERIFIED

### R002 — CONTRACTS.md state machine contradicts PLAN.md and omits required states

- Severity: P2
- Status: VERIFIED
- Deferred: no
- Reviewed commit: `87e123a`
- Location: docs/CONTRACTS.md:44-76 (state diagram), docs/CONTRACTS.md:114.
- Failure scenario and impact: DUR-002 says contradictions become blocking findings, and M1 (DUR-005) will build its schema directly from this frozen contract. Defects in the frozen contract:
  1. **Cooperating and non-cooperating sinks are not distinguished.** The diagram sends every "ambiguous external outcome" to `RECONCILIATION_REQUIRED`, and the ambiguous-effect trace (line 114) gives that outcome with no sink distinction. PLAN.md:146 and PLAN.md:287 say a lost response from the *cooperating* sink is resolved by retrying with the same effect key and reusing the stored receipt, or by a receipt lookup; only the non-cooperating endpoint (PLAN.md:321) requires reconciliation. A schema/state machine built from this contract would either send every cooperating-sink timeout to manual reconciliation, which breaks the cooperating-sink guarantee and the F-campaign expectations, or depart from the frozen contract.
  2. **Every accepted result passes through `WAITING_TIMER`.** The diagram's only path to `SUCCEEDED/FAILED` goes through `WAITING_TIMER`, so a successful activity would need a timer before completing. There is also no path from an accepted result back to scheduling the next node of a multi-node workflow.
  3. **No approval-wait state.** PLAN.md:325-333 requires a durable approval wait that holds no lock or worker slot, plus a dispatch-grant step.
  4. **No attempt state machine.** DUR-002 scope requires "workflow/attempt state diagrams". PLAN.md:179 defines attempt dispatch state, claim, heartbeat deadline, and terminal outcome, but no attempt lifecycle (e.g. created → dispatchable → claimed → succeeded/failed/timed-out/replaced/outcome-unknown) is given.
  5. **`RECONCILIATION_REQUIRED` has no exits** and is not classified as terminal or non-terminal. PLAN.md:321 allows audited operator resolve/abandon, and it is unclear whether "any non-terminal state → CANCELED" applies to it.
  6. **No state for unsupported versions.** PLAN.md:203 requires an explicit paused state with a reason.
- Evidence: the diagram at docs/CONTRACTS.md:44-76 and the line-114 trace, compared with PLAN.md:146, 179, 203, 287, 321, 325-333.
- Suggested correction: Revise the workflow diagram to:
  - add a scheduling loop for later nodes, a direct accepted-result → completion edge, and retry-backoff as the only use of `WAITING_TIMER`;
  - add `WAITING_APPROVAL`, the unsupported-version paused state, and `RECONCILIATION_REQUIRED` exits (audited resolve/abandon), with terminal/non-terminal status stated for each state.

  Also:
  - add an attempt state diagram;
  - split the ambiguous-effect outcome into cooperating (same-key retry or receipt lookup; reconciliation only if the lookup is itself unavailable past policy) and non-cooperating (`RECONCILIATION_REQUIRED`, no automatic retry);
  - bump the contract version, or note the change as a pre-freeze correction, before DUR-005 begins.
- Suggested validation: Claude re-walks every PLAN.md section 4–8 guarantee against the revised diagrams. No state or transition used by those sections should be missing or contradicted.

#### Codex response – round 2

- Change made or reason for disagreement: Replaced the v1 draft with `dur-002.v2` in `docs/CONTRACTS.md`. The workflow diagram now distinguishes retry/backoff timers from accepted results, includes `WAITING_APPROVAL`, `PAUSED_UNSUPPORTED_VERSION`, `RECONCILIATION_REQUIRED` exits, terminal `ABANDONED`, and direct next-node scheduling. Added the complete attempt lifecycle and cooperating versus non-cooperating effect recovery semantics.
- Fix commit: `ed62eca`
- Tests and results: `docs/CONTRACTS.md` was cross-checked against the cited PLAN sections; the Go/Python check suite passes.
- Status: ADDRESSED

#### Claude verification – round 2

- Verification commit: `c78803e`
- Evidence and remaining concerns: docs/CONTRACTS.md (`dur-002.v2`, D006):
  - lines 54-65: the state table gives each state's terminal status and exits;
  - line 68: accepted results go directly back to `RUNNABLE` or to a terminal state;
  - lines 56-61: `WAITING_APPROVAL`, `PAUSED_UNSUPPORTED_VERSION`, and `RECONCILIATION_REQUIRED` exits, plus terminal `ABANDONED`, are present;
  - lines 86-101: the attempt lifecycle is present, and cooperating and non-cooperating unknown outcomes are separated.

  All six R002 items are resolved. Smaller ambiguities are tracked in R012 (P3). The new traces introduce a separate authority/lock-order contradiction, tracked as R010.
- Status: VERIFIED

### R003 — Required race walkthroughs lack per-ordering outcomes, transaction boundaries, and durable-record locations

- Severity: P2
- Status: VERIFIED
- Deferred: no
- Reviewed commit: `87e123a`
- Location: docs/CONTRACTS.md:107-115 ("Required ordering traces").
- Failure scenario and impact: DUR-002 acceptance requires "one explicit outcome per ordering", and its validation requires "paper traces [that] include transaction boundaries and the location of every durable record". The committed traces are one-line table rows:
  - **Worker timeout/result race** (line 112) and **approval/cancellation race** (line 115) say only "one lock/serialized winner is recorded", without the outcome for each winner. They do not say, for example: timeout first → attempt replaced and late result rejected with no state change; result first → result accepted and timeout is a no-op. For approval/cancellation, PLAN.md:333 draws a specific boundary: cancellation before the durable dispatch grant prevents dispatch; after the grant it is best effort, and the effect may still apply. The trace does not state that.
  - **Scheduler handoff** (line 111) covers only "A starts a new write after B's takeover". It omits the ordering PLAN.md:224 calls out: A's transaction validated ownership before expiry and still holds the lease row lock while B tries to acquire it. There, A commits and B waits and then gets a higher epoch.
  - **Outbox duplicate** (line 113) omits the relay crash *before* broker acknowledgement (relay-claim expiry → republish), and never says where the consumer's idempotent disposition (event inbox) is stored or when the Kafka offset is committed relative to it.
  - **No trace shows** which transaction writes which rows (workflow, attempt, history, outbox, inbox, effect ledger, approval intent), or which store — engine database, effect-service ledger, or Kafka — holds each record at each step.

  With these gaps, DUR-005 through DUR-016 would each resolve these orderings ad hoc, which is the ambiguity the M0 contract freeze was meant to remove.
- Evidence: docs/CONTRACTS.md:107-115 against the DUR-002 acceptance/validation text and PLAN.md:224, 236-240, 248-250, 259, 269, 333.
- Suggested correction: Expand each of the six scenarios into a numbered step trace. For each step, record the actor and the transaction boundary (BEGIN/COMMIT), the rows locked/written and their store, and the durable outcome for **each** ordering of the racing operations. Include the orderings listed above. A per-scenario subsection or a `docs/PROTOCOLS.md` companion (PLAN.md section 11) are both acceptable.
- Suggested validation: Claude checks each trace for a complete ordering list and a single outcome per ordering. It also cross-checks that every durable record mentioned exists in the PLAN.md section 5 data model.

#### Codex response – round 2

- Change made or reason for disagreement: Expanded all six traces in `docs/CONTRACTS.md` into numbered per-ordering walkthroughs. They now identify BEGIN/COMMIT boundaries, EDB/K/EL record locations, the lock-held scheduler handoff, both timeout/result winners, pre-ack and post-ack outbox crashes, cooperating/non-cooperating effect outcomes, and cancellation before versus after the approval grant.
- Fix commit: `ed62eca`
- Tests and results: The revised contract was reviewed against PLAN.md sections 5-8; the Go/Python check suite passes.
- Status: ADDRESSED

#### Claude verification – round 2

- Verification commit: `c78803e`
- Evidence and remaining concerns: docs/CONTRACTS.md:134-260 expands the six scenarios into numbered traces with BEGIN/COMMIT boundaries, stores (EDB/K/EL), and an outcome for each ordering:
  - the lock-held scheduler handoff (lines 154-169);
  - both winners of the timeout/result race (171-191);
  - relay crashes before and after the broker acknowledgement, and inbox-before-offset ordering (193-208);
  - cooperating and non-cooperating ambiguous effects (210-237);
  - cancellation before and after the grant (239-260).

  The completeness requirement of R003 is met. However, the traces as written give workers and clients workflow-transition authority and skip the lease lock in the timeout transaction. That contradiction is recorded separately as R010 (P2) instead of reopening R003.
- Status: VERIFIED

### R004 — DUR-003 has no reusable failpoint API, seeded workflow fixtures, or fake activities; the seed has no effect

- Severity: P2
- Status: VERIFIED
- Deferred: no
- Reviewed commit: `87e123a`
- Location: python/faults/control.py:74-87 (the command is hard-coded to `-m faults.target`), python/faults/target.py:28, tests/test_fault_control.py:53-61.
- Failure scenario and impact: DUR-003 scope is "Seeded workflow fixtures, fake activities, named barrier/failpoint API, controller process, trace schema". The goal is to make failures reproducible *before the engine grows*. The committed harness:
  - **can only drive its own toy process.** No library exists that the Go runtime or a Python worker could call to declare a named failpoint and block on the controller, and the controller cannot launch any other command. The first scheduler/worker crash tests (M1/M2, most of which are Go-side) therefore have no failpoint mechanism, so the M0 gate is met in name only.
  - **has no seeded workflow fixture or fake activity.**
  - **uses the seed for nothing except `sha256(seed:boundary)`** (target.py:28). `test_same_seed_repeats_same_boundary_trace` therefore passes by construction.
  - **never repeats a crash.** DUR-003 validation asks to "repeat one controlled crash with the same seed", but the repeated test (lines 53-61) repeats a *release*; the kill path runs once and checks no trace stability.
- Evidence: code references above; `git grep -n failpoint 87e123a` finds no Go code; the controller accepts only `--seed/--boundary/--skip-boundary` for the fixed target.
- Suggested correction: Either (a) or (b):
  - **(a) Implement the missing scope minimally:**
    - a failpoint client in both Go (`internal/faults` or similar) and Python that reads a controller channel from the environment, reports `boundary_reached`, and blocks on release/kill;
    - a controller that launches an arbitrary target command;
    - a small seeded fixture generator (seed → deterministic workflow definition and fake activity inputs/outcomes) plus one fake activity that hits a named failpoint;
    - a test that repeats the *kill* scenario with the same seed and compares normalized traces.
  - **(b) Split out the missing items:** record them as an explicit named subtask (e.g. DUR-003B) with a user-approved decision and a dependency before the first M1/M2 crash test. DUR-003 is then accepted only for the delivered subset.
- Suggested validation: A Go test process and a Python test process each stop at a named failpoint under the controller and are killed there. Two runs with the same seed produce identical normalized traces, and a different seed changes the fixture-derived fields.

#### Codex response – round 2

- Change made or reason for disagreement: Added reusable environment-configured failpoint clients in `internal/faults` and `python/faults/client.py`, an arbitrary-command TCP controller, seeded Python fixtures/fake activity, and `cmd/fault-fixture` for Go. The tests repeat the actual kill scenario with the same seed and assert that a different seed changes fixture fields.
- Fix commit: `ed62eca`
- Tests and results: `tests/test_fault_control.py` passes 7/7, including Python and Go targets, repeated kills, release/acknowledgement, early exit, noisy output, and heavy stderr.
- Status: ADDRESSED

#### Claude verification – round 2

- Verification commit: `c78803e`
- Evidence and remaining concerns: internal/faults/client.go and python/faults/client.py are environment-configured failpoint clients. `FaultController.start(command=...)` launches arbitrary commands. python/faults/fixtures.py derives the fixture and fake activity from `random.Random(seed)`. cmd/fault-fixture drives the Go client. `test_kill_crash_repeats_with_same_seed_and_changes_with_new_seed` repeats the kill scenario and compares normalized traces and fields; `test_go_process_uses_the_same_failpoint_protocol` exercises a Go process. Claude ran `ci.ps1 -WithRace` on `c78803e` (11 passed) and `tests/test_fault_control.py` 15 times (15/15 PASS, including the Go test). Remaining limitations (not blocking): the Go path is only killed, never released, so the Go `Emit("released")` path is untested; the Go test assumes pytest runs from the repository root, where `go run ./cmd/fault-fixture` resolves; no runtime/worker code calls a failpoint yet, which is expected before M1.
- Status: VERIFIED

### R005 — Fault controller reports target crashes and protocol errors as "boundary not reached"

- Severity: P2
- Status: VERIFIED
- Deferred: no
- Reviewed commit: `87e123a`
- Location: python/faults/control.py:33-37, 100-114, 116-134.
- Failure scenario and impact: The trace exists to prove *where* a fault happened. Two paths produce false evidence:
  1. **Target exits before its boundary** (startup crash, import error, bad arguments). `wait_for_boundary` waits the full timeout, then records only `boundary_timeout`, with no process-exit event before it. The trace cannot distinguish "the target never reached the boundary because it crashed" from "the target hung".
  2. **Any non-JSON line on the target's stdout** (a log line, a warning) makes `json.loads` raise inside the daemon reader thread. The thread dies silently and all later output is ignored. A boundary that *was* reached is then recorded as `boundary_timeout`. JSON that decodes to a non-object is handled, but invalid JSON is not.

  Real engine processes print logs, so once R004 wires them in, the second case becomes likely. Campaign traces would then report "not reached" for faults that were injected, a harness-sensitivity failure. `stderr` is also piped but never read, so a target that writes a lot to stderr can block on a full pipe and look like a timeout.
- Evidence: Claude's scratch probe (not committed): a killed target gave `result None waited 2.00s events ['process_started', 'boundary_timeout']`; a target printing `starting up` and then a valid `boundary_reached` gave `events ['process_started', 'boundary_timeout']`, plus a `JSONDecodeError` traceback from `_read_output`.
- Suggested correction:
  - **Separate the protocol channel from stdout.** Use a dedicated channel (a pipe or socket handed to the target), or prefix protocol lines and ignore/record everything else instead of raising.
  - **Keep the reader thread alive.** Catch decode errors and record a `protocol_error` or `target_output` event.
  - **Report early exits.** Have `wait_for_boundary` also notice process exit (e.g. put a sentinel on the queue when stdout reaches EOF) and record `process_exited_before_boundary` with the return code.
  - **Drain stderr.** Read it into the trace or a log file.
- Suggested validation: Tests covering (a) target exits before the boundary → a distinct trace event returned before the timeout; (b) non-JSON output before the boundary → boundary still detected; (c) heavy stderr output does not block.

#### Codex response – round 2

- Change made or reason for disagreement: Moved the control protocol to a dedicated loopback channel, leaving stdout/stderr as drained logs. Protocol decode errors are recorded without killing the reader. Process monitoring reports `process_exited_before_boundary` with the return code, and boundary timeouts are reserved for a live target that never reports the boundary.
- Fix commit: `ed62eca`
- Tests and results: Tests cover early exit, non-protocol stdout, 2,000 stderr lines, and normal boundary detection; the full check suite passes.
- Status: ADDRESSED

#### Claude verification – round 2

- Verification commit: `c78803e`
- Evidence and remaining concerns: The protocol now runs over a token-authenticated loopback TCP channel (control.py `start`, `_read_protocol`). Decode errors become `protocol_error` records without killing the reader (`_handle_protocol_line`). `_watch_process` queues `ProcessExited`, and `wait_for_boundary` records `process_exited_before_boundary`. stdout and stderr are drained by separate threads. The tests cover early exit (return code 17, with no `boundary_timeout`), noisy stdout, and 2,000 stderr lines. Claude's scratch probe of a target that reports its boundary and immediately calls `os._exit(9)` was classified correctly in 40/40 runs. A reader/watcher enqueue-ordering race remains theoretically possible but was not observed.
- Status: VERIFIED

### R006 — Fault traces lack the target's acknowledgement and are rewritten non-atomically

- Severity: P3
- Status: VERIFIED
- Deferred: no
- Reviewed commit: `87e123a`
- Location: python/faults/control.py:52-65, 108-114, 144-147; python/faults/target.py:46-53.
- Failure scenario and impact:
  - **Release is never confirmed by the target.** The target emits `released` after it receives the release command, but the reader drops every event except `boundary_reached`. A release trace therefore shows only the controller's command and an exit code, not that the target resumed past the boundary.
  - **The trace file can be lost mid-run.** `_record` rewrites the whole JSONL file on every event, so a controller crash mid-write can truncate the evidence of earlier events. Appending plus flushing would keep the earlier lines.
  - **`pause()` does nothing but record.** The target is already blocked, so "pause" here means "hold at the barrier". That is acceptable, but the docs should say so, and no test shows the target stays held (e.g. no `released` event before the release command).
- Evidence: code references above; the probe's release trace was `['process_started', 'boundary_reached', 'command', 'process_exited']`.
- Suggested correction: Record the target's `released` acknowledgement, and add an assertion that it arrives only after the release command. Append-and-flush the JSONL trace instead of rewriting it. Document the semantics of `pause`.
- Suggested validation: The release test asserts the order `command(release)` → `released` → `process_exited`, and a truncated-write simulation keeps all earlier records.

#### Codex response – round 2

- Change made or reason for disagreement: The controller now records the target's `released` protocol event, tests pause → release → acknowledgement → exit ordering, and appends/flushed/fsyncs each JSONL trace record rather than rewriting the whole file. `pause()` is documented as holding the already-reported barrier.
- Fix commit: `ed62eca`
- Tests and results: The release test validates monotonic trace sequences and parseable appended records; the full check suite passes.
- Status: ADDRESSED

#### Claude verification – round 2

- Verification commit: `c78803e`
- Evidence and remaining concerns: The `released` protocol event is now recorded, and the release test asserts `boundary_reached` < `command` < `released` < `process_exited` with contiguous sequence numbers. `_record` appends, flushes, and fsyncs one line per record. The semantics of `pause()` are documented in tests/faults/README.md. New side effect: reusing a trace path concatenates runs whose sequence numbers restart at 1, tracked as R011 (P3).
- Status: VERIFIED

### R007 — Go and Python partition implementations disagree on invalid UTF-8; vectors are duplicated and lack a non-ASCII case

- Severity: P3
- Status: VERIFIED
- Deferred: no
- Reviewed commit: `87e123a`
- Location: internal/partition/partition.go:21-26; python/durable_contracts/partition.py:18-24; docs/partition-map-v1.md; internal/partition/partition_test.go; tests/test_partition_vectors.py.
- Failure scenario and impact: The contract says workflow IDs are non-empty UTF-8 strings.
  - **Invalid UTF-8 is accepted in Go only.** Go hashes any byte string, so `"wf-\xff\xfe"` → partition 8, while Python cannot encode the equivalent (surrogate-escaped) string and raises `UnicodeEncodeError`. If an API layer ever passes raw bytes through, the Go and Python implementations disagree instead of both rejecting the input.
  - **The vectors are defined in three places.** They are copied into the doc, the Go test, and the Python test, with no shared vector file, so a future edit can drift in one place.
  - **Non-ASCII input is never tested**, although it is part of the UTF-8 contract. Claude checked that `wf-é-日本` → 3 in both implementations.
- Evidence: Claude's scratch Go/Python probe results in Round 1.
- Suggested correction: In Go, reject IDs where `!utf8.ValidString(workflowID)`. Move the vectors into one checked-in file (e.g. `api/partition-map-v1.vectors.json`) that both test suites load, and add at least one non-ASCII vector and one invalid-UTF-8 rejection case.
- Suggested validation: Both suites load the shared vector file, pass, and reject invalid UTF-8.

#### Codex response – round 2

- Change made or reason for disagreement: Go now rejects invalid UTF-8 explicitly. The duplicated vectors moved to `api/partition-map-v1.vectors.json`, both suites load that file, and it includes the non-ASCII `wf-é-日本` case plus separate invalid-UTF-8 rejection tests.
- Fix commit: `ed62eca`
- Tests and results: Go partition tests and Python partition tests pass in the full check suite.
- Status: ADDRESSED

#### Claude verification – round 2

- Verification commit: `c78803e`
- Evidence and remaining concerns: internal/partition/partition.go now rejects `!utf8.ValidString`. Both suites load `api/partition-map-v1.vectors.json`, which contains `wf-é-日本` → 3; this matches Claude's round-1 independent value. Both suites also have invalid-UTF-8 rejection tests. Checks passed in Claude's `ci.ps1 -WithRace` run.
- Status: VERIFIED

### R008 — Scaffold issues that will break or mislead in DUR-005

- Severity: P3
- Status: VERIFIED
- Deferred: no
- Reviewed commit: `198fd4b` (unchanged at `87e123a`)
- Location: deploy/local/Dockerfile.runtime:3-5; scripts/migrate.ps1:9-14; scripts/ci.ps1:21-26.
- Failure scenario and impact:
  1. **The runtime image will stop building.** Dockerfile.runtime copies only `go.mod` and `cmd/`. As soon as `cmd/runtime` imports `internal/...` (e.g. `internal/partition`) or gains a `go.sum`, `docker compose up --build` fails.
  2. **Migrations are re-run every time.** `migrate.ps1` replays every `*.up.sql` file on each bootstrap and never checks `engine.schema_migrations`. That works only while every migration is idempotent; the first non-idempotent DUR-005 migration (e.g. `ALTER TABLE ... ADD COLUMN`) fails or double-applies on the next bootstrap.
  3. **The service check overstates its coverage.** DUR-004 scope lists "real database/Kafka integration", but `-WithServices` runs health checks only. The acceptance criterion requires unimplemented checks to be reported explicitly, and `ci.ps1` does not say that integration tests do not exist yet.
- Evidence: file contents at the listed lines.
- Suggested correction: Copy `internal/` (and `go.sum` once it exists) into the build stage. Make `migrate.ps1` skip versions already recorded in `engine.schema_migrations`, with each migration recording its own version. Have `ci.ps1 -WithServices` print that PostgreSQL/Kafka integration tests are not yet implemented, until they are.
- Suggested validation: After DUR-005 adds a non-idempotent migration, run `bootstrap.ps1 -StartServices` twice. Build the runtime image with an `internal/` import.

#### Codex response – round 2

- Change made or reason for disagreement: `Dockerfile.runtime` now copies `internal/`. `migrate.ps1` inspects `engine.schema_migrations`, skips recorded versions, and verifies a newly applied migration records its numeric filename version. `ci.ps1 -WithServices` explicitly states that M0 has no PostgreSQL/Kafka integration tests and runs service health/durability smoke checks only.
- Fix commit: `ed62eca`
- Tests and results: Post-fix bootstrap rebuilt both runtime images and passed health/migration smoke; the migration script reported `Skipping already applied 000001_bootstrap.up.sql`; restart smoke and `ci.ps1 -WithRace -WithServices` passed.
- Status: ADDRESSED

#### Claude verification – round 2

- Verification commit: `c78803e`
- Evidence and remaining concerns: Claude built `deploy/local/Dockerfile.runtime` from a scratch export of `c78803e` (exit 0; `version` printed `0.1.0-dev`). A second build, with a blank import of `internal/partition` added to `cmd/runtime/main.go` in the scratch copy, also succeeded; both throwaway images were removed. `scripts/migrate.ps1`, run against the live local database, printed `Skipping already applied 000001_bootstrap.up.sql` (exit 0; worktree unchanged). Its ledger check and post-apply confirmation logic were reviewed; Claude did not execute a non-idempotent migration because that would modify the shared local database. `ci.ps1 -WithServices` now prints that M0 has no integration tests (scripts/ci.ps1:22).
- Status: VERIFIED

### R009 — Handoff base commit excludes the DUR-001 implementation it claims to hand off

- Severity: P3
- Status: VERIFIED
- Deferred: no
- Reviewed commit: `5184a3d`
- Location: REVIEW.md "Codex handoff" (`Base commit: 198fd4b`); docs/BUILD_LOG.md.
- Failure scenario and impact: The handoff scope is "DUR-001 through DUR-004", but DUR-001 was implemented *in* `198fd4b`. A reviewer diffing `198fd4b..87e123a`, as declared, would never see the DUR-001 scaffold (Compose, Dockerfiles, scripts, runtime/worker stubs, migrations), which had not been reviewed before. That is a mismatch between the handoff declaration and the repository state.
- Evidence: `git show --stat 198fd4b` (37 files, including all DUR-001 scaffolding); the previous REVIEW.md said "No review performed yet."
- Suggested correction: In future handoffs, set the base to the last *reviewed* commit (here `d722cf7`), or list per-task base/target commits. This round already reviewed `d722cf7..87e123a` for DUR-001, so no further action is needed for M0 beyond correcting the handoff field on the next update.
- Suggested validation: The next handoff's base commit matches the last reviewed commit.

#### Codex response – round 2

- Change made or reason for disagreement: The next handoff will use `d722cf7`, the last reviewed planning baseline, as the base and will identify the new fixed target commit explicitly. The original scaffold remains in the review range rather than being implicitly excluded.
- Fix commit: `ed62eca`
- Tests and results: The current review response records the corrected base policy; final commit hashes will be filled after this fix pass is committed.
- Status: ADDRESSED

#### Claude verification – round 2

- Verification commit: `deb9815`
- Evidence and remaining concerns: The handoff now declares base `d722cf7` and target `c78803e`. `git log d722cf7..deb9815` shows the scaffold `198fd4b`, `87e123a`, the fix commit `ed62eca`, and the docs-only commits `d8adc93`, `5afc4f5` (.gitignore), `c78803e` (D005), and `deb9815` (handoff). The declaration matches the repository state. The target `c78803e` contains all code changes; `deb9815` changes only REVIEW.md and docs/BUILD_LOG.md.
- Status: VERIFIED

### R010 — v2 race traces let workers and clients make owner-only workflow transitions and skip the lease lock

- Severity: P2
- Status: VERIFIED
- Deferred: no
- Reviewed commit: `c78803e`
- Location: docs/CONTRACTS.md:173-177 (timeout wins), 185-191 (result wins), 241-246 (cancellation before grant), 250-253 (grant before cancellation). These conflict with docs/CONTRACTS.md:23-28 (actor table), 105-111 (lock order), and PLAN.md:216-218, 232, 238, 269-271.
- Failure scenario and impact: The v2 traces, introduced by the R003 fix, contradict the actor and lock rules frozen in the same document. DUR-005 and DUR-011 will implement whichever version they read.
  1. **A worker transaction advances the workflow.** In "result wins", the worker's control-API transaction `T_result` also makes "the next node `RUNNABLE` or the workflow terminal in this same transaction". The actor table says workers may write only heartbeat, checkpoint, and result, and that workflow/node transitions belong to the scheduler holding `(partition, owner, epoch)`. PLAN.md:269-271 says accepted results are stored together with a completion outbox event and that "the current owner handles the state transition". If implemented as written, a workflow transition commits without lease validation, which bypasses the scheduler fencing boundary that DUR-002 exists to freeze.
  2. **The lock order is inverted.** `T_result` locks the attempt first and then has to lock the workflow/node rows in order to advance them, while `T_timeout` locks workflow → node → attempt. Concurrent result and timeout transactions can then deadlock, which contradicts the declared order "lease, workflow, node/attempt".
  3. **`T_timeout` never locks or validates the lease row.** It is a scheduler transaction that writes a replacement attempt and a dispatch outbox row, yet it "locks `workflow`, `node`, and `attempt`" only. A stale former owner following this trace could create a replacement attempt after takeover. That breaks the scheduler-ownership guarantee (PLAN.md:143) at the very race this trace documents.
  4. **Clients and approvers perform owner transitions.** `T_cancel` "records cancellation" and a later transaction "sees `CANCELED`", and `T_approve` writes the grant and moves the workflow on. Neither validates ownership, and the actor table says clients only *request* cancellation or approval. The contract should say which rows these API transactions may write (cancellation request, approval decision, grant) and whether the `CANCELED` / `WAITING_APPROVAL → RUNNABLE` transitions are applied by the owner or are deliberately non-owner transitions, and if the latter, why that is safe.
- Evidence: the quoted trace lines against the actor table and lock-order paragraph in the same file, and PLAN.md:216 ("Every scheduler-authorized state change locks and validates the lease row in the same transaction").
- Suggested correction:
  - In "result wins", limit `T_result` to the result receipt, attempt `SUCCEEDED`, history, and completion outbox/wake-up. Add a separate owner transaction (lease → workflow → node/attempt) that consumes the wake-up and schedules the next node or terminal state.
  - Add the lease lock and owner/epoch/expiry validation as the first step of `T_timeout`.
  - Define the control-API lock order explicitly, e.g. lock the workflow row before the attempt row if the API must read workflow state.
  - For cancellation and approval, either record request/decision/grant rows that the owner applies, or declare them as explicitly authorized non-owner transitions serialized by the workflow-row lock and revision check, and update the actor table to match.
  - Bump to `dur-002.v3`, or amend D006, before DUR-005.
- Suggested validation: Claude re-walks all six traces against the actor table and the lock order. Every scheduler-authorized write must be preceded by a lease lock and validation. No transaction may acquire locks against the declared order. No actor may write a row that the actor table forbids.

#### Codex response - round 3

- Change made or reason for disagreement: Advanced the contract to `dur-002.v3`. Worker result transactions now record only the attempt result, completion wake-up, and history; the lease-owning scheduler performs downstream workflow transitions in a separate lease-first transaction. Timeout, reconciliation, cancellation application, and approval application all lock and validate the lease before workflow/node/attempt rows. Client cancellation requests and approver decisions are intent records only. The lock-order text and all affected traces were updated accordingly.
- Fix commit: `eb32162`
- Tests and results: `scripts/check.ps1` passed with 12 Python tests; `scripts/ci.ps1 -WithRace` passed with Go race tests and 12 Python tests. The revised contract was cross-checked against its actor table, transition permissions, and PLAN.md lease-authority requirement.
- Status: ADDRESSED

#### Claude verification – round 3

- Verification commit: `f1f35ff` (target `eb32162`)
- Evidence and remaining concerns: docs/CONTRACTS.md (`dur-002.v3`):
  - lines 23-35: the actor table limits workers to result receipts plus completion wake-ups, and clients/approvers to intent records; only the lease owner applies transitions and creates grants.
  - lines 133-150: the control-API and intent transactions lock `workflow` then `node/attempt`, never take the lease, and never change workflow state.
  - lines 206-238: `T_timeout` now locks and validates the lease first, and a separate owner `T_advance` performs downstream scheduling.
  - lines 283-290 and 296-326: reconciliation, cancellation application, and approval application are owner transactions that take the lease first.

  Lock-order check: worker/intent transactions take only `workflow → node/attempt`, and owner transactions take `lease → workflow → node/attempt`. The relative order of the shared rows is consistent and no transaction takes the lease after `workflow`, so no lock cycle is possible. The text at lines 140-142 calls the worker order a "prefix" of the scheduler order; it is actually an ordered subsequence (R014). All four R010 items are resolved. A separate timeout-safety gap is recorded as R013.
- Status: VERIFIED

### R011 — Reusing a trace path mixes runs with restarting sequence numbers

- Severity: P3
- Status: VERIFIED
- Deferred: no
- Reviewed commit: `c78803e`
- Location: python/faults/control.py `_record` (append mode) and `start`.
- Failure scenario and impact: Since the R006 fix, each record is appended to `trace_path`, but nothing truncates the file, refuses an existing path, or tags records with a run ID. Two controller runs with the same `--trace` path produce one file whose `sequence` values restart at 1, and a later checker cannot tell which records belong to which run. Campaign scripts that reuse a per-scenario path would silently merge evidence.
- Evidence: Claude's scratch probe ran two release runs with the same path and got sequences `[1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6]`.
- Suggested correction: In `start()`, create the trace file exclusively (fail if it exists) or truncate it explicitly, and add a per-run `run_id` (e.g. the controller token or a UUID) to every record.
- Suggested validation: A test that starts two controllers with the same trace path gets either an error or two distinct `run_id` values with contiguous sequences per run.

#### Codex response - round 3

- Change made or reason for disagreement: Fault traces now carry a per-run `run_id`, and `start()` creates a supplied trace path exclusively, failing rather than appending a second run with restarted sequence numbers. Added regression coverage for run identity and path reuse.
- Fix commit: `eb32162`
- Tests and results: The fault-control suite passed 8/8, including the exclusive-path regression; each run's trace records have one run ID and contiguous sequences.
- Status: ADDRESSED

#### Claude verification – round 3

- Verification commit: `f1f35ff` (target `eb32162`)
- Evidence and remaining concerns: python/faults/control.py: `_prepare_trace` opens the trace path with mode `x` before the process starts, and every record carries `run_id`. `test_trace_path_is_exclusive_and_run_id_is_present` expects `FileExistsError` on reuse, and the release test asserts a single `run_id` with contiguous sequences. Claude ran `ci.ps1 -WithRace` on `eb32162` (12 passed) and `tests/test_fault_control.py` 10 more times (10/10 PASS).
- Status: VERIFIED

### R012 — Remaining state-table ambiguities in `dur-002.v2`

- Severity: P3
- Status: VERIFIED
- Deferred: no
- Reviewed commit: `c78803e`
- Location: docs/CONTRACTS.md:54-79, 88-95; docs/DECISIONS.md D006.
- Failure scenario and impact: Small inconsistencies that DUR-005 would otherwise have to guess:
  1. **Rejected approvals map to an unclear terminal state.** The table says a rejected approval returns "to a recorded no-action terminal result", while the diagram says `FAILED/no-action`. PLAN.md section 14 scores correct restraint separately from failures, so mapping rejection to `FAILED` would distort those metrics.
  2. **Cancellation exits are incomplete.** `CANCELED` is shown as an exit only from `RUNNABLE`. The table does not list cancellation for `WAITING_ACTIVITY`, `WAITING_TIMER`, `PAUSED_UNSUPPORTED_VERSION`, or `RECONCILIATION_REQUIRED`, or say what happens to an in-flight non-effect attempt.
  3. **The attempt diagram is incomplete.** It gives the non-cooperating `OUTCOME_UNKNOWN` attempt no exit (operator resolution), and does not say whether `DISPATCHABLE` can time out or be cancelled before a claim.
  4. **D006 and the contract disagree about `WAITING_TIMER`.** D006 says retry/backoff is "the only use" of it, while CONTRACTS.md:58 also allows "an explicit timer".
- Evidence: the cited lines.
- Suggested correction: Name the rejection outcome (e.g. a terminal `SUCCEEDED` with a no-action result, or a distinct `REJECTED`), list the cancellation exits for every non-terminal state, complete the attempt exits, and align the `WAITING_TIMER` wording in D006 and the contract. These can be folded into the R010 revision.
- Suggested validation: Claude checks the revised state table.

#### Codex response - round 3

- Change made or reason for disagreement: Completed the state-table revision with a distinct terminal `REJECTED` no-action outcome, cancellation exits for every non-terminal workflow state, complete pre-claim and unknown-outcome attempt exits, and an explicit owner-applied cancellation/approval model. Amended D006 and `dur-002.v3` so `WAITING_TIMER` means retry/backoff expiry only.
- Fix commit: `eb32162`
- Tests and results: `git diff --check` passed; the revised contract and D006 amendment were reviewed together before handoff.
- Status: ADDRESSED

#### Claude verification – round 3

- Verification commit: `f1f35ff` (target `eb32162`)
- Evidence and remaining concerns: The four R012 items are resolved in `dur-002.v3`:
  1. terminal `REJECTED` is distinct from `FAILED` (CONTRACTS.md:61, 66, 95-96);
  2. cancellation exits are listed for every non-terminal state (58-63, 70-87);
  3. the attempt diagram has pre-claim cancellation and exits for non-cooperating unknown outcomes (100-121);
  4. `WAITING_TIMER` is retry/backoff only in both CONTRACTS.md:60 and the D006 amendment.

  A stale row in the transition-permissions table is recorded as R014.
- Status: VERIFIED

### R013 — Timeout of a claimed non-cooperating effect attempt creates an automatic replacement (repeat of an irreversible effect)

- Severity: P2
- Status: VERIFIED
- Deferred: no
- Reviewed commit: `eb32162`
- Location: docs/CONTRACTS.md:104 (attempt diagram `TIMED_OUT --> REPLACED --> replacement attempt`), 211-217 (the "Timeout wins" trace writes a replacement attempt and a dispatch outbox row unconditionally), 59 (`WAITING_ACTIVITY` exits), 277-290 (the non-cooperating trace assumes the worker survives to record `OUTCOME_UNKNOWN`). These conflict with PLAN.md:145, 147, 286-287, and 321.
- Failure scenario and impact:
  1. A worker claims an attempt for the non-cooperating test endpoint (or for an approved remediation that reaches it).
  2. The worker sends the request, and the endpoint applies the change.
  3. The worker then crashes, pauses, or is partitioned from the control API before it records anything.
  4. The heartbeat deadline passes. Following "Timeout wins", the lease owner writes `TIMED_OUT`, a replacement attempt, and a dispatch outbox row.
  5. A new worker claims the replacement and calls the non-idempotent endpoint again, so the irreversible effect is applied twice.

  PLAN.md:286 states that "a timeout ... does not prove the worker stopped", and PLAN.md:147/321 require ambiguous non-cooperating outcomes to stop automatic retries and become `RECONCILIATION_REQUIRED`. As written, the contract covers only the case where the worker survives to record `OUTCOME_UNKNOWN`. The more common crash case is sent down the generic replacement path. This is the project's central demonstration: "a tool applies ... then crashes at an inconvenient boundary". For the cooperating sink, replacement is safe only because the replacement reuses the same logical effect key, and the contract does not say so explicitly either. The gap existed in `dur-002.v1` and `v2`; Claude missed it in rounds 1 and 2.
- Evidence: the cited contract lines; no trace or state-table entry distinguishes timeout handling by effect class, or claimed from unclaimed attempts.
- Suggested correction:
  1. Make timeout handling depend on the activity's declared effect class and on the attempt's claim state:
     - **Unclaimed (`DISPATCHABLE`) timeout or lost notification:** redispatch, preserving the attempt identity (PLAN.md:275).
     - **Claimed pure activity, or a cooperating effect:** `TIMED_OUT` → replacement that reuses the same logical effect key and grant scope. The cooperating sink deduplicates or returns the existing receipt.
     - **Claimed non-cooperating effect:** `TIMED_OUT`/`OUTCOME_UNKNOWN` with no replacement. The owner applies `RECONCILIATION_REQUIRED` in the same lease-first transaction.
  2. Update the attempt diagram, the `WAITING_ACTIVITY` row, and "Timeout wins" (or add a trace 3b) to match.
  3. Record the effect class as a required property of the activity definition.
- Suggested validation: Claude re-walks trace 3 for each effect class. Later, DUR-016 and the F-campaign should include a "non-cooperating worker killed after the effect, before any result" case whose expected outcome is `RECONCILIATION_REQUIRED` with the endpoint's private ledger showing exactly one applied mutation.

#### Codex response - round 4

- Change made or reason for disagreement: Advanced the contract to `dur-002.v4` and made timeout handling depend on both claim state and the activity's immutable effect class. An unclaimed `DISPATCHABLE` attempt is redispatched with the same attempt identity; a claimed `PURE_ACTIVITY` may be replaced; a claimed `COOPERATING_EFFECT` replacement reuses the same effect key and grant scope; a claimed `NON_COOPERATING_EFFECT` records an unknown outcome and moves to `RECONCILIATION_REQUIRED` without a replacement. Added the required effect-class definition property and updated D006.
- Fix commit: `b993d71`
- Tests and results: The contract trace was re-walked for unclaimed, pure, cooperating, and non-cooperating timeout branches. The full check and race suites passed; no runtime engine or paid/model execution is claimed.
- Status: ADDRESSED

#### Claude verification – round 4

- Verification commit: `62f035b` (target `b993d71`)
- Evidence and remaining concerns: docs/CONTRACTS.md (`dur-002.v4`):
  - lines 52-58: every activity definition declares an immutable effect class.
  - lines 67, 108-129: the `WAITING_ACTIVITY` row, attempt diagram, and explanatory text branch on claim state and effect class.
  - lines 238-266 ("Timeout wins"): lease-first `T_timeout` reads the effect class, then chooses:
    - unclaimed → redispatch the same attempt identity, no replacement;
    - claimed pure → replacement;
    - claimed cooperating → replacement with the same logical effect key and grant scope;
    - claimed non-cooperating → `TIMED_OUT` + `OUTCOME_UNKNOWN` + `RECONCILIATION_REQUIRED` in the same transaction, with no replacement or dispatch.
  - The permission table (line 186) requires the declared effect class for timeout/redispatch; the D006 amendment matches.

  Re-walking the R013 scenario (effect applied, worker crashes silently, deadline passes) now ends in `RECONCILIATION_REQUIRED` with no second call. Codex's statement that the DUR-016/F-campaign test is a future obligation is accepted; no engine code exists yet. A minor inconsistency in how late non-cooperating results are handled is recorded as R016 (P3).
- Status: VERIFIED

### R014 — Residual `dur-002.v3` wording inconsistencies

- Severity: P3
- Status: VERIFIED
- Deferred: no
- Reviewed commit: `eb32162`
- Location: docs/CONTRACTS.md:133-142, 154-165.
- Failure scenario and impact:
  1. **Stale approval row.** The transition-permissions row "Approve action | ... and valid grant" predates v3. In v3, the approver records a decision *without* a grant and the owner creates the grant, so the row is circular. The table also has no rows for recording a cancellation request, applying a cancellation, or applying an approval, or for which actor performs each.
  2. **Ambiguous revision check.** Line 133 says the control API "validates the current workflow revision". If DUR-005/011 read this as "the revision must equal the revision captured at claim time", any unrelated owner transition, such as recording a best-effort cancellation after a grant, would reject a valid in-flight result. That would turn an applied effect into a lost result. PLAN.md:232 says completion is checked against attempt state, not stale copied metadata.
  3. **"Prefix" wording.** Lines 140-142 call `workflow → node/attempt` a "prefix" of the scheduler order; it is an ordered subsequence. The safety conclusion is still correct.
- Evidence: the cited lines.
- Suggested correction:
  - Split the approval row into "Record approval decision" (approver) and "Apply decision + create grant" (lease owner), and add the cancellation request/apply rows.
  - State that the control API checks the attempt/claim token and that the workflow is non-terminal, not equality with a claim-time revision.
  - Fix the wording at lines 140-142.
- Suggested validation: Claude re-reads the table against the actor table.

#### Codex response - round 4

- Change made or reason for disagreement: Split transition permissions into approver-recorded decisions and scheduler-applied approval/grant transitions, and added cancellation request/application rows. The worker control API now checks the persisted non-terminal workflow and current attempt/claim token rather than requiring equality with a claim-time workflow revision. Replaced "prefix" with "ordered subsequence".
- Fix commit: `b993d71`
- Tests and results: The transition-permission table and transaction-boundary text were cross-checked against the v4 actor table and R010 walkthroughs.
- Status: ADDRESSED

#### Claude verification – round 4

- Verification commit: `62f035b` (target `b993d71`)
- Evidence and remaining concerns: The permission table now separates "Record approval decision" (approver) from "Apply approval + create grant" (lease owner) and adds cancellation request/apply rows (CONTRACTS.md:186-191). The control API validates a non-terminal workflow plus the current attempt/claim, and explicitly does not reject a result because of an unrelated revision change (152-163). "Prefix" is replaced with "ordered subsequence".
- Status: VERIFIED

### R015 — Handoff body still describes the round-2 state

- Severity: P3
- Status: VERIFIED
- Deferred: no
- Reviewed commit: `f1f35ff`
- Location: REVIEW.md "Codex handoff" (checks and limitations bullets).
- Failure scenario and impact: The handoff target was updated to `eb32162`, but the body still lists "11 pytest tests" (twice) where the round-3 response reports 12. It also says "Claude round-2 verification remains pending", and its summary does not mention `dur-002.v3` or the trace `run_id`/exclusive-path change. CLAUDE.md requires recording mismatches between a handoff declaration and the observed state. The effect is small, but a reader of the handoff alone would get the wrong check evidence.
- Evidence: REVIEW.md handoff lines at `f1f35ff`; Claude's run on `eb32162` collected 12 tests.
- Suggested correction: Refresh the handoff's summary, check counts, and limitations on each handoff update.
- Suggested validation: The next handoff matches its target commit.

#### Codex response - round 4

- Change made or reason for disagreement: Refreshed the current handoff to report `dur-002.v4`, the per-run/exclusive trace evidence, 12 Python tests in both check entries, and Claude round-3 verification as the pending review stage.
- Fix commit: `b993d71`
- Tests and results: The handoff was checked against the recorded `ci.ps1 -WithRace` output and the current target commit.
- Status: ADDRESSED

#### Claude verification – round 4

- Verification commit: `62f035b` (target `b993d71`)
- Evidence and remaining concerns: The handoff at `62f035b` names target `b993d71`, mentions `dur-002.v4` and the per-run/exclusive trace evidence, reports 12 Python tests in both check entries, and names round-4 verification as pending. Claude's run on `b993d71` collected 12 tests. `b68aef8` and `62f035b` change only REVIEW.md and docs/BUILD_LOG.md.
- Status: VERIFIED

### R016 — Late result for a timed-out non-cooperating attempt is both "rejected" and "handled as reconciliation evidence"

- Severity: P3
- Status: VERIFIED
- Deferred: no
- Reviewed commit: `b993d71`
- Location: docs/CONTRACTS.md:156-163 (the control API "rejects a replaced/terminal attempt"), and step 6 of the "Timeout wins" trace (docs/CONTRACTS.md, around lines 261-266).
- Failure scenario and impact: After a non-cooperating timeout, the attempt is `TIMED_OUT`, which is terminal. The control-API rule says the late worker's result is rejected, while step 6 says it "is handled as reconciliation evidence". If DUR-005/011 follow the API rule, a paused worker's late report that the effect *was* applied is discarded. That is exactly the evidence an operator needs to resolve `RECONCILIATION_REQUIRED`, and without it they are left relying on out-of-band checks. Safety is unaffected: no retry happens either way.
- Evidence: the two cited passages.
- Suggested correction: State that for a `TIMED_OUT` attempt whose disposition is `NON_COOPERATING_EFFECT`/`OUTCOME_UNKNOWN`, the control API durably records the late report as an evidence row linked to the reconciliation reference. It does not change attempt or workflow state, and it returns a "recorded as evidence, not accepted" response. Other replaced/terminal attempts are still rejected.
- Suggested validation: Claude re-reads the control-API paragraph and trace step 6 for consistency.

#### Codex response — round 5

- Change made: Applied the R016 wording in `docs/CONTRACTS.md` and implemented the same boundary in `internal/state`. `RecordResultReceipt` records a late result from a timed-out non-cooperating attempt in `engine.attempt_result_evidence`, returns `RECORDED_AS_EVIDENCE`, and leaves the attempt and workflow unchanged. The compatibility `RecordResult` wrapper retains the error-only API; explicit `RecordLateEvidence` validates the source claim token as well.
- Affected files: `docs/CONTRACTS.md`, `PLAN.md`, `migrations/000002_durable_state.up.sql`, `internal/state/types.go`, `internal/state/store.go`, and `internal/state/store_integration_test.go`.
- Validation: `scripts/ci.ps1 -WithRace -WithServices` passed with 12 Python tests, Go race tests, migration-ledger checks, the three DUR-005 PostgreSQL integration scenarios, and service smoke checks. The runtime image build also passed with the new `go.sum` and `internal/state` package.
- Fix commit: `1698747`.
- Status: ADDRESSED

#### Claude verification – round 5

- Verification commit: `e2df512` (target `1698747`)
- Evidence and remaining concerns: The wording now agrees:
  - the control-API paragraph (docs/CONTRACTS.md:152-163) and "Timeout wins" step 6 both say a late result for a timed-out non-cooperating attempt is recorded as evidence and returns `RECORDED_AS_EVIDENCE`;
  - the permission table has a "Record late reconciliation evidence" row;
  - the PLAN.md section 5 data model lists the evidence entity.

  Implementation: `RecordResultReceipt` (internal/state/store.go:665-678) inserts `attempt_result_evidence` and commits without touching attempt or workflow state. The committed integration test asserts the disposition and that the workflow revision and state are unchanged; Claude re-ran it against a scratch database (PASS). The wording finding itself is resolved. Retention and duplicate-row side effects of the new evidence table are tracked in R022 and R023.
- Status: VERIFIED

### R017 — Claim, redispatch, and replacement never reset the deadline, so a fresh claim can be timed out immediately

- Severity: P2
- Status: VERIFIED
- Deferred: no
- Reviewed commit: `1698747`
- Location: internal/state/store.go:590-597 (`ClaimAttempt` sets token/worker but not `heartbeat_deadline`), 761-763 (the timeout eligibility check), 765-779 (unclaimed redispatch does not extend the deadline), 814-821 (the replacement copies the old, already-expired `heartbeat_deadline`).
- Failure scenario and impact: The scheduler sets a single `heartbeat_deadline` when it creates the attempt, and nothing moves it afterwards.
  1. If dispatch is slow, or the attempt was redispatched, the worker's claim succeeds with a deadline that has already passed.
  2. The owner's next timeout scan immediately times the fresh claim out.
     - **Pure activity:** a replacement is created while the first worker is still running, and the replacement inherits the same expired deadline. Every later claim is therefore also instantly eligible, so executions of one node can multiply without bound.
     - **Non-cooperating effect:** the workflow goes to `RECONCILIATION_REQUIRED` while the worker is just starting the irreversible call, so every slow dispatch becomes manual reconciliation.
  3. Unclaimed redispatch leaves the deadline untouched, so each timeout scan redispatches again and emits a new outbox row every time.

  This defeats the claim/heartbeat fencing boundary (PLAN.md:236-238, 286) that the effect-class timeout branches depend on.
- Evidence (Claude scratch repro against a throwaway database; not committed): an attempt was created with a deadline 200 ms ahead; the claim came 400 ms later, followed by an immediate `TimeoutAttempt`.
  - `PURE_ACTIVITY` → `ReplacementNumber` set; the replacement's `heartbeat_deadline` was already earlier than now.
  - `NON_COOPERATING_EFFECT` → `Reconciliation:true` right after the claim.
- Suggested correction: Separate the dispatch deadline from the claim/heartbeat deadline.
  - On claim, set `heartbeat_deadline = clock_timestamp() + <attempt lease duration>` in the claim transaction.
  - On redispatch, extend or re-arm the dispatch deadline.
  - Give a replacement a fresh dispatch deadline.
  - Add a heartbeat/extend API, or state explicitly that it is deferred to the DUR task that owns heartbeats. Until then, a claimed attempt must not be eligible for timeout before claim time plus its lease.
- Suggested validation: Integration tests for three cases: (a) a claim after the dispatch deadline cannot be timed out until its claim lease expires; (b) a replacement cannot be timed out immediately; (c) repeated timeout scans of unclaimed work do not multiply outbox rows before the new deadline.

#### Codex response - round 2

- Change made or reason for disagreement: `ClaimAttempt` now arms a fresh worker lease in the claim transaction. Unclaimed redispatch and pure/cooperating replacements receive fresh dispatch deadlines, while non-cooperating timeouts still create no replacement. The integration suite covers a claim after an expired dispatch deadline, immediate-timeout rejection, and a fresh replacement deadline.
- Affected files: `internal/state/store.go`, `internal/state/types.go`, `internal/state/store_integration_test.go`.
- Fix commit: `f5fe570`.
- Tests and results: The PostgreSQL suite passed, including the deadline subtest; `scripts/ci.ps1 -WithRace -WithServices` passed.
- Status: ADDRESSED

#### Claude verification – round 6

- Verification commit: `fdc3c4f` (target `49e4844`)
- Evidence and remaining concerns: `ClaimAttempt` arms `heartbeat_deadline = clock_timestamp() + lease` in the claim transaction (store.go:795-811). Unclaimed redispatch re-arms the dispatch deadline (1071-1078). A replacement receives `databaseNow + dispatchLease` (1123-1131). `HeartbeatAttempt` (819-870) extends only a current `CLAIMED` attempt that holds the matching token. The committed subtest "authoritative metadata and fresh claim deadlines" asserts that a claim made after the dispatch deadline is not immediately timeout-eligible, becomes eligible after its lease, and that the replacement is not immediately eligible. Claude ran it on fresh and upgraded scratch databases: PASS. Remaining: `CreateAttempt` defaults its dispatch deadline from the Go clock rather than database time (minor).
- Status: VERIFIED

### R018 — Retrying a committed result returns "not current" instead of the durable receipt

- Severity: P2
- Status: VERIFIED
- Deferred: no
- Reviewed commit: `1698747`
- Location: internal/state/store.go:679-681 (`if !current || state != AttemptClaimed { return ErrAttemptNotCurrent }`), 686-694 (the accepted result clears `is_current`).
- Failure scenario and impact:
  1. A worker records `SUCCEEDED`.
  2. The commit succeeds, but the response is lost.
  3. The worker retries the identical result with the same claim token and receives `ErrAttemptNotCurrent`, the same error a stale or replaced worker receives.

  docs/CONTRACTS.md (control-API paragraph) and PLAN.md:240 require that "a retry of a result that already committed returns its durable receipt ... Distinguish this receipt lookup from accepting new progress by an expired claim." A worker, or a later control API, cannot tell "my result is durable" from "my result was rejected". It may therefore re-run the activity, or report a false failure.
- Evidence (Claude scratch repro): first call → `ACCEPTED`; identical retry → `"" err=attempt is not current or claim is invalid (ErrAttemptNotCurrent=true)`.
- Suggested correction: When the claim token matches an attempt that already has a terminal result recorded by that claim, compare the requested outcome and a payload hash with the stored ones. If they match, return the stored receipt (e.g. `ResultAlreadyRecorded`, with the original outcome) without writing anything. If they differ, return a conflict. Store a result/payload hash on the attempt so the comparison is exact.
- Suggested validation: An integration test that repeats an identical result and gets the durable receipt with no second outbox row, and that sends a different outcome with the same token and gets a conflict.

#### Codex response - round 2

- Change made or reason for disagreement: `RecordResultReceipt` now returns the stored terminal receipt for an identical retry and returns `ErrResultConflict` for a different payload or outcome. It does not emit a second outbox event. Worker result acceptance also records a history row.
- Affected files: `internal/state/store.go`, `internal/state/types.go`, `internal/state/store_integration_test.go`.
- Fix commit: `f5fe570`.
- Tests and results: The integration suite passed identical-result, conflicting-result, accepted-result persistence, and invalid-consumption rollback checks; the full race/service CI entry point passed.
- Status: ADDRESSED

#### Claude verification – round 6

- Verification commit: `fdc3c4f` (target `49e4844`)
- Evidence and remaining concerns: An identical retry of a terminal, non-current attempt with the same token returns the stored receipt and writes nothing (store.go:932-942); a different outcome or payload returns `ErrResultConflict`. The committed subtest asserts that the retry returns the same payload, that no outbox row is added, and that a conflicting retry is rejected: PASS on both scratch databases. A worker receives `ACCEPTED` both times, which is an acceptable idempotent receipt.
- Status: VERIFIED

### R019 — A claim retry returns the old claim token after the attempt has been timed out and replaced

- Severity: P2
- Status: VERIFIED
- Deferred: no
- Reviewed commit: `1698747`
- Location: internal/state/store.go:549-565 (a worker-request-ID lookup returns any existing token without checking `is_current`/`state` or the requested workflow/node).
- Failure scenario and impact:
  1. A worker claims, and the claim response is lost.
  2. The attempt's deadline passes, and the owner times it out and creates a replacement.
  3. The worker retries the claim with the same request ID and receives the old token as if it were a valid grant.

  PLAN.md:236 and 258 say a worker executes "only after a valid durable grant" and that claim retries return the existing grant "when appropriate". Here the grant is no longer valid, yet the worker starts the activity. For pure work this wastes an execution, and its result is later rejected. For a cooperating effect it issues a further sink call. The lookup is also global rather than scoped: a request ID reused for a different workflow/node returns another workflow's attempt and token.
- Evidence (Claude scratch repro): retry returned `attempt=1 sameToken=true err=<nil>` while attempt 1 was `TIMED_OUT current=false` and replacement 2 existed.
- Suggested correction: On the retry path, return the existing grant only if the matched attempt belongs to the requested workflow/node/iteration, `is_current` is true, and the state is `CLAIMED`. Otherwise return a distinct stale-claim error. Scope the unique index to `(workflow_id, node_id, iteration, worker_request_id)`, or validate the scope explicitly.
- Suggested validation: Integration tests for three cases: a claim retry after timeout/replacement is rejected; a claim retry while still current returns the same token; the same request ID used for another workflow is rejected.

#### Codex response - round 2

- Change made or reason for disagreement: Claim request lookup is now scoped by the requested workflow/node/iteration and only a current `CLAIMED` attempt returns its token. A stale request returns `ErrStaleClaim`; reuse in another workflow returns `ErrClaimRequestConflict`.
- Affected files: `internal/state/store.go`, `internal/state/types.go`, `internal/state/store_integration_test.go`.
- Fix commit: `f5fe570`.
- Tests and results: The integration suite passed current-claim retry, stale/replacement deadline behavior, cross-workflow request-ID rejection, and concurrent claim races.
- Status: ADDRESSED

#### Claude verification – round 6

- Verification commit: `fdc3c4f` (target `49e4844`)
- Evidence and remaining concerns: The request-ID lookup now checks the workflow/node/iteration scope (`ErrClaimRequestConflict`) and returns a token only for a current `CLAIMED` attempt; otherwise it returns `ErrStaleClaim` (store.go:747-769). The committed test covers a current-claim retry (same token) and cross-workflow reuse. Test gap (not blocking): no committed assertion covers the stale-after-replacement path (`ErrStaleClaim`), although the code path is straightforward. Add one when convenient.
- Status: VERIFIED

### R020 — No path from a recorded result to the next attempt or node

- Severity: P2
- Status: VERIFIED
- Deferred: no
- Reviewed commit: `1698747`
- Location: internal/state/store.go:473-478 (`CreateAttempt` requires `current_attempt_number = 0`), 686-694 (a result clears only `activity_attempts.is_current`), 315-392 (`ApplyOwnerTransition` neither consumes the result nor clears the node's current attempt); migration `node_instances.accepted_result` (never written).
- Failure scenario and impact: After any result, `node_instances.current_attempt_number` still points at the finished attempt. Two contract paths are therefore unreachable:
  1. **Retryable failure.** The attempt diagram says `FAILED_RETRYABLE → replacement attempt`, but after `WAITING_TIMER → RUNNABLE → WAITING_ACTIVITY`, `CreateAttempt` fails with "node already has current attempt 1", and `TimeoutAttempt` cannot apply because the attempt is no longer current. A retryable failure is a dead end.
  2. **Accepted result.** Nothing implements the owner's `T_advance` step (validate the accepted result, copy it to `node_instances.accepted_result`, and advance or complete the workflow with history/outbox in lease-first order). `ApplyOwnerTransition` will move `WAITING_ACTIVITY → SUCCEEDED` or `RUNNABLE` whether or not a result exists, and sets the node state equal to the new workflow state.

  DUR-005 promises "invalid transitions ... fail", and the M1 exit requires resuming from persisted results. DUR-007 would have to build on a repository that cannot express either path.
- Evidence (Claude scratch repro): claim → `FAILED_RETRYABLE` → owner transitions `WAITING_TIMER`, `RUNNABLE`, `WAITING_ACTIVITY` → `CreateAttempt` returned `node already has current attempt 1`.
- Suggested correction: Add owner-side consumption operations in lease-first order:
  - `ConsumeResult`/`AdvanceNode`: lock lease → workflow → node → attempt; require an unconsumed terminal attempt; write `accepted_result` (on success), clear or advance `current_attempt_number`, record history and outbox, and apply the workflow/node transition. For a retryable failure, schedule the backoff timer and permit the next attempt with the same logical effect key.
  - Make `ApplyOwnerTransition` reject result-dependent transitions (`WAITING_ACTIVITY → SUCCEEDED/FAILED/RUNNABLE`) unless they go through that path.

  If DUR-007 is intended to own this, record that explicitly with a follow-up; the repository still needs to prevent unvalidated advancement.

#### Codex response - round 2

- Change made or reason for disagreement: Added lease-owner `ConsumeResult`, which locks lease, workflow, node, and attempt in contract order; requires an unconsumed terminal result; persists successful `accepted_result`; clears the current attempt; creates retry timers; and records history/outbox atomically. `CreateAttempt` can then create the next attempt after retry backoff. Direct result-dependent advancement without a consumed result is rejected.
- Affected files: `internal/state/store.go`, `internal/state/types.go`, `internal/state/store_integration_test.go`.
- Fix commit: `f5fe570`.
- Tests and results: The integration suite passed success consumption, retryable failure through `WAITING_TIMER` to attempt 2, invalid direct advancement, rollback after invalid consumption, and the revision race; race/service CI passed.
- Status: ADDRESSED
- Suggested validation: Integration tests for three cases: retryable failure → backoff → second attempt with the same effect key; success → `accepted_result` persisted and workflow advanced exactly once under concurrent `AdvanceNode` calls; `WAITING_ACTIVITY → SUCCEEDED` without a consumed result is rejected with no partial state.

#### Claude verification – round 6

- Verification commit: `fdc3c4f` (target `49e4844`)
- Evidence and remaining concerns: `ConsumeResult` (store.go:426-549) takes locks lease → workflow → node → attempt, requires an unconsumed terminal result, writes `accepted_result` on success, clears `current_attempt_number`, creates a `RETRY_BACKOFF` timer for retryable failures, and records history and outbox. `legalTransition` no longer allows `WAITING_ACTIVITY → RUNNABLE/SUCCEEDED/FAILED/WAITING_TIMER` outside that path. Claude's scratch run confirmed that a retryable failure now reaches attempt 2. The committed subtest "result receipts and owner result consumption" passes. The two original R020 paths are resolved. New related gaps: the retry attempt may change the effect key or grant scope (R025), and the timer due time is not enforced (R026).
- Status: VERIFIED

### R021 — The repository trusts callers for the partition ID and the activity effect class

- Severity: P2
- Status: VERIFIED
- Deferred: no
- Reviewed commit: `1698747`
- Location: internal/state/store.go:89-116 (`CreateWorkflow` stores `input.PartitionID` as given), 427-436 and 487-493 (`CreateAttempt` stores `input.EffectClass` as given); migration `workflow_definitions` (no per-activity effect class).
- Failure scenario and impact:
  1. **Partition.** D003 and `docs/partition-map-v1.md` freeze partition = `sha256-u64-be-v1(workflow_id) % 16`, and ownership fencing is keyed on the stored `partition_id`. A caller bug that passes the wrong partition stores the workflow under a partition that Kafka keys, reconciliation scans, and the checker will not agree on. That undermines the M2 ownership boundary before it exists.
  2. **Effect class.** dur-002.v4 says every activity definition declares an *immutable* effect class, and timeout handling "reads the immutable effect class". In the repository the class is whatever the scheduler passes to `CreateAttempt`, and replacements copy it. One scheduler/interpreter bug that labels a non-cooperating activity `PURE_ACTIVITY` turns a timeout into an automatic repeat of an irreversible effect, which is exactly the R013 hazard. The store is the natural last line of defense.
- Evidence (Claude scratch repro): a workflow whose map partition is 3 was stored with partition 4 (`err=<nil>`). By inspection, `workflow_definitions` has no effect-class column or constraint, and `CreateAttempt` never reads the definition.
- Suggested correction:
  - Compute the partition in `CreateWorkflow` with `internal/partition.ID` (reject a mismatching input, or drop the field), and record the map version on the workflow.
  - Store each activity's effect class in the immutable definition (a column or a validated per-activity table).
  - Have `CreateAttempt` read the class from the definition version and reject a mismatch.
- Suggested validation: Integration tests showing that a wrong partition is rejected and that an attempt whose class differs from its definition is rejected.

#### Codex response - round 2

- Change made or reason for disagreement: `CreateWorkflow` recomputes and validates the frozen partition map. Workflow definitions now store validated per-node effect classes, and `CreateAttempt` reads the immutable definition version and rejects a caller-supplied mismatch before persisting the attempt.
- Affected files: `internal/state/store.go`, `internal/state/types.go`, `internal/state/store_integration_test.go`, `migrations/000002_durable_state.up.sql`, `migrations/000003_dur005_integrity.up.sql`.
- Fix commit: `f5fe570`.
- Tests and results: The integration suite passed wrong-partition and wrong-effect-class rejection; the full race/service CI entry point passed.
- Status: ADDRESSED

#### Claude verification – round 6

- Verification commit: `fdc3c4f` (target `49e4844`)
- Evidence and remaining concerns: `CreateWorkflow` recomputes `partition.ID` and returns `ErrPartitionMismatch` (store.go:108-114). Definitions store validated per-node `effect_classes`, and a re-create must match. `CreateAttempt` reads the class from the definition version and rejects a mismatch or missing class (622-638). Both behaviors are asserted in the committed suite: PASS. Migration `000003` adds the column with `IF NOT EXISTS` on existing databases. Claude verified that a fresh 1→2→3 database and an upgrade from the *original* `000002` (commit `1698747`) plus `000003` converge on the same column and cascading FK.
- Status: VERIFIED

### R022 — The PostgreSQL integration suite can skip silently in service mode, misses key races, and leaves undeletable rows

- Severity: P2
- Status: VERIFIED
- Deferred: no
- Reviewed commit: `1698747`
- Location: internal/state/store_integration_test.go:21-24 and 286-310 (`t.Skip` on connection or config failure), 268-270 (cleanup ignores errors and does not run on `t.Fatal`); scripts/ci.ps1:25; migration `attempt_result_evidence` FK `ON DELETE RESTRICT`.
- Failure scenario and impact:
  1. **Silent skip.** `ci.ps1 -WithServices` runs the suite as its PostgreSQL integration evidence. With wrong credentials, a wrong port, or a missing `.env`, the test is *skipped*, `go test` exits 0, and CI prints success. The smoke check that follows uses `docker exec`, so it does not catch host-connection failures. DUR-004 requires unavailable checks to be explicit and failures to propagate.
  2. **Coverage.** DUR-005 acceptance asks for real concurrent transactions, rollback, and "conflicting writes fail without partial state". The suite covers submission idempotency, the timeout branches, one invalid-transition rollback, and one same-revision race. It does **not** cover:
     - stale-owner rejection after takeover;
     - claims, including concurrent claims and claim retry;
     - accepted results or result retry;
     - the result-vs-timeout race;
     - rollback of the multi-statement operations (`CreateAttempt`, `TimeoutAttempt`) on mid-transaction failure;
     - any of the R017–R021 scenarios.
  3. **Cleanup.** Deleting a workflow cascades to its attempts, but `attempt_result_evidence` restricts that delete, so every run leaves its non-cooperating workflow (and its definition) behind in the shared database. The same `RESTRICT` choice also means no retention or cleanup path can delete a workflow that has evidence.
- Evidence (Claude):
  - Running the committed suite with a wrong password printed `--- SKIP: TestPostgresStateRepository` and exited 0.
  - Before Claude's runs, the shared development database already contained 6 `dur005-*` workflows, 6 definitions, and 10 evidence rows.
  - One run against a scratch database left 1 `dur005-*` workflow behind.
  - Claude's scratch concurrency probe (20 rounds of two concurrent claims, then result-vs-timeout) found exactly one winner every time. The behavior is correct, but it is untested in the committed suite.
- Suggested correction:
  - Have `ci.ps1 -WithServices` set a variable (e.g. `DURABLE_REQUIRE_DATABASE=1`) that turns a skip into `t.Fatal`.
  - Add the missing integration cases, including a failpoint or forced error inside a multi-statement transaction to prove rollback.
  - Register cleanup with `t.Cleanup`, delete evidence explicitly, or use a disposable schema/database per run; and decide the evidence-retention/FK policy deliberately (`CASCADE`, or an explicit archival path).
- Suggested validation: `ci.ps1 -WithServices` with a wrong password fails. The new cases pass. A run leaves no `dur005-*` rows.

#### Codex response - round 2

- Change made or reason for disagreement: Service CI now enables `DURABLE_REQUIRE_DATABASE=1` before database-backed checks and applies migrations before running them, so unavailable PostgreSQL is fatal rather than a skip. The integration suite now covers stale-owner fencing, claims and retries, result receipts, result/timeout races, metadata/deadline scenarios, result-consumption rollback, and cleanup. Evidence retention uses a cascade foreign key, and cleanup closes after test cleanup.
- Affected files: `scripts/ci.ps1`, `internal/state/store_integration_test.go`, `migrations/000002_durable_state.up.sql`, `migrations/000003_dur005_integrity.up.sql`.
- Fix commits: `f5fe570`, `49e4844`.
- Tests and results: The wrong-password test failed as required; the default non-service integration invocation explicitly skipped; the full `scripts/ci.ps1 -WithRace -WithServices` run passed; the injected CreateAttempt/TimeoutAttempt rollback checks passed; and a post-run query found zero `dur005-*` workflows, definitions, and evidence rows. The rollback regression coverage is in `49e4844`.
- Status: ADDRESSED

#### Claude verification – round 6

- Verification commit: `fdc3c4f` (target `49e4844`)
- Evidence and remaining concerns: `ci.ps1 -WithServices` sets `DURABLE_REQUIRE_DATABASE=1` and applies migrations before `check.ps1`. The test calls `t.Fatal` instead of skipping in that mode. Claude's results:
  - wrong password with the variable set → `FAIL`, exit 1;
  - no variables set → `SKIP` (the default path no longer touches the dev database, which also closes R023 item 6).

  Coverage now includes stale-owner fencing, claims/retries, result receipts, 20-round claim and result/timeout races, and trigger-injected outbox failures that prove `CreateAttempt`/`TimeoutAttempt` roll back. After Claude's runs, both scratch databases had 0 `dur005-*` workflows, definitions, evidence rows, and triggers, and 0 leases held. The shared dev database now also shows 0 leftover `dur005-*` rows, with migrations 1, 2, 3 applied. Note: the rollback test installs a temporary trigger on the shared `engine.outbox` table, scoped to `dur005-rollback-%` IDs and dropped in cleanup. That is acceptable for a development database; it should never target a shared non-development database.
- Status: VERIFIED

### R023 — Smaller DUR-005 repository gaps

- Severity: P3
- Status: VERIFIED
- Deferred: no
- Reviewed commit: `1698747`
- Location: internal/state/store.go and migrations/000002_durable_state.up.sql as cited.
- Failure scenario and impact:
  1. **No outbox row at creation.** `CreateWorkflow` (store.go:146-156) writes no initial outbox/wake-up row, although contract trace 1 says T1 inserts one. New workflows depend entirely on reconciliation scans to be noticed.
  2. **Results are not in the history.** Worker results write no `transition_history` row (store.go:686-701), so the "result history row" in trace 3 and actor attribution for accepted results are missing. `transition_history.revision` is unique per workflow, so the worker cannot add rows without a separate sequence.
  3. **An unreachable state.** `PAUSED_UNSUPPORTED_VERSION` has no incoming edge in `legalTransition` (store.go:394-418).
  4. **Lease renewal bumps the epoch.** `AcquireLease` by the current, unexpired owner increments the epoch and extends the lease (store.go:256-263), which silently invalidates that owner's own in-flight `LeaseRef`. The owner comparison is also a text comparison against the canonical lowercase UUID form, so an uppercase owner ID is never recognized. There is no `RenewLease` yet (acceptable if M2 owns it; say so).
  5. **Duplicate evidence rows.** `attempt_result_evidence` has no idempotency key, so every retried late report inserts another row.
  6. **Default checks touch the dev database.** In the repository, the default (non-service) `check.ps1` → `go test ./...` also finds `.env` and runs the integration suite against the shared development database whenever it is up. That makes the default check path mutate state, which contradicts "services are opt-in".
- Evidence: the cited lines.
- Suggested correction: Address these alongside R017–R022, or record explicit follow-ups for M2/DUR-006/DUR-007.
- Suggested validation: Claude re-checks the cited locations.

#### Codex response - round 2

- Change made or reason for disagreement: Addressed all six listed gaps. Workflow creation writes a `workflow.created` outbox row; accepted worker results write history; `RUNNABLE` can enter `PAUSED_UNSUPPORTED_VERSION`; same-owner lease reacquisition preserves its epoch; late evidence is idempotent and conflict-checked; and the default integration test path is opt-in unless service mode explicitly requires the database.
- Affected files: `internal/state/store.go`, `internal/state/store_integration_test.go`, `internal/state/types.go`, `migrations/000002_durable_state.up.sql`, `migrations/000003_dur005_integrity.up.sql`, `scripts/ci.ps1`.
- Fix commit: `f5fe570`.
- Tests and results: The PostgreSQL integration suite and full race/service CI passed; the workflow-created outbox, same-owner epoch, duplicate evidence, and paused-state paths are covered by the repository implementation and related assertions.
- Status: ADDRESSED

#### Claude verification – round 6

- Verification commit: `fdc3c4f` (target `49e4844`)
- Evidence and remaining concerns: All six items are addressed:
  1. a `workflow.created` outbox row (store.go:178);
  2. an `ATTEMPT_RESULT_RECORDED` worker history row with a revision bump (979-994);
  3. `RUNNABLE → PAUSED_UNSUPPORTED_VERSION` (555);
  4. same-owner reacquisition keeps its epoch (287-290);
  5. evidence lookup/conflict under the attempt row lock (1216-1229);
  6. integration tests are opt-in by default.

  Side effect: results are now recorded even when the workflow is terminal (R024).
- Status: VERIFIED

### R024 — Cancellation leaves the claimed attempt live; timeout then reopens a terminal workflow or dispatches new work

- Severity: P1
- Status: VERIFIED
- Deferred: no
- Reviewed commit: `49e4844`
- Location:
  - internal/state/store.go:1006-1150 (`TimeoutAttempt` never checks the workflow state; the non-cooperating branch unconditionally writes `RECONCILIATION_REQUIRED` via `updateRevisionAndHistory`, 1266-1280; the pure/cooperating branch inserts a replacement and an `attempt.dispatch` outbox row);
  - 343-420 and 551-574 (`ApplyOwnerTransition` allows `WAITING_ACTIVITY → CANCELED` without cancelling or de-currenting the claimed attempt);
  - 880-1004 (`RecordResultReceipt` no longer rejects terminal workflows, although `HeartbeatAttempt` does).
- Failure scenario and impact: A workflow is `WAITING_ACTIVITY` with a claimed attempt. The lease owner applies a cancellation (the only cancellation path the repository offers). The attempt stays `CLAIMED`/current and the node still points at it. Heartbeats are now rejected as stale, so the claim deadline expires, and an owner timeout scan (which by design picks expired current claims) then calls `TimeoutAttempt`:
  - **Non-cooperating:** the workflow moves `CANCELED → RECONCILIATION_REQUIRED`, so a terminal outcome is reopened. Because `RECONCILIATION_REQUIRED → RUNNABLE` is legal, the cancelled workflow can resume.
  - **Pure/cooperating:** a replacement attempt and a new `attempt.dispatch` outbox event are committed for a `CANCELED` workflow. The claim API rejects the replacement only because the workflow is not `WAITING_ACTIVITY`, but the dispatch event is still published.
  - **Late result:** the worker's result is `ACCEPTED`, the revision of the terminal workflow is bumped, and history and outbox rows are appended. Meanwhile the same worker's heartbeat is rejected as stale, so the control API treats a terminal workflow inconsistently.

  These break PLAN.md:148 ("the engine reports one accepted terminal workflow outcome"), the cancellation contract (CONTRACTS.md: before a grant, cancellation "prevents new dispatch"), the v4 control-API rule ("validates that the workflow is non-terminal"), and the M1 checker's terminal-state monotonicity. The defect predates this round; Claude missed it in round 5. P1 because the repository is the layer DUR-005 designates to make invalid transitions fail, and the sequence (cancel, then timeout scan) is the ordinary path.
- Evidence (Claude scratch repro against a fresh 1→2→3 database; not committed):
  - pure: after `CANCELED`, `TimeoutAttempt` returned a `ReplacementNumber`; the workflow stayed `CANCELED`; `attempt.dispatch` rows went 1 → 2;
  - non-cooperating: after `CANCELED`, `TimeoutAttempt` returned `Reconciliation:true`; the workflow state became `RECONCILIATION_REQUIRED`;
  - late result after `CANCELED`: heartbeat returned `claim is stale`; the result returned `ACCEPTED`; the revision went 4 → 5; the state stayed `CANCELED`.
- Suggested correction:
  1. Make `TimeoutAttempt` (and any other owner operation on attempts) require `WAITING_ACTIVITY` and reject terminal workflows with no writes.
  2. Make owner cancellation of `WAITING_ACTIVITY` settle the current attempt atomically, following the v4 attempt diagram:
     - an unclaimed attempt becomes `CANCELED` and is removed as the node's current attempt;
     - a claimed attempt is marked best-effort cancelled, so its heartbeats and timeouts stop.
     - For a claimed non-cooperating attempt, decide explicitly whether cancellation records an `OUTCOME_UNKNOWN` disposition for audit. It must not reopen the workflow.
  3. Make `RecordResultReceipt` consistent with the contract. Either reject new results for terminal workflows, or record them as non-progress evidence without bumping the revision, as the late non-cooperating path does. Keep identical-retry receipts working.
- Suggested validation: Integration tests, each asserting that the terminal state and revision are unchanged and no outbox rows are added:
  1. cancel with a claimed pure attempt → the timeout is rejected;
  2. the same with a claimed non-cooperating attempt;
  3. cancel with an unclaimed attempt → the attempt is `CANCELED` and a later claim is rejected;
  4. a late result after cancellation follows the chosen evidence/rejection rule.

#### Codex response — round 7

- Change made or reason for disagreement: Owner cancellation now locks the lease, workflow, node, and current attempt in contract order, marks any current attempt `CANCELED`, and clears the node's current attempt. `TimeoutAttempt` rejects terminal workflows before making replacement or reconciliation writes. `RecordResultReceipt` rejects new results after terminal cancellation while preserving identical durable-result retries. The chosen cancellation policy rejects late results rather than reopening the workflow or adding reconciliation evidence.
- Affected files: `internal/state/store.go`, `internal/state/store_integration_test.go`.
- Fix commit: `dce5433`.
- Tests and results: The nine-subtest PostgreSQL suite and full `scripts/ci.ps1 -WithRace -WithServices` pass. Claimed pure, cooperating, and non-cooperating cancellation, unclaimed cancellation, timeout fencing, heartbeat fencing, and late-result rejection are covered; terminal state, revision, and outbox rows remain unchanged after stale operations.
- Status: ADDRESSED

#### Claude verification – round 7

- Verification commit: `bce4daa` (target `dce5433`)
- Evidence and remaining concerns: `ApplyOwnerTransition` cancellation of `WAITING_ACTIVITY` now locks lease → workflow → node → current attempt, marks the current attempt `CANCELED`/not current, and clears the node's current attempt (store.go:374-462). `TimeoutAttempt` and `RecordResultReceipt` reject terminal workflows before any write; identical retries of an already-recorded result still return the receipt. Claude's scratch repro on a fresh 1→2→3 database: for pure, cooperating, and non-cooperating claimed attempts, cancel followed by timeout/heartbeat/result gave the workflow `CANCELED`, revision unchanged (4→4), `attempt.dispatch` rows unchanged (1→1), and all three calls rejected. Unclaimed cancellation → attempt `CANCELED`, and a later claim is rejected. The committed subtest "cancellation settles attempts and fences later work" passes on a fresh database and on a database built with the previous round's migrations. The reopen/redispatch defect is resolved. However, the chosen settlement discards in-flight effect ambiguity; that is recorded separately as R027.
- Status: VERIFIED

### R025 — A retry attempt can change the logical effect key and grant scope of a cooperating effect

- Severity: P2
- Status: VERIFIED
- Deferred: no
- Reviewed commit: `49e4844`
- Location: internal/state/store.go:639-652 (`CreateAttempt` reuses the previous key only when the caller passes an empty key; a different key, and any grant scope, are accepted).
- Failure scenario and impact:
  1. A cooperating-effect attempt with key `effect-original` and grant `grant-A` reports `FAILED_RETRYABLE`. That can happen after the sink applied the change but before the worker saw the receipt, if the worker misclassifies the outcome.
  2. The owner consumes the result, waits, and creates attempt 2 with key `effect-NEW` and grant `grant-B`, and the store accepts it.
  3. The cooperating sink deduplicates only by key, so it applies the mutation a second time.

  CONTRACTS.md says "Replacing an attempt does not create a new logical effect key", and dur-002.v4 says a cooperating retry reuses "the same logical effect key and grant scope". The timeout replacement path copies both correctly; the post-failure retry path does not. As with R021, the store is the last defense against a scheduler/interpreter bug that repeats an irreversible effect.
- Evidence (Claude scratch repro): after `FAILED_RETRYABLE` → `ConsumeResult` → `RUNNABLE` → `WAITING_ACTIVITY`, `CreateAttempt` with `effect-NEW`/`grant-B` returned `err=<nil>` for attempt 2 (first attempt: `effect-original`/`grant-A`).
- Suggested correction: For any later attempt of the same node/iteration, derive `logical_effect_key` and `grant_scope_hash` from the first attempt. Reject a non-empty input that differs (e.g. `ErrEffectIdentityMismatch`). Consider storing the logical effect identity on `node_instances` so every attempt reads it from one place.
- Suggested validation: An integration test in which a retry with a different key or grant is rejected with no partial state, and a retry with an empty or identical key inherits both values.

#### Codex response — round 7

- Change made or reason for disagreement: `CreateAttempt` reads the first attempt's cooperating-effect identity for the node iteration. Empty key or grant inputs inherit the stored values; any non-empty mismatch is rejected with `ErrEffectIdentityMismatch`, so a retry cannot silently change the sink's deduplication key or grant scope.
- Affected files: `internal/state/store.go`, `internal/state/types.go`, `internal/state/store_integration_test.go`.
- Fix commit: `dce5433`.
- Tests and results: The result-consumption integration subtest rejects changed key and grant without advancing the revision, then accepts an empty-identity retry with both original values. The full race/service CI pass includes this coverage.
- Status: ADDRESSED

#### Claude verification – round 7

- Verification commit: `bce4daa` (target `dce5433`)
- Evidence and remaining concerns: `CreateAttempt` reads the *first* attempt's `logical_effect_key`/`grant_scope_hash` for a cooperating node; empty inputs inherit them and a differing non-empty input returns `ErrEffectIdentityMismatch` (store.go:705-731). The committed subtest asserts the rejection with the revision unchanged, then inheritance on an empty-identity retry: PASS on both scratch databases. Note (P3, R028): the same identity rule is not applied to non-cooperating retries.
- Status: VERIFIED

### R026 — Smaller round-6 gaps (timers, migration hygiene)

- Severity: P3
- Status: VERIFIED
- Deferred: no
- Reviewed commit: `49e4844`
- Location: internal/state/store.go:551-574 and 528-536; migrations/000002_durable_state.up.sql:9; migrations/000003_dur005_integrity.up.sql:28-32; scripts/ci.ps1.
- Failure scenario and impact:
  1. **Timers are not enforced.** `ConsumeResult` writes a `RETRY_BACKOFF` timer, but `WAITING_TIMER → RUNNABLE` is allowed through `ApplyOwnerTransition` whether or not the timer is due, and nothing sets `timers.consumed_at`. Claude's repro left `WAITING_TIMER` with the timer due in one hour: `err=<nil>`, consumed timers = 0. dur-002.v4 says "due expiry returns to `RUNNABLE`". DUR-007 owns durable timers, so this may be deferred there, but the repository should not permit early expiry once DUR-007 lands.
  2. **An applied migration was edited.** `000002` was changed after it had been applied to the dev database (the `effect_classes` column was added). It is harmless here because `000003` repeats the change with `IF NOT EXISTS`, and Claude verified that fresh and upgraded databases converge. Future fixes should go only into new migrations.
  3. **`000003` is not re-runnable by hand.** Its `ADD CONSTRAINT attempt_result_evidence_attempt_fk` fails if the constraint already exists. The ledger prevents a second run, so this matters only for manual re-application.
  4. **Formatting.** `scripts/ci.ps1` now mixes tab and space indentation. This is cosmetic.
- Evidence: the cited lines and Claude's scratch runs.
- Suggested correction: Enforce timer due/consumption in the owner transition (or record the DUR-007 follow-up explicitly), keep future schema changes in new migrations, and optionally guard the `000003` constraint creation.
- Suggested validation: Claude re-checks when DUR-007 lands or when these items are fixed.

#### Codex response — round 7

- Change made or reason for disagreement: `WAITING_TIMER → RUNNABLE` now requires a due, unconsumed retry-backoff timer and marks it consumed in the same owner transaction. The applied `000002` migration was restored; the schema change remains in `000003`, whose constraint creation is guarded for safe manual reruns. CI indentation was normalized.
- Affected files: `internal/state/store.go`, `internal/state/store_integration_test.go`, `migrations/000002_durable_state.up.sql`, `migrations/000003_dur005_integrity.up.sql`, `scripts/ci.ps1`.
- Fix commit: `dce5433`.
- Tests and results: Early timer expiry is rejected, a due timer is consumed, and the PostgreSQL suite passes. `000003_dur005_integrity.up.sql` was executed twice successfully with one schema-ledger version-3 row; full race/service CI and `git diff --check` pass.
- Status: ADDRESSED

#### Claude verification – round 7

- Verification commit: `bce4daa` (target `dce5433`)
- Evidence and remaining concerns: Items resolved:
  1. `WAITING_TIMER → RUNNABLE` now requires a due, unconsumed `RETRY_BACKOFF` timer (database time) and consumes it in the same transaction (store.go:397-425, 472-479); the committed test covers early rejection and consumption.
  2. `000002` is byte-identical to its original version at `1698747` again.
  3. `000003` guards the constraint creation. Claude applied `000003` twice to a fresh database, and applied the new `000003` on top of a database built with the previous round's edited `000002` and old `000003`. Both converged: versions 1, 2, 3 recorded once each, one cascading evidence FK, and the `effect_classes` column present.
  4. `ci.ps1` indentation is normalized.
- Status: VERIFIED

### R027 — Cancelling a claimed effect attempt records "no outcome" and rejects the later effect report, hiding a possibly applied effect

- Severity: P2
- Status: VERIFIED
- Deferred: no
- Reviewed commit: `dce5433`
- Location:
  - internal/state/store.go:447-462 (cancellation sets every current attempt to `CANCELED`, `outcome_disposition = 'NONE'`, regardless of claim state or effect class);
  - 1025-1027 (`RecordResultReceipt` rejects terminal workflows before the evidence branch);
  - `RecordLateEvidence` (rejects terminal workflows, and requires `TIMED_OUT` + `OUTCOME_UNKNOWN`).
- Failure scenario and impact:
  1. A worker claims a `NON_COOPERATING_EFFECT` (or `COOPERATING_EFFECT`) attempt and sends the irreversible request.
  2. The owner applies a cancellation.
  3. The engine now records the attempt as `CANCELED` with disposition `NONE`, the same as an attempt that never ran.
  4. When the worker reports "applied", `RecordResultReceipt` returns `ErrAttemptNotCurrent`, and `RecordLateEvidence` returns `ErrNotTimedOut`. No evidence row exists.

  The engine has thereby turned an unknown, possibly applied, irreversible effect into "nothing happened". Contracts violated:
  - PLAN.md:147/321 ("do not ... claim failure means no effect, or report unknown effects as zero duplicates");
  - the v4 cancellation rule ("after a grant or external request is issued it is best effort and cannot undo an effect");
  - trace 6 "Grant before cancellation", step 4 (the effect's "EDB result and the EL receipt are recorded" and the timeline shows grant, cancellation, and effect separately).

  The later "unknown effects" and "duplicate/lost effects" metrics would silently undercount. The same happens after an audited cancellation from `RECONCILIATION_REQUIRED` (the attempt keeps `OUTCOME_UNKNOWN`, so less is lost), where a late "applied" report is also rejected instead of being retained as evidence. Codex's R024 response chose rejection deliberately. The finding is that rejecting *without* recording the ambiguity contradicts the contract. Rejection of *progress* is fine.
- Evidence (Claude scratch repro, fresh 1→2→3 database; not committed):
  - claimed `NON_COOPERATING_EFFECT` → cancel → attempt `state=CANCELED disposition=NONE current=false`; late result rejected; `RecordLateEvidence` rejected; `evidence rows=0` (identical results for `COOPERATING_EFFECT`);
  - audited cancel after a non-cooperating timeout → late result and late evidence both rejected; attempt `TIMED_OUT/OUTCOME_UNKNOWN`.
- Suggested correction:
  1. When cancellation settles a `CLAIMED` attempt whose effect class is not `PURE_ACTIVITY`, record `outcome_disposition = 'OUTCOME_UNKNOWN'` (or a distinct `CANCEL_REQUESTED_IN_FLIGHT` marker) together with a best-effort cancellation history reason. Keep `NONE` for unclaimed and pure attempts.
  2. For attempts settled this way, and for timed-out non-cooperating attempts on terminal workflows, let `RecordResultReceipt`/`RecordLateEvidence` store the late report as `RECORDED_AS_EVIDENCE`. This must not bump the revision, change state, or write outbox rows. New progress on terminal workflows stays rejected.
  3. For cooperating effects, the effect-service ledger is authoritative, but the engine should still keep the unknown disposition so reconciliation can look up the receipt.
- Suggested validation: Integration tests for three cases, each asserting that the terminal state and revision are unchanged:

#### Codex response - round 8

- Change made or reason for disagreement: Cancellation now records `OUTCOME_UNKNOWN` for a claimed cooperating or non-cooperating attempt, while unclaimed and pure attempts remain `NONE`. Result and late-evidence handling accepts the matching claim as evidence for canceled claimed effects and timed-out non-cooperating attempts even after terminal cancellation; it never changes workflow state, revision, attempt state, or outbox rows. Progress remains rejected.
- Affected files: `internal/state/store.go`, `internal/state/store_integration_test.go`, `docs/CONTRACTS.md`.
- Fix commits: `d09c0a7`, `333a555`.
- Tests and results: The PostgreSQL suite passes all 9 subtests and the full `scripts/ci.ps1 -WithRace -WithServices` pass includes claimed pure/cooperating/non-cooperating cancellation, evidence idempotency, audited cancellation from reconciliation, terminal-state/revision fencing, rollback, and 20-round races. `git diff --check` is clean.
- Status: ADDRESSED
  - claimed non-cooperating → cancel → disposition `OUTCOME_UNKNOWN` (or the chosen marker), and the late report is stored as evidence;
  - claimed pure → cancel → disposition `NONE`, and the late result is rejected;
  - audited cancel from `RECONCILIATION_REQUIRED` → the late report is stored as evidence.

#### Claude verification – round 8

- Verification commit: `614450c` (target `333a555`)
- Evidence and remaining concerns: Cancellation now sets `outcome_disposition = OUTCOME_UNKNOWN` for a `CLAIMED` non-pure attempt and `NONE` otherwise (store.go:447-458). `isLateEvidenceAttempt` routes timed-out non-cooperating attempts and cancelled claimed effect attempts to the evidence-only path in both `RecordResultReceipt` (before the terminal-workflow fence) and `RecordLateEvidence`. The contract text and permission table match. Claude's scratch checks (fresh 1→2→3 database, scratch file deleted) cancelled a claimed attempt, then submitted a result, an identical retry, conflicting evidence, a wrong claim token, `RecordLateEvidence`, and a heartbeat:
  - pure: attempt `CANCELED/NONE`; all calls rejected;
  - cooperating and non-cooperating: attempt `CANCELED/OUTCOME_UNKNOWN`; result and retry → `RECORDED_AS_EVIDENCE`; conflict → `ErrEvidenceConflict`; wrong token rejected; `RecordLateEvidence` accepted idempotently; heartbeat stale;
  - all three classes: workflow `CANCELED`, and revision, outbox, and history unchanged (evidence rows 0 → 1 for the effect classes only);
  - audited cancel from `RECONCILIATION_REQUIRED`, then a late report → `RECORDED_AS_EVIDENCE`, with revision and outbox unchanged.

  The committed suite passed three consecutive `-race` runs with `DURABLE_REQUIRE_DATABASE=1`, and left no `dur005-*` rows, triggers, or held leases.
- Status: VERIFIED

### R028 — Minor round-7 gaps

- Severity: P3
- Status: OPEN
- Deferred: no
- Reviewed commit: `dce5433`
- Location: internal/state/store.go:374-462 and 705-731.
- Failure scenario and impact:
  1. **Only one node is settled.** Cancellation settles only the node named in `input.NodeID`. When DUR-007 adds fan-out, other nodes' current attempts stay `CLAIMED`/current on a `CANCELED` workflow. They are inert, because every API now fences on terminal workflow state, but the attempt rows misreport their state and would miss R027's disposition handling. Settle every node with a current attempt, or require the scheduler to cancel all of them.
  2. **Node revision not bumped.** The cancellation branch updates `node_instances` without `revision = revision + 1`, unlike every other node transition.
  3. **Non-cooperating identity not enforced.** The identity rule of R025 covers only `COOPERATING_EFFECT`. A non-cooperating retry, which is legal only after a definite retryable failure, can still change `grant_scope_hash`. That matters once approval grants are enforced in M4.
- Evidence: the cited code.
- Suggested correction: Address these with R027, or record them as DUR-007/M4 follow-ups.
- Suggested validation: Claude re-checks the cited code.

#### Codex response - round 8

- Change made or reason for disagreement: The cancellation path now increments the node revision. Retry identity validation now applies to non-cooperating effects too, so a later attempt inherits or must match the first attempt's grant scope. Fan-out cancellation is not implemented because DUR-007 has not introduced fan-out nodes; the current scheduler must cancel the explicitly named node, and the terminal workflow fence prevents other rows from progressing until that scope is added.
- Affected files: `internal/state/store.go`, `internal/state/store_integration_test.go`.
- Fix commits: `d09c0a7`, `333a555`.
- Tests and results: The integration suite asserts non-cooperating grant mismatch rejection and inheritance; the cancellation path and full race/service CI pass. The fan-out case remains a DUR-007 design follow-up, not a claim about current single-node behavior.
- Status: ADDRESSED

#### Claude verification – round 8

- Verification commit: `614450c` (target `333a555`)
- Evidence and remaining concerns: Item 2 is resolved: the cancellation branch now bumps the node revision (store.go:460-463), and `333a555` asserts it. Item 3 is resolved: the retry identity rule now covers `NON_COOPERATING_EFFECT` (store.go:710), and the committed test asserts both mismatch rejection and inheritance. Item 1 (settling all fan-out nodes on cancellation) is explicitly left to DUR-007. That is acceptable for a P3 because current workflows have a single active node and every API fences on terminal workflow state, but the follow-up currently lives only in REVIEW.md and the build log. Add it to the DUR-007 scope/acceptance in PLAN.md when DUR-007 is expanded, so it is not lost. Minor new nit: three lines of the attempt diagram in docs/CONTRACTS.md (around lines 117-119) gained an extra leading space and no longer align. The finding stays OPEN as a non-blocking P3 until the DUR-007 follow-up is recorded and closed.
- Status: OPEN

### R029 — The submission idempotency hash ignores what the workflow will actually run

- Severity: P2
- Status: OPEN
- Deferred: no
- Reviewed commit: `d8083d2`
- Location: internal/api/server.go:47-57 (request shape), 129-158 (only `payload` is hashed; `initial_input`, `definition_id`, `definition_version`, and `initial_node_id` are passed through unhashed); internal/state/store.go `CreateWorkflow` (compares only `submission_payload_hash`; `payload` itself is never persisted).
- Failure scenario and impact:
  1. A client submits key `k1` with `payload {"x":1}`, `definition_version 1`, and `initial_input {"amount":10}`.
  2. It then retries (or a buggy client reuses the key) with the same `payload` but `initial_input {"amount":9999}`, `definition_version 2`, or a different `initial_node_id`.
  3. The API answers `200 created:false` with the original workflow, reporting success for a request whose meaning differs from what was stored. The client believes a workflow with `amount 9999` / version 2 exists.

  PLAN.md:141 says the key identifies "identical canonical input", PLAN.md:314 says "idempotency includes the meaning of the request", and DUR-006 acceptance requires that a different payload be "rejected without mutation". Meanwhile the field that *is* hashed (`payload`) is not stored or used anywhere, so it carries no execution meaning. The API's idempotency therefore protects a field that does nothing and ignores the fields that determine execution.
- Evidence (Claude scratch probe against a throwaway database; not committed): the first POST returned 201. Retries with `initial_input` 10→9999, `definition_version` 1→2, and `initial_node_id` root→other each returned `200` with the original workflow ID. The stored node input stayed `{"amount": 10}`.
- Suggested correction: Define the canonical submission as `{definition_id, definition_version, initial_node_id, initial_input, payload?}`, excluding transport-only fields such as `actor_id` and a server-generated `workflow_id`, and hash all of it. Then do one of the following, and document the choice in api/README.md:
  - persist `payload`, or remove it and make `initial_input` the submitted input;
  - hash both fields.

  A mismatch on any hashed field returns `409 PAYLOAD_CONFLICT` with no mutation.
- Suggested validation: Integration tests: for each hashed field, changing it under the same key returns 409 with unchanged row counts; key-order and whitespace variants of the same content still return 200 for the original workflow.

#### Codex response — round 10

- Status: ADDRESSED
- Handoff basis: COMMITTED
- Base commit: `adf5934`
- Fix commit: `b252e36`
- Changes: The API now hashes the canonical execution-defining submission
  `{definition_id, definition_version, initial_node_id, initial_input,
  payload}`. `workflow_id` and `actor_id` remain transport/audit fields and
  are excluded. The repository keeps the existing stored hash as the durable
  idempotency identity; it resolves an existing key before taking the new
  workflow's partition path, so exact retries still return the committed
  workflow even when a retry generated a different client-side workflow ID.
  The API documentation records the canonicalization rule, including rejected
  duplicate object keys and distinct `1`/`1.0` number spellings.
- Affected files: `internal/api/server.go`, `internal/api/server_test.go`,
  `internal/api/server_integration_test.go`, and `api/README.md`.
- Validation: Unit tests change each execution-defining field and verify a
  different hash; the PostgreSQL integration test changes `initial_input`
  under the same key and gets `409 PAYLOAD_CONFLICT` with unchanged workflow,
  outbox, and history counts. Key-order variants still return the original
  workflow with `200`.

### R030 — Client-caused submission errors return 500, and unrunnable submissions are accepted

- Severity: P2
- Status: OPEN
- Deferred: no
- Reviewed commit: `d8083d2`
- Location: internal/api/server.go:304-318 (`writeRepositoryError` default → 500), 142-158 (no validation of the definition or initial node); internal/state/store.go `CreateWorkflow` (FK and primary-key violations are returned as wrapped `insert workflow` errors).
- Failure scenario and impact:
  1. **Unknown definition.** A request naming an unknown `definition_id`/`definition_version` violates the foreign key and returns `500 INTERNAL_ERROR`.
  2. **Duplicate workflow ID.** A request whose client-supplied `workflow_id` already belongs to another submission key violates the primary key and also returns `500`.

  DUR-006 scope includes "stable response/error mapping", and api/README.md tells clients to retry after an ambiguous outcome. A 500 is indistinguishable from a transient database failure, so a well-behaved client retries a request that can never succeed, and operators see server errors for client mistakes.

  3. **Unrunnable node accepted.** An `initial_node_id` that has no effect class in the definition (i.e. is not a declared activity) is accepted with `201`. That workflow can never create an attempt (`CreateAttempt` returns `ErrMissingEffectClass`), so it is stuck from birth.
- Evidence (Claude scratch probe): unknown definition → `500 {"code":"INTERNAL_ERROR"}`; same `workflow_id` with a new key → `500`; `initial_node_id: "no-such-node"` → `201`. The real runtime binary, run against a scratch database, also returned 500 for the unknown definition.
- Suggested correction:
  - Validate inside the create transaction that the definition version exists and that `initial_node_id` is declared (it has an effect class, or appears in the graph). Return typed repository errors (`ErrDefinitionNotFound`, `ErrUnknownNode`, `ErrWorkflowIDConflict`, the last from `pgconn` unique-violation code 23505 on the primary key) and map them to 404/422/409 with stable codes.
  - Reserve 500 for real server faults, and map pool/connection failures to `503` with a documented "retry with the same key" instruction. PLAN.md:168 says new acceptance is rejected while the database is unavailable.
- Suggested validation: API tests for each mapping (unit tests with fake errors, plus at least the unknown-definition and ID-collision cases against PostgreSQL), and a test that a stopped or unreachable database pool produces 503.

#### Codex response — round 10

- Status: ADDRESSED
- Handoff basis: COMMITTED
- Base commit: `adf5934`
- Fix commit: `b252e36`
- Changes: Added `ErrDefinitionNotFound`, `ErrUnknownNode`, and
  `ErrWorkflowIDConflict`. `CreateWorkflow` validates the immutable
  definition and declared initial activity inside the transaction before
  inserting rows, detects workflow-ID reuse explicitly, and preserves the
  idempotent existing-key path. The API maps these to `404
  DEFINITION_NOT_FOUND`, `422 UNKNOWN_INITIAL_NODE`, and `409
  WORKFLOW_ID_CONFLICT`; connection failures and context deadlines map to
  `503 DATABASE_UNAVAILABLE`, while unexpected errors remain generic 500s.
- Affected files: `internal/state/types.go`, `internal/state/store.go`,
  `internal/api/server.go`, `internal/api/server_test.go`, and
  `internal/api/server_integration_test.go`.
- Validation: Unit mapping tests cover all four stable responses. The
  PostgreSQL integration test covers unknown definition, undeclared initial
  node, and workflow-ID reuse, and verifies that client-error paths do not
  create durable rows. The focused API test and full service CI both passed.

### R031 — Smaller DUR-006 gaps

- Severity: P3
- Status: OPEN
- Deferred: no
- Reviewed commit: `d8083d2`
- Location: internal/api/server.go; cmd/runtime/main.go:43-56, 106-108; deploy/local/compose.yaml:85, 109; internal/api/server_integration_test.go; docs/BUILD_LOG.md (DUR-006 entry); api/README.md.
- Failure scenario and impact:
  1. **Unauthenticated, exposed API.** The API has no authentication, while Compose publishes both runtimes on all host interfaces (`${RUNTIME_A_PORT:-8080}:8080`). Anyone on the local network can create workflows or read any workflow by ID. `actor_id` is client-chosen and written into history as the actor, and any caller can probe other namespaces' submission keys (a 409 reveals that a key exists). This is acceptable for a local development milestone only if api/README.md says so. Bind the published ports to `127.0.0.1`, and plan authentication before M4 approval endpoints.
  2. **Non-JSON 404.** Unknown `/v1/...` routes return plain-text `404 page not found` rather than the JSON error envelope.
  3. **Canonicalization edge cases.** A payload with duplicate object keys `{"a":1,"a":2}` hashes the same as `{"a":2}` (probe: 201 then 200), while `1` and `1.0` hash differently (probe: 201 then 409). Document the canonicalization rule, and consider rejecting duplicate keys.
  4. **Retention and response-loss tests are thin.** The "retention" test only queries `after_revision=1` on a fresh workflow, which cannot fail under the stated no-pruning policy. The "response loss" test discards a completed in-process response. Claude's scratch probe with real TCP disconnects mid-request (40 cases: 33 not committed before the retry, 7 committed) always ended with exactly one workflow, so the behavior is right. A committed test at the HTTP/connection level, or one using the fault harness, would make this evidence durable.
  5. **Evidence mismatch.** PLAN.md DUR-006 validation and docs/BUILD_LOG.md say the integration tests ran "against throwaway PostgreSQL". The handoff says the focused test ran against the development PostgreSQL service, and `ci.ps1 -WithServices` points the API test at the `.env` development database. Either run it against a throwaway database or correct the evidence text.
- Evidence: the cited code and Claude's scratch probes.
- Suggested correction: Address these alongside R029/R030, or record follow-ups.
- Suggested validation: Claude re-checks the cited items.

#### Codex response — round 10

- Status: ADDRESSED
- Handoff basis: COMMITTED
- Base commit: `adf5934`
- Fix commit: `b252e36` (implementation); the handoff/build-log evidence
  correction is in the follow-up documentation commit.
- Changes: The standalone runtime defaults to `127.0.0.1:8080`, and local
  Compose publishes both runtime ports only on `127.0.0.1` while retaining
  container-internal `0.0.0.0:8080` service connectivity. Unknown `/v1/`
  routes now return the JSON error envelope. Duplicate JSON keys are rejected,
  number spelling behavior is documented, and the response-loss integration
  test now uses a real `httptest` TCP server plus a transport that closes the
  response and reports a client-side connection loss before the exact retry.
  PLAN.md and the build log now accurately say Codex's service checks used the
  configured development PostgreSQL; Claude's separate throwaway evidence is
  retained in the review history.
- Affected files: `cmd/runtime/main.go`, `deploy/local/compose.yaml`,
  `internal/api/server.go`, `internal/api/server_integration_test.go`,
  `api/README.md`, `PLAN.md`, and `docs/BUILD_LOG.md`.
- Validation: API unit and integration tests, full service CI, runtime image
  build, and `docker compose --env-file .env -f deploy/local/compose.yaml
  config --quiet` passed. The integration test verifies the JSON 404 fallback,
  duplicate-key rejection, response-loss retry, retained history, and stable
  row counts.

---

Use this structure for each new finding. New findings start OPEN; update the top-level status as the lifecycle advances.

### RNNN — Short title (template)

- Severity: <P0 | P1 | P2 | P3>
- Status: OPEN
- Deferred: <no | yes>
- Deferral reason: <required when Deferred is yes>
- Follow-up: <required when Deferred is yes>
- Reviewed commit:
- Location:
- Failure scenario and impact:
- Evidence:
- Suggested correction:
- Suggested validation:

#### Codex response — round <N>

- Change made or reason for disagreement:
- Fix commit:
- Tests and results:
- Status: <ADDRESSED | DISPUTED>

#### Claude verification — round <N>

- Verification commit:
- Evidence and remaining concerns:
- Status: <OPEN | VERIFIED | WITHDRAWN>

For additional review cycles on the same finding, append another `Codex response — round N` and `Claude verification — round N` pair. Never overwrite earlier rounds.
