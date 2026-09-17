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

## Codex handoff

- Task: DUR-005 — Schema and state repository
- Task status: READY_FOR_REVIEW
- Handoff basis: COMMITTED
- Base commit: `79ba118`
- Target commit: `1698747`
- Scope and implementation summary: Added the numbered DUR-005 PostgreSQL migration and Go repository for immutable workflow definitions, idempotent workflow creation, partition leases, owner-fenced transitions, node/attempt lifecycle, worker claims/results, effect-class-aware timeout branches, durable outbox/history, and late non-cooperating-result evidence. R016 is addressed in the v4 contract and schema/repository path. CI service mode now applies numbered migrations and runs the PostgreSQL repository integration suite before the existing smoke checks.
- Checks run and results:
  - `scripts/ci.ps1 -WithRace -WithServices`: PASS with task-local Go/UV caches and an explicit pytest basetemp; Go formatting/vet/tests/build, Ruff, strict mypy, 12 Python tests, Go race tests, migration skip checks, three DUR-005 PostgreSQL integration subtests, and service smoke checks passed.
  - `docker build -f deploy/local/Dockerfile.runtime .`: PASS; the image copied `go.sum` and `internal/` and built the runtime.
  - `git diff --check`: PASS before handoff documentation changes.
- Skipped checks and reasons: No clean bootstrap or restart-smoke run was performed because those workflows recreate the user’s running containers. No remote CI exists. Full workflow-engine, scheduler, Kafka-relay, hard-kill durability, and paid/model behavior remain outside DUR-005.
- Known limitations: This task implements the durable state repository and its PostgreSQL transaction boundaries, not the complete engine. The service evidence uses the existing single-node local topology. The integration suite uses generated `dur005-*` identities and does not claim production retention, failover, or exactly-once behavior.

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
- Status: ADDRESSED
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
