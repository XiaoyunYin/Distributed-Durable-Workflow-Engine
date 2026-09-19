# DUR-032 final reproduction and release checklist

Status: READY_FOR_REVIEW. This checklist is the final M8 validation record. It
separates documentation/claim positioning from executable environment checks
and never treats an unavailable check as passed.

Base commit: c0757e4, the DUR-031 closeout. Final implementation target:
2ea3726. No paid call, live-model run,
external action, protected guarantee change, or experiment rerun is part of
DUR-032.

## Claim positioning required by R090

The final README leads with comparative findings rather than the correctness
campaign:

1. DUR-026: with four fixed worker processes, two schedulers kept up with the
   tested 2/s offered rate where one scheduler did not on the engine path.
2. DUR-035: in the quoted campaign, direct notification was faster than Kafka
   at the resolved terminal stage; ready-to-claim direct versus Kafka was
   unresolved and varied across campaigns.
3. DUR-028: at one SHA-256 work unit per chunk, every-chunk checkpointing took
   7.5552 times the boundary-only median in the recorded in-process panic
   workload; this is not a general checkpoint policy or crossover estimate.

The M5 48/48 campaign is reported separately as bounded validation, not as a
comparative headline finding. DUR-034's safeguard-cost comparison is also
explicitly absent because its clean reruns did not resolve a cost effect.
When the RQ8 adversarial result is reused, its counts travel with its rates:
defended 2/20 above its clean-clean baseline versus plain 6/20.

## Clean-checkout demonstration

Run from a clean disposable checkout or worktree at the final implementation
target. These commands deliberately use a task-specific Go cache and do not
reuse the reviewed worktree's ignored environment files:

    git worktree add .scratch/dur032-clean <final-target>
    Set-Location .scratch/dur032-clean
    $env:GOCACHE = '<task-specific cache>'
    pwsh -NoProfile -File scripts/bootstrap.ps1
    if ((git status --porcelain) -ne '') { throw 'clean checkout is dirty after bootstrap' }
    pwsh -NoProfile -File scripts/check.ps1

Bootstrap without -StartServices installs the frozen Python environment and
checks the Compose configuration without recreating shared services. The
non-service check runs formatting, vet, Go tests/build, Ruff, mypy, and Python
tests; database-backed tests skip unless service mode is explicitly enabled.

For service-mode validation, from a checkout with the local .env and running
services:

    pwsh -NoProfile -File scripts/ci.ps1 -WithServices -WithRace

This serializes database-backed package tests, applies migrations, runs the
integration suites, restores relays, and runs the dependency smoke check. No
live-model or paid-provider work is included.

## Migration and lifecycle verification

The safe, non-recreating checks are:

    pwsh -NoProfile -File scripts/migrate.ps1
    pwsh -NoProfile -File scripts/smoke.ps1
    docker compose --env-file .env -f deploy/local/compose.yaml config --quiet

scripts/restart-smoke.ps1 is the lifecycle check that recreates only the
PostgreSQL and Kafka containers, verifies a database marker and Kafka topic
survive, and removes both markers. It must only be run when interrupting the
shared development services is acceptable:

    pwsh -NoProfile -File scripts/restart-smoke.ps1

If it is not run, record that as skipped with the reason; do not infer hard-kill
or volume durability from smoke.ps1.

## Validation record

- Clean checkout at a6b7083 bootstrapped successfully in a disposable
  worktree after selecting the installed CPython 3.12 interpreter. The first
  attempt was blocked by the host's uv Python-directory ACL and the sandbox's
  package-network restriction; the retry used a task-local uv cache and
  approved dependency download. The checkout remained clean after bootstrap.
- The clean checkout's non-service check passed after setting a writable
  pytest basetemp: Go tests/build, vet, formatting, Ruff, mypy, and 41 Python
  tests. The first Python pass had 38 passes and 3 setup errors because pytest
  scanned a permission-restricted global temp directory; no test failed. The
  explicit basetemp rerun passed all 41.
- The first service-mode CI run exposed a real fixture issue in
  TestM3KafkaTaskAndEventRoundTrip: a new Kafka group started at LastOffset
  after the test had published its records, so the event was missed. The
  production NewKafkaSource behavior was preserved; the integration test now
  uses a test-only FirstOffset constructor for its fresh group. The focused
  isolated test passed in 0.73 seconds, and the final full
  ci.ps1 -WithServices -WithRace run passed all serial Go checks, 41 Python
  tests, integration suites, and smoke checks.
- The final code change is test-only: production NewKafkaSource still starts a
  new group at LastOffset, while the M3 integration fixture constructs its
  fresh group at FirstOffset. This keeps production semantics unchanged and
  makes the test independent of whether publication happens before first group
  assignment.
- migrate.ps1 skipped every already-applied migration through 000014. The
  read-only smoke check passed before and after restart smoke. The restart
  check recreated only PostgreSQL/Kafka, retained its database marker and Kafka
  topic, removed both markers, and the full topology returned healthy.
- No live-model call, paid-provider call, external action, release, or tag was
  performed.

## Final evidence checklist

- [x] Clean disposable checkout bootstrapped with frozen dependencies.
- [x] Non-service checks pass from that checkout.
- [x] Service-mode race/integration checks pass, or accepted prior results are
      linked with a reason this documentation-only task did not rerun them.
- [x] Migrations are idempotent on the declared development database.
- [x] PostgreSQL/Kafka/runtime/worker/telemetry/Prometheus smoke passes.
- [x] Restart smoke passes, or is explicitly recorded as skipped.
- [x] R090 positioning is represented: three comparative findings first,
      48/48 correctness validation separately, safeguard-cost non-result
      explicit, and R089 counts carried with rates.
- [ ] Final Claude review covers implementation, claims, release limits, and
      the test-only Kafka fixture correction.
- [ ] Release/tag is created only after explicit authorization.

## Scope and limitations

This is a single-node Docker Desktop/WSL2 development environment. Passing
local checks does not establish multi-host durability, production scale,
authentication, hard-kill equivalence for every fault, remote CI, or arbitrary
external-effect exactly-once behavior. The final release remains bounded to the
reviewed commits and artifacts listed in docs/TECHNICAL_REPORT.md and
docs/INTERVIEW_EVIDENCE.md.
