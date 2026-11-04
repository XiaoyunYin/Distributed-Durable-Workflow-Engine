# K1 local lifecycle evidence

DUR-053 K1 on base `cf10befa896a9843f56fba685d6f0991d22487a1`.
Uncommitted working tree, PROVISIONAL handoff; Claude review/commit/hosted CI
remain pending. This directory contains local code/test evidence, with no
Kubernetes deployment, K4-K8 campaign, capacity, SLO or HA claim.

- [Manifest and file inventory](manifest.json): exact commands, results,
  phase budgets, limitations, source hashes and reference provenance.
- [Final validation](validation.json): full Go race/vet, Ruff, mypy and pytest;
  PowerShell/Compose limitations are explicit. Python: 201 passed, 2 unrelated
  DUR-055 skips for the missing separate database URL.
- [Guard mutations](mutations.json): 58/58 detections, 40 new K1 lifecycle
  cases, passing named baselines and expected semantic failures. Both CI
  wrappers use the same manifest. Compile errors or skips are not detections.
- [Shutdown witness](shutdown-witness.log): committed competing acquisition,
  hook cleanup, frozen-reference legacy parity, DB-outage readiness/liveness,
  HTTP join, result acceptance/abort/replacement ordering and held startup hook.
- [Worker witness](worker-witness.log): disposition/commit/claim interleavings,
  response loss, commit failure, revocation, polling/executor ownership,
  monotonic deadlines and abort watchdog.
- [Real Kafka membership diagnostic](kafka-membership-witness.json): one
  direct barrier/stub comparison. The legacy peer remained unassigned in the
  4 s observation window; the new peer was assigned in 3.125 s while work was
  blocked. This is assignment-only and censored in legacy mode, not the
  protocol-v2 durable-sentinel assignment-stall campaign metric.
- [Prior attempts](prior-attempts.json): failed harness/launcher/decoder runs
  and earlier passing snapshots, retained without replacing final evidence.
- [Observed cleanup](cleanup.json): owned local scratch removal and retained
  inaccessible pre-existing pytest directory.

Default runtime order is readiness 503, endpoint-removal delay, scheduler
pass/release, relay batch and LISTEN join, current ingestion disposition, HTTP
shutdown completion, then store close. Individual deadlines bound failed
or uncooperative dependencies. Default worker drain stops delivery/claim
admission and leaves the group while bounded activities finish and continue
heartbeats/results. A separate monotonic watchdog exits at abort without a
synthetic failure/result or voluntary claim release (I13).

Durable inbox disposition still precedes Kafka commit, which precedes claim.
PostgreSQL leases, epochs, attempts and claim tokens retain authority. The
late-result test observes an expired current claim accepted before replacement,
then HTTP STALE_* only after attempt 2 commits. A failed DB release can leave
TTL recovery; bounded shutdown does not imply DB availability.

Campaign legacy flags retain the original runtime cancellation and cleanup
body and worker poll-thread/two-second-join behavior. Frozen pre-K1 source
fixtures match `cf10bef` bytes and are test references, never deployment images.
On Windows, stdin cancels the same NotifyContext path because POSIX SIGTERM
cannot be sent; hosted Linux service CI runs the real signal branch.
Acquisition timing in the test is an observer bound that includes process exit,
not K4's release-commit-to-acquisition-commit metric.

The temporary validation databases were migrated via native PostgreSQL access
and dropped after each run. Docker control API access is denied and pwsh 7
is absent, so no local Compose build/restart/smoke or PowerShell wrapper ran.
No new-commit hosted CI or Claude verification is represented as complete.


## R232 follow-up (2026-11-04)

Round 155 R232 is addressed in the uncommitted working tree at base `5ebcabc`.
Fatal startup and role failures now survive the bounded default drain and reach
main for logging and exit status 1; a clean drain still exits 0. The legacy
path, shutdown order and budgets, and lease/fencing behavior were not changed.

- [R232 validation](validation.json): final full Go race suite on a fresh
  18-migration database; Go vet/gofmt, Ruff, mypy, and pytest (200 passed, 3
  skipped). Temporary databases were dropped.
- [R232 mutations](mutations-r232.json): Bash gate PASS, 59/59 cases on a fresh
  18-migration mutation database, including the fatal-error propagation
  removed-guard mutation. `mutation-gate.ps1` was not run because pwsh 7 is
  unavailable; both wrappers consume the shared manifest.
- [R232 database cleanup](r232-database-cleanup.json): task databases dropped
  and absence verified.

This is a PROVISIONAL handoff. No commit, PowerShell run, hosted CI, or Claude
verification is claimed.

## Round 156 R232b follow-up (2026-11-04)

Claude found that Round 155's fatal-error path could still wait forever for a
role or store-close goroutine after its deadline. The fix bounds the final role
join to two seconds and returns immediately after the existing two-second store
close deadline. Normal shutdown order and the legacy path remain unchanged.

- The fresh-database Go race suite includes both new subprocess regressions.
  The focused scheduler-stall and store-close-stall processes returned fatal
  errors and exited nonzero in 13.3 s and 4.2 s, respectively.
- The shared manifest adds a removed-group-wait-bound mutation; the Bash gate
  detected all 60/60 cases (59 behavioral and one configuration tripwire),
  recorded in `mutations-r232.json`.
- Fresh test and mutation databases use 18 numbered migrations and are dropped
  with absence verified. Temporary files and caches are confined to
  `D:\coding agent files\durable-engine\codex\r232b` and removed before handoff.
- The work remains PROVISIONAL and uncommitted. Docker control API access is
  denied, so no local Compose or hosted checks are claimed.
