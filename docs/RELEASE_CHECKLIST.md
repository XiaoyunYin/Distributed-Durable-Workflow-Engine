# DUR-032 final reproduction and release checklist

Status: DONE at reviewed target `c455ffd`, base `effdc39`.
Claude's round-49 verdict is `NO_BLOCKING_FINDINGS`; R091/R093 are VERIFIED.
R089/R090 are VERIFIED; R092 remains OPEN P3 because the user-requested
role-play does not establish personal user fluency. No release or tag is
authorized without explicit user approval.
This supersedes the earlier `2ea3726` checklist: that test-only Kafka correction
was insufficient. The deployed scheduler/worker path and production consumer
startup policy are now in scope.

Base: `c0757e4` (DUR-031 closeout). Executable validation target: `78fa7f9`,
including wiring commit `6d276af`. The latest REVIEW.md handoff identifies the
documentation-inclusive review target. No paid call, live-model run, new
measurement campaign, external remediation, or protected-scope change was made.

## Claims and attribution

The README and interview pack lead with three bounded comparative findings:

1. DUR-026: with four fixed worker processes, two schedulers kept up with the
   tested 2/s offered rate where one did not, on the engine path.
2. DUR-035: direct notification was faster than Kafka at the resolved terminal
   stage in the quoted campaign; dispatch-stage separation varied across runs.
3. DUR-028: at one SHA-256 work unit per chunk, every-chunk checkpointing took
   7.5552 times the boundary-only median in the in-process panic workload.
   This is not a general checkpoint policy or crossover estimate.

The M5 48/48 campaign is separate bounded validation, not a comparative
headline. DUR-034 resolved no safeguard-cost effect. RQ8 rates retain their
counts: defended 2/20 versus plain 6/20 changes above each profile's clean-clean
baseline (a four-case difference). R089/R090 are VERIFIED in round 48.
The walkthroughs were performed by Codex at the user's request; they are not
evidence of the user's personal fluency. DUR-031 remains IN_PROGRESS for the
user's three exercises, recorded as pending in the interview pack (R092).

## What the deployed path does

With `RUNTIME_SCHEDULER=enabled`, both runtime replicas scan the configured
namespace (`local-runtime` by default), acquire their own partition lease, and
run the interpreter. Activities are dispatched through the outbox/relay/Kafka
path to Python workers. A worker records the durable delivery/inbox disposition
before committing its offset, then claims the exact delivered attempt and
reports its result. Event consumption and fallback scans wake the scheduler.

The local activity allowlist is `pure.echo/v1` and `pure.add/v1`; unknown
versions pause rather than silently executing. The deployed demo does not
instantiate its own Engine. Effects/approvals are not automatically exposed as
Python activities; their production-path evidence remains DUR-033A's separate
reviewed integration campaign. APIs remain unauthenticated and localhost-only.

Production `NewKafkaSource` now uses FirstOffset for a group without a committed
offset. Existing group offsets still take precedence. The integration test
uses this constructor and publishes before the new consumer group starts;
there is no alternate test-only FirstOffset constructor.

## Reproduce

From this repository with Docker Desktop and the documented toolchain:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/final-reproduce.ps1 -Commit 78fa7f9
```

The script verifies target ancestry, creates a detached clean worktree, selects
a unique Compose project and unused ports, generates local credentials, and
requires no pre-existing project containers or volumes. It then runs:

- bootstrap with services and all migrations;
- the real deployed demo;
- `ci.ps1 -WithServices -WithRace`;
- PostgreSQL/Kafka restart smoke, a second deployed demo, and final smoke;
- an assertion that tracked source is still clean.

Only the generated test project's containers, network and volumes are removed.
`-KeepOnFailure` preserves a failed project for diagnosis. Ignored worktrees
remain for audit. Dependency/download and Docker build caches may be reused;
this is fresh source and service state, not a cacheless machine installation.
Do not run volume cleanup against the ordinary development project.

For a demo against the ordinary local stack, use `scripts/local-demo.ps1`
after rebuilding that stack explicitly. The validation below did not update
the user's original running containers.

## Observed validation

At `78fa7f9`, project `dur032-check-19e869b3` passed every script assertion:

- Fresh detached checkout; both images built; 14 migrations applied to fresh
  volumes; initial topology smoke passed.
- Both deployed demos reached SUCCEEDED with result 6, two Python claims,
  two task inbox rows, eight event inbox rows, and valid independent checker.
- Go formatting, vet, build, serial normal/race suites; Ruff; mypy (27 source
  files); all 45 Python tests; M1/M3/M4/DUR-033A focused integration suites;
  real Kafka round trip using the production constructor; service smoke.
- Restart smoke asserted the PostgreSQL marker count and retained Kafka topic
  across container recreation. Post-restart demo and final smoke passed.
- Tracked source remained clean. Temporary project containers/volumes were
  removed; the ordinary development stack was untouched.
- Separately, the current checker passed all 48 historical fault traces
  offline against committed durable snapshots. No campaign was rerun.

The first isolated run at `6d276af` passed the deployed demo and Go checks,
then reported 42 Python passes and three setup errors: the configured pytest
temporary directory's parent did not exist. `78fa7f9` creates it; the complete
fresh rerun above passed all 45. The first test project's resources were also
removed after diagnosis.

The captured outer PowerShell `2>&1 | Tee-Object` command returned 1 even though
the child completed with `passed=True`. A minimal stderr-writing child with
explicit exit 0 reproduced the wrapper's false pipeline status. The observed
checks and cleanup are recorded here; the outer command is not claimed to
have exited 0. Local log: `bin/dur032-selfcheck/reproduce-78fa7f9.log` (ignored).
Use the direct script invocation above to avoid conflating native stderr with
a failed PowerShell pipeline.

## Release gates and limits

Round-48 correction validation: the R091 fencing test passed five focused
race runs and the clean-export `go test -race -p 1 ./... -count=1` suite,
vet and build, using a freshly migrated throwaway database. Removing the
guard in an isolated build overlay failed with the expected stale-owner
error mismatch and persisted wakeup/reconciliation mutation; the takeover-only
case failed independently too. Production code and migrations are unchanged.
The private database was empty of workflows/definitions/wakeups/source
documents after tests and was dropped. This correction did not rerun Python,
real Kafka, image builds, restart smoke, offline traces or studies; the
earlier reproduction and round-48 reviewer checks remain their evidence.

- [x] Fresh source and fresh service-volume bootstrap with frozen dependencies.
- [x] Deployed API/Kafka/Python execution before and after dependency restart.
- [x] Service-mode serial Go race/integration suites and 45 Python tests.
- [x] Migration, smoke and dependency container-recreation persistence checks.
- [x] Historical 48-trace offline checker validation.
- [x] R089 counts, R090 headline structure, and delegated attribution corrected.
- [x] R091's mutation-sensitive lease-fence regression is verified by Claude.
- [ ] Claude accepts the user-requested role-play as satisfying R092; personal
      user fluency is otherwise still outstanding.
- [x] Claude's round-50 review verifies R057, R083 and R088 at `0002e75`.
- [x] Claude reviews the final implementation and claims at target `c455ffd`.
- [x] DUR-032 moves to DONE after the committed non-blocking review.
- [x] User authorized and Codex created the local annotated tag `v0.1.0` at
      the final release commit; no remote publication was performed.

R057, R083 and R088 were verified in round 50; R092 remains the open
user-attribution item under DUR-031. R093 is
VERIFIED; the README and report now say M7 figures
predate the deployed wiring and were not rerun on it. The R019 claim-retry
gap now has committed coverage in `internal/api/runtime_integration_test.go`;
Claude has not verified this new test yet.

The authorized `v0.1.0` tag remains at `7e4137d`, before the residual
correction implementation/evidence commit `0002e75` and its follow-up docs.
Those corrections are intentionally post-tag and are intended for the next
tag; the two snapshots must not be conflated.

This single-node Docker Desktop/WSL2 result does not establish multi-host
durability, production scale/authentication, sustained-load performance,
hard-kill/host-failure durability, remote CI, or arbitrary external-effect
exactly-once execution. Database-backed package tests remain serial because
their partition-lease fixtures share a database. No paid/model study was
rerun; the reviewed report's limits and evidence remain unchanged.
