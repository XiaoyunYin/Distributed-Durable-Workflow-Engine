# DUR-046 mutation gate

The manifest contains selected safety mutations, one per contract guard. It
includes behavioral mutations and configuration tripwires.
The gate exports the committed `HEAD` into a disposable directory, applies
exactly one mutation, and requires the check declared by that case to fail. A
compile error, skipped test, no-test result, or unrelated failure is not a
detection. Behavioral cases require a non-zero named-test result containing
their expected failure message; the configuration tripwire checks that the
Compose lock-timeout setting is present.

The database-backed cases require `DURABLE_MUTATION_DATABASE_URL` pointing to
a disposable PostgreSQL database with the numbered migrations applied. The
runner sets `DURABLE_REQUIRE_DATABASE=1`, so an unavailable database fails the
gate instead of becoming a skip. The source checkout is never modified by a
mutation. Results are written only when the caller requests an output path.

Examples:

```powershell
$env:DURABLE_MUTATION_DATABASE_URL = $env:DURABLE_DATABASE_URL
pwsh -File scripts/mutation-gate.ps1
```

```bash
DURABLE_MUTATION_DATABASE_URL="$DURABLE_DATABASE_URL" bash scripts/mutation-gate.sh
```

The mutation set is intentionally selective: it protects representative
ownership, effect authorization, transport, fault-checker, parser, and
redaction boundaries. It is not a proof that every line is mutation-covered.

## Checked-in result provenance

`results-powershell.json` and `results-bash.json` are the hosted outputs from
GitHub Actions run
[`35941481466`](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/35941481466),
artifact `dur046-mutation-results-35941481466`, on source commit
`4f98911b6e0bdf2bd196492da1d6b88d1a1e172c`. Each report contains all 18
manifest cases: 17 behavioral detections and one configuration tripwire.
These are hosted-run evidence snapshots, not a claim that the gates were
rerun in the current local environment.

## Current CI layout and timing

The Bash and PowerShell gates each run four shards, `with-k6-a`, `with-k6-b`,
`without-k6-a` and `without-k6-b`.
Each shard bootstraps its own Compose stack and mutation database, isolates
the runtime/worker relays, then drops the database, restores the relays,
uploads results and diagnostics, and tears down its stack. Each mutation
step retains its 75-minute limit within a 100-minute job.

The selector pins all IDs starting with `k6` to the `with-k6` pool. It sorts the
remaining cases by descending recorded cost and ascending case ID, placing
each on the shard with the lower estimated load. Ties go to `with-k6`.
The per-shell [weights](ci-case-costs.csv) use the larger of the case wall
times in runs [44973530768](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/44973530768)
and [44981105834](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/44981105834),
rounded up to milliseconds. A case without measurements uses the upper
median of known costs (one second if none are available). The selector reads
the manifest on every run, and preserves its order within each shard.

The selector then splits each pool into `-a` and `-b`, keeping each
command and environment group within a pool together for baseline reuse. The ordinary split columns
record the higher case wall time from runs
[44995155454](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/44995155454)
and [44998501161](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/44998501161).
The `with-k6` split columns use the same method on runs
[45020845008](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45020845008)
and [45025143230](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45025143230).
Measured groups retain their placement as unmeasured cases are added.
The Phase C connection-trace cases use `ci_shard` to run alongside the Phase C
connection-budget renderer in `with-k6-b` on both platforms. Pinned groups
leave the existing automatic split placements in place.

`Mutation gate coverage` requests all eight result artifacts for the current
run and attempt. It requires both matrices to pass and verifies each report
against the tested checkout. For each shell, every manifest ID must appear
exactly once across its four reports with a passing baseline and the expected
detection.

Result artifacts are `dur046-mutation-results-<platform>-<shard>-<run_id>-<attempt>`
and contain `results-powershell.json` or `results-bash.json`. Diagnostics
use `mutation-<platform>-diagnostics-<shard>-<run_id>-<attempt>`.
The service non-race suite and deployed smoke stay in
`PostgreSQL, Kafka, and deployed smoke`. `PostgreSQL race and K6 fixtures`
runs the serial race suite, the sink contract race check, and the K6 G1/G2
fixtures on a separate stack. Both jobs retain their 25-minute limit.

The standalone shell entry points default to the complete manifest.
New reports retain the v1 schema
and all existing fields, adding monotonic wall-clock seconds:

- Top-level `duration_seconds` covers the campaign through report assembly,
  excluding final scratch cleanup. `setup_duration_seconds` covers manifest,
  interpreter, environment/cache and source-export preparation;
  `baseline_duration_seconds` and `restored_baseline_duration_seconds` measure
  the initial and final Go baselines.
- Each case's `duration_seconds` includes drift checking, preparation, its
  named-test baseline, the mutation command and source restoration.
  `setup_duration_seconds` measures preparation through command/environment
  selection; `baseline_duration_seconds` measures the named-test baseline;
  `mutation_duration_seconds` measures the mutated command. Patch/restoration
  and result processing account for the remaining case time.

Historical checked-in reports above predate timing fields and remain unchanged.
Pilot `ci_proof` requires every main-push gate, including both mutation jobs,
to succeed, and the tooling inventory includes the workflow and runner sources.
