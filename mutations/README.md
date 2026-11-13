# DUR-046 mutation gate

The manifest contains selected safety mutations, one per contract guard. It
currently has 596 cases, including behavioral mutations and configuration tripwires.
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

`PowerShell mutation gate` (`mutation-powershell`) and `Linux mutation gate`
(`mutation-linux`) run in parallel on separate Ubuntu runners, each with its
own bootstrapped Compose stack and migration-complete mutation database.
Each gate stops deployed runtime/worker relays before testing, then drops its
database, restores the relays, uploads results and diagnostics, and tears down
the stack. Each mutation step has a 75-minute limit within a 100-minute job.
`PostgreSQL, Kafka, and deployed smoke` (`services`) retains the service/race
and DUR-055 retrieval checks with a 25-minute job limit. Its retained steps
totaled 596 and 773 seconds in runs 43303598471 and 43317291102, respectively;
25 minutes gives at least 1.94x measured headroom.

Results artifacts are `dur046-mutation-results-powershell-<run_id>` and
`dur046-mutation-results-linux-<run_id>`, containing `results-powershell.json`
and `results-bash.json`, respectively. Diagnostic artifacts are
`mutation-powershell-diagnostics-<run_id>` and
`mutation-linux-diagnostics-<run_id>`; services keeps
`service-diagnostics-<run_id>`.

Both shell entry points use `scripts/mutation-runner.py` and run the complete
manifest with the same detection criteria. New reports retain the v1 schema
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
