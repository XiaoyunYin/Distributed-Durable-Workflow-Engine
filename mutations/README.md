# DUR-046 mutation gate

The manifest contains selected safety mutations, one per contract guard. It
currently has seventeen behavioral mutations plus one configuration tripwire.
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
