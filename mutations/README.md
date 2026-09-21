# DUR-046 mutation gate

The manifest contains selected safety mutations, one per contract guard. The
gate exports the committed `HEAD` into a disposable directory, applies exactly
one mutation, runs its named semantic test, and requires a non-zero result with
the expected failure message. A compile error, skipped test, no-test result,
or unrelated failure is not a detection.

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
