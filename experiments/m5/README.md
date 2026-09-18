# M5 engine correctness evidence

This directory contains the machine-readable results of the controller-driven
F01-F11 campaign and the local outage episodes. The campaign uses committed
PostgreSQL-backed tests that exercise the engine's durable state, transport,
worker, effect, checkpoint, lease, timer, join, and cancellation boundaries.
The mapping is explicit in `scripts/m5-campaign.ps1`; the script forces
`DURABLE_REQUIRE_DATABASE=1` and rejects any selected test that skips.

The campaign enumerates 16 atomic cases across F01-F11 and runs each with
seeds 11, 23, and 47: 48 records total, with a controller trace and an
independent checker verdict for every record. The Go fixture target creates a
real durable prefix, pauses at the named boundary, and is killed by the
controller. The checker loads the trace and joins its explicit durable
identity fields to the PostgreSQL rows before the package assertion runs.

Each trace also has a same-named JSON file under `durable/`. It is an
independent snapshot of the checker-owned durable rows captured before the
campaign cleanup, so the committed evidence remains verifiable after the
development database is cleaned. Recheck the archived set without PostgreSQL
with:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/m5-archive-check.ps1
```

The campaign uses a per-run nonce in every fixture workflow/definition and
passes the three declared seeds into the durable initial input; rerunning the
campaign does not reuse a prior fixture identity. It builds `m5-fixture` and
`fault-checker` once before starting boundary control, so the boundary timeout
does not include a cold `go run` compilation.

Run from the repository root after PostgreSQL/Kafka services are available:

```powershell
$env:DURABLE_REQUIRE_DATABASE = "1"
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/m5-campaign.ps1 -StopRuntimeRelays
```

The output records the case ID, ordering, seed, controller/checker/Go status,
package, test pattern, boundary, durable observation, trace path, and captured
output for every case/run. `-StopRuntimeRelays` temporarily stops
only runtime/worker relay services so shared outbox fixtures cannot be consumed
before the selected test, then restores them. F12 remains the agent-specific
M6 campaign.

`outage-recovery.json` is the separate DUR-025 artifact. It records dependency
health and unresolved-work snapshots independently before, during, and after
Kafka, PostgreSQL, worker, and runtime episodes. A PostgreSQL-down snapshot is
explicitly unavailable rather than being interpreted as zero outstanding work.
