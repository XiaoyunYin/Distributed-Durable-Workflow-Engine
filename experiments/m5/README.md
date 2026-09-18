# M5 engine correctness evidence

This directory contains the machine-readable results of the bounded F01-F11
campaign. The campaign uses the same committed PostgreSQL-backed tests that
exercise the engine's durable state, transport, worker, effect, checkpoint,
lease, timer, join, and cancellation boundaries. The mapping is explicit in
`scripts/m5-campaign.ps1`; a requested fault is not counted as executed unless
the selected test returns zero and its assertions observe the durable outcome.

Run from the repository root after PostgreSQL/Kafka services are available:

```powershell
$env:DURABLE_REQUIRE_DATABASE = "1"
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/m5-campaign.ps1 -StopRuntimeRelays
```

The output records the exact package, test pattern, exit code, start time, and
captured test output for every family. `-StopRuntimeRelays` temporarily stops
only runtime/worker relay services so shared outbox fixtures cannot be consumed
before the selected test, then restores them. F12 remains the agent-specific
M6 campaign. The M5 outage/restart evidence is produced separately by
`scripts/restart-smoke.ps1` and `scripts/smoke.ps1`.
