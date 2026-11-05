# DUR-053 absolute-schedule load generator

The Compose prerequisite driver calls this command with a fresh namespace,
run ID, seed, arm and new output path. It installs one-node PURE definitions
through the fixture database connection and submits only through the HTTP API.
It never executes activities or waits for workflow completion. The independent
checker performs terminal and external-witness reconciliation separately.

Arrival i is scheduled at t0+i/rate on Go's monotonic clock. In-flight HTTP
requests are capped, with no queue waiting for a completion. Expired intervals
and capacity misses remain failed manifest rows. The generator retains a plan
before any send and a final row for every offered arrival, including full
request/response, schedule/send/finish times, lateness and uncertainty.
Transport errors do not trigger hidden retries. The default request timeout
is 10 s; changing the cap/rate/count is explicit in the manifest.

Arms short/medium/long use positions 0-7 dur048.sleep, 8 pure.echo, 9 pure.add
in every ten arrivals. Worker configuration fixes the arm duration at
100/5000/45000 ms respectively. CPU is 100% dur053.cpu/v1, with independently
computed expected digest and bounded units (default 10000). Inputs and keys
are deterministic for the recorded run ID, seed and sequence. Workloads are
never rewritten to DUR-050 aliases.

The full prerequisite proof entry point for Claude/CI is:

```powershell
pwsh -File scripts/dur053-kubernetes.ps1 -Context compose -Scenario prerequisites
```

The output is a prerequisite proof only, not K3 pilot or capacity/SLO evidence.
