# Portfolio recovery evidence

This directory contains supplemental hiring-facing evidence. Existing M7
artifacts remain authoritative for their original task scopes; this directory
does not replace or rewrite them.

## DUR-041a local recovery

Run the scoped wrapper from a dedicated local Compose/database environment:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/portfolio-local-recovery.ps1 -StartServices
```

The preserved-process isolation arms are run separately so their fault
mechanisms are not conflated:

```powershell
# Pause/resume the original scheduler container.
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/dur041a-isolation.ps1 -Fault pause -Seed 410099

# Remove the original scheduler from the PostgreSQL network while it keeps running.
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/dur041a-isolation.ps1 -Fault network -Seed 410104
```

The wrapper reuses the reviewed DUR-027 process fixtures and writes a new
campaign directory under `experiments/portfolio/local-recovery/`. It promotes
only two local fault families:

- owner reconnects after takeover and attempts stale-epoch operations;
- owner process self-exits at an acknowledged boundary and the peer recovers
  useful affected work.

The wrapper requires confirmed target death, takeover, useful progress, fenced
stale-owner writes, and clean source-harness reconciliation. It reports
median/min/max for takeover and useful-progress delays, with counts and
denominators.

The lease-contention repair was authorized and independently verified. The
lock-held takeover case remains separately labelled and is not promoted by the
pause/reconnect or network-disconnect/reconnect arm. The source harness
contains a separate lock-contention probe, while the repair's bounded-wait and
scheduler-continuation tests provide the local liveness evidence. The result is
local Docker Desktop/WSL2 process evidence, not multi-host or database-host
durability.

Generated campaign directories contain protocol, observation, and summary
artifacts. Review the source artifact and its independent checker output before
quoting any result.

## Recorded run

The committed seed `410041` run is available at
[`local-410041/summary.json`](local-recovery/local-410041/summary.json), with
the full source episodes in
[`local-410041/dur027-source.json`](local-recovery/local-410041/dur027-source.json)
and scope metadata in
[`local-410041/protocol.json`](local-recovery/local-410041/protocol.json).
It reports 60 in-scope episodes, 180 fenced stale-owner writes, and zero false
takeovers. Those numbers are local process evidence only; the lock-held
takeover case remains separately labelled. The new scenario-1 run is available
at [`isolation-410099/summary.json`](local-recovery/isolation-410099/summary.json):
it preserved the original runtime-a container through Docker pause/unpause,
observed peer takeover from epoch 647 to 653, and rejected the retained old
epoch without changing revision 1. The packet-level arm is available at
[`network-410104/summary.json`](local-recovery/network-410104/summary.json):
Docker removed runtime-a from the PostgreSQL network while preserving its
container and PID, the peer took over from epoch 785 to 788, and the
reconnected old owner was fenced without changing revision 3.
