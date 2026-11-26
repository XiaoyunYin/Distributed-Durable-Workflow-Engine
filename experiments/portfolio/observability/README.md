# Short-workflow observability

The [J3 protocol](protocol.md) registers the deadline cohort, alert rules, worker outage and controls before measurement. Reviewed campaign evidence belongs in a sibling directory named for the campaign ID.

Slice 1 is unmeasured. The J3 Actions workflow tests the rules with the pinned
Prometheus image, runs the replay fixtures against PostgreSQL and checks the
exporter build. Its manual pilot restores the pinned images on a fresh kind
cluster and exercises the private overlay with API-submitted short workflows and
one synthetic foreign attempt. Pilot artifacts retain the digest checks, deny
probes, Grafana API and range results, and the durable dispatch and worker
receipts. The foreign scope uses the same database and broker as the target.

`scripts/j3_replay.py` takes a normalized evidence JSON file and the exporter's
`visibility.jsonl`. The evidence file contains the target namespace, integer
UTC nanosecond interval and clock brackets, scoped plan, journal, final workflow
rows and history, and the rule evaluations with their original source and
scrape timestamps. Each evaluation supplies its observed firing rules. The
replay reports the visibility-band checks and the conservative attainment.

The dashboard accepts observer metrics for Ready slots, database lease
takeovers and stale worker-control rejections. The pilot supplies the witnessed
Ready slots from Ready pods and their worker witness sessions. Stale Ready-slot
observations and unavailable takeover or stale-rejection inputs appear as gaps.

Select `qualification` when dispatching J3 observability on the qualification
head. The job runs the healthy foreign fixture, three 300-second outages at A,
and the baseline matched-healthy and scrape-loss controls needed for the forecast.
Each episode starts with fresh database, broker, witness and telemetry state.
The real partition-lease probe runs only in the unmeasured `pilot` mode.

`scripts/j3_schedule.py OUTPUT OUTAGE1 OUTAGE2 OUTAGE3 --measurements FILE`
uses actual PostgreSQL admission and terminal commit times. It records the rate
population, each stop time, recovery time, shifted bounds and arrival volume.
The forecast uses the greater of the reservation and measured baseline for each
allowance, then charges Δ to both extended arms. The job seals the schedule
before the full-volume stop, restore and identity drain. It saves each episode,
forecast input, verdict and hash index in the attempt's Actions artifact.

The full-volume checker runs in the existing observer pod. On a failed exit,
the diagnostic retains pod state, pod cgroup counters and node kernel OOM
records. Linux emits the `oom-kill:constraint=…` summary, adds `oom_memcg`
and `task_memcg` paths, then prints the victim task, PID and UID. The cgroup
helper uses kernfs paths from the hierarchy root, so the kernel record can have
Docker ancestors absent from the kind node's namespaced membership. The matcher
accepts the observer pod's namespaced path at a path-component boundary, including
its descendants. [OOM summary](https://github.com/torvalds/linux/blob/v6.8/mm/oom_kill.c#L412-L422),
[memory cgroup fields](https://github.com/torvalds/linux/blob/v6.8/mm/memcontrol.c#L1607-L1619),
[cgroup helper](https://github.com/torvalds/linux/blob/v6.8/include/linux/cgroup.h#L558-L570),
[kernfs path origin](https://github.com/torvalds/linux/blob/v6.8/fs/kernfs/dir.c#L240-L266).
