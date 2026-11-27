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

Episode artifacts retain the exporter and Prometheus container logs. The
exporter's `J3_CYCLE` records include SQL and demand durations, JSON decode time,
wire bytes, total cycle time and failure classes. The qualification sampler also
records its wall time and process CPU deltas. Each qualification episode adds
the timestamped PostgreSQL container log and one-second runner load and CPU and
I/O pressure samples. Slow statement logging and nested plans at 500 ms are
scoped to the `j3_exporter` login. Plan analysis uses row instrumentation with
per-node timings disabled. [PostgreSQL role defaults](https://www.postgresql.org/docs/18/sql-alterrole.html) ·
[PostgreSQL auto_explain](https://www.postgresql.org/docs/18/auto-explain.html).

The `j3_exporter` observer role defaults to `jit=off`. This observer setting
preserves the registered projection, one-statement snapshot and 2 s statement
timeout. Engine roles keep their settings. Qualification verifies the role
default and the effective value in a new exporter session.

The observer reads the Ready workers' `/witness` endpoints in one batch over
the existing observer-to-worker TCP/8081 route. The sample records its batch
duration and any read failures. An unwitnessed worker supplies zero slots.

Sender completion uses an atomic in-pod exit record with status zero and a
closed journal. The harness gives the local exec client 30 seconds after it
observes completion. It records and terminates a lingering client while the
registered identity drain continues. When the local client exits first, the
harness allows 30 seconds for the exit record to appear. Sender and drain
failures retain the observer process table and the local client status.

The `sql-benchmark` dispatch compares the deployed and candidate SQL using the
same candidate exporter and pinned PostgreSQL. Each starts with 20,000 synthetic
target workflows and 2,141 foreign fixtures. Idle and concurrent-write phases
retain the full cycle timing series, p50/p99/max, write counts and EXPLAIN plans.
The candidate gates on p99 ≤ 1.0 s and max ≤ 1.5 s with zero failed cycles.
Both variants use the engine's existing indexes. The benchmark verifies and
retains their definitions before and after J3 setup. A missed gate records an
R398 finding in `benchmark.json` and stops before deployment.

The same dispatch also compares `jit=on` and `jit=off` on the deployed query
before loading the benchmark workflows or running `ANALYZE`. Both arms use
the qualification's pinned PostgreSQL image and native JIT thresholds, with
the same role diagnostics and 2 s timeout. `jit-floor/` retains the effective
settings, EXPLAIN plans, whether the body plans showed actual JIT activation,
and p50/p99/max SQL times from 12 calls per arm. The seeded cycle gate uses the
deployed exporter role settings in both projections.

The deployed `j3_sample()` body differs textually from the registered SQL.
It restructures the CTEs, uses `json` in place of `jsonb`, and joins history
only for successful workflows. The benchmark verifies semantic equivalence by
comparing the full outputs in the same statement snapshot and records the
result as `same_snapshot_projection_equal`. [Registered SQL, lines 29–58](protocol.md?plain=1#L29-L58) ·
[Same-snapshot check](../../../scripts/j3_benchmark.py#L611-L624).

The benchmark uses a local demand stub. The pod-inventory CPU comparison uses the pinned
kubectl and a synthetic API, so its results cover inventory calls and parsing.
The job packages the candidate OCI layout for sealing after measurements pass.
The exporter lock uses the pinned build from
[benchmark run 45780147776](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45780147776).
The workflow checks the archive and build inputs against the lock. Each
episode verifies the running exporter digest before arming.

`scripts/j3_schedule.py OUTPUT OUTAGE1 OUTAGE2 OUTAGE3 --measurements FILE`
uses actual PostgreSQL admission and terminal commit times. It records the rate
population, each stop time, recovery time, shifted bounds and arrival volume.
The forecast uses the greater of the reservation and measured baseline for each
allowance, then charges Δ to both extended arms. The job seals the schedule
before the full-volume stop, restore and identity drain. It saves each episode,
forecast input, verdict and hash index in the attempt's Actions artifact.

The full-volume check uses the [locked J2 checker](../load-envelope/checker.lock.json).
The job reproduces it with the [J2 builder](../../../scripts/j2_checker.py),
verifies the installed hash in the observer pod, and runs it with the locked
`GOMEMLIMIT` and J3's namespace and terminal checks. `checker-binary.json`
records the source, tree, verified digest and memory setting. On a failed exit,
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
