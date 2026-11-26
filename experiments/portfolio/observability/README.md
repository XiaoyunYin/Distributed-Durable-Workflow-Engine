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

The first slice 2 change adds qualification tooling. The overlay pilot retains
raw Prometheus matrices, rule health, visibility and both replay results. The
real partition-lease probe belongs to that unmeasured pilot. The qualification
runner needs its own episode entry point without that probe.

`scripts/j3_schedule.py OUTPUT OUTAGE1 OUTAGE2 OUTAGE3` derives the sealed
schedule from three separate outage records. Each record contains the origin,
actual scale commands, the ordered pre-restore backlog witness and the durable
admission and terminal commit timestamps. The output preserves the stop rule,
throughput population, exact rate and recovery-time ratios, shifted bounds,
arrival volume and cap verdict. The hosted episode runner must collect those
inputs, continue offers across the outage and complete the full-volume run.

The qualification checks use the existing J2 timestamp and attempt-identity
formats. The J3 checker diagnostics collect pod, cgroup and kernel evidence on
the kind node and sample checker RSS outside the observer cgroup. The hosted
runner must wire these checks into its closeout before deleting the episode.
