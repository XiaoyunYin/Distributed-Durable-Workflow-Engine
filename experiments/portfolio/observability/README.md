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
Ready slots. The takeover and stale-rejection panels have gaps until their
observer evidence is available.
