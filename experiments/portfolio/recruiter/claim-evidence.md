# Resume claim-to-evidence map

Use only the claim set that matches the target role. Wording is deliberately
bounded to the measured configuration and population; the linked artifacts are
the source of truth.

## Backend and distributed-systems roles

| Resume-ready claim | Evidence | Scope boundary |
|---|---|---|
| Built a Go workflow engine using PostgreSQL as the durable authority, Kafka transactional-outbox transport, Python workers, lease epochs, attempt claims, and result receipts. | [Architecture](../../../README.md#architecture); [M5 fault campaign](../../m5/f01-f11-results.json); implementation and tests under `internal/`. | The M5 campaign is 16 named cases across three seeds (48/48 checked), not a proof of universal exactly-once behavior. |
| Recovered useful work across 60 local owner-failure episodes: 60 takeovers, 60 useful recoveries, 180 fenced stale-owner writes, and zero false takeovers. | [DUR-041a local recovery summary](../local-recovery/local-410041/summary.json). | Docker Desktop/WSL2 process faults; not independent-host or database-host recovery. |
| In a two-scheduler engine-path study, two schedulers kept up with the tested 2 workflows/s offered rate where one scheduler did not, with a fixed four-process worker pool. | [DUR-026 results](../../m7/dur026/results.json). | Single-host Docker Desktop/WSL2 engine-path harness; not end-to-end deployed capacity or maximum sustainable throughput. |
| Exercised preserved-process AWS network isolation and the owner-lock arm; the original worker's late result was rejected as `STALE_ATTEMPT`, and the independent checker found zero superseded-epoch transitions. | [DUR-049 protocol](../cloud-recovery/dur049-aws-20260923-full-0adbac0/protocol.json); [network episode](../cloud-recovery/dur049-aws-20260923-full-0adbac0/network-isolation.json); [owner-lock episode](../cloud-recovery/dur049-aws-20260923-full-0adbac0/owner-lock-isolation.json). | Controller-started cold standby, one region and one dependency host. Owner-lock backend reaping was observed through TCP keepalive; the campaign overrode the production 10-second idle timeout with a transaction-local 2-minute timeout, so that production reaping path was not measured. Not database-host durability, warm-active failover, or an SLO. |
| Detected all 17 selected behavioral safety mutations on hosted CI, backed by an independent reference model and Go fuzz targets. | [Hosted mutation results](../../../mutations/results-powershell.json); [gate and run provenance](../../../mutations/README.md); [reference model](../../../internal/reference/model.go); [CI workflow](../../../.github/workflows/ci.yml). | Selected guard cases only, not complete mutation coverage; the manifest also contains one separately classified configuration tripwire. |
| Measured checkpoint overhead for one SHA-256 work unit per chunk: every-chunk checkpointing took 7.5552x the boundary-only median in the recorded crash model while saving 99 replayed chunks. | [DUR-028 results](../../m7/dur028/results.json). | One pure 200-chunk workload and in-process panic; not a general checkpoint policy or crossover estimate. |

## AI infrastructure roles

| Resume-ready claim | Evidence | Scope boundary |
|---|---|---|
| Built and evaluated keyword, hybrid, and dense retrieval on 120 held-out synthetic queries, reporting ranking and delivered Recall@K beside paired live-agent outcomes rather than promoting a retrieval winner unsupported by end-to-end results. | [DUR-029 final retrieval](../../m7/dur029/retrieval-final.json); [live evaluation](../../m7/dur029/live-evaluation.json). | Synthetic frozen corpus; the live model had 0/16 document-dependent safe successes per retrieval arm, so retrieval ranking did not establish better task success. |
| Evaluated evidence redaction and injection defenses with clean-clean baselines: excess injection-associated proposal changes were 2/20 defended versus 6/20 plain; the redaction-off control leaked five seeded canaries while enabled-redaction profiles leaked none. | [DUR-029 live evaluation](../../m7/dur029/live-evaluation.json), including computed profile rates, counts, and the negative control. | One model, prompt/schema, sampling regime, and synthetic 20-case profile samples; rates are small-sample evidence, not a general prompt-injection guarantee. |
| Exercised citation and approval boundaries in an incident-agent workflow: across 120 recorded executions, 27 proposals were approved, 27 completed, and zero completed without approval. | [DUR-029 live evaluation](../../m7/dur029/live-evaluation.json); [production integration test](../../../internal/incident/production_integration_test.go); [production path](../../../internal/incident/production.go). | The 120-execution result is from M6's local SQLite/MCP adapter, not the production Go integration. DUR-033A separately tests the Go API/engine/effect-service path; no external action was authorized. |

## Interview rule

Say what was measured, identify the artifact and population, then name the
failure model and what it cannot establish. Do not turn a test count into an
exactly-once claim, a synthetic AI sample into general model quality, or a
cold-standby campaign into production availability.
