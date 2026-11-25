# J2 slice 1b volume qualification

Hosted evidence is pending. Slice 3 has no qualified volume or eligible soak rate until a full attempt passes.

The `qualification` dispatch in the J2 workflow uses the **rebuilt registered profile**. It starts a fresh kind cluster and uses the pinned short-workflow sender at four submissions per second, below the lowest registered grid rate. Live submission produces the actual database history, results, outbox and worker witness bytes. The plan includes identical workload settings and resource limits for both volume attempts.

| Attempt | Distinct full workflow identities | Soak rates eligible if it passes |
| --- | ---: | --- |
| Primary | 26,678 | A and AB |
| Fallback after an archived primary failure | 21,002 | A |

Each attempt has a 300-minute job timeout. The registered 600-second drain starts at the scheduled end of population. It includes the 30 seconds of unchanged terminal, outbox, receipt and witness evidence and the live namespace export through the in-pod checker. A separate 25-minute allowance covers offline replay, freeze, verified teardown and upload. The workflow starts the fallback on a fresh runner after uploading the primary failure, using the same checkout and image pins.

The artifact includes the arrival plan and journal, raw witness ledgers, live report and byte-preserved snapshot, and a replay with Docker networking disabled. The replay compares deterministic checker verdict fields and independently reconciles every expected short-workflow identity and result. It also checks the four phase joins for coverage. The freeze records file sizes and SHA-256 hashes. Container cgroup peaks and OOM counters, process RSS, pod observations, exporter sample gaps, stage exit codes and wall times accompany the reports.
