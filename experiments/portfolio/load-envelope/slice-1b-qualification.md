# J2 slice 1b volume qualification

**Qualification failed during runner inventory on both volumes.** The slice 3 volume pin is unset and the eligible soak-rate set is empty. Both trials stopped before workflow submission.

The `qualification` dispatch in the J2 workflow uses the **rebuilt registered profile**. It starts a fresh kind cluster and uses the pinned short-workflow sender at four submissions per second, below the lowest registered grid rate. Live submission produces the actual database history, results, outbox and worker witness bytes. The plan includes identical workload settings and resource limits for both volume attempts.

| Attempt | Distinct full workflow identities | Soak rates eligible if it passes |
| --- | ---: | --- |
| Primary | 26,678 | A and AB |
| Fallback after an archived primary failure | 21,002 | A |

Each attempt has a 300-minute job timeout. The registered 600-second drain starts at the scheduled end of population. It includes the 30 seconds of unchanged terminal, outbox, receipt and witness evidence and the live namespace export through the in-pod checker. A separate 25-minute allowance covers offline replay, freeze, verified teardown and upload. The workflow starts the fallback on a fresh runner after uploading the primary failure, using the same checkout and image pins.

The artifact includes the arrival plan and journal, raw witness ledgers, live report and byte-preserved snapshot, and a replay with Docker networking disabled. The replay compares deterministic checker verdict fields and independently reconciles every expected short-workflow identity and result. It also checks the four phase joins for coverage. The freeze records file sizes and SHA-256 hashes. Container cgroup peaks and OOM counters, process RSS, pod observations, exporter sample gaps, stage exit codes and wall times accompany the reports.

## Hosted results

Dispatch `45592772474`, attempt 1, used tooling commit `fe3db86e8447aa09d82990cf012dac0946cebb44`. Both public GitHub-hosted runners reported four vCPUs and runner image `20261116.133.1`. The fallback used a fresh cluster after the primary artifact uploaded. [Actions dispatch](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45592772474)

| Attempt | Planned | Submitted | Job wall time | Cluster live time | Evidence ZIP |
| --- | ---: | ---: | ---: | ---: | --- |
| Primary | 26,678 | 0 | 3m14s | 157 s | 317,670 bytes, SHA-256 `c1c01b1bf17f7a7190c0ff2f92278beb0bbf88810c603c81950470a77d0296b0`. [Artifact 15012800281](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45592772474/artifacts/15012800281) |
| Fallback | 21,002 | 0 | 2m53s | 148 s | 318,381 bytes, SHA-256 `d5db938607b3cd982951f66351630e8f4250e284103bf49a6e884540e78b3b78`. [Artifact 15011663368](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45592772474/artifacts/15011663368) |

Both setups restored the pinned images, confirmed the database was empty, retained the topic descriptions and recorded one exporter DB connection. Each empty-namespace probe captured eight distinct exporter samples, with maximum gaps of 1.001 s on both runners, rounded to milliseconds. Both cleanup records passed. Both downloaded ZIPs matched the Actions digests, and every file in each freeze index matched its byte count and SHA-256.

The runner inventory raised `FileNotFoundError` while reading `/sys/fs/cgroup/cpu.max`. It assumed the CPU quota file was available at the hierarchy root. Full-population exporter samples, container peaks, checker RSS and exit statuses, drain, live export and offline reconciliation were unperformed. The review item is to resolve the actual runner process cgroup and its mount, record which limit files the kernel exposes, and preserve strict checks for the observer, exporter and offline checker container limits and OOM events. The attempted qualification tooling is unchanged pending review. [Linux cgroup v2 CPU and process membership](https://docs.kernel.org/admin-guide/cgroup-v2.html)
