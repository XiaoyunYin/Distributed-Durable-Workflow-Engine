# J2 slice 1b volume qualification

The slice 3 volume pin is unset and the eligible soak-rate set is empty. In dispatch 45768781911, the 26,678-identity primary and the 21,002-identity fallback both returned `capacity_failure` after confirmed OOM kills during live export.

The `qualification` dispatch in the J2 workflow uses the **rebuilt registered profile**. It starts a fresh kind cluster and uses the pinned short-workflow sender at four submissions per second, below the lowest registered grid rate. Live submission produces the actual database history, results, outbox and worker witness bytes. The plan includes identical workload settings and resource limits for both volume attempts.

| Attempt | Distinct full workflow identities | Soak rates eligible if it passes |
| --- | ---: | --- |
| Primary | 26,678 | A and AB |
| Fallback after an archived primary `capacity_failure` | 21,002 | A |

Each attempt has a 300-minute job timeout. The registered 600-second drain starts at the scheduled end of population. It includes the 30 seconds of unchanged terminal, outbox, receipt and witness evidence and the live namespace export through the in-pod checker. A separate 25-minute allowance covers offline replay, freeze, verified teardown and upload. A primary `capacity_failure` with an uploaded archive starts the fallback on a fresh runner with the same checkout and image pins. An `invalid` verdict halts the dispatch with the available evidence retained.

The artifact includes the arrival plan and journal, raw witness ledgers, live report and byte-preserved snapshot, and a replay with Docker networking disabled. The replay compares deterministic checker verdict fields and independently reconciles every expected short-workflow identity and result. It also checks the four phase joins for coverage. The freeze records file sizes and SHA-256 hashes. Container cgroup peaks and OOM counters, process RSS, pod observations, exporter sample gaps, stage exit codes and wall times accompany the reports.

## Hosted results

Dispatch `45592772474`, attempt 1, used tooling commit `fe3db86e8447aa09d82990cf012dac0946cebb44`. Both public GitHub-hosted runners reported four vCPUs and runner image `20261116.133.1`. The fallback used a fresh cluster after the primary artifact uploaded. [Actions dispatch](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45592772474)

| Attempt | Verdict | Planned | Submitted | Job wall time | Cluster live time | Evidence ZIP |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| Primary | `invalid` | 26,678 | 0 | 3m14s | 157 s | 317,670 bytes, SHA-256 `c1c01b1bf17f7a7190c0ff2f92278beb0bbf88810c603c81950470a77d0296b0`. [Artifact 15012800281](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45592772474/artifacts/15012800281) |
| Fallback | `invalid` | 21,002 | 0 | 2m53s | 148 s | 318,381 bytes, SHA-256 `d5db938607b3cd982951f66351630e8f4250e284103bf49a6e884540e78b3b78`. [Artifact 15011663368](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45592772474/artifacts/15011663368) |

Both setups restored the pinned images, confirmed the database was empty, retained the topic descriptions and recorded one exporter DB connection. Each empty-namespace probe captured eight distinct exporter samples, with maximum gaps of 1.001 s on both runners, rounded to milliseconds. Both cleanup records passed. Both downloaded ZIPs matched the Actions digests, and every file in each freeze index matched its byte count and SHA-256.

The runner inventory raised `FileNotFoundError` while reading `/sys/fs/cgroup/cpu.max`. It assumed the CPU quota file was available at the hierarchy root. The old workflow treated both failed closeouts as volume failures and ran the fallback automatically. The two archived attempts provide setup evidence only. Full-population sampling, drain, live export and offline reconciliation remain unexercised by this dispatch. [Linux cgroup v2 CPU and process membership](https://docs.kernel.org/admin-guide/cgroup-v2.html)

### Dispatch 45614099335

Attempt 1 used `b0807ac5aa5abb70672027359e15dcbe950ed4e7`. Runner inventory resolved the qualifier to `/sys/fs/cgroup/system.slice/hosted-compute-agent.service`. It recorded all four runner cgroup files. The primary accepted all 26,678 submissions and reached 26,678 succeeded with zero unpublished outbox entries and zero claimed attempts. [Actions dispatch](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45614099335)

| Volume | Verdict | Accepted | Observer peak captured | Checker RSS | Evidence ZIP |
| --- | --- | ---: | ---: | ---: | --- |
| Primary 26,678 | `invalid` | 26,678 | 1,787,183,104 bytes | Unavailable | 13,454,983 bytes, SHA-256 `a18bded9dcf9d7dd770befd3cd470304de7e5effdd890ffcdbc2bbbbcc34d605`. [Artifact 15031778362](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45614099335/artifacts/15031778362) |
| Fallback 21,002 | Skipped | — | — | — | — |

The live checker exited 137 after 274.6 s and produced an empty JSON report. The observer's last full sample had zero OOM events. The next probe found no container PID while kubelet still reported the observer running with zero restarts. The archive contains no positive OOM kill or restart evidence. The classified closeout halted the dispatch and retained the artifact. Offline replay was unperformed.

### Dispatch 45768781911

Attempt 1 used tooling commit `a61be6aca29c9eb1d08219cdadd11db2f41887fa`. Both jobs ran the rebuilt registered profile. In each job, every submitted identity was accepted and succeeded, with zero unpublished outbox entries and zero claimed attempts at the final quiet database observation. [Actions dispatch](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45768781911)

| Volume | Verdict | Accepted | Sampled observer cgroup peak | Pod cgroup peak at diagnosis | Checker sampled VmHWM / RSS at kill | Evidence ZIP |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| Primary 26,678 | `capacity_failure` | 26,678 | 2,002,292,736 bytes | 2,147,487,744 bytes | 1,856,908 / 2,092,936 KiB | 13,852,979 bytes, SHA-256 `6772cce77171ed4012feb403b2282015fd1fa809d885779362a7476d75e069bf`. [Artifact 15099033018](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45768781911/artifacts/15099033018) |
| Fallback 21,002 | `capacity_failure` | 21,002 | 1,870,766,080 bytes | 2,147,483,648 bytes | 1,752,880 / 2,093,040 KiB | 10,899,388 bytes, SHA-256 `d10e085e782ed97552c13ba23c25148d0cb1f8f4f8850a6bbab780abd978b5bd`. [Artifact 15105783055](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45768781911/artifacts/15105783055) |

The live checkers exited 137 after 464.39 s and 281.53 s, respectively. Both pod cgroups recorded `oom_kill=4` and `oom_group_kill=1` while kubelet still reported the observers running with zero restarts. Each node's kernel log identifies the checker in the observer pod's memory cgroup. RSS at kill is the sum of anonymous, file-backed and shared RSS from that kernel line. The live resource sampler retained 458 primary and 278 fallback samples, with maximum gaps of 1.053 s and 1.050 s, respectively.

Both jobs reached teardown with empty live checker reports. Offline replay was unperformed. Both cleanups passed, both ZIPs matched their Actions digests, and all 127 freeze entries verified in each archive. The terminal verdict job confirmed both `capacity_failure` results and ended the dispatch.
