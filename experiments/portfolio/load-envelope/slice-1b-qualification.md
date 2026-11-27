# J2 slice 1b volume qualification

The registered slice 3 volume pin is **26,678 distinct full short-workflow identities**, with soak rates **A and AB** eligible. [Slice 1c registered qualification](#slice-1c-registered-qualification)

In the earlier slice 1b dispatch `45768781911`, the 26,678-identity primary and the 21,002-identity fallback both returned `capacity_failure` after confirmed OOM kills during live export.

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

The subsequent checker revision, standalone binary pin and pre-qualification are recorded in [slice 1c checker tooling](slice-1c-checker.md).

## Slice 1c engineering pre-qualification

All four dispatches were unmeasured engineering runs in `checker-prequalification` mode.

| Tooling commit | Engineering run outcome |
| --- | --- |
| `e298ed1` | Both jobs stopped at the checker lock's `go.mod` hash check before loading fixtures. [Run 45883761097](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45883761097) |
| `4bbfc66` | All gates passed at 21,002. At 26,678, live VmHWM reached 1.64 GiB and offline VmHWM reached 1.42 GiB, exceeding the 1.4 GiB gate. [Run 45884706097](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45884706097) |
| `fbce0d9` | The `1100MiB` and `1200MiB` settings passed live and offline at both volumes. The unset control exceeded the VmHWM gate at 26,678. [Run 45889171119](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45889171119) |
| `03f3c76` | The locked checker with `GOMEMLIMIT=1200MiB` passed all live and offline gates at 21,002 and 26,678. [Run 45891341815](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45891341815) |

## Slice 1c registered qualification

Dispatch `45893066798`, attempt 1, passed at tooling commit `03f3c767a8143e47e0cd725fd33f1ce16a72faba`. The primary accepted all **26,678** identities in 6,670 s. The final drain recorded 26,678 succeeded, zero unpublished outbox entries and zero claimed attempts. The verdict qualified soak rates A and AB under the registered 26,678-identity volume pin. The fallback was skipped. [Actions dispatch](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45893066798) · [Verdict job](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45893066798/job/140205320974) · [Measurement protocol](protocol.md)

Both checker paths used SHA-256 `823646d3fbd3584b6d22d305ab7aefd4d3f74f9b4e6e12f5604b122bc7be717e` and `GOMEMLIMIT=1200MiB`. Both exited 0 with `valid: true` and zero cgroup OOM events.

| Path | Checker wall time | Checker VmHWM | cgroup memory peak |
| --- | ---: | ---: | ---: |
| Live | 40.4 s (40.8 s stage) | 1,111,820 KiB (1.06 GiB) | 1,258,627,072 bytes in the node-side samples |
| Offline | 12.3 s | 1,137,196 KiB | 1,166,303,232 bytes |

The node-side sampler took 41 live samples and recorded a maximum checker VmHWM of 1,112,280 KiB. The observer limit was 2,147,483,648 bytes. Live export completed 94.3 s after the scheduled end, within the 600 s drain and the 480 s engineering target. Verified artifact upload completed 73.7 s after export.

Offline reconciliation returned `identical_verdict: PASS` for 26,678 identities and observed all 26,678 phase joins. Offline replay took 36.8 s. Telemetry recorded 6,758 valid scrapes, zero failed queries and zero failed scrapes. The maximum query and scrape gaps were 1.25 s and 1.00 s, respectively. Teardown passed.

Archive **15158465873**, `j2-volume-26678-45893066798-1`, is 241,102,239 bytes with SHA-256 `7e8e5d09f3ca5499b36dd9ddb88afaccb550f6ebaed552d62dd731191542202f`. The downloaded ZIP and all 144 frozen entries matched their recorded sizes and hashes. [Qualification archive](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45893066798/artifacts/15158465873)
