# J2 slice 1c checker tooling, 2026-11-26

Round 530 authorized a separately pinned checker after the observer OOM kills at both volumes in dispatch `45768781911`. The revision streams reports and offline decoding, hashes tables incrementally, indexes the reconciliation scans, and shares one structural verdict with the campaign check. The observer limit is 2 GiB, the registered drain is 600 s, and qualification still requires zero OOM, restart or truncation findings.

The executable SHA-256 is `823646d3fbd3584b6d22d305ab7aefd4d3f74f9b4e6e12f5604b122bc7be717e`. The lock records source `fbce0d98e4e8c82d57eba09b68bb71a76fd30355`, its tree, Go 1.27.4, the committed module input hashes and the static Linux/amd64 build settings. Jobs reproduce the binary from that source and check the lock before running. The observer receives the bytes at `/tmp/checker` on its existing scratch mount and verifies the SHA-256 in the pod. Offline replay bind-mounts that same binary read-only and verifies it inside the container before execution. The shared wrapper uses the locked `GOMEMLIMIT=1200MiB` for live export, offline replay and every measured cell. [Checker lock](checker.lock.json) · [Runtime and worker image lock](images.lock.json)

The PR parity job builds `03ae92f` and runs complete and negative offline cases plus live PostgreSQL parity. The `checker-prequalification` dispatch matrix bulk-loads 21,002 and 26,678 workflows into separate pinned, migrated PostgreSQL instances. Fixture rows come from the linked K3 `short-1` snapshot in run `43220876354`. The fixture verifies the retained member hash, uses the J2 8:1:1 activity mix, and generates 26 lease acquisitions per workflow. Its arrivals, witness and durable IDs reconcile to the same population.

Each pre-qualification live export and offline replay gets a fresh 2 GiB, 1-CPU cgroup with swap disabled. Both paths must exit 0 with PASS at each volume, with checker VmHWM ≤1.4 GiB, cgroup `memory.peak` ≤1.75 GiB and checker wall time ≤240 s. The evidence records phase timestamps and heap figures, the final five samples, the 300 MiB final-step and 300 s live diagnostics, and the extract and verify profiles measured on the runner.

Hosted pre-qualification and qualification retain per-table counts, SHA-256 values and exact bytes of the exported JSON `rows` arrays. Compare the qualification tables with the corresponding fixture volume. The fixture is unrepresentative if the hosted evidence exceeds it. The qualification summary reports scheduled end through completed live export against the 480 s engineering target separately from the registered verdict.

Dispatch `45884706097` cleared all gates at 21,002. Both paths at 26,678 exceeded the VmHWM gate, at 1.64 GiB live and 1.42 GiB offline. Checker wall times were 30.3 s and 9.4 s, respectively, with zero OOMs. [Pre-qualification run](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45884706097)

Round 538 authorized a campaign allocation reduction and a paired memory-limit trial. The reconciliation reuses sort keys and decode values, sizes its maps from the input, and compares compacted result JSON before canonical hashing. The arrival decoder streams the normal path with legacy data semantics.

Comparison dispatch `45889171119` on `fbce0d9` recorded these live and offline results. Each pair is live / offline. Every trial exited with PASS and recorded zero OOMs. [Comparison run](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45889171119)

| Volume | Setting | VmHWM (GiB) | cgroup peak (GiB) | Wall (s) | GCs | Resource gates |
| ---: | --- | --- | --- | --- | --- | --- |
| 21,002 | unset | 1.18 / 1.07 | 1.18 / 1.07 | 20.4 / 6.4 | 25 / 13 | PASS |
| 21,002 | 1100MiB | 1.02 / 1.02 | 1.02 / 1.02 | 20.7 / 6.5 | 26 / 14 | PASS |
| 21,002 | 1200MiB | 1.09 / 1.05 | 1.09 / 1.05 | 20.4 / 6.4 | 26 / 13 | PASS |
| 26,678 | unset | 1.46 / 1.42 | 1.46 / 1.42 | 29.6 / 9.2 | 26 / 14 | FAIL (VmHWM) |
| 26,678 | 1100MiB | 1.07 / 1.07 | 1.07 / 1.07 | 29.6 / 9.4 | 30 / 17 | PASS |
| 26,678 | 1200MiB | 1.12 / 1.17 | 1.11 / 1.17 | 29.4 / 9.2 | 29 / 15 | PASS |

Round 543 selected `1200MiB` to allow more room above the live heap if hosted inputs exceed the fixture. The highest measured VmHWM with that setting was 1.17 GiB, leaving 0.23 GiB below the pre-qualification gate and 0.83 GiB below the observer limit. The registered thresholds, volumes, 600 s drain and qualification criteria are unchanged.

Run `checker-prequalification` with comparison disabled to verify the locked binary and setting at both volumes. After both runs pass and two consecutive attempt-1 PR rounds are green, `qualification` starts at 26,678 identities and runs the 21,002 fallback only after an archived primary `capacity_failure`. Future exploratory comparisons can use `compare_gomemlimit=true` with the dispatched commit.
