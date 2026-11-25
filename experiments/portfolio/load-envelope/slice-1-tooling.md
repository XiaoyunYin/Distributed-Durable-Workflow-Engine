# J2 slice 1 offline replay and qualification scope

The replay used the archived K3 v2 `short-1` episode from run `43220876354`. `j2_offline.py` checks each replay input against the committed source manifest. The historical journal predates the J2 plan format. Replay reconstructs its schedule from the recorded rate, counts, seed and producer UTC origin and cross-checks every planned offset and identity against the journal and committed submissions.

```bash
repo=XiaoyunYin/Distributed-Durable-Workflow-Engine
source=experiments/portfolio/k8s-recovery/k3-pilot-v2/k3-pilot-4a/43220876354/manifest.json
gh api "repos/$repo/actions/artifacts/13949840254/zip" > "$TMPDIR/k3.zip"
echo "08d3ef99912ae7bdf4d11b54594febfb6a0c8677a047534d428a0ca7a77218e4  $TMPDIR/k3.zip" | sha256sum -c -
for pass in 1 2; do
  python3 scripts/j2_offline.py "$source" "$TMPDIR/k3.zip" short-1 "$TMPDIR/j2-replay-$pass.json"
done
sha256sum "$TMPDIR"/j2-replay-{1,2}.json
```

Both outputs had SHA-256 `929d50155a6499cc79da142c6750a80fa8756241984182f4ada6d8704d7b2a91`. All 5,138 archived sends matched committed admissions, and all 4,281 in-window identities joined all four phases with no missing or censored boundaries. All three B.6/B.6.1 checks passed on this historical input. The archived clock-bracket width was 495,000 ns. The stock sender recorded UTC and monotonic sends with a maximum difference of 10,315 ns, so delivery validity uses the recorded monotonic schedule. [Archived K3 artifact](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/43220876354)

The fixture controls shift execution and end-to-end by exactly 2 s while preserving the other three phases, and withhold an acknowledged workflow's terminal while keeping the success list length constant with a duplicate. The PR smoke runs these controls and the named decision mutations in isolated copies.

The first overlay pilot stopped when the shared Kafka helper used its original container default. The second pilot applied the real application profile, passed the PostgreSQL read-only and privilege checks, and stopped during exporter rollout. Its events show that kubelet tried node loopback for the exporter image. The overlay now restores the pinned image and preloads it via the node's configured mirror, as for the application images. Both pilots retained failure artifacts and verified cleanup. [Kafka setup failure](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45574726863) · [Image rollout failure](https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine/actions/runs/45575281969)

Volume qualification moves to slice 1b under the slice-size guard. It must retain live and offline verdicts, identity sets and byte counts, peak cgroup memory and checker RSS, restart/OOM evidence, sample gaps and wall time. The registered primary population is 26,678 identities. Retain any failure and use the 21,002 fallback only with the same tooling, allowing only soak A on success. Review any tooling change prompted by a qualification failure before measuring.
