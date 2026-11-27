# J2 slice 3 replay validation

The engineering validation is archive-only. The source is attempt 1 of Actions run `46028317534`, campaign `eng-inc2-real-grid`, head `345d8e30db99948bc9db81430f9b7844c604dcda`. The source contains 15 short-clock grid checkpoints, three short-clock continuous checkpoints and three AB decisions. Use a standard GitHub-hosted runner. The reporter repeats the sealed 26,678-identity qualification archive three times in the full and Release cases to exercise full-volume checker, transfer and scratch use. Those three repeats are budget fixtures outside the engineering episode population.

## Actions replay

Run these after the reviewed changes are available on `codex/j2-slice3`.

```powershell
gh workflow run j2-load-envelope.yml --ref codex/j2-slice3 -f mode=measure-replay -f campaign_id=eng-inc2-real-grid -f source_run_id=46028317534 -f replay_case=full
gh workflow run j2-load-envelope.yml --ref codex/j2-slice3 -f mode=measure-replay -f campaign_id=eng-inc2-real-grid -f source_run_id=46028317534 -f replay_case=partial
```

- `full` should independently replay all 18 checkpoints and reproduce AB and the three ordered grid digests. The raw attribution fixture should support relay publication in the complete case and yield `unresolved` for missing broker evidence, conflicting components and a drain-only tail. Engineering should produce zero candidate performance sentences.
- `partial` deliberately withholds job 1's C checkpoint. It should retain the other 14 grid and three continuous replays, mark authorization `unverifiable` and withhold every dependent sentence. It should still upload and verify its sealed partial report.
- Each run should retain `j2-measure-replay-<run>-1` and `j2-measure-replay-receipt-<run>-1`. Review `campaign-replay.json`, all three job reports, `archive-index.json`, `reporter-budget.json`, and the upload verification receipt. Check per-archive durations and sizes, checker scratch, free disk, the 90-minute cap and upload verification within nine minutes. The 85-minute ledger includes that upload reserve.

## Reviewer Release copy

From the reviewed worktree, set `$reportRun` to the successful attempt-one `full` replay run ID. The reviewer creates the public J2 evidence release and runs the publisher with their authenticated GitHub CLI. The publisher verifies the original Actions ZIPs against their source metadata and freeze inventories, uploads the same bytes, then verifies the public download and every member again. An existing asset can be reused only when its public bytes and ZIP inventory match. Keep the local index to review alongside the evidence.

```powershell
$repo = 'XiaoyunYin/Distributed-Durable-Workflow-Engine'
$tag = 'eng-inc3-j2-evidence'
$head = (git rev-parse HEAD).Trim()
$reportRun = [long](Read-Host 'Successful full replay run ID')
$out = Join-Path (Get-Location) '..\j2-inc3-release-index'
$python = 'D:/portfolio-projects/durable-agent-execution-engine/.venv/Scripts/python.exe'
gh release create $tag --repo $repo --target $head --title 'J2 engineering evidence for slice 3 replay' --notes 'Archive-only engineering evidence for replay of Actions run 46028317534. Engineering timing and budget fixtures only.'
& $python scripts/j2_publish_evidence.py --campaign-id eng-inc2-real-grid --source-run 46028317534 --release-tag $tag --report-run $reportRun --output $out
$index = "eng-inc2-real-grid--r46028317534--a1--replay-r${reportRun}--index.json"
$sha = (Get-FileHash -LiteralPath (Join-Path $out $index) -Algorithm SHA256).Hash.ToLowerInvariant()
gh workflow run j2-load-envelope.yml --ref codex/j2-slice3 -f mode=measure-replay -f campaign_id=eng-inc2-real-grid -f source_run_id=46028317534 -f replay_case=release -f "release_tag=$tag" -f "release_index=$index" -f "release_index_sha256=$sha"
```

The Release replay should produce the full replay outcome from the digest-pinned index and public Release ZIPs. Check that the report records `transport: release`, `release_verified: true`, copies the Release asset records into its evidence index and independently verifies the same 18 checkpoint outcomes and decision. Preserve the report and its upload receipt alongside the source manifest and Release index when reviewing the copy.
