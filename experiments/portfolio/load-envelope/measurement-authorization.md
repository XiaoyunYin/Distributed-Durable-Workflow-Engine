# J2 measurement authorization

The reviewing Claude operator owns the authorization ledger and publishes the generated `authorization.json` on `j2-authorization/<campaign_id>`. The workflow uses a read-only token. It reads the reviewer’s branch at an immutable commit SHA before bootstrap and checks the same record again before each traffic episode.

Branch protection for `j2-authorization/**` is recommended defence in depth for the repository owner. Use reviewer-only updates and disallow force pushes and branch deletion. Engineering validation does not require branch protection because only the reviewer pushes these branches and the workflow token is read-only. Registered measurement still requires a protected ref.

## Engineering dispatches

Use the exact reviewed source checkout after the implementation has been committed and pushed. The `eng-` IDs select short clocks with the registered rates, job order and verdict rules. The two dispatches use separate ledgers.

Run this PowerShell setup from the reviewed checkout. The helper generates the canonical JSON from the committed source, checks it with the workflow validator, and publishes the bytes using the reviewer’s GitHub credentials. Each validation branch is created once.

```powershell
$repo = 'XiaoyunYin/Distributed-Durable-Workflow-Engine'
$head = (git rev-parse --verify HEAD).Trim()
$reviewer = (gh api user --jq .login).Trim()

function Publish-J2EngineeringLedger([string] $campaign, [long] $runId) {
    if (!$campaign.StartsWith('eng-')) { throw 'Expected an engineering campaign ID.' }
    $branch = "j2-authorization/$campaign"
    $ledger = Join-Path ([IO.Path]::GetTempPath()) "j2-authorization-$([guid]::NewGuid()).json"
    try {
        python scripts/j2_measure_authorization.py authorize --campaign-id $campaign --run-id $runId --head $head --reviewer $reviewer --output $ledger
        if ($LASTEXITCODE -ne 0) { throw 'Authorization generation failed.' }
        python scripts/j2_measure_authorization.py verify $ledger
        if ($LASTEXITCODE -ne 0) { throw 'Authorization verification failed.' }
        $content = [Convert]::ToBase64String([IO.File]::ReadAllBytes($ledger))
        gh api --method POST "repos/$repo/git/refs" -f "ref=refs/heads/$branch" -f "sha=$head"
        if ($LASTEXITCODE -ne 0) { throw 'Could not create the reviewer branch.' }
        gh api --method PUT "repos/$repo/contents/authorization.json" -f "branch=$branch" -f "message=Authorize $campaign run $runId" -f "content=$content"
        if ($LASTEXITCODE -ne 0) { throw 'Could not publish the generated record.' }
    } finally {
        if (Test-Path -LiteralPath $ledger) { Remove-Item -LiteralPath $ledger }
    }
}
```

Dispatch the real-grid rehearsal and identify its exact new Actions run ID. Publish the binding before bootstrap. The workflow waits up to six minutes for a missing record within the startup allowance.

```powershell
gh workflow run j2-load-envelope.yml --repo $repo --ref codex/j2-slice3 -f mode=measure -f campaign_id=eng-inc2-real-grid
$runId = [long](Read-Host 'Enter the new eng-inc2-real-grid Actions run ID')
Publish-J2EngineeringLedger 'eng-inc2-real-grid' $runId
```

For the refusal test, publish a separate record bound to the known offline fixture run ID, then dispatch the new run. All three workers should retain a run-ID refusal before starting their runner profiles.

```powershell
Publish-J2EngineeringLedger 'eng-inc2-wrong-run' 46022452922
gh workflow run j2-load-envelope.yml --repo $repo --ref codex/j2-slice3 -f mode=measure -f campaign_id=eng-inc2-wrong-run
```

## Closing a zero-request dispatch

The reporter archives `campaign-dispatch-manifest.json` with three dispatch receipts and their reasons. A recorded HTTP send consumes the authorization. A released sender with missing or ambiguous evidence leaves its disposition unresolved. Both states block a replacement.

If the archived manifest proves zero requests, run the following against a local copy of the ledger and the exact downloaded manifest bytes. The close command checks the campaign, run, head, attempt and all three job dispositions, then records the reason and the manifest SHA-256. A closed ledger can be published separately or together with the next authorization. Append only after review of the next run at its exact source head.

```powershell
python scripts/j2_measure_authorization.py close-no-requests --campaign-id $campaign --run-id $priorRunId --reason $reason --manifest $manifest --output $ledger
python scripts/j2_measure_authorization.py verify $ledger
python scripts/j2_measure_authorization.py authorize --campaign-id $campaign --run-id $nextRunId --head $head --reviewer $reviewer --output $ledger
python scripts/j2_measure_authorization.py verify $ledger
```

`verify` checks the shared schema and, for an active record, the exact checkout head and committed control digest. Before any replacement bootstrap, the workflow confirms each prior attempt is complete, downloads its immutable manifest and verifies the proof hash. Keep the reviewer ref unchanged throughout an active run.

The engineering report checks receipt integrity and agreement of the live grid decisions. Full snapshot replay and publication are separate from this validation.
