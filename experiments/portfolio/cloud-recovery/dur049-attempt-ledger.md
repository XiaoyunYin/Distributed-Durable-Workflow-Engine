# DUR-049 attempt ledger

This ledger preserves the retained attempt history; it does not convert a
failed or incomplete run into evidence of an engine defect. A `FAIL` records
that the harness did not satisfy its then-current gate. Where the retained
observations cannot distinguish an engine recovery miss from a harness or
environment problem, the classification says so explicitly. `MISSING` means
there is no retained directory or result from which to infer a status.

The initial suffix series is `a` through `as` (45 slots): 8 PASS, 22 FAIL,
3 retained IN_PROGRESS records, and 12 slots without a retained directory.
The round-71/72 supplemental series below adds 25 runs: 9 PASS and 16 FAIL.
The later September 23 follow-up attempts are listed separately. These counts
are attempt history, not independent statistical samples; protocols, fixtures,
and acceptance predicates changed between attempts.
Retained initial artifacts use the directory pattern
`dur049-aws-20260922-{host,network}-{suffix}/`; supplemental records are in
the explicitly named `dur049-aws-20260923-round*` directories below this
folder. A missing suffix has no corresponding directory.

## Initial suffix series (`a`–`as`)

| Suffix | Arm | Harness commit | Status and retained error | Classification and reasoning |
|---|---|---|---|---|
| a | host stop | — | MISSING; no retained directory | No artifact or commit is available; outcome is unknown. |
| b | host stop | — | MISSING; no retained directory | No artifact or commit is available; outcome is unknown. |
| c | network | `32e1eb550cbac138b6c98bf4550687594b84aaad` | FAIL; AWS CLI returned exit 252 while parsing/using invalid JSON | Environment/tool invocation failure; the campaign did not establish a network-recovery result. |
| d | network | `2b8677f015deeffa8eba132e3907f3c3764687d9` | IN_PROGRESS; no terminal result recorded | Incomplete attempt; not a pass. |
| e | network | `2b8677f015deeffa8eba132e3907f3c3764687d9` | FAIL; app host 1 did not own partition 5 before isolation | Harness precondition failure; the requested fault did not begin from the required owner state. |
| f | network | `9b3c2ca1f14f1ed2ae435cf6d2df55f7da50e072` | FAIL; workflow was not observed CLAIMED with an owning lease before deadline | Harness precondition was not established; no recovery conclusion is assigned. |
| g | network | `1cbe165f4622302041e89d88d64561cb204128ab` | FAIL; no replacement attempt committed after fault | Recovery progress was not observed; retained evidence does not attribute cause to engine versus harness/environment. |
| h | network | `3dcbf471f1b39fa5f6ac21efcd20053930c8da3f` | FAIL; no replacement attempt committed after fault | Recovery progress was not observed; cause is unresolved from retained evidence. |
| i | network | `eba550f63cb131da1e47a0b8ad2bf9bc9ec2bbfb` | FAIL; no replacement attempt committed after fault | Recovery progress was not observed; cause is unresolved from retained evidence. |
| j | network | `56a54b1f7991e906282105f5184a69ea721d4716` | FAIL; no replacement attempt committed after fault | Recovery progress was not observed; cause is unresolved from retained evidence. |
| k | network | `56a54b1f7991e906282105f5184a69ea721d4716` | FAIL; no replacement attempt committed after fault | Recovery progress was not observed; cause is unresolved from retained evidence. |
| l | network | `7a339becbe1e47964136fa94ba6448da41904bec` | FAIL; no replacement attempt committed after fault | Recovery progress was not observed; cause is unresolved from retained evidence. |
| m | network | `ae2b8de2e7eb1772352622560e285842bea66e77` | FAIL; no replacement attempt committed after fault | Recovery progress was not observed; cause is unresolved from retained evidence. |
| n | network | `785d4eacafa0fa19b5c55edbeba08be268f297ab` | FAIL; no replacement attempt committed after fault | Recovery progress was not observed; cause is unresolved from retained evidence. |
| o | host stop | `785d4eacafa0fa19b5c55edbeba08be268f297ab` | PASS | Historical host-stop PASS; retain as its own run, not as a substitute for the later final matrix. |
| p | host stop | — | MISSING; no retained directory | No artifact or commit is available; outcome is unknown. |
| q | host stop | `be749882d188e84c641c06293a3675ef9ab2f122` | PASS | Historical host-stop PASS; earlier protocol revision. |
| r | host stop | `61b83030dddfcd831c47d974ffa2868cb317ba94` | FAIL; workflow was not found in PostgreSQL | Harness/fixture failure; the target workflow was absent, so no recovery result can be inferred. |
| s | host stop | `61b83030dddfcd831c47d974ffa2868cb317ba94` | FAIL; useful replacement progress was not committed | Progress was not observed; retained evidence does not establish whether the engine or harness/environment caused it. |
| t | host stop | `fdc3ea9e48d01c0c3d0d9a053bdc9b8e5c12a8b7` | FAIL; SQL syntax error in `COALESCE(owner_id::text,)` | Definite harness SQL defect; observation query did not execute. |
| u | host stop | `5020e082b3b53966c4a83bb74691b7f3a2e7ae94` | FAIL; useful replacement progress was not committed | Progress was not observed; cause is unresolved from retained evidence. |
| v | host stop | `d87c55c1510983e07b399e776aa0b1abe6c88372` | FAIL; useful replacement progress was not committed | Progress was not observed; cause is unresolved from retained evidence. |
| w | host stop | — | MISSING; no retained directory | No artifact or commit is available; outcome is unknown. |
| x | host stop | — | MISSING; no retained directory | No artifact or commit is available; outcome is unknown. |
| y | host stop | — | MISSING; no retained directory | No artifact or commit is available; outcome is unknown. |
| z | host stop | — | MISSING; no retained directory | No artifact or commit is available; outcome is unknown. |
| aa | host stop | `ddb48c3e0d40870d226c825184f9b63e8fc881c7` | FAIL; useful replacement progress was not committed | Progress was not observed; cause is unresolved from retained evidence. |
| ab | host stop | `99d5b3121f44cfc55d885946ff38d97bd568e82b` | IN_PROGRESS; no terminal result recorded | Incomplete attempt; not a pass. |
| ac | host stop | — | MISSING; no retained directory | No artifact or commit is available; outcome is unknown. |
| ad | host stop | `ad96ec759afc1a249d36ce7e7d28ff32b207f97a` | IN_PROGRESS; no terminal result recorded | Incomplete attempt; not a pass. |
| ae | host stop | `0c7a07d8511ed5907b0d6ee9c9dfe753046eb9ee` | FAIL; no durable TIMEOUT_REPLACEMENT transition after takeover | Recovery progress was not observed; cause is unresolved from retained evidence. |
| af | host stop | `32c5f63f074312dff1cbbc49d7373b77397a6d96` | FAIL; no durable TIMEOUT_REPLACEMENT transition after takeover | Recovery progress was not observed; cause is unresolved from retained evidence. |
| ag | host stop | `e13e85f32b00ca5dfec49dd7305e0c19b0e4bf44` | PASS | Historical host-stop PASS; earlier protocol revision. |
| ah | host stop | — | MISSING; no retained directory | No artifact or commit is available; outcome is unknown. |
| ai | host stop | `9966d5164c5ce880eff333be70adb3ea296b35cb` | FAIL; no durable TIMEOUT_REPLACEMENT transition after takeover | Recovery progress was not observed; cause is unresolved from retained evidence. |
| aj | host stop | — | MISSING; no retained directory | No artifact or commit is available; outcome is unknown. |
| ak | host stop | `e45c40223ea47efed288f2419997070c4e1589b5` | PASS | Historical host-stop PASS; earlier protocol revision. |
| al | host stop | `d67d61375b25a5e34fad879bc6ff8d5122417c78` | FAIL; harness expected recovery epoch 18 but observed transition epoch 21 | Harness criterion was invalid: per-pass lease acquisition advances epochs, so exact equality to a single takeover epoch was not valid. |
| am | host stop | `2da86ece151e76ce0f4b042fdc0ca056d9d00071` | FAIL; harness expected recovery epoch 122 but observed transition epoch 131 | Same invalid exact-epoch assumption under per-pass acquisition; harness failure, not evidence of a stale write. |
| an | host stop | `b190093af0e8117b3b919dd5d2f300fc182f7539` | PASS | Final-generation host-stop PASS from the initial suffix series. |
| ao | host stop | `8c898fbe8183f37b32ff21dd8326bed47973f5be` | PASS | Final-generation host-stop PASS; retained raw artifact. |
| ap | network | — | MISSING; no retained directory | No artifact or commit is available; outcome is unknown. |
| aq | network | — | MISSING; no retained directory | No artifact or commit is available; outcome is unknown. |
| ar | network with server-side PostgreSQL session termination | `e7eee695d7af957cb54bc5d385047e35b038da43` | PASS | Historical network-labelled variant includes server-side session termination; not a network-partition-only result. |
| as | network with server-side PostgreSQL session termination | `460f423be895ee0a43706580659d3ea67814d0e7` | PASS | Historical terminated-session variant; keep distinct from later network-only runs. |

The initial suffix series contains 12 missing slots (`a`, `b`, `p`, `w`–`z`,
`ac`, `ah`, `aj`, `ap`, `aq`). Their status and cause are unknown; no files were
reconstructed for them.

## Supplemental retained runs

| Run | Arm | Harness commit | Status and retained error | Classification and reasoning |
|---|---|---|---|---|
| round71/run-1 | host setup | `0a69f02baa163d1190598927adbb99583ddc6378` | FAIL; frozen fixture definition differed | Harness fixture/preflight mismatch; no recovery episode. |
| round71/run-2 | host stop | `d0a02cf12aa1e440be3039ff41c12722344f1b6f` | FAIL; expected superseded-result rejection, got durable-receipt conflict | Rejection mismatch; retained output does not establish a successful stale-result observation or distinguish protocol expectation from engine behavior. |
| round71/run-3 | host deployment | `5aa4ad719aace44b1d062e6e712e1dad56ad7383` | FAIL; worker container became unhealthy during readiness | Environment/readiness failure; no fault episode. |
| round71/run-4 | host deployment | `cabf6813616470538dbb028157fbc61e7f70dcd9` | FAIL; remote app reported invalid runtime image ID | Deployment/configuration failure; no fault episode. |
| round71/run-5 | host stop | `2da4b68371a0d573aa46fbeeff4d051d390568b3` | FAIL; workflow not observed CLAIMED with owning lease | Required precondition absent; no recovery conclusion. |
| round71/run-6 | host stop | `0372594f93b11efe00550607575f8daa30bb5c4b` | FAIL; workflow not observed CLAIMED with owning lease | Required precondition absent; no recovery conclusion. |
| round72 | host stop | `f7dd686aac7bf2a315420ae786a6698b1c6b1146` | FAIL; workflow not observed CLAIMED within 300s | Precondition/recovery failure; retained observations do not attribute cause. |
| round72-host-alone | host stop | `f7dd686aac7bf2a315420ae786a6698b1c6b1146` | FAIL; timestamp parse error | Harness date-format defect; episode could not be interpreted. |
| round73-host-stop | host stop | `602ea1c1e4152921b7dd7dcd3e8e9e1c9f309e3d` | PASS | Separate retained host-stop PASS. |
| round73-lock-held | lease-row lock | `602ea1c1e4152921b7dd7dcd3e8e9e1c9f309e3d` | FAIL; remote Compose parser rejected the generated file | Harness command/quoting failure; fault was not run. |
| round73-network-only | network isolation | `602ea1c1e4152921b7dd7dcd3e8e9e1c9f309e3d` | PASS | Historical network-only PASS, before the later strict original-worker result-observation gate. |
| round74-lock-held | lease-row lock | `62d85362388726acb97dc75faaad10a83e1c5e32` | FAIL; lock-holder marker not observed | Fault confirmation failed; not evidence that the lock was held. |
| round75-lock-held | lease-row lock | `127f6bafe033a72933d2b9d0b77348a499281c6e` | FAIL; independent partition progress not observed within about 99.6s | Progress was not observed; cause is not attributable from this artifact alone. |
| round76-lock-held | lease-row lock | `2ca2f584b236895eaaf7f5d38eecc898e03272f8` | FAIL; no replacement-attempt result after first new-owner acquisition | Later investigation showed the harness assumed replacement; the original result could be consumed after lock release. Predicate was too strict. |
| round77-lock-held | lease-row lock | `f7810da5b3f68a12299c358768b3c1fd734eb70d` | FAIL; no replacement-attempt result after first new-owner acquisition | Durable rows showed the original result was consumed after lock release; replacement was not required. Harness expectation mismatch. |
| round78-lock-held | lease-row lock | `bb907f42186e3202be1d2edf6e638cf411a28b1d` | FAIL; refused to overwrite existing evidence directory | Safe no-overwrite guard stopped the run; no new episode occurred. |
| round79-lock-held | lease-row lock | `9b81cb0502de1003e2ffd502b110f3de7c49fed1` | PASS | Corrected lock-held predicate; row-lock observation, same-attempt consumption ordering, and independent partition progress recorded. |
| round80-final | network, host stop, lease-row lock | `48e42e32ff3588d4fd42c8c8a57e4312f941b82a` | PASS | All three arms passed; each reports zero superseded transitions and zero duplicate effect calls for the pure fixture. |
| round81-final | network, host stop, lease-row lock | `48e42e32ff3588d4fd42c8c8a57e4312f941b82a` | PASS | Independent repeat of the three-arm matrix; zero superseded transitions and zero duplicate effect calls for the pure fixture. |
| round82-network-result | network isolation/reconnect | `55a0ff5142c4e589569d273202bedd228199c184` | PASS | Supplemental result-observation run; not the final strict log-matcher version. |
| round83-network-result | network isolation/reconnect | `55a0ff5142c4e589569d273202bedd228199c184` | PASS | Supplemental result-observation repeat; not the final strict log-matcher version. |
| round84-network-final | network isolation/reconnect | `7b329b090d675ac09b303f433120b08ba3e736d6` | FAIL; original worker stale-result log not matched within 90s | Harness log predicate was too strict about retry metadata; not evidence of an accepted stale result. |
| round85-network-final | network isolation/reconnect | `d7c42ef3e4b198a4666625ed5d6f087ded358491` | FAIL; original worker stale-result log not matched within 90s | Harness log matcher did not align with emitted retry/iteration fields; corrected in later code. |
| round86-network-final | strict network-only isolation/reconnect | `0c8165eb3634c9d6dcace370f5d260ab464fa9e5` | PASS | Session termination disabled; original runtime/worker retained; worker’s late result rejected as `STALE_ATTEMPT`; live and offline checker valid. |
| round87-network-final | strict network-only isolation/reconnect | `0c8165eb3634c9d6dcace370f5d260ab464fa9e5` | PASS | Independent strict repeat; session termination disabled; stale result rejection and live/offline checker valid. |

## Final remediation attempts (2026-09-23)

| Attempt directory | Arm | Source commit | Status and retained error | Classification and reasoning |
|---|---|---|---|---|
| `dur049-aws-20260923-round71-final-3ef1a3a` | full campaign setup | `3ef1a3af9a4ce5d22fa8309d2003077c57706a26` | FAIL; Terraform output was parsed as invalid JSON (`Invalid JSON primitive: terraform.exe`) | Local harness output/quoting defect; no campaign result. |
| `dur049-aws-20260923-round71-final-3ef1a3a-terraform-retry1` | AWS preflight | `3ef1a3af9a4ce5d22fa8309d2003077c57706a26` | FAIL; `admin-learning` profile not found | Environment credential/profile failure before AWS provisioning; no campaign result. |
| `dur049-aws-20260923-round71-final-3ef1a3a-aws-retry1` | full campaign | `3ef1a3af9a4ce5d22fa8309d2003077c57706a26` | FAIL; owner-lock marker omitted required `idle_timeout_scope` | Earlier arms ran, but the required owner-lock evidence schema was incomplete; not a full pass. |
| `dur049-aws-20260923-round71-final-4ebef2b` | full campaign | `4ebef2b0109ae7f55ed4698c9a61668bd94c5724` | FAIL; no first post-fault acquisition by a new owner observed | Required takeover observation absent; no recovery conclusion. |
| `dur049-aws-20260923-round71-final-881966a` | owner-lock isolation | `881966add1dcbec9762f273d86d4be57d01d0d88` | FAIL; actual runtime backend not observed idle in transaction while hook held `ConsumeResult` | Fault precondition was not proven; no owner-isolation result. |
| `dur049-aws-20260923-round71-r131` | owner-lock deployment | `cbd1a9ea1c97b0d7147a5640ec0fa80f24ccdea4` | FAIL; remote dependency Compose reported `service "postgres" is not running` | Deployment/setup failure; no owner-lock episode. |
| `dur049-aws-20260923-round71-r131-readiness-retry1` | network/readiness setup | `cbd1a9ea1c97b0d7147a5640ec0fa80f24ccdea4` | FAIL; remote readiness command exited 1 | Required runtime readiness was not established; retained output does not support a recovery conclusion. |
| `dur049-aws-20260923-round71-r131-ready1` | dependency preflight | `cbd1a9ea1c97b0d7147a5640ec0fa80f24ccdea4` | FAIL; observed dependency server settings `10s|0` did not match declared reaping settings | Configuration precondition failed; no owner-isolation result. |
| `dur049-aws-20260923-round71-r131-runtime-rebuild1` | owner-lock isolation | `6087e4faab6eaec42f3ab14cb09acb461cb16f4c` | FAIL; stale `ReleaseLease` observation changed the peer-owned lease row | Rejected by the campaign safety gate; the later stable-lease comparison and full run supersede this attempt, not its retained failure. |
| `dur049-aws-20260923-round71-r131-stable-lease1` | owner-lock + same-owner epoch control | `04fb8a1d99000ec3021fc47b24a4814914a2057a` | FAIL; same-owner probe output was marked FAIL although stale transition was rejected | R129 reporting-order defect: the PASS predicate ran before cleanup evidence was populated. Superseded by the dedicated R129 control and full campaign after `0adbac0`. |
| `dur049-aws-20260923-r129-control-0adbac0` | owner-lock isolation + same-owner epoch control | `0adbac0816a0a65d0ccafed6dae6c8efd717edec` | `OWNER_LOCK_ISOLATION_PASS_OTHER_ARMS_NOT_REQUESTED`; same-owner control PASS | Focused control only, not a full matrix. The probe retained and terminalized its workflow without an outbox row; after consumer drain, the global open-obligation set was unchanged. |
| `dur049-aws-20260923-full-0adbac0` | four-arm full campaign | `0adbac0816a0a65d0ccafed6dae6c8efd717edec` | PASS | Preserved-process network isolation, forced application-host stop, live-holder contention, and original-runtime owner-lock isolation all passed. The R129 control recorded the active peer lease, rejected the stale transition without changing revision, retained a CANCELED workflow with zero outbox rows, and left the global open-obligation set unchanged after consumer drain. |

## Interpretation boundary

The strict final network-only sample is four runs: two network episodes in
rounds 80/81 and two standalone runs in rounds 86/87. Each records server-side
session termination disabled and the original worker's late result rejected as
`STALE_ATTEMPT`; the older `ar`/`as` variants used server-side session
termination and remain separately labelled. The three-arm rounds 80 and 81
provide six successful episodes (two per arm). The pure fixture produced zero
effect records and zero duplicate effect calls; it did not exercise a
non-idempotent external effect. The earlier closeout artifact separately
records five historical poison obligations. The full `0adbac0` campaign's
R129 control does not create more obligations: its before/after global sets
match after consumer drain.
