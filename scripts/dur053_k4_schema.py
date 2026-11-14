"""Closed K4 evidence contracts, shared by intake and offline re-derivation.

Every object has exactly its declared fields. Nullable values remain explicit;
missing observations are never silently converted to zeros.
"""

from __future__ import annotations

import json
from pathlib import Path

from dur053_k3_policy import require
from dur053_pilot import finite


def obj(**fields):
    return fields


def array(item):
    return [item]


def nullable(item):
    return (item, type(None))


def enum(*values):
    return frozenset(values)


NUMBER = "number"
HASH = "sha256"
SHA = "sha1"
IDENTITY = obj(namespace=str, workflow=str, node=str, iteration=int, attempt=int, token_hash=HASH)
MEMBER = obj(at=NUMBER, present=bool, width=NUMBER)
SESSION = obj(
    session=str,
    pod_uid=str,
    container_id=str,
    role=str,
    start=NUMBER,
    last_alive=NUMBER,
    death=nullable(NUMBER),
    death_evidence=str,
    exit_code=nullable(int),
    exit_signal=nullable(int),
    signals=array(obj(at=NUMBER, signal=int, evidence=str)),
    readiness=array(MEMBER),
    endpoints=array(MEMBER),
)
INVOCATION = obj(
    id=str,
    identity=IDENTITY,
    session=str,
    start=NUMBER,
    finish=nullable(NUMBER),
    last_alive=NUMBER,
    abort=nullable(NUMBER),
    abort_evidence=str,
    outcome=str,
)
COMMIT_BRACKET = obj(
    statement=NUMBER, finalized=NUMBER, transaction=str, session=str, pod_uid=str, container_id=str
)
CLAIM = obj(
    identity=IDENTITY, session=str, order=int, at=NUMBER, commit_bracket=nullable(COMMIT_BRACKET)
)
HISTORY = obj(
    identity=IDENTITY,
    order=int,
    at=NUMBER,
    kind=enum("claim", "supersede", "cancel", "receipt", "transition", "terminal"),
    receipt=str,
    partition=int,
    owner=str,
    epoch=int,
    scheduler_epoch=nullable(int),
    actor_kind=str,
    revision=int,
)
RECEIPT = obj(
    id=str,
    identity=IDENTITY,
    session=str,
    order=int,
    at=NUMBER,
    category=enum("committed", "rejected", "evidence_only"),
    outcome=enum("success", "retryable", "failure"),
    rejection=str,
)
WORKFLOW = obj(
    workflow=str,
    key=str,
    state=str,
    terminal=nullable(NUMBER),
    clock_width=NUMBER,
    first_observed=nullable(NUMBER),
)
HTTP = obj(
    role=str,
    request=str,
    body_hash=HASH,
    attempt=int,
    operation=str,
    at=NUMBER,
    status=int,
    error=str,
)
WITNESS = obj(
    session=str,
    pod_uid=str,
    container_id=str,
    sha256=HASH,
    data=str,
    acknowledged=array(str),
    complete=bool,
    errors=int,
)
LEASE = obj(
    order=int,
    at=NUMBER,
    kind=enum("acquire", "release"),
    partition=int,
    owner=str,
    epoch=int,
    session=str,
    transaction=str,
)
CALLBACK = obj(
    change=str,
    event=enum("revoke", "rebalance_start", "assigned", "stable", "disposed"),
    at=NUMBER,
    consumer=str,
    session=str,
    generation=int,
    partitions=array(int),
    survivors=array(str),
    trigger_uid=str,
    arrival=str,
    disposition_order=nullable(int),
)
ARRIVAL = obj(
    id=str,
    key=str,
    workflow=str,
    phase=enum("warmup", "measured"),
    scheduled=NUMBER,
    sent=nullable(NUMBER),
    outcome=enum("accepted", "rejected", "uncertain", "unsent"),
    status=int,
    error=str,
    request_hash=HASH,
    attempts=int,
)
SAMPLE = obj(
    at=NUMBER,
    source=str,
    width=NUMBER,
    gap=NUMBER,
    error=str,
    oldest_claimable=nullable(NUMBER),
    active_claims=array(obj(identity=IDENTITY, pod_uid=str)),
    pods=array(obj(pod_uid=str, role=str, ready=bool, eligible=bool)),
    replicas=int,
    busy_slots=int,
    total_slots=int,
)
OVERLAP = obj(
    role=enum("worker", "scheduler", "api", "relay"),
    session=str,
    pod_uid=str,
    at=NUMBER,
    end=NUMBER,
    acknowledgment=str,
    request=str,
    publication=str,
    workflows=array(str),
    claim=nullable(IDENTITY),
    partition=nullable(int),
    owner=str,
    epoch=nullable(int),
)
EVICTION = obj(
    pod_uid=str,
    role=str,
    replicas=int,
    requested=NUMBER,
    scheduled=NUMBER,
    responded=NUMBER,
    status=int,
    allowed_before=int,
    allowed_after=int,
    denial=str,
    replaced_by=str,
    replacement_ready=nullable(NUMBER),
    replacement_ready_upper=nullable(NUMBER),
)
TARGET = obj(
    session=str,
    pod_uid=str,
    container_id=str,
    deleted=nullable(NUMBER),
    replacement_uid=str,
    replacement_ready=nullable(NUMBER),
    restart_before=int,
    restart_after=int,
)
CONFIRMATION = obj(
    schema=enum("dur053.k4-confirmation.v1"),
    episode=str,
    kind=enum("rollout", "rollback", "scale-down", "kill", "eviction"),
    target=str,
    intervention=NUMBER,
    reaction=nullable(NUMBER),
    observed_overlap_first_at=nullable(NUMBER),
    completed=nullable(NUMBER),
    removed=nullable(NUMBER),
    overlap=array(OVERLAP),
    targets=array(TARGET),
    image_a=str,
    image_a_prime=str,
    binary_identity=HASH,
    binary_proof=obj(runtime_a=HASH, runtime_a_prime=HASH, worker_a=HASH, worker_a_prime=HASH),
    source_commit=SHA,
    build=str,
    lineage_a=str,
    lineage_a_prime=str,
    revision_a=str,
    revision_a_prime=str,
    rollout_status=str,
    setup_completed=nullable(NUMBER),
    chaos_version=str,
    chaos_kind=str,
    chaos_object=str,
    injection_status=str,
    evictions=array(EVICTION),
    eviction_retry_interval=NUMBER,
    roles=array(obj(role=str, replicas=int, disruptions_allowed=int, pod_uids=array(str))),
    no_cordon=bool,
    pdb_bypassed=bool,
    harness_targeted=bool,
    deletion_cost_used=bool,
)
HOLD_STATE = obj(
    host_start=NUMBER,
    host_end=NUMBER,
    observer=NUMBER,
    path=str,
    existed=bool,
    exists=bool,
    mtime=NUMBER,
)
HOLD = obj(
    role=str,
    hold_set=HOLD_STATE,
    hold_released=nullable(HOLD_STATE),
    bound_expiry=NUMBER,
    bound_expired=bool,
    measured_arrivals=int,
)
EPISODE = obj(
    schema=enum("dur053.k4-episode.v1"),
    episode=str,
    replacement_for=str,
    arm=enum("baseline", "treatment"),
    job=str,
    run_id=str,
    job_id=str,
    head_sha=SHA,
    tree_sha=SHA,
    freeze_sha256=HASH,
    namespace=str,
    status=enum("PASS", "FAIL", "invalid", "unexercised", "interrupted"),
    metrics=enum("derivable", "not-derivable"),
    missing_exports=array(str),
    budget_scope=enum("registered-campaign", "engineering-validation"),
    measurement_claim=bool,
    seed=int,
    hold=nullable(HOLD),
    rate=NUMBER,
    L=NUMBER,
    W=NUMBER,
    warmup_start=NUMBER,
    confirmation_deadline=NUMBER,
    intervention=NUMBER,
    window_end=NUMBER,
    reconciliation_end=NUMBER,
    timeout=bool,
    witness_available=bool,
    witness_activity=array(NUMBER),
    quiet_since=nullable(NUMBER),
    clock_brackets=array(obj(start=NUMBER, end=NUMBER, width=NUMBER)),
    baseline_flags=array(str),
    images=obj(runtime=str, worker=str),
    files=obj(
        timeline=HASH,
        leases=HASH,
        confirmation=HASH,
        arrivals=HASH,
        kafka=HASH,
        witness=HASH,
        observers=HASH,
    ),
)
INFRA_REASONS = enum(
    "setup-before-warmup",
    "harness-oom",
    "harness-timeout",
    "collection-failure",
    "runner-loss",
    "harness-confirmation-miss",
)
DISPOSITION = obj(
    episode=str,
    replacement_for=str,
    classification=enum("valid", "infrastructure-invalid", "unexercised"),
    reason=enum("", *INFRA_REASONS),
    evidence=array(str),
)
JOB = obj(
    schema=enum("dur053.k4-job.v1"),
    job=str,
    run_id=str,
    job_id=str,
    head_sha=SHA,
    merge_ref_sha=SHA,
    tree_sha=SHA,
    freeze_sha256=HASH,
    budget_scope=enum("registered-campaign", "engineering-validation"),
    measurement_claim=bool,
    status=enum("PASS", "FAIL", "invalid", "unexercised", "interrupted", "CARRY_OVER"),
    registered_live_seconds=int,
    charged_seconds=int,
    elapsed_live_seconds=nullable(int),
    live_start=nullable(NUMBER),
    live_end=nullable(NUMBER),
    cleanup=enum("PASS", "UNTRUSTWORTHY"),
    episodes=array(str),
    assigned=array(str),
    pending=array(str),
    replacement_for=str,
    dispositions=array(DISPOSITION),
    intake_stopped=array(str),
    artifact_sha256="hashmap",
)
SAFETY = obj(
    episodes=array(str),
    unavailable=array(str),
    lost_accepted=int,
    duplicate_accepted=int,
    stale_committed=int,
    superseded_owner_transitions=int,
    failed_workflows=int,
    canceled_workflows=int,
    http_5xx=int,
    passed=bool,
)
SCHEMAS = {
    "dur053.k4-continuation-acks.v1": obj(
        schema=enum("dur053.k4-continuation-acks.v1"),
        acknowledgements=array(
            obj(
                job_id=str,
                manifest_sha256=HASH,
                reviewer=enum("Claude"),
                acknowledged=bool,
                review_round=int,
            )
        ),
    ),
    "dur053.k4-progress.v1": obj(schema=enum("dur053.k4-progress.v1"), job=str, started=array(str)),
    "dur053.k4-harness.v1": obj(
        schema=enum("dur053.k4-harness.v1"),
        episode=str,
        reason=INFRA_REASONS,
        component=enum("setup", "collector", "driver", "runner"),
        at=NUMBER,
        warmup_start=nullable(NUMBER),
        source=str,
    ),
    "dur053.k4-episode.v1": EPISODE,
    "dur053.k4-job.v1": JOB,
    "dur053.k4-confirmation.v1": CONFIRMATION,
    "dur053.k4-timeline.v1": obj(
        schema=enum("dur053.k4-timeline.v1"),
        episode=str,
        namespace=str,
        sessions=array(SESSION),
        claims=array(CLAIM),
        invocations=array(INVOCATION),
        history=array(HISTORY),
        receipts=array(RECEIPT),
        workflows=array(WORKFLOW),
        http=array(HTTP),
        witness=array(WITNESS),
    ),
    "dur053.k4-lease-audit.v1": obj(
        schema=enum("dur053.k4-lease-audit.v1"), episode=str, rows=array(LEASE)
    ),
    "dur053.k4-kafka.v1": obj(schema=enum("dur053.k4-kafka.v1"), episode=str, rows=array(CALLBACK)),
    "dur053.k4-arrivals.v1": obj(
        schema=enum("dur053.k4-arrivals.v1"), episode=str, rows=array(ARRIVAL)
    ),
    "dur053.k4-witness.v1": obj(
        schema=enum("dur053.k4-witness.v1"), episode=str, rows=array(WITNESS)
    ),
    "dur053.k4-observers.v1": obj(
        schema=enum("dur053.k4-observers.v1"), episode=str, rows=array(SAMPLE)
    ),
}


def validate(value, spec, path="$", *, label="k4-schema"):
    if spec is CONFIRMATION and isinstance(value, dict):
        value = {"observed_overlap_first_at": None, **value}

    def check(ok):
        require(ok, f"{label} {path}")

    if isinstance(spec, dict):
        check(type(value) is dict and value.keys() == spec.keys())
        for key, child in spec.items():
            validate(value[key], child, path + "." + key, label=label)
    elif isinstance(spec, list):
        check(type(value) is list)
        for i, row in enumerate(value):
            validate(row, spec[0], f"{path}[{i}]", label=label)
    elif isinstance(spec, tuple):
        if value is not None:
            validate(value, spec[0], path, label=label)
    elif isinstance(spec, frozenset):
        check(type(value) is str and value in spec)
    elif spec == NUMBER:
        finite(value, f"{label} {path}", zero=True)
    elif spec in (HASH, SHA):
        check(
            type(value) is str
            and len(value) == (64 if spec == HASH else 40)
            and all(c in "0123456789abcdef" for c in value)
        )
    elif spec == "hashmap":
        check(type(value) is dict)
        for key, digest in value.items():
            check(type(key) is str)
            validate(digest, HASH, path + "." + key)
    else:
        check(type(value) is spec)


def pairs(rows):
    result = {}
    for key, value in rows:
        require(key not in result, "k4-json-duplicate-key")
        result[key] = value
    return result


def load(path: Path) -> dict:
    value = json.loads(path.read_bytes(), object_pairs_hook=pairs)
    require(type(value) is dict and value.get("schema") in SCHEMAS, "k4-schema-name")
    if value["schema"] == "dur053.k4-confirmation.v1" and "reaction" not in value:
        value["reaction"] = None  # Retained pre-R301 bytes; never rewrite originals.
    if value["schema"] == "dur053.k4-confirmation.v1":
        value.setdefault("observed_overlap_first_at", None)
    validate(value, SCHEMAS[value["schema"]])
    return value
