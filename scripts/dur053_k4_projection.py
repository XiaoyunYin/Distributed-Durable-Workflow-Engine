"""Closed, compact publication of verified K4 metrics; no acceptance decisions."""

from collections import Counter
from copy import deepcopy

from dur053_k4 import registry

LIST_BOUND = 8
COPY_FIELDS = frozenset(
    [
        "schema",
        "episode",
        "replacement_for",
        "status",
        "intervention_outcome",
        "budget_scope",
        "reaction",
        "partial_confirmation_issues",
        "complete",
        "valid",
        "issues",
        "latency",
        "no_active_fault",
        "offered",
        "accepted",
        "rejected",
        "uncertain",
        "unsent",
        "rates",
        "throughput",
        "slo",
        "abandoned",
        "wasted_original_seconds",
        "reexecuted_seconds",
        "busy_seconds",
        "sigterm_to_exit",
        "results_during_drain",
        "assignment_change_counts",
        "http",
        "submission_errors",
        "duplicate_executions",
        "executed_iterations",
        "canceled_iterations",
        "claimed_unexecuted",
        "accepted_results",
        "accepted_results_per_executed_non_canceled_iteration",
        "retryable_receipts",
        "final_failure_receipts",
        "stale_committed",
        "stale_rejected",
        "evidence_only",
        "superseded_owner_transitions",
        "duplicate_effects",
        "oldest_claimable_age",
        "scale_up_delay",
        "limiting_component",
        "replicas",
        "limitations",
        "pair_binding",
        "fault_window",
        "fault_minus_no_active_fault_p99",
        "polling_observation_error",
        "safety",
        "scale_down_removal",
        "metrics",
        "missing_exports",
        "evidence",
    ]
)
OMIT_FIELDS = frozenset(["busy_removals"])  # Defective v1 retention is not a published metric.
RAW_FIELDS = frozenset(
    [
        "assignment_stalls",
        "observer_gaps",
        "readiness_endpoint_delay",
        "recovery",
        "partition_reacquisition",
    ]
)
ROW_FIELDS = {
    "assignment_stalls": [
        "change",
        "trigger_uid",
        "consumers",
        "assignment_only",
        "useful_disposition",
        "total",
        "censored",
        "eligible_undisposed",
        "assignment_only_lower",
        "total_lower",
        "longest_active",
    ],
    "readiness_endpoint_delay": ["pod_uid", "lower", "upper"],
    "recovery": ["workflow", "replacement", "first_useful", "terminal", "censored", "deadline"],
    "partition_reacquisition": [
        "partition",
        "old_epoch",
        "mode",
        "seconds",
        "first_useful",
        "terminal_progress",
    ],
}
COMPONENTS = {
    "assignment_stalls": [
        "assignment_only",
        "useful_disposition",
        "total",
        "assignment_only_lower",
        "total_lower",
        "longest_active",
    ],
    "readiness_endpoint_delay": ["lower", "upper"],
    "recovery": ["replacement", "first_useful", "terminal", "deadline"],
    "partition_reacquisition": ["seconds", "first_useful"],
}


def bounded_labels(values):
    """Keep small identity lists; disclose exact counts and truncation for larger ones."""
    values = list(values)
    return dict(
        count=len(values), values=deepcopy(values[:LIST_BOUND]), truncated=len(values) > LIST_BOUND
    )


def raw_projection(key, rows):
    from dur053_k4_evidence import population

    if key == "observer_gaps":
        return dict(count=len(rows), population=population(rows))
    allowed = set(ROW_FIELDS[key])
    if any(set(row) != allowed for row in rows):
        raise ValueError(f"unknown or missing {key} row field")
    censored = sum(row.get("censored", False) for row in rows)
    components = {}
    for component in COMPONENTS[key]:
        # B.7 excludes censored changes from stall timing populations. Their
        # observed lower bounds remain separate populations, never imputed ends.
        excluded = [
            row
            for row in rows
            if key == "assignment_stalls"
            and component in {"assignment_only", "useful_disposition", "total"}
            and row["censored"]
        ]
        included = [row for row in rows if row not in excluded]
        absent = [row for row in included if row[component] is None]
        components[component] = population(
            [row[component] for row in included if row[component] is not None],
            missing=sum(not row.get("censored", False) for row in absent),
            censored=len(excluded) + sum(row.get("censored", False) for row in absent),
        )
    labels = {}
    for field in ROW_FIELDS[key]:
        if field in COMPONENTS[key] or field == "censored":
            continue
        values = [row[field] for row in rows]
        if field == "terminal_progress":
            labels[field] = dict(
                count=sum(map(len, values)),
                population=population([value for group in values for value in group]),
                units="source absolute timestamps; no new elapsed-time derivation",
            )
        elif field in {"consumers", "eligible_undisposed"}:
            labels[field] = dict(
                count=sum(map(len, values)),
                per_row_count=population([len(group) for group in values]),
                identities=bounded_labels(sorted({value for group in values for value in group})),
            )
        elif field == "mode":
            labels[field] = dict(sorted(Counter(values).items()))
        else:
            labels[field] = bounded_labels(values)
    return dict(count=len(rows), censored=censored, components=components, labels=labels)


def episode_projection(metric, removal=None):
    unknown = set(metric) - COPY_FIELDS - RAW_FIELDS - OMIT_FIELDS
    if unknown:
        raise ValueError("unknown K4 metric fields: " + ", ".join(sorted(unknown)))
    result = {}
    for key, value in sorted(metric.items()):
        if key in OMIT_FIELDS:
            continue
        if key in RAW_FIELDS:
            result[key] = raw_projection(key, value)
        else:
            # These fields are scalars, labels, bindings or already summarised
            # objects. Diagnostic labels (issues/limitations) always stay intact.
            result[key] = deepcopy(value)
    for key in ("issues", "intervention_outcome", "limitations"):
        result.setdefault(key, None)  # Not supplied by stopped-intake metrics.
    result["busy_pods_removed"] = deepcopy(removal)
    return result


def reporting_projection(metrics, manifests, removals=None):
    projected = [
        episode_projection(metric, (removals or {}).get(metric["episode"])) for metric in metrics
    ]
    by_id = {m["replacement_for"] or m["episode"]: i for i, m in enumerate(projected)}
    provenance = {d["episode"]: (m, d) for m in manifests for d in m["dispositions"]}
    results = []
    for metric in projected:
        manifest, disposition = provenance[metric["episode"]]
        results.append(
            dict(
                **{k: metric[k] for k in ("episode", "status", "valid", "complete", "issues")},
                disposition=deepcopy(disposition),
                job=manifest["job"],
                run_id=manifest["run_id"],
            )
        )
    cells = []
    registered = registry()
    for event, duration, arm in sorted({(r["event"], r["duration"], r["arm"]) for r in registered}):
        members = [
            r for r in registered if (r["event"], r["duration"], r["arm"]) == (event, duration, arm)
        ]
        indices = [by_id[r["episode"]] for r in members if r["episode"] in by_id]
        cells.append(
            dict(
                event=event,
                duration=duration,
                arm=arm,
                kind=members[0]["kind"],
                registered_population=5 if members[0]["kind"] == "core" else 3,
                registered_episodes=[r["episode"] for r in members],
                episode_metric_indices=indices,
                episodes=[projected[i]["episode"] for i in indices],
                missing=[r["episode"] for r in members if r["episode"] not in by_id],
                failed=[
                    projected[i]["episode"]
                    for i in indices
                    if projected[i]["status"] != "PASS"
                    or not projected[i]["valid"]
                    or not projected[i]["complete"]
                ],
            )
        )
    return dict(episode_metrics=projected, cell_metrics=cells, episode_results=results)
