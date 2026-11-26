"""Join raw Prometheus evaluations to their exact exporter visibility samples."""

from bisect import bisect_right
from decimal import Decimal

from j2_offline import utc_ns
from j3_replay import ALERTS, SECOND, WINDOWS, require, unique


def epoch_ns(value):
    ns = Decimal(str(value)) * SECOND
    require(ns == int(ns), "non-integral Prometheus timestamp")
    return int(ns)


def evaluation_rows(series, stream, target, start_ns, end_ns, rule_health):
    """Input matrices contain raw samples, including original evaluation times."""
    rules = unique(rule_health, lambda rule: rule.get("name"))
    required_rules = {
        *(
            f"j3:{name}"
            for name in (
                "expected",
                "valid",
                "oldest_fire",
                "oldest_clear",
                "burn_fire",
                "burn_clear",
            )
        ),
        *ALERTS,
    }
    require(set(rules) == required_rules, "missing or unexpected rule health")
    require(
        all(rule["health"] == "ok" and not rule.get("lastError") for rule in rules.values()),
        "rule evaluation error",
    )
    indexed = unique(
        series,
        lambda s: (
            s["metric"]["__name__"],
            s["metric"].get("window"),
            s["metric"].get("alertname"),
        ),
    )

    def metric(name, window=None, alert=None):
        s = indexed.get((name, window, alert))
        if s is None:
            return {}
        labels = s["metric"]
        require(all(labels.get(k) == v for k, v in target.items()), "wrong Prometheus target scope")
        if name == "ALERTS":
            require(labels.get("alertstate") == "firing", "non-firing alert matrix")
        return unique(((epoch_ns(t), v) for t, v in s["values"]), lambda pair: pair[0])

    up = metric("up")
    up_ticks = sorted(up)
    expected = metric("j3:expected")
    at_ticks = sorted(t for t in expected if start_ns <= t <= end_ns)
    require(
        at_ticks
        and all(expected[t][1] == "1" for t in at_ticks)
        and at_ticks[0] - start_ns <= 5 * SECOND
        and end_ns - at_ticks[-1] <= 5 * SECOND,
        "missing enclosing rule evaluations",
    )
    valid = metric("j3:valid")
    firing = {alert: metric("ALERTS", alert=alert) for alert in ALERTS}
    samples = unique(stream, lambda sample: sample["sample_ns"] / SECOND)
    signals = {
        name: metric("j3_" + name)
        for name in (
            "sample_timestamp_seconds",
            "demand_timestamp_seconds",
            "claimable_attempts",
            "oldest_claimable_age_seconds",
        )
    }
    windows = {
        window: {field: metric("j3_" + field, window) for field in ("eligible", "bad")}
        for window in WINDOWS
    }

    def value_at(signal, tick):
        require(tick in signal, f"required metric missing at scrape {tick}")
        return signal[tick][1]

    evaluations = []
    for at in at_ticks:
        row = {
            "at_ns": at,
            "rule_ok": True,
            "valid": at in valid,
            "firing": [alert for alert, values in firing.items() if at in values],
        }
        if row["valid"]:
            i = bisect_right(up_ticks, at) - 1
            require(i >= 0, "valid evaluation has no matching scrape")
            scrape = up_ticks[i]
            require(up[scrape][1] == "1" and valid[at][1] == "1", "invalid success marker")
            source = float(value_at(signals["sample_timestamp_seconds"], scrape))
            require(source in samples, f"missing visibility evidence for source {source}")
            sample = samples[source]
            demand = sample["demand"]
            require(demand is not None, "valid evaluation has no demand witness")
            demand_ns = utc_ns(demand["sampled_at"])
            require(
                float(value_at(signals["demand_timestamp_seconds"], scrape)) == demand_ns / SECOND,
                "demand timestamp differs from the exporter witness",
            )
            row.update(
                sample_ns=sample["sample_ns"],
                scrape_ns=scrape,
                demand_ns=demand_ns,
                oldest_claimable_age_seconds=float(
                    value_at(signals["oldest_claimable_age_seconds"], scrape)
                ),
                claimable_attempts=int(value_at(signals["claimable_attempts"], scrape)),
                windows={
                    window: {
                        field: int(value_at(signal, scrape)) for field, signal in values.items()
                    }
                    for window, values in windows.items()
                },
            )
        evaluations.append(row)
    return evaluations
