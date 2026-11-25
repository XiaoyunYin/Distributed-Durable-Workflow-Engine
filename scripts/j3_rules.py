"""Render the registered rules for one immutable target and their promtool vectors."""

import argparse
import json
from pathlib import Path

KEY = "campaign,namespace"
WINDOWS = ("2m", "10m", "all")
FIELDS = {
    "sql_ok": "== 1",
    "demand_ok": "== 1",
    "sample_timestamp_seconds": None,
    "demand_timestamp_seconds": None,
    "oldest_claimable_age_seconds": ">= 0",
    "claimable_attempts": ">= 0",
    "active_claims": ">= 0",
}
DOMAIN = ("OldestClaimable", "DeadlineBudgetBurn")
LABELS = {"campaign": "j3-unit", "namespace": "j3-target"}


def render(labels):
    matcher = ",".join(f"{k}={json.dumps(labels[k])}" for k in KEY.split(","))

    def series(field, window=None):
        selection = 'job="j3",' + matcher
        if window:
            selection += f',window="{window}"'
        return f"{field}{{{selection}}}"

    def metric(field, window=None):
        return series("j3_" + field, window)

    def presence(expr):
        return f"group by ({KEY}) ({expr})"

    up = series("up")
    fresh_up = f"{up} == 1 and (time() - timestamp({up}) >= 0 and time() - timestamp({up}) <= 5)"
    valid = [f"count by ({KEY}) ({up}) == 1", presence(fresh_up)]
    match = f"on({KEY},job,instance)"
    for field, comparison in FIELDS.items():
        m = metric(field)
        if comparison:
            condition = f"{m} {comparison}"
        else:
            condition = f"time() - {m} >= 0 and time() - {m} <= 5"
        valid += [
            f"count by ({KEY}) ({m}) == 1",
            presence(f"{condition} and {match} (timestamp({m}) == {match} timestamp({up}))"),
        ]
    count, age = metric("claimable_attempts"), metric("oldest_claimable_age_seconds")
    valid.append(presence(f"{count} > 0 or ({count} == 0 and {age} == 0)"))
    for window in WINDOWS:
        e, b = (metric(f, window) for f in ("eligible", "bad"))
        valid += [f"count by ({KEY}) ({m}) == 1" for m in (e, b)]
        valid += [presence(f"timestamp({m}) == {match} timestamp({up})") for m in (e, b)]
        valid.append(presence(f"{b} >= 0 and ({b} <= {e})"))

    def recording(name, expr):
        return {"record": "j3:" + name, "expr": expr}

    def normalized(metric_name):
        return f"j3:{metric_name}{{{matcher}}}"

    def latch(name, fire, clear):
        previous = presence(f'ALERTS{{{matcher},alertname="{name}",alertstate="firing"}} == 1')
        return {
            "alert": name,
            "expr": f"{normalized(fire)} or ({previous} unless on({KEY}) {normalized(clear)})",
        }

    def volume(window):
        return presence(metric("eligible", window) + " >= 100")

    def burn(window, operator):
        # 14.4 times a 1% budget is exactly 18/125 of eligible admissions.
        return presence(
            f"{metric('bad', window)} * 125 {operator} {metric('eligible', window)} * 18"
        )

    valid_name = normalized("valid")
    sufficient = f"{valid_name} and {volume('2m')} and {volume('10m')}"
    return {
        "groups": [
            {
                "name": "j3",
                "interval": "5s",
                "rules": [
                    {**recording("expected", "vector(1)"), "labels": labels},
                    recording("valid", " and ".join(f"({expr})" for expr in valid)),
                    recording(
                        "oldest_fire",
                        f"{valid_name} and {presence(f'{count} > 0 and {age} > 28.540')}",
                    ),
                    recording(
                        "oldest_clear",
                        f"{valid_name} and {presence(f'{count} == 0 or {age} <= 28.540')}",
                    ),
                    recording(
                        "burn_fire", f"{sufficient} and {burn('2m', '>')} and {burn('10m', '>')}"
                    ),
                    recording(
                        "burn_clear",
                        f"{sufficient} and ({burn('2m', '<=')} or {burn('10m', '<=')})",
                    ),
                    latch(DOMAIN[0], "oldest_fire", "oldest_clear"),
                    latch(DOMAIN[1], "burn_fire", "burn_clear"),
                    {
                        "alert": "ScrapeAbsent",
                        "expr": f"{normalized('expected')} unless {valid_name}",
                        "for": "5s",
                    },
                ],
            }
        ]
    }


def vector_tests():
    default = {
        "up": 1,
        "sql_ok": 1,
        "demand_ok": 1,
        "sample_timestamp_seconds": "time",
        "demand_timestamp_seconds": "time",
        "oldest_claimable_age_seconds": 1,
        "claimable_attempts": 1,
        "active_claims": 0,
        **{f"{name}:{w}": n for w in WINDOWS for name, n in (("eligible", 125), ("bad", 0))},
    }
    hot = {"oldest_claimable_age_seconds": 28.541, "bad:2m": 19, "bad:10m": 19}
    cases = []

    def case(name, segments, expected, extras=()):
        flags = []
        for duration, overrides in segments:
            flags += [{**default, **overrides}] * duration
        inputs = []
        for key in default:
            name_part, _, window = key.partition(":")
            metric = "up" if key == "up" else "j3_" + name_part
            values = [i if row[key] == "time" else row[key] for i, row in enumerate(flags)]
            labels = {**LABELS, "job": "j3", "instance": "exporter"}
            if window:
                labels["window"] = window
            if any(v != "_" for v in values):
                selector = ",".join(f"{k}={json.dumps(v)}" for k, v in labels.items())
                inputs.append(
                    {"series": f"{metric}{{{selector}}}", "values": " ".join(map(str, values))}
                )
        checks = [
            {
                "eval_time": f"{at}s",
                "alertname": alert,
                "exp_alerts": [{"exp_labels": LABELS}] if alert in firing else [],
            }
            for at, firing in expected.items()
            for alert in (*DOMAIN, "ScrapeAbsent")
        ]
        cases.append(
            {"name": name, "input_series": inputs + list(extras), "alert_rule_test": checks}
        )

    empty = {key: "_" for key in default}
    idle = {
        **{f"eligible:{w}": 0 for w in WINDOWS},
        "claimable_attempts": 0,
        "oldest_claimable_age_seconds": 0,
    }
    case(
        "no traffic, strict thresholds, and one hot window",
        [
            (5, idle),
            (5, {"oldest_claimable_age_seconds": 28.539, "bad:2m": 17, "bad:10m": 17}),
            (5, {"oldest_claimable_age_seconds": 28.540, "bad:2m": 18, "bad:10m": 18}),
            (5, {**hot, "bad:10m": 18}),
            (5, {}),
            (6, {**hot, "bad:2m": 18}),
        ],
        {0: (), 5: (), 10: (), 15: DOMAIN[:1], 20: (), 25: DOMAIN[:1]},
    )
    case(
        "99 versus 100, mature stalled admissions, late completion leaves a miss",
        [
            (5, {"eligible:2m": 99, "bad:2m": 99, "bad:10m": 125}),
            (5, {"eligible:2m": 100, "bad:2m": 100, "bad:10m": 125}),
            (5, {"eligible:2m": 101, "bad:2m": 101, "bad:10m": 125}),
            (6, {"eligible:2m": 101, "bad:2m": 101, "bad:10m": 125}),
        ],
        {0: (), 5: DOMAIN[1:], 10: DOMAIN[1:], 15: DOMAIN[1:]},
    )
    foreign = {
        "series": 'up{campaign="j3-unit",namespace="foreign",job="j3",instance="exporter"}',
        "values": "1x30",
    }
    for name, invalid in (
        ("disappeared target with wrong-namespace scrape", empty),
        ("up zero", {"up": 0}),
        ("demand read failure", {"demand_ok": 0}),
        ("SQL failure under HTTP 200", {"sql_ok": 0}),
        ("stale demand", {"demand_timestamp_seconds": 0}),
        ("expired SQL timestamp", {"sample_timestamp_seconds": 0}),
        ("missing demand field", {"active_claims": "stale"}),
        ("missing cohort field", {"bad:all": "stale"}),
        ("mixed scrape timestamps", {"bad:all": "_"}),
    ):
        case(
            name,
            [(6, {}), (4, {**hot, **invalid}), (5, {**hot, **invalid}), (6, {})],
            {5: (), 10: (), 15: ("ScrapeAbsent",), 20: ()},
            [foreign],
        )
    case(
        "firing rules survive a gap beyond lookback and receiver timeout",
        [
            (5, hot),
            (326, empty),
            (4, {"eligible:10m": 99, "oldest_claimable_age_seconds": 30}),
            (6, {}),
        ],
        {
            0: DOMAIN,
            5: DOMAIN,
            10: (*DOMAIN, "ScrapeAbsent"),
            325: (*DOMAIN, "ScrapeAbsent"),
            330: (*DOMAIN, "ScrapeAbsent"),
            335: (),
        },
        [{"series": 'receiver_status{status="resolved"}', "values": "1x340"}],
    )
    case(
        "firing burn holds until fresh sufficient volume",
        [
            (5, hot),
            (10, {"sql_ok": 0}),
            (5, {"eligible:2m": 99}),
            (6, {}),
        ],
        {0: DOMAIN, 10: (*DOMAIN, "ScrapeAbsent"), 15: DOMAIN[1:], 20: ()},
    )
    return {"rule_files": ["rules.yml"], "evaluation_interval": "5s", "tests": cases}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    for name, value in (("rules", render(LABELS)), ("tests", vector_tests())):
        # JSON is a subset of YAML. Promtool reads both with the same parser.
        (args.output / f"{name}.yml").write_text(json.dumps(value, indent=2) + "\n")
