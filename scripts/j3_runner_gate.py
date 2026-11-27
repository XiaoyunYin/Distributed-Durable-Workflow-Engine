"""Collect the registered idle PSI baseline and admit J3 runners before bootstrap."""

import argparse
import json
import os
import re
import subprocess
import time
from fractions import Fraction
from math import ceil
from pathlib import Path

LOCK = (
    Path(__file__).resolve().parents[1]
    / "experiments/portfolio/observability/runner-gate.lock.json"
)
WINDOW_SECONDS = 120
BASELINE_SIZE = 10
ATTEMPT_CAP = 3
SECOND = 1_000_000_000


def runner_cpu(cpuinfo=Path("/proc/cpuinfo")):
    models = sorted(
        {
            value.strip()
            for line in cpuinfo.read_text(encoding="utf-8").splitlines()
            for key, _, value in [line.partition(":")]
            if key.strip() == "model name"
        }
    )
    cores = os.sysconf("SC_NPROCESSORS_ONLN")
    if not models or cores <= 0:
        raise ValueError("missing hosted runner CPU inventory")
    return {"cpu_models": models, "online_cores": cores}


def campaign_id():
    return f"j3-r419-{os.environ['GITHUB_RUN_ID']}"


def save(path, record):
    path.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")


def io_full_total(text):
    (full,) = [line for line in text.splitlines() if line.startswith("full ")]
    return int(dict(part.split("=") for part in full.split()[1:])["total"])


def io_full_share(samples):
    """Return the io-full percentage as an exact fraction."""
    times = [row["monotonic_ns"] for row in samples]
    totals = [io_full_total(row["io"]) for row in samples]
    if (
        len(times) != WINDOW_SECONDS + 1
        or any(not i * SECOND <= t - times[0] < (i + 1) * SECOND for i, t in enumerate(times))
        or any(a > b for a, b in zip(totals, totals[1:], strict=False))
    ):
        raise ValueError("invalid idle PSI window")
    return Fraction(100_000 * (totals[-1] - totals[0]), times[-1] - times[0])


def idle_trace(path):
    rows = []
    started = time.monotonic_ns()
    with path.open("w", encoding="utf-8", buffering=1) as stream:
        for index in range(WINDOW_SECONDS + 1):
            if index:
                remaining = started + index * SECOND - time.monotonic_ns()
                if remaining > 0:
                    time.sleep(remaining / SECOND)
            row = {"at_ns": time.time_ns(), "monotonic_ns": time.monotonic_ns()}
            if not index:
                started = row["monotonic_ns"]
            try:
                for source in ("io", "cpu"):
                    row[source] = (
                        Path(f"/proc/pressure/{source}").read_text(encoding="ascii").strip()
                    )
            finally:
                stream.write(json.dumps(row) + "\n")
            rows.append(row)
    return io_full_share(rows)


def lock_threshold(path):
    lock = json.loads(path.read_bytes())
    x = lock["x_tenths_percentage_point"]
    if (
        lock["window_seconds"] != WINDOW_SECONDS
        or lock["baseline_size"] != BASELINE_SIZE
        or type(x) is not int
        or x < 5
    ):
        raise ValueError("invalid runner gate lock")
    return Fraction(x, 10)


def candidate_threshold(shares):
    if len(shares) != BASELINE_SIZE or any(share < 0 or share > 100 for share in shares):
        raise ValueError("expected ten complete baseline shares")
    return max(5, ceil(30 * max(shares)))


def previous_attempts(names, stage, run, workflow_attempt):
    pattern = re.compile(rf"j3-runner-gate-{re.escape(stage)}-{run}-(\d+)-(admitted|not-armed)")
    prior = {}
    for name in names:
        if match := pattern.fullmatch(name):
            attempt = int(match[1])
            if attempt >= workflow_attempt or attempt in prior:
                raise ValueError("conflicting stage attempt history")
            prior[attempt] = match[2]
    return prior


def evaluate_gate(record, share, lock, prior):
    record.update(
        stage_attempt=len(prior) + 1,
        prior_rejections=sum(label == "not-armed" for label in prior.values()),
        io_full_share_pct=float(share),
        io_full_share_ratio=str(share),
    )
    if record["stage_attempt"] > ATTEMPT_CAP:
        reason = "attempt_cap"
    elif "admitted" in prior.values():
        reason = "stage_already_admitted"
    elif not lock.is_file():
        reason = "missing_lock"
    else:
        threshold = lock_threshold(lock)
        record["x_pct"] = float(threshold)
        reason = None if share < threshold else "io_full_at_or_above_threshold"
    record.update(decision="admitted" if reason is None else "not-armed", reason=reason)
    return record


def identity():
    return {
        "run": os.environ["GITHUB_RUN_ID"],
        "head": os.environ["GITHUB_SHA"],
        "workflow_attempt": int(os.environ["GITHUB_RUN_ATTEMPT"]),
    }


def collect(stage, output, index=None):
    output.mkdir(parents=True, exist_ok=True)
    record = {**identity(), "stage": stage, "window_seconds": WINDOW_SECONDS}
    sampling_error = None
    try:
        share = idle_trace(output / "idle-pressure.jsonl")
        # GitHub's hosted Linux runner supplies this case-sensitive name.
        image_version = os.environ["ImageVersion"]  # noqa: SIM112
        record["runner"] = {**runner_cpu(), "image_version": image_version}
    except (ValueError, OSError, KeyError) as error:
        sampling_error = error
    if stage == "baseline":
        if sampling_error:
            raise sampling_error
        record.update(
            baseline_index=index, io_full_share_pct=float(share), io_full_share_ratio=str(share)
        )
        save(output / "baseline.json", record)
        return
    record["stage_attempt"] = None  # Unavailable history must fail closed.
    try:
        run = record["run"]
        names = subprocess.check_output(
            [
                "gh",
                "api",
                "--paginate",
                f"repos/{os.environ['GITHUB_REPOSITORY']}/actions/runs/{run}/artifacts",
                "--jq",
                ".artifacts[].name",
            ],
            text=True,
            timeout=45,
        ).splitlines()
        prior = previous_attempts(names, stage, run, record["workflow_attempt"])
        record["stage_attempt"] = len(prior) + 1
        if sampling_error:
            raise sampling_error
        evaluate_gate(record, share, LOCK, prior)
    except (ValueError, OSError, KeyError, subprocess.SubprocessError) as error:
        record.update(decision="not-armed", reason="gate_evidence_unavailable", error=str(error))
    record["campaign"] = campaign_id()
    save(output / "runner-gate.json", record)
    if record["decision"] == "not-armed":
        save(output.parent / "verdict.json", {"status": "NOT_ARMED", **record})
    with Path(os.environ["GITHUB_OUTPUT"]).open("a", encoding="utf-8") as outputs:
        outputs.write(f"decision={record['decision']}\n")


def baseline_report(source, output):
    records = [json.loads(path.read_bytes()) for path in sorted(source.rglob("baseline.json"))]
    if sorted(row["baseline_index"] for row in records) != list(range(1, BASELINE_SIZE + 1)):
        raise ValueError("expected the ten distinct baseline jobs")
    current = identity()
    for row in records:
        if (
            any(row[key] != value for key, value in current.items())
            or row["stage"] != "baseline"
            or row["window_seconds"] != WINDOW_SECONDS
        ):
            raise ValueError("baseline provenance mismatch")
    shares = [Fraction(row["io_full_share_ratio"]) for row in records]
    x = candidate_threshold(shares)
    output.mkdir(parents=True, exist_ok=True)
    save(
        output / "runner-gate.candidate.json",
        {
            "baseline_run": current["run"],
            "baseline_head": current["head"],
            "baseline_attempt": current["workflow_attempt"],
            "baseline_size": BASELINE_SIZE,
            "window_seconds": WINDOW_SECONDS,
            "max_baseline_share_pct": float(max(shares)),
            "x_tenths_percentage_point": x,
            "x_pct": x / 10,
        },
    )
    save(output / "baseline-records.json", records)
    prior_share = Fraction(482, 100)
    save(
        output / "prior-run-comparison.json",
        {
            "run": "45980467098",
            "verdict": "FAIL",
            "pre_origin_io_full_share_pct": float(prior_share),
            "x_pct": x / 10,
            "numerically_below_x": prior_share < Fraction(x, 10),
            "classification": "informational_only_window_included_setup",
        },
    )


def verify_admission(output, stage):
    record = json.loads((output / "runner-gate" / "runner-gate.json").read_bytes())
    if (
        any(record[key] != value for key, value in identity().items())
        or record["stage"] != stage
        or record["campaign"] != campaign_id()
        or record["decision"] != "admitted"
        or not 1 <= record["stage_attempt"] <= ATTEMPT_CAP
        or Fraction(record["io_full_share_ratio"]) >= lock_threshold(LOCK)
    ):
        raise ValueError("missing current runner gate admission")
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "mode", choices=("baseline", "report", "calibration", "confirmation", "measurement")
    )
    parser.add_argument("output", type=Path)
    parser.add_argument("--index", type=int)
    parser.add_argument("--source", type=Path)
    args = parser.parse_args()
    if args.mode == "report":
        baseline_report(args.source, args.output)
    else:
        collect(args.mode, args.output, args.index)


if __name__ == "__main__":
    main()
