"""Check the frozen J2 approval packet before an eligible runner bootstraps."""

from pathlib import Path
from urllib.request import Request

from j2_barrier import PIN_FILES, canonical, decode, digest, expiry
from j2_measure_authorization import control_identity, git_head, git_output
from j2_qualification_evidence import DRAIN_SECONDS, read, require
from j2_replay_storage import (
    QUALIFICATION_NAME,
    prerequisite_archives,
    run_metadata,
    supplemental_source,
    verify_prerequisite,
)
from j2_schedule import (
    CAP,
    HOLD,
    MEASURED,
    MEASURED_CAMPAIGN,
    ORDERS,
    RATES,
    REQUEST_TIMEOUT,
    STEP,
    WARMUP,
    population,
    registered_schedule,
    soak_population,
)

FINAL_REHEARSAL = "eng-inc4-final"
PACKET = Path("experiments/portfolio/load-envelope") / MEASURED_CAMPAIGN
SCHEMA = "j2.pre-dispatch.v1"
EVIDENCE_FILES = (
    "seed-vectors.json",
    "pins/verification.json",
    "qualification/verification.json",
    "controls/verification.json",
    "replay/verification.json",
    "budget.json",
    "test-inventory.md",
    "approval.md",
)
TOOLING_FILES = (
    ".github/workflows/j2-load-envelope.yml",
    ".github/actions/j2-integrated-cell/action.yml",
    "scripts/j2_dispatch_manifest.py",
    "scripts/j2_measure.py",
    "scripts/j2_measure_authorization.py",
    "scripts/j2_measure_replay.py",
    "scripts/j2_publish_evidence.py",
    "scripts/j2_replay_storage.py",
)


def text_sha(path):
    return digest(path.read_bytes().replace(b"\r\n", b"\n"))


def schedule_contract():
    return {
        "rates_per_second": RATES,
        "orders": {f"job-{job}": list(order) for job, order in enumerate(ORDERS, 1)},
        "grid_scheduled": {rate: population(rate, WARMUP + MEASURED) for rate in RATES},
        "qualified_volume": 26678,
        "continuous": {
            rate: {
                "bound": soak_population(rate),
                "warmup_scheduled": population(rate, WARMUP),
                "hold_scheduled": population(rate, WARMUP + HOLD) - population(rate, WARMUP),
                "step_scheduled": population("C", STEP),
                "scheduled": population(rate, WARMUP + HOLD) + population("C", STEP),
            }
            for rate in RATES
            if soak_population(rate) <= 26678
        },
        "windows_seconds": {
            "grid": {"warmup": [0, WARMUP], "measured": [WARMUP, WARMUP + MEASURED]},
            "continuous": {
                "warmup": [0, WARMUP],
                "hold": [WARMUP, WARMUP + HOLD],
                "step": [WARMUP + HOLD, WARMUP + HOLD + STEP],
            },
        },
        "sender_cap": CAP,
        "request_timeout": REQUEST_TIMEOUT,
        "drain_seconds_including_quiet": DRAIN_SECONDS,
    }


def verify_package(root, pins):
    directory = root / PACKET
    manifest_path = directory / "manifest.json"
    require(manifest_path.is_file(), "frozen J2 campaign manifest is missing")
    manifest = read(manifest_path)
    require(
        manifest["schema"] == SCHEMA
        and manifest["campaign_id"] == MEASURED_CAMPAIGN
        and manifest["schedule"] == schedule_contract()
        and git_head(manifest["protocol_commit"])
        and git_head(manifest["tooling"]["implementation_basis"]),
        "frozen J2 campaign identity, revisions or registered schedule differ",
    )
    require(
        manifest["pins"]
        == pins
        == {label: text_sha(root / path) for label, path in PIN_FILES.items()},
        "frozen J2 pin hashes differ",
    )
    protocol_blob = f"{manifest['protocol_commit']}:{PIN_FILES['protocol']}"
    require(
        digest(git_output(root, "show", protocol_blob)) == pins["protocol"],
        "frozen J2 protocol revision differs from its pin",
    )
    git_output(
        root, "merge-base", "--is-ancestor", manifest["tooling"]["implementation_basis"], "HEAD"
    )
    require(
        manifest["tooling"]["source_hashes"]
        == {path: text_sha(root / path) for path in TOOLING_FILES},
        "frozen J2 tooling hashes differ",
    )
    files = manifest["evidence"]
    require(
        set(files) == set(EVIDENCE_FILES)
        and all(
            (directory / name).is_file() and files[name] == text_sha(directory / name)
            for name in EVIDENCE_FILES
        ),
        "frozen J2 prerequisite or evidence hash differs",
    )
    seeds = read(directory / "seed-vectors.json")["vectors"]
    require(
        seeds
        == {
            f"{MEASURED_CAMPAIGN}/job-{job}/{cell}": registered_schedule(
                MEASURED_CAMPAIGN,
                f"job-{job}",
                cell,
                "seed-check",
                "j2-harness",
                0,
                "AB" if cell == "soak" else cell,
            )["seed"]
            for job in (1, 2, 3)
            for cell in (*RATES, "soak")
        },
        "frozen J2 seed vectors differ",
    )
    controls = read(directory / "controls/verification.json")
    supplemental = supplemental_source(root=root)
    require(
        controls["controls_sha256"] == digest(canonical(control_identity(root))),
        "frozen J2 controls applicability differs from the checkout",
    )
    require(
        {name: verdict["status"] for name, verdict in controls["verdicts"].items()}
        == {
            "execution_delay": "PASS",
            "withheld_terminal": "PASS",
            "healthy_low_load": "PASS",
            "capped_sender": "sender_invalid",
        },
        "frozen J2 control verdicts differ",
    )
    qualification = read(directory / "qualification/verification.json")
    require(
        qualification["reconciliation"]["identities"] == manifest["schedule"]["qualified_volume"]
        and qualification["reconciliation"]["identical_verdict"] == "PASS",
        "frozen J2 qualification verdict or population differs",
    )
    checked = {}
    for group, record in (("qualification", qualification), ("controls", controls)):
        require(record["status"] == "PASS", f"frozen J2 {group} verification did not pass")
        for name, entry in record["archives"].items():
            source = entry["source"]
            verify_prerequisite(name, source, supplemental)
            checked[name] = {"id": source["id"], "digest": source["digest"]}
    require(
        set(checked) == set(prerequisite_archives(supplemental))
        and checked == manifest["prerequisites"],
        "frozen J2 prerequisite inventory differs",
    )
    return {
        "manifest_sha256": text_sha(manifest_path),
        "evidence": files,
        "prerequisites": checked,
        **({"supplemental_controls": supplemental} if supplemental else {}),
    }


def confirm_prerequisites(api, supplemental=None):
    records = {}
    if supplemental is not None:
        run = run_metadata(api, supplemental["run"])
        require(
            run["head_sha"] == supplemental["head"] and run["conclusion"] == "success",
            "supplemental controls run did not pass at the frozen head",
        )
    for name, (ident, *_) in prerequisite_archives(supplemental).items():
        meta = decode(api.request(f"{api.root}/artifacts/{ident}", name, authenticated=True))
        verify_prerequisite(name, meta, supplemental)
        require(
            meta["expired"] is False and expiry(meta["expires_at"]), f"expired prerequisite {name}"
        )
        records[name] = {"id": ident, "expires_at": meta["expires_at"], "digest": meta["digest"]}
        if name == QUALIFICATION_NAME:
            location = api.request(
                f"{api.root}/artifacts/{ident}/zip",
                name,
                authenticated=True,
                redirect=True,
            )
            request = Request(location, headers={"Range": "bytes=0-0"})
            with api.opener.open(request, timeout=min(10, api.budget.remaining())) as reply:
                require(
                    reply.status in {200, 206} and len(reply.read(1)) == 1,
                    "qualification ZIP is inaccessible",
                )
    return records
