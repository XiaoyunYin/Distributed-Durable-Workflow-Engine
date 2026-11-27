"""Read frozen J2 Actions evidence and verify its optional Release copy."""

import hashlib
import re
import stat
import subprocess
import zipfile
from pathlib import Path
from urllib.parse import quote
from urllib.request import urlopen

from j2_barrier import (
    ENGINEERING_GRID_SCOPE,
    JOBS,
    PIN_FILES,
    REGISTERED_GRID_SCOPE,
    Campaign,
    decode,
    digest,
    read_bundle,
    sha256,
)
from j2_checker import ROOT
from j2_harness_evidence import safe_name
from j2_integrated_report import (
    CONTROL_ARTIFACTS,
    CONTROL_HEAD,
    CONTROL_RUN,
    QUALIFICATION_ARTIFACT,
    QUALIFICATION_HASH,
    QUALIFICATION_HEAD,
    UNCOMPRESSED_BOUND,
    ZIP_BOUND,
    download,
)
from j2_measure_authorization import engineering, git_head
from j2_qualification_evidence import file_record, require
from j2_schedule import MEASURED_CAMPAIGN, RATES

QUALIFICATION_RUN = 45893066798
QUALIFICATION_NAME = f"j2-volume-26678-{QUALIFICATION_RUN}-1"
INDEX_SCHEMA = "j2.release-evidence.v1"
MAX_INDEX_BYTES = 8 << 20
RELEASE_PART_BYTES = 64 << 20


def source_campaign(repository, run, campaign_id, head, attempt=1):
    require(
        (engineering(campaign_id) or campaign_id == MEASURED_CAMPAIGN)
        and git_head(head)
        and type(run) is int
        and run > 0
        and attempt == 1,
        "invalid replay campaign, run, attempt or head",
    )
    scope = ENGINEERING_GRID_SCOPE if engineering(campaign_id) else REGISTERED_GRID_SCOPE
    pins = {
        label: digest(
            subprocess.check_output(
                ["git", "show", f"{head}:{path}"],
                cwd=ROOT,
                timeout=20,
            )
        )
        for label, path in PIN_FILES.items()
    }
    return Campaign(repository, run, attempt, head, campaign_id, pins, 26678, scope)


def run_metadata(api, run):
    data = decode(api.request(f"{api.root}/runs/{run}", "source Actions run", authenticated=True))
    require(
        data["id"] == run
        and data["event"] == "workflow_dispatch"
        and data["path"] == ".github/workflows/j2-load-envelope.yml"
        and data["run_attempt"] == 1
        and git_head(data["head_sha"]),
        "source run is not an attempt-one J2 workflow dispatch",
    )
    return data


def source_names(campaign):
    names = {f"j2-measure-dispatch-{campaign.run}-{campaign.attempt}": ("shared", "dispatch")}
    for job in JOBS:
        slug = f"job-{job}"
        names[campaign.name("final", job) + "-partial"] = (slug, "final-partial")
        names[f"j2-measure-authorization-stop-{campaign.run}-{campaign.attempt}-{slug}"] = (
            slug,
            "authorization-stop",
        )
        for rate in (*RATES, "soak"):
            names[campaign.checkpoint(job, rate)] = (
                slug,
                "continuous" if rate == "soak" else f"grid-{rate}",
            )
        for kind in ("grid", "grid-evidence", "decision", "final"):
            names[campaign.name(kind, job)] = (slug, kind)
    return names


def prerequisite_archives():
    return {
        QUALIFICATION_NAME: (
            QUALIFICATION_ARTIFACT,
            QUALIFICATION_HASH,
            QUALIFICATION_RUN,
            QUALIFICATION_HEAD,
            "qualification",
        ),
        **{
            name: (
                artifact,
                sha,
                CONTROL_RUN,
                CONTROL_HEAD,
                "controls-receipt" if "receipt" in name else "controls",
            )
            for name, (artifact, sha) in CONTROL_ARTIFACTS.items()
        },
    }


def verify_prerequisite(name, meta):
    artifact, checksum, run, head, kind = prerequisite_archives()[name]
    require(
        meta["id"] == artifact
        and meta["name"] == name
        and meta["digest"] == f"sha256:{checksum}"
        and meta["workflow_run"]["id"] == run
        and meta["workflow_run"]["head_sha"] == head,
        f"{kind} archived source differs",
    )
    return run, kind


def metadata_source(barrier, *, prior_report=None):
    api, campaign = barrier.api, barrier.campaign
    names = source_names(campaign)
    items = barrier.retry(lambda: api.artifacts(campaign, set(names)))
    require(bool(items), "no retained artifacts identify the source campaign")
    for name, meta in items.items():
        job, kind = names[name]
        meta.update(source_run=campaign.run, source_attempt=campaign.attempt, job=job, kind=kind)
    for name, (artifact, _, _, _, _) in prerequisite_archives().items():
        meta = barrier.retry(
            lambda artifact=artifact: decode(
                api.request(
                    f"{api.root}/artifacts/{artifact}",
                    "prerequisite artifact",
                    authenticated=True,
                )
            )
        )
        run, kind = verify_prerequisite(name, meta)
        meta.update(source_run=run, source_attempt=1, job="shared", kind=kind)
        items[name] = meta
    if prior_report is not None:
        prior = barrier.retry(lambda: run_metadata(api, prior_report))
        previous = Campaign(campaign.repository, prior_report, 1, prior["head_sha"], "", {}, 0, "")
        require(prior["conclusion"] == "success", "replay report dispatch did not pass")
        kinds = ("replay", "replay-receipt")
        names = {f"j2-measure-{kind}-{prior_report}-1": kind for kind in kinds}
        reports = barrier.retry(lambda: api.artifacts(previous, set(names)))
        require(set(reports) == set(names), "replay report or upload receipt is unavailable")
        for name, meta in reports.items():
            meta.update(source_run=prior_report, source_attempt=1, job="shared", kind=names[name])
        items.update(reports)
    return items


def asset_name(campaign_id, meta):
    return (
        f"{campaign_id}--r{meta['source_run']}--a{meta['source_attempt']}--"
        f"{meta['job']}--{meta['kind']}--artifact-{meta['id']}.zip"
    )


def release_part_layout(name, size):
    if size <= RELEASE_PART_BYTES:
        return []
    count = (size + RELEASE_PART_BYTES - 1) // RELEASE_PART_BYTES
    return [
        (
            f"{name}.part-{part:03d}-of-{count:03d}",
            min(RELEASE_PART_BYTES, size - (part - 1) * RELEASE_PART_BYTES),
        )
        for part in range(1, count + 1)
    ]


def release_tag_ok(campaign_id, tag):
    if engineering(campaign_id):
        return isinstance(tag, str) and re.fullmatch(r"eng-[a-z0-9-]{1,60}-j2-evidence", tag)
    return campaign_id == MEASURED_CAMPAIGN and tag == f"{MEASURED_CAMPAIGN}-evidence"


def public_url(repository, tag, member):
    require(
        safe_name(member) and Path(member).name == member, "release asset must be a plain filename"
    )
    filename = quote(member, safe="")
    return f"https://github.com/{repository}/releases/download/{quote(tag, safe='')}/{filename}"


def _copy_public_asset(source, stream, expected, remaining, whole=None):
    require(source.status == 200, "release asset response is not successful")
    hashed = hashlib.sha256()
    left = expected["bytes"]
    while chunk := source.read(1 << 20):
        remaining()
        left -= len(chunk)
        require(left >= 0, "release asset exceeds its recorded size")
        hashed.update(chunk)
        if whole is not None:
            whole.update(chunk)
        stream.write(chunk)
    require(
        left == 0 and hashed.hexdigest() == expected["sha256"],
        "release asset size or digest differs",
    )


def public_download(url, target, expected, remaining, *, opener=urlopen):
    require(
        type(expected["bytes"]) is int
        and 0 < expected["bytes"] <= ZIP_BOUND
        and sha256(expected["sha256"]),
        "release asset size or digest is invalid",
    )
    with opener(url, timeout=min(15, remaining())) as source, target.open("wb") as stream:
        _copy_public_asset(source, stream, expected, remaining)


def pull_release_parts(release, asset, target, expected, remaining):
    parts = asset["parts"]
    layout = release_part_layout(asset["name"], expected["bytes"])
    require(
        isinstance(parts, list) and bool(layout) and len(parts) == len(layout),
        "release part count differs from the archive size",
    )
    ids = set()
    for part, (name, size) in zip(parts, layout, strict=True):
        require(isinstance(part, dict), "release part record must be an object")
        ident = part.get("id")
        require(
            part.get("name") == name
            and part.get("url") == public_url(release["repository"], release["tag"], name)
            and type(ident) is int
            and ident > 0
            and ident not in ids
            and part.get("bytes") == size
            and sha256(part.get("sha256")),
            "release part identity, order, size or digest differs from the index",
        )
        ids.add(ident)
    whole = hashlib.sha256()
    try:
        with target.open("wb") as stream:
            for part in parts:
                with urlopen(part["url"], timeout=min(15, remaining())) as source:
                    _copy_public_asset(source, stream, part, remaining, whole)
        require(whole.hexdigest() == expected["sha256"], "reassembled release ZIP digest differs")
    except Exception:
        target.unlink(missing_ok=True)
        raise


def archive_members(path, kind, remaining=lambda: 60):
    """Hash a bounded ZIP inventory and require the original freeze or bundle where applicable."""
    with zipfile.ZipFile(path) as bundle:
        entries = [item for item in bundle.infolist() if not item.is_dir()]
        names = {entry.filename for entry in entries}
        require(
            len(names) == len(entries)
            and bool(names)
            and all(safe_name(name) for name in names)
            and not any(stat.S_ISLNK(item.external_attr >> 16) for item in entries)
            and sum(item.file_size for item in entries) <= UNCOMPRESSED_BOUND,
            "unsafe, duplicate or over-budget archive members",
        )
        files = {}
        for item in entries:
            hashed, size = hashlib.sha256(), 0
            with bundle.open(item) as stream:
                while chunk := stream.read(1 << 20):
                    remaining()
                    size += len(chunk)
                    require(size <= item.file_size, "ZIP member exceeds its recorded size")
                    hashed.update(chunk)
            require(size == item.file_size, "ZIP member was truncated")
            files[item.filename] = {"bytes": size, "sha256": hashed.hexdigest()}
        freeze = files.get("freeze.json")
        if freeze is not None:
            require(freeze["bytes"] <= MAX_INDEX_BYTES, "freeze exceeds index limit")
            declared = decode(bundle.read("freeze.json"))["files"]
            require(
                declared == {name: value for name, value in files.items() if name != "freeze.json"},
                "ZIP members differ from the frozen index",
            )
        elif kind in {"grid", "grid-evidence", "decision"}:
            require(path.stat().st_size <= 1 << 20, "small grid bundle exceeds allowance")
            read_bundle(path.read_bytes(), "evidence" if kind == "grid-evidence" else kind)
        else:
            require(
                kind
                in {
                    "dispatch",
                    "controls-receipt",
                    "replay-receipt",
                    "final-partial",
                    "authorization-stop",
                },
                "archive is missing its freeze index",
            )
        return {"freeze": freeze, "members": files}


def pull_archive(barrier, meta, target, *, release=None, remaining=None):
    if release is None:
        download(barrier.api, meta, target)
        return
    recomputed = asset_name(release["campaign"], meta)
    asset = meta["release"]
    require(
        asset["name"] == recomputed,
        "release asset identity differs from the evidence index",
    )
    expected = {"bytes": meta["size_in_bytes"], "sha256": meta["digest"][7:]}
    require(
        type(expected["bytes"]) is int
        and 0 < expected["bytes"] <= ZIP_BOUND
        and sha256(expected["sha256"])
        and all(asset.get(key) == value for key, value in expected.items()),
        "release asset size or digest differs from the original evidence",
    )
    remaining = remaining or barrier.api.budget.remaining
    if "parts" in asset:
        pull_release_parts(release, asset, target, expected, remaining)
        return
    url = public_url(release["repository"], release["tag"], recomputed)
    require(
        asset["url"] == url and asset["id"] > 0,
        "release asset identity differs from the evidence index",
    )
    public_download(url, target, expected, remaining)


def load_release_index(repository, campaign_id, run, tag, name, checksum, *, opener=urlopen):
    require(
        release_tag_ok(campaign_id, tag) and sha256(checksum),
        "unreviewed release tag or index digest",
    )
    prefix = f"{campaign_id}--r{run}--a1--"
    require(
        re.fullmatch(re.escape(prefix) + r"(?:replay-r[1-9]\d*--)?index\.json", name),
        "release index filename does not bind the source run",
    )
    with opener(public_url(repository, tag, name), timeout=15) as reply:
        raw = reply.read(MAX_INDEX_BYTES + 1)
        require(
            reply.status == 200 and len(raw) <= MAX_INDEX_BYTES and digest(raw) == checksum,
            "release index size or digest differs",
        )
    doc = decode(raw)
    require(
        doc["schema"] == INDEX_SCHEMA
        and doc["repository"] == repository
        and doc["tag"] == tag
        and doc["campaign"] == campaign_id
        and doc["run"] == run
        and doc["attempt"] == 1,
        "release index belongs to a different campaign or run",
    )
    return doc


def release_member_record(path, meta, remaining):
    require(
        file_record(path) == {"bytes": meta["size_in_bytes"], "sha256": meta["digest"][7:]},
        "archive differs from the Actions artifact size or digest",
    )
    inventory = archive_members(path, meta["kind"], remaining)
    if "release" in meta:
        require(inventory == meta["release"]["inventory"], "release member hashes differ")
    return inventory


def verify_report_source(path, meta, campaign, records):
    run = meta["source_run"]
    with zipfile.ZipFile(path) as bundle:
        if meta["kind"] == "replay":
            report = decode(bundle.read("campaign-replay.json"))
            require(
                (report["campaign"], report["source_run"], report["source_head"], report["attempt"])
                == (campaign.campaign, campaign.run, campaign.head, campaign.attempt)
                and report["report_head"] == meta["workflow_run"]["head_sha"],
                "published report has a different source campaign or report head",
            )
        elif meta["kind"] == "replay-receipt":
            receipt = decode(bundle.read("artifact.json"))
            timing = decode(bundle.read("upload-timing.json"))
            report = records[f"j2-measure-replay-{run}-1"]
            require(
                all(
                    receipt[key] == report[key] for key in ("id", "digest", "size_in_bytes", "name")
                )
                and 0 <= timing["verified_upload_seconds"] <= 540,
                "archived upload receipt refers to a different report or overran its reserve",
            )
