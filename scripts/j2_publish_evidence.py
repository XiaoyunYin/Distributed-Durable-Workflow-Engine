"""Reviewer-only publication of already-verified J2 evidence to an existing Release."""

import argparse
import filecmp
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from j2_barrier import ActionsAPI, Barrier, Budget, canonical, digest
from j2_measure_authorization import origin_repository
from j2_qualification_evidence import file_record, require
from j2_replay_storage import (
    INDEX_SCHEMA,
    asset_name,
    metadata_source,
    public_download,
    public_url,
    pull_archive,
    release_member_record,
    release_part_layout,
    release_tag_ok,
    run_metadata,
    source_campaign,
    supplemental_source,
    verify_report_source,
)

PART_UPLOAD_RETRIES = 3


def gh_json(*args):
    return json.loads(subprocess.check_output(["gh", "api", *args], timeout=90))


def assets(repository, release_id):
    pages = gh_json(
        f"repos/{repository}/releases/{release_id}/assets?per_page=100",
        "--paginate",
        "--slurp",
    )
    rows = [row for page in pages for row in page]
    result = {row["name"]: row for row in rows}
    require(len(result) == len(rows), "release has duplicate asset names")
    return result


def upload(repository, tag, path, budget):
    try:
        subprocess.run(
            ["gh", "release", "upload", tag, str(path), "--repo", repository],
            timeout=min(300, budget.remaining()),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=True,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
        stderr = error.stderr
        if isinstance(stderr, bytes):
            stderr = stderr.decode("utf-8", errors="replace")
        if stderr:
            print(f"{path.name}: {stderr.rstrip()}", file=sys.stderr, flush=True)
        raise


def discard_starter(repository, name, item):
    endpoint = f"repos/{repository}/releases/assets/{item['id']}"
    current = gh_json(endpoint)
    require(
        current["name"] == name and current["id"] == item["id"],
        "starter asset identity changed before cleanup",
    )
    if current["state"] == "starter":
        subprocess.run(["gh", "api", "--method", "DELETE", endpoint], timeout=90, check=True)
        return None
    return current


def publish_asset(repository, tag, release_id, path, existing, budget, *, retries=0):
    name = path.name
    for attempt in range(retries + 1):
        budget.remaining()
        if attempt:
            existing.pop(name, None)
            existing.update(assets(repository, release_id))
        item = existing.get(name)
        if retries and item is not None and item["state"] == "starter":
            item = discard_starter(repository, name, item)
            if item is None:
                existing.pop(name)
        if item is not None:
            break
        try:
            upload(repository, tag, path, budget)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            if attempt == retries:
                raise
            continue
        existing.update(assets(repository, release_id))
        item = existing.get(name)
        require(item is not None, "new release asset is unavailable")
        break
    url = public_url(repository, tag, name)
    record = file_record(path)
    require(
        item["state"] == "uploaded"
        and item["size"] == record["bytes"]
        and item["browser_download_url"] == url
        and item["id"] > 0,
        f"conflicting release asset metadata for {name}",
    )
    existing[name] = item
    return {"name": name, "url": url, "id": item["id"], **record}


def archive_parts(path, layout):
    with path.open("rb") as source:
        for name, size in layout:
            part = path.with_name(name)
            with part.open("xb") as target:
                left = size
                while left:
                    chunk = source.read(min(left, 1 << 20))
                    require(chunk, "source ZIP was truncated while splitting")
                    target.write(chunk)
                    left -= len(chunk)
            yield part


def publish_archive(repository, tag, release_id, path, existing, budget):
    layout = release_part_layout(path.name, path.stat().st_size)
    if not layout:
        return publish_asset(repository, tag, release_id, path, existing, budget)
    record = file_record(path)
    parts = []
    for part in archive_parts(path, layout):
        parts.append(
            publish_asset(
                repository, tag, release_id, part, existing, budget, retries=PART_UPLOAD_RETRIES
            )
        )
        part.unlink()
    return {"name": path.name, **record, "parts": parts}


def publish(repository, campaign_id, run, tag, output, prior_report):
    require(
        os.environ.get("GITHUB_ACTIONS") != "true" and release_tag_ok(campaign_id, tag),
        "use a reviewer shell and a J2 evidence release tag",
    )
    release = gh_json(f"repos/{repository}/releases/tags/{tag}")
    require(
        release["tag_name"] == tag and not release["draft"] and "J2" in release["name"],
        "release must already be public and named as J2 evidence",
    )
    release_id = release["id"]
    existing = assets(repository, release_id)
    api = ActionsAPI(
        repository,
        subprocess.check_output(["gh", "auth", "token"], text=True, timeout=15).strip(),
        Budget(time.monotonic() + 5400),
        lambda *_args, **_details: None,
    )
    source = run_metadata(api, run)
    require(source["status"] == "completed", "evidence source run still active")
    campaign = source_campaign(repository, run, campaign_id, source["head_sha"])
    barrier = Barrier(campaign, api)
    jobs = api.jobs(campaign, allow_failed=True)
    supplemental = supplemental_source(head=campaign.head)
    records = metadata_source(barrier, prior_report=prior_report, supplemental=supplemental)
    output.mkdir(parents=True, exist_ok=True)
    release_source = {"repository": repository, "tag": tag, "campaign": campaign_id}
    for meta in records.values():
        with tempfile.TemporaryDirectory(dir=output) as work:
            directory = Path(work)
            path = directory / asset_name(campaign_id, meta)
            barrier.retry(lambda meta=meta, path=path: pull_archive(barrier, meta, path))
            inventory = release_member_record(path, meta, api.budget.remaining)
            if meta["kind"] in {"replay", "replay-receipt"}:
                verify_report_source(path, meta, campaign, records, supplemental=supplemental)
            published = publish_archive(repository, tag, release_id, path, existing, api.budget)
            downloaded = directory / "verified.zip"
            pull_archive(
                barrier, {**meta, "release": published}, downloaded, release=release_source
            )
            if "parts" in published:
                require(
                    filecmp.cmp(path, downloaded, shallow=False), "reassembled ZIP bytes differ"
                )
            require(
                release_member_record(downloaded, meta, api.budget.remaining) == inventory,
                "public release ZIP membership differs",
            )
            meta["release"] = {**published, "inventory": inventory}
            print(f"Verified {path.name} {published['sha256']}", flush=True)
    doc = {
        "schema": INDEX_SCHEMA,
        "repository": repository,
        "campaign": campaign_id,
        "head": campaign.head,
        "run": run,
        "attempt": campaign.attempt,
        "tag": tag,
        "release_id": release_id,
        "jobs": {str(job): item for job, item in jobs.items()},
        "artifacts": records,
        **({"supplemental_controls": supplemental} if supplemental else {}),
    }
    suffix = f"replay-r{prior_report}--" if prior_report else ""
    name = f"{campaign_id}--r{run}--a{campaign.attempt}--{suffix}index.json"
    raw = canonical(doc)
    local = output / name
    if local.exists():
        require(local.read_bytes() == raw, "local release index conflicts with existing file")
    else:
        local.write_bytes(raw)
    item = publish_asset(repository, tag, release_id, local, existing, api.budget)
    with tempfile.TemporaryDirectory(dir=output) as work:
        public_download(item["url"], Path(work) / "index.json", item, api.budget.remaining)
    print(
        json.dumps({"release_tag": tag, "release_index": name, "release_index_sha256": digest(raw)})
    )


def main():
    from j2_checker import ROOT

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign-id", required=True)
    parser.add_argument("--source-run", required=True, type=int)
    parser.add_argument("--release-tag", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--report-run", type=int)
    args = parser.parse_args()
    publish(
        origin_repository(ROOT),
        args.campaign_id,
        args.source_run,
        args.release_tag,
        args.output,
        args.report_run,
    )


if __name__ == "__main__":
    main()
