"""Reviewer-only publication of already-verified J2 evidence to an existing Release."""

import argparse
import json
import os
import subprocess
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
    release_tag_ok,
    run_metadata,
    source_campaign,
    verify_report_source,
)


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


def publish_asset(repository, tag, release_id, path, existing, budget):
    name = path.name
    if name not in existing:
        subprocess.run(
            ["gh", "release", "upload", tag, str(path), "--repo", repository],
            timeout=min(300, budget.remaining()),
            check=True,
        )
        current = assets(repository, release_id)
        require(name in current, "new release asset is unavailable")
        existing.update(current)
    item = existing[name]
    url = public_url(repository, tag, name)
    record = file_record(path)
    require(
        item["state"] == "uploaded"
        and item["size"] == record["bytes"]
        and item["browser_download_url"] == url
        and item["id"] > 0,
        f"conflicting release asset metadata for {name}",
    )
    return {"name": name, "url": url, "id": item["id"], **record}


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
    records = metadata_source(barrier, prior_report=prior_report)
    output.mkdir(parents=True, exist_ok=True)
    for meta in records.values():
        with tempfile.TemporaryDirectory(dir=output) as work:
            directory = Path(work)
            path = directory / asset_name(campaign_id, meta)
            barrier.retry(lambda meta=meta, path=path: pull_archive(barrier, meta, path))
            inventory = release_member_record(path, meta, api.budget.remaining)
            if meta["kind"] in {"replay", "replay-receipt"}:
                verify_report_source(path, meta, campaign, records)
            published = publish_asset(repository, tag, release_id, path, existing, api.budget)
            downloaded = directory / "verified.zip"
            public_download(published["url"], downloaded, published, api.budget.remaining)
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
