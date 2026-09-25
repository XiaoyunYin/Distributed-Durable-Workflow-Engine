import re
from pathlib import Path


def _docker_stages(dockerfile: str) -> list[tuple[str | None, str]]:
    headers = list(re.finditer(r"(?im)^FROM\s+.*?(?:\s+AS\s+(\S+))?\s*$", dockerfile))
    stages: list[tuple[str | None, str]] = []
    for index, match in enumerate(headers):
        end = headers[index + 1].start() if index + 1 < len(headers) else len(dockerfile)
        stages.append((match.group(1), dockerfile[match.end() : end]))
    return stages


def test_default_runtime_image_excludes_campaign_binaries() -> None:
    root = Path(__file__).resolve().parents[1]
    stages = _docker_stages((root / "deploy/local/Dockerfile.runtime").read_text())
    names = [name for name, _ in stages]
    assert names[-1] == "runtime", "default Docker build must end at the production runtime stage"

    default_stage = stages[-1][1]
    default_binary_copies = re.findall(r"(?m)^COPY --from=\S+ /out/(\S+) /(\S+)\s*$", default_stage)
    assert default_binary_copies == [("runtime", "runtime")]
    assert "dur034_ablation" not in default_stage
    assert "dur049_owner_lock_isolation" not in default_stage

    campaign_stage = dict(stages)["dur049-campaign"]
    expected_campaign_binaries = {
        "runtime-dur049-owner-lock",
        "dur043-stale-probe",
        "dur049-checker",
        "dur049-lock-holder",
        "dur049-db-probe",
        "dur050-fixture",
    }
    copied_campaign_binaries = set(
        re.findall(
            r"(?m)^COPY --from=dur049-campaign-build /out/(\S+) /\S+\s*$",
            campaign_stage,
        )
    )
    assert copied_campaign_binaries == expected_campaign_binaries

    aws_compose = (root / "deploy/aws/app-compose.yaml").read_text()
    local_compose = (root / "deploy/local/compose.yaml").read_text()
    assert re.search(r"(?m)^      target: dur049-campaign$", aws_compose)
    assert "target: dur049-campaign" not in local_compose
