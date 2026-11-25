"""Kill J2 decision mutations in isolated source copies with named tests."""

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
J2 = Path("experiments/portfolio/load-envelope")
CASES = json.loads((ROOT / "mutations/j2-manifest.json").read_bytes())["cases"]


@pytest.mark.parametrize("case", CASES, ids=[c["id"] for c in CASES])
def test_registered_decision_mutation(case, tmp_path):
    relative = Path(case["file"])
    source = (ROOT / relative).read_text(encoding="utf-8")
    assert source.count(case["old"]) == 1, f"{case['id']} must match exactly one patch site"
    go = relative.suffix == ".go"
    supporting = (
        [Path("go.mod"), Path("go.sum"), J2 / "exporter/main_test.go"]
        if go
        else [relative, Path("deploy/k8s/connection-budget.json")]
    )
    test_name, _, selector = case["test"].partition("::")
    if go:
        command = ["go", "test", "-p", "1", f"./{J2.as_posix()}/exporter", "-run", f"^{test_name}$"]
        failure = f"--- FAIL: {test_name}"
    else:
        test_file = J2 / test_name
        supporting.append(test_file)
        command = [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "--basetemp",
            str(tmp_path / "pytest"),
            f"{test_file.as_posix()}::{selector}",
        ]
        failure = f"::{selector}"
    for path in supporting:
        target = tmp_path / path
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / path, target)
    target = tmp_path / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(source.replace(case["old"], case["new"]), encoding="utf-8")
    result = subprocess.run(
        command,
        cwd=tmp_path,
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1", "GOMAXPROCS": "2"},
        text=True,
        capture_output=True,
        timeout=60,
        check=False,
    )
    assert result.returncode == 1 and any(
        failure in line and line.startswith(("FAILED ", "--- FAIL: "))
        for line in result.stdout.splitlines()
    ), result.stdout + result.stderr
