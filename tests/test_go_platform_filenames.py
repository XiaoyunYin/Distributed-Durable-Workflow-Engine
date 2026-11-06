from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
from go_platform_filenames import ALLOWLIST, unexpected_platform_files  # noqa: E402


def test_tracked_go_files_have_only_intentional_platform_suffixes() -> None:
    targets = subprocess.check_output(["go", "tool", "dist", "list"], text=True)
    paths = (
        subprocess.check_output(["git", "ls-files", "-z", "--", "*.go"], cwd=ROOT)
        .decode()
        .split("\0")
    )
    assert not unexpected_platform_files([p for p in paths if (ROOT / p).is_file()], targets)


def test_platform_suffix_guard_rejects_accidental_file(tmp_path: Path) -> None:
    accidental = tmp_path / "foo_linux_test.go"
    accidental.write_text("package example\n", encoding="utf-8")
    paths = [accidental.name, "foo_windows.go", "foo_amd64_test.go", "foo_linux_arm64.go"]
    targets = "linux/amd64 windows/amd64 linux/arm64"
    assert unexpected_platform_files(paths, targets) == sorted(paths), (
        "platform suffix guard bypassed"
    )
    assert unexpected_platform_files(list(ALLOWLIST), targets) == []
    assert (
        unexpected_platform_files(
            ["windows.go", "windows_test.go", "foo_windows_extra_test.go"], targets
        )
        == []
    )
