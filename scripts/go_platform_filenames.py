"""Detect implicit platform constraints outside the intentional file inventory."""

from pathlib import PurePosixPath

ALLOWLIST = {
    "cmd/dur027-lease/process_windows.go",
    "cmd/dur034-ablation/process_cpu_windows.go",
    "cmd/dur035-dispatch/process_cpu_windows.go",
    "cmd/dur050-loadgen/cpu_linux.go",
    "internal/dur050/clock_linux.go",
}


def unexpected_platform_files(paths: list[str], targets: str) -> list[str]:
    suffixes = {name for target in targets.split() for name in target.split("/")}
    return sorted(
        path
        for path in paths
        if path.endswith(".go")
        and "_" in PurePosixPath(path).stem.removesuffix("_test")
        and PurePosixPath(path).stem.removesuffix("_test").rsplit("_", 1)[-1] in suffixes
        and path not in ALLOWLIST
    )
