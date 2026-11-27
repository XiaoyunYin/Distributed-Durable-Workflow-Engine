"""Build a J2 sender from immutable source for hosted rehearsals."""

import argparse
import os
import re
import subprocess
import tarfile
import tempfile
from pathlib import Path

from j2_qualification_evidence import file_record, require, write

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = "./cmd/dur053-loadgen"
J2_PACKAGE = "./cmd/j2-sender"
BUILD_SCHEMAS = {PACKAGE: "j2.stock-k4-build.v1", J2_PACKAGE: "j2.continuous-build.v1"}
TOOLCHAIN = "go1.27.4"
BUILD_ENV = {
    "CGO_ENABLED": "0",
    "GOOS": "linux",
    "GOARCH": "amd64",
    "GOAMD64": "v1",
    "GOENV": "off",
    "GOFLAGS": "",
    "GOEXPERIMENT": "",
    "GOTOOLCHAIN": TOOLCHAIN,
    "GOWORK": "off",
}
BUILD_FLAGS = ("-p", "1", "-mod=readonly", "-trimpath", "-buildvcs=false")


def build(source, binary, manifest, *, package=PACKAGE):
    require(re.fullmatch(r"[0-9a-f]{40}", source) is not None, "full source commit required")
    require(package in BUILD_SCHEMAS, "unexpected J2 sender package")
    require(not binary.exists() and not manifest.exists(), "sender output already exists")
    binary = binary.resolve()
    binary.parent.mkdir(parents=True, exist_ok=True)
    env = {**os.environ, **BUILD_ENV}

    def checked(*args, cwd=ROOT):
        return subprocess.check_output(args, cwd=cwd, env=env, text=True, timeout=30).strip()

    tree = checked("git", "rev-parse", source + "^{tree}")
    source_paths = ("go.mod", "go.sum", package.removeprefix("./"), "internal", "migrations")
    with tempfile.TemporaryDirectory(dir=binary.parent) as temp:
        folder = Path(temp)
        archive = folder / "source.tar"
        checked(
            "git",
            "-c",
            "core.autocrlf=false",
            "-c",
            "core.eol=lf",
            "archive",
            f"--output={archive}",
            source,
            *source_paths,
        )
        with tarfile.open(archive) as data:
            data.extractall(folder, filter="data")
        version = checked("go", "env", "GOVERSION", cwd=folder)
        require(version == TOOLCHAIN, f"unexpected stock sender toolchain: {version}")
        result = folder / "sender"
        subprocess.run(
            ["go", "build", *BUILD_FLAGS, "-o", str(result), package],
            cwd=folder,
            env=env,
            check=True,
            timeout=240,
        )
        inputs = {name: file_record(folder / name) for name in ("go.mod", "go.sum")}
        build_info = checked("go", "version", "-m", str(result), cwd=folder)
        result.replace(binary)
    write(
        manifest,
        {
            "schema": BUILD_SCHEMAS[package],
            "source": source,
            "tree": tree,
            "archive_paths": source_paths,
            "modules": inputs,
            "version": version,
            "env": BUILD_ENV,
            "flags": BUILD_FLAGS,
            "package": package,
            "binary": file_record(binary),
            # go version -m prefixes the variable output path on the first line.
            "build_info": build_info.splitlines()[1:],
        },
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source")
    parser.add_argument("binary", type=Path)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--package", choices=BUILD_SCHEMAS, default=PACKAGE)
    args = parser.parse_args()
    build(args.source, args.binary, args.manifest, package=args.package)


if __name__ == "__main__":
    main()
