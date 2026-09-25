"""Append an observer CSV while preserving and checking its canonical header."""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path


def append_rows(source: Path, destination: Path) -> None:
    if not source.is_file():
        raise FileNotFoundError(f"observer CSV is missing: {source}")
    if source.resolve() == destination.resolve():
        raise ValueError("observer source and aggregate destination must differ")

    with source.open("rb") as source_stream:
        header = source_stream.readline()
        if not header or not header.strip() or not header.endswith(b"\n"):
            raise ValueError(f"observer CSV has no complete header line: {source}")

        if destination.exists():
            with destination.open("rb") as destination_stream:
                existing_header = destination_stream.readline()
            if existing_header != header:
                raise ValueError(
                    f"observer CSV header differs byte-for-byte: {source} vs {destination}"
                )
        else:
            with destination.open("xb") as destination_stream:
                destination_stream.write(header)

        with destination.open("ab") as destination_stream:
            shutil.copyfileobj(source_stream, destination_stream)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("observer_csv", type=Path)
    parser.add_argument("aggregate_csv", type=Path)
    args = parser.parse_args()
    append_rows(args.observer_csv, args.aggregate_csv)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
