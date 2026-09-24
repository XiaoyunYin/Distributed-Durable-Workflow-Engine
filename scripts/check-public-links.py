#!/usr/bin/env python3
"""Check repository-local Markdown links without requiring network access."""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path
from urllib.parse import unquote, urlsplit


LINK_RE = re.compile(r"(?<!!)\[[^\]]*\]\((<[^>]+>|[^)]+)\)")
HEADING_RE = re.compile(r"^#{1,6}\s+(.+?)\s*#*\s*$")
REMOTE_SCHEMES = {"http", "https", "mailto", "tel", "data"}


def github_slug(text: str) -> str:
    text = re.sub(r"`([^`]*)`", r"\1", text).lower()
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"[^\w\- ]", "", text, flags=re.UNICODE)
    return re.sub(r"\s+", "-", text.strip())


def heading_ids(path: Path) -> set[str]:
    seen: dict[str, int] = {}
    ids: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        match = HEADING_RE.match(line)
        if not match:
            continue
        base = github_slug(match.group(1))
        count = seen.get(base, 0)
        ids.add(base if count == 0 else f"{base}-{count}")
        seen[base] = count + 1
    return ids


def markdown_files(root: Path) -> list[Path]:
    result = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "--", "*.md"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )
    return [root / line for line in result.stdout.splitlines() if line.endswith(".md")]


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    try:
        files = markdown_files(root)
    except (OSError, subprocess.CalledProcessError) as exc:
        print(f"cannot enumerate Markdown files with git: {exc}", file=sys.stderr)
        return 2

    errors: list[str] = []
    checked = 0
    slug_cache: dict[Path, set[str]] = {}
    for source in files:
        if not source.is_file():
            continue
        try:
            contents = source.read_text(encoding="utf-8")
        except UnicodeDecodeError as exc:
            errors.append(f"{source.relative_to(root)}: not UTF-8: {exc}")
            continue
        for match in LINK_RE.finditer(contents):
            target = match.group(1).strip()
            if target.startswith("<") and target.endswith(">"):
                target = target[1:-1]
            parsed = urlsplit(target)
            if parsed.scheme.lower() in REMOTE_SCHEMES or target.startswith("//"):
                continue
            checked += 1
            relative = unquote(parsed.path)
            destination = (source.parent / relative).resolve() if relative else source.resolve()
            try:
                destination.relative_to(root)
            except ValueError:
                errors.append(f"{source.relative_to(root)}: link escapes repository: {target}")
                continue
            if not destination.exists():
                errors.append(f"{source.relative_to(root)}: missing local target: {target}")
                continue
            if parsed.fragment and destination.is_file():
                ids = slug_cache.setdefault(destination, heading_ids(destination))
                fragment = unquote(parsed.fragment)
                if fragment not in ids:
                    errors.append(
                        f"{source.relative_to(root)}: missing heading #{fragment} in "
                        f"{destination.relative_to(root)}"
                    )

    if errors:
        print("Public Markdown link check FAILED:")
        print("\n".join(f"- {error}" for error in errors))
        return 1
    print(f"Public Markdown link check PASS: {checked} local links across {len(files)} Markdown files.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
