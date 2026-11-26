#!/usr/bin/env bash
set -euo pipefail

for kind in images exporter; do
    lock="experiments/portfolio/load-envelope/$kind.lock.json"
    artifact=$(jq -er .artifact "$lock")
    archive="$RUNNER_TEMP/j2-$kind.zip"
    gh api "repos/$GITHUB_REPOSITORY/actions/artifacts/$artifact/zip" > "$archive"
    echo "$(jq -er .artifact_sha256 "$lock")  $archive" | sha256sum -c -
    unzip -q "$archive" -d "$RUNNER_TEMP/j2-$kind-layouts"
done
