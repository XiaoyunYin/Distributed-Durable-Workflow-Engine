#!/usr/bin/env bash
set -euo pipefail
out=$1
mode=${2:-verify}
case "$mode" in
    verify|candidate) ;;
    *) echo "unknown exporter build mode: $mode" >&2; exit 2 ;;
esac
dir=experiments/portfolio/observability/exporter
tag=localhost:5001/j3-exporter:build
mkdir -p "$out"
sha256sum "$dir"/Dockerfile "$dir"/Dockerfile.dockerignore "$dir"/main.go go.mod go.sum > "$out/source.sha256"
git rev-parse HEAD > "$out/source.txt"
docker build --provenance=false --platform=linux/amd64 -f "$dir/Dockerfile" -t "$tag" .
container=$(docker create "$tag")
trap 'docker rm -f "$container" >/dev/null' EXIT
docker cp "$container:/exporter" "$out/binary"
sha256sum "$out/binary" | cut -d' ' -f1 > "$out/binary.sha256"
rm "$out/binary"
lock=experiments/portfolio/observability/exporter.lock.json
if [[ "$mode" == verify && -f "$lock" ]]; then
    diff <(jq -er .binary_sha256 "$lock") "$out/binary.sha256"
    exit
fi
docker run -d --name j3-build-registry -p 127.0.0.1:5001:5000 "$(jq -er .registry deploy/k8s/tools.lock.json)"
trap 'docker rm -f "$container" j3-build-registry >/dev/null' EXIT
docker push "$tag"
docker inspect --format '{{index .RepoDigests 0}}' "$tag" > "$out/image.txt"
PYTHONPATH=scripts python3 - "$out" <<'PY'
import sys
from pathlib import Path

from j2_oci import retain

out = Path(sys.argv[1])
retain(out / "exporter", (out / "image.txt").read_text().strip())
PY
