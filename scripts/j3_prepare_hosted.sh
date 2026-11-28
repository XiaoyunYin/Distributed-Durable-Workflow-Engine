#!/usr/bin/env bash
set -euo pipefail

evidence=$1
mode=$2
mkdir -p "$evidence"
exec > >(tee "$evidence/restore.log") 2>&1
for lock in experiments/portfolio/load-envelope/images.lock.json experiments/portfolio/observability/exporter.lock.json; do
  name=$(basename "$lock" .lock.json)
  artifact=$(jq -er .artifact "$lock")
  archive="$RUNNER_TEMP/j3-$name.zip"
  gh api "repos/$GITHUB_REPOSITORY/actions/artifacts/$artifact/zip" > "$archive"
  echo "$(jq -er .artifact_sha256 "$lock")  $archive" | sha256sum -c -
  unzip -q "$archive" -d "$RUNNER_TEMP/j3-$name-layout"
done
exporter_layout="$RUNNER_TEMP/j3-exporter-layout"
exporter_lock=experiments/portfolio/observability/exporter.lock.json
sha256sum -c "$exporter_layout/source.sha256"
diff <(jq -er .build_head "$exporter_lock") "$exporter_layout/source.txt"
diff <(jq -er .binary_sha256 "$exporter_lock") "$exporter_layout/binary.sha256"
CGO_ENABLED=0 go build -p 1 -trimpath -buildvcs=false -o "$RUNNER_TEMP/j3-loadgen" ./cmd/dur053-loadgen
sha256sum "$RUNNER_TEMP/j3-loadgen" > "$evidence/sender.sha256"
if [[ "$mode" == pilot ]]; then
  go test -p 1 ./experiments/portfolio/observability/probe
else
  prom=$(PYTHONPATH=scripts python3 -c 'from j3_overlay import PINS,PROM; print(PINS[PROM])')
  python3 scripts/j3_rules.py "$evidence/promtool"
  for args in 'check rules rules.yml' 'test rules tests.yml'; do
    read -r -a checker_args <<< "$args"
    docker run --rm --entrypoint=/bin/promtool -v "$evidence/promtool:/rules:ro" -w /rules "$prom" "${checker_args[@]}"
  done
fi
