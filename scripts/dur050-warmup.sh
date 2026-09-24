#!/usr/bin/env bash
set -euo pipefail

: "${DUR050_WARMUP_WORKFLOW_IDS_FILE:?reset-block must set the workflow-ID output path}"
: "${DUR050_LOADGEN_CONFIG_FILE:?set the frozen DUR-050 family-definition config path}"
loadgen_bin="${DUR050_LOADGEN_BIN:-/opt/durable-agent-execution-engine/bin/dur050-loadgen}"
run_id="warmup-$(date -u +%Y%m%dT%H%M%S)-$$"
output="/var/tmp/dur050-${run_id}.csv"

test -x "$loadgen_bin"
test -s "$DUR050_LOADGEN_CONFIG_FILE"
"$loadgen_bin" \
  -config "$DUR050_LOADGEN_CONFIG_FILE" \
  -run-id "$run_id" \
  -rate 64 \
  -count 8 \
  -output "$output" \
  -workflow-ids-output "$DUR050_WARMUP_WORKFLOW_IDS_FILE"
