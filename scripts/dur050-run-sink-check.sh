#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 FROZEN_CONFIG OUTPUT_DIR" >&2
  exit 2
fi

config_path=$1
output_dir=$2
script_dir=$(cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(cd -- "$script_dir/.." && pwd)
loadgen_bin="$repo_root/bin/dur050-loadgen"
sink_bin="$repo_root/bin/dur050-sink"
[[ -s "$config_path" ]] || { echo "campaign config is missing: $config_path" >&2; exit 2; }
[[ ! -e "$output_dir" ]] || { echo "refusing to overwrite output directory: $output_dir" >&2; exit 2; }
for command_name in jq curl pidstat python3; do
  command -v "$command_name" >/dev/null || { echo "$command_name is required" >&2; exit 2; }
done
[[ -x "$loadgen_bin" && -x "$sink_bin" ]] || { echo "loadgen and sink binaries must be built in $repo_root/bin" >&2; exit 2; }

namespace=$(jq -er '.namespace' "$config_path")
[[ "$namespace" =~ ^dur050-[A-Za-z0-9-]+$ ]] || { echo "config namespace must be dur050-*" >&2; exit 2; }
run_id="sink-check-$(date -u +%Y%m%dT%H%M%S)-$$"
mkdir -p -- "$(dirname -- "$output_dir")"
mkdir -m 0700 -- "$output_dir"
config_copy="$output_dir/sink-config.json"
requests_csv="$output_dir/generator-requests.csv"
summary_json="$requests_csv.summary.json"
pidstat_file="$output_dir/pidstat.txt"
artifact_json="$output_dir/generator-sink.json"
sink_pid=
loadgen_pid=
pidstat_pid=
start_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)
loadgen_exit=127
request_rows=0

cleanup() {
  local status=$?
  if [[ -n "$pidstat_pid" ]] && kill -0 "$pidstat_pid" 2>/dev/null; then kill "$pidstat_pid" 2>/dev/null || true; fi
  if [[ -n "$loadgen_pid" ]] && kill -0 "$loadgen_pid" 2>/dev/null; then kill "$loadgen_pid" 2>/dev/null || true; fi
  if [[ -n "$sink_pid" ]] && kill -0 "$sink_pid" 2>/dev/null; then kill "$sink_pid" 2>/dev/null || true; fi
  if [[ ! -e "$artifact_json" ]]; then
    python3 - "$artifact_json" "$run_id" "$start_utc" "$loadgen_exit" "$status" <<'PY'
import json, sys
from pathlib import Path
path, run_id, started, loadgen_exit, exit_status = sys.argv[1:]
with Path(path).open("x", encoding="utf-8") as stream:
    json.dump({"schema": "dur050-generator-sink.v1", "status": "FAIL",
               "run_id": run_id, "started_at_utc": started,
               "loadgen_exit": int(loadgen_exit), "runner_exit": int(exit_status),
               "failure": "runner exited before validation completed"}, stream, indent=2)
    stream.write("\n")
PY
  fi
  return "$status"
}
trap cleanup EXIT

jq --arg api_url "http://127.0.0.1:8787" --arg run_id "$run_id" \
  '.api_url = $api_url | .run_id = $run_id' "$config_path" > "$config_copy"
"$sink_bin" -addr 127.0.0.1:8787 > "$output_dir/sink.log" 2>&1 &
sink_pid=$!
ready=0
for _ in $(seq 1 100); do
  if curl --silent --show-error --fail --output /dev/null http://127.0.0.1:8787/healthz; then ready=1; break; fi
  if ! kill -0 "$sink_pid" 2>/dev/null; then break; fi
  sleep 0.1
done
(( ready == 1 )) || { cat "$output_dir/sink.log" >&2; echo "loopback sink did not become healthy" >&2; exit 1; }

loadgen_started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)
"$loadgen_bin" -config "$config_copy" -rate 64 -duration 5m -request-timeout 2s \
  -output "$requests_csv" > "$output_dir/loadgen.log" 2>&1 &
loadgen_pid=$!
pidstat -I -h -r -u -p "$loadgen_pid" 1 > "$pidstat_file" 2>&1 &
pidstat_pid=$!
set +e
wait "$loadgen_pid"
loadgen_exit=$?
set -e
loadgen_pid=
kill "$pidstat_pid" 2>/dev/null || true
wait "$pidstat_pid" 2>/dev/null || true
pidstat_pid=
loadgen_finished_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)
kill "$sink_pid" 2>/dev/null || true
wait "$sink_pid" 2>/dev/null || true
sink_pid=

if [[ -s "$requests_csv" ]]; then
  request_rows=$(python3 - "$requests_csv" <<'PY'
import csv, sys
with open(sys.argv[1], newline="", encoding="utf-8") as stream:
    rows = list(csv.DictReader(stream))
if any(row.get("outcome") != "accepted" for row in rows):
    raise SystemExit("sink check contains a non-accepted scheduled request")
print(len(rows))
PY
  )
fi

python3 - "$artifact_json" "$run_id" "$start_utc" "$loadgen_started_utc" \
  "$loadgen_finished_utc" "$loadgen_exit" "$request_rows" "$summary_json" \
  "$pidstat_file" <<'PY'
import hashlib, json, sys
from pathlib import Path
artifact, run_id, started, load_started, load_finished, exit_code, row_count, summary_path, pidstat_path = sys.argv[1:]
summary_file = Path(summary_path)
summary = json.loads(summary_file.read_text(encoding="utf-8")) if summary_file.is_file() else None
valid = (
    int(exit_code) == 0
    and int(row_count) == 19_200
    and summary is not None
    and summary.get("status") == "PASS"
    and summary.get("scheduled") == 19_200
    and summary.get("accepted") == 19_200
    and summary.get("generator_cpu", {}).get("status") == "PASS"
)
pidstat = Path(pidstat_path)
result = {
    "schema": "dur050-generator-sink.v1",
    "status": "PASS" if valid else "FAIL",
    "label": "CALIBRATION — NOT RESULTS",
    "run_id": run_id,
    "started_at_utc": started,
    "loadgen_started_at_utc": load_started,
    "loadgen_finished_at_utc": load_finished,
    "sink_bind": "127.0.0.1:8787",
    "offered_rate_per_second": 64,
    "duration_seconds": 300,
    "observer_qps": None,
    "request_rows": int(row_count),
    "loadgen_exit": int(exit_code),
    "loadgen_summary": summary,
    "pidstat_file": pidstat.name,
    "pidstat_sha256": hashlib.sha256(pidstat.read_bytes()).hexdigest() if pidstat.is_file() else None,
    "reason": None if valid else "expected 19,200 accepted requests and PASS generator CPU validation",
}
with Path(artifact).open("x", encoding="utf-8") as stream:
    json.dump(result, stream, indent=2, sort_keys=True)
    stream.write("\n")
if not valid:
    raise SystemExit(1)
print(json.dumps({"status": result["status"], "request_rows": result["request_rows"]}, sort_keys=True))
PY
