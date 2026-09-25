#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: $0 CONFIG BLOCK_ID FAMILY OUTPUT_DIR" >&2
  echo "       FAMILY is seq-8 or fanout-8; run after one fresh-block reset" >&2
}

if [[ $# -ne 4 ]]; then usage; exit 2; fi
config_path=$1
block_id=$2
family=$3
output_dir=$4

[[ -s "$config_path" ]] || { echo "campaign config is missing: $config_path" >&2; exit 2; }
[[ "$block_id" =~ ^[A-Za-z0-9-]{1,32}$ ]] || { echo "invalid block ID" >&2; exit 2; }
[[ "$family" == seq-8 || "$family" == fanout-8 ]] || { echo "family must be seq-8 or fanout-8" >&2; exit 2; }
[[ ! -e "$output_dir" ]] || { echo "refusing to overwrite existing output directory: $output_dir" >&2; exit 2; }
[[ -n "${DUR050_OBSERVER_SECRET_PARAMETER:-}" ]] || { echo "DUR050_OBSERVER_SECRET_PARAMETER is required" >&2; exit 2; }
command -v aws >/dev/null || { echo "aws CLI is required" >&2; exit 2; }
command -v jq >/dev/null || { echo "jq is required" >&2; exit 2; }
command -v python3 >/dev/null || { echo "python3 is required to safely encode the observer password" >&2; exit 2; }

namespace=$(jq -er '.namespace' "$config_path")
[[ "$namespace" =~ ^dur050-[A-Za-z0-9-]+$ ]] || { echo "config namespace must be dur050-*" >&2; exit 2; }
database_host=${DUR050_DATABASE_PRIVATE_IP:?DUR050_DATABASE_PRIVATE_IP is required}
database_name=${DUR050_DATABASE_NAME:?DUR050_DATABASE_NAME is required}
[[ "$database_host" =~ ^[0-9.]+$ && "$database_name" =~ ^[A-Za-z0-9_]+$ ]] || { echo "database target is malformed" >&2; exit 2; }

mkdir -m 0700 -- "$output_dir"
mkdir -m 0700 -- "$output_dir/runs"
printf 'family,block_id,sample,run_id,workflow_id,outcome,terminal_state,scheduled_at_utc,scheduled_at_monotonic_ns,first_terminal_observed_at_utc,observed_at_monotonic_ns,observed_latency_ms,max_preterminal_gap_ms,query_count,observer_qps,valid,invalid_reason\n' > "$output_dir/unloaded-latency.csv"
printf 'record_type,sequence,workflow_id,scheduled_at_utc,observed_at_utc,state,created_at_db,updated_at_db,terminal_transition_at_db,query_duration_ms,poll_gap_ms,max_poll_gap_ms,max_preterminal_gap_ms,observer_qps,valid,reason,scheduled_at_monotonic_ns,observed_at_monotonic_ns\n' > "$output_dir/unloaded-observer-polls.csv"
: > "$output_dir/loadgen-summaries.jsonl"

observer_password=$(aws ssm get-parameter \
  --name "$DUR050_OBSERVER_SECRET_PARAMETER" \
  --with-decryption \
  --region us-west-1 \
  --query 'Parameter.Value' \
  --output text)
[[ -n "$observer_password" ]] || { echo "observer secret is empty" >&2; exit 2; }
encoded_password=$(python3 -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1], safe=""))' "$observer_password")
unset observer_password
export DUR050_OBSERVER_DATABASE_URL="postgresql://dur050_observer:${encoded_password}@${database_host}:5432/${database_name}?sslmode=disable"
unset encoded_password

for sample in $(seq 1 100); do
  run_id="${block_id}-${family//-}-$(printf '%03d' "$sample")"
  workflow_id="${namespace}-${run_id}-000001"
  run_dir="$output_dir/runs/$run_id"
  mkdir -m 0700 -- "$run_dir"

  "$(dirname "$0")/../bin/dur050-observer" \
    -mode fine \
    -workflow-id "$workflow_id" \
    -output "$run_dir/observer.csv" \
    -timeout 5m > "$run_dir/observer.log" 2>&1 &
  observer_pid=$!

  observer_ready=0
  for _ in $(seq 1 300); do
    if ! kill -0 "$observer_pid" 2>/dev/null; then
      cat "$run_dir/observer.log" >&2
      echo "fine observer failed before sample $sample" >&2
      exit 1
    fi
    if [[ -s "$run_dir/observer.csv" ]] && (( $(wc -l < "$run_dir/observer.csv") >= 2 )); then observer_ready=1; break; fi
    sleep 0.01
  done
  (( observer_ready == 1 )) || { echo "fine observer did not begin within 3 seconds" >&2; exit 1; }

  set +e
  "$(dirname "$0")/../bin/dur050-loadgen" \
    -config "$config_path" \
    -single-family "$family" \
    -run-id "$run_id" \
    -rate 1 \
    -count 1 \
    -output "$run_dir/submission.csv" \
    -workflow-ids-output "$run_dir/accepted-workflow-ids.txt" \
    > "$run_dir/loadgen.log" 2>&1
  loadgen_status=$?
  wait "$observer_pid"
  observer_status=$?
  set -e

  if (( loadgen_status != 0 )); then
    cat "$run_dir/loadgen.log" >&2 || true
    cat "$run_dir/observer.log" >&2 || true
    echo "unloaded sample $sample failed (loadgen=$loadgen_status observer=$observer_status); partial evidence retained" >&2
    exit 1
  fi
  if (( observer_status != 0 )) && ! grep -Fq 'observer run invalid: observer_sampling_gap_exceeded' "$run_dir/observer.log"; then
    cat "$run_dir/observer.log" >&2 || true
    echo "unloaded sample $sample observer failed for a reason other than the retained sampling-gap invalidation" >&2
    exit 1
  fi

  jq -e '.status == "PASS" and .scheduled == 1 and .accepted == 1' "$run_dir/submission.csv.summary.json" >/dev/null
  python3 - "$family" "$block_id" "$sample" "$run_id" "$run_dir/submission.csv" "$run_dir/observer.csv" "$output_dir/unloaded-latency.csv" <<'PY'
import csv, re, sys
family, block_id, sample, run_id, submission_path, observer_path, output_path = sys.argv[1:]
with open(submission_path, newline="", encoding="utf-8") as stream:
    submissions = [row for row in csv.DictReader(stream) if row["record_type"] == "submission"]
with open(observer_path, newline="", encoding="utf-8") as stream:
    observations = list(csv.DictReader(stream))
submissions = [row for row in submissions if row["outcome"] == "accepted"]
terminals = [row for row in observations if row["record_type"] == "first_terminal_observation"]
summaries = [row for row in observations if row["record_type"] == "summary"]
if len(submissions) != 1 or len(terminals) != 1 or len(summaries) != 1:
    raise SystemExit(f"expected one accepted submission, terminal observation, and observer summary; got {len(submissions)}, {len(terminals)}, {len(summaries)}")
submission, terminal, summary = submissions[0], terminals[0], summaries[0]
if submission["workflow_id"] != terminal["workflow_id"]:
    raise SystemExit("submission and terminal observation workflow IDs differ")
latency_ms = (int(terminal["observed_at_monotonic_ns"]) - int(submission["scheduled_at_monotonic_ns"])) / 1_000_000
query_match = re.search(r"queries=(\d+)", summary["reason"])
if query_match is None:
    raise SystemExit("observer summary lacks query count")
valid = terminal["valid"].lower() == "true" and summary["valid"].lower() == "true"
reason = terminal["reason"]
if summary["valid"].lower() != "true":
    reason = ";".join(filter(None, [reason, summary["reason"]]))
with open(output_path, "a", newline="", encoding="utf-8") as stream:
    writer = csv.writer(stream)
    writer.writerow([family, block_id, sample, run_id, submission["workflow_id"], submission["outcome"], terminal["state"], submission["scheduled_at_utc"], submission["scheduled_at_monotonic_ns"], terminal["observed_at_utc"], terminal["observed_at_monotonic_ns"], f"{latency_ms:.6f}", terminal["max_preterminal_gap_ms"], query_match.group(1), summary["observer_qps"], str(valid).lower(), reason])
PY
  jq -c --arg run_id "$run_id" --arg family "$family" '{run_id:$run_id,family:$family,summary:.}' "$run_dir/submission.csv.summary.json" >> "$output_dir/loadgen-summaries.jsonl"
  tail -n +2 "$run_dir/observer.csv" >> "$output_dir/unloaded-observer-polls.csv"
  if (( sample == 1 )); then
    cp "$run_dir/submission.csv" "$output_dir/submission-rows.csv"
    printf 'sample,run_id\n%d,%s\n' "$sample" "$run_id" > "$output_dir/submission-run-map.csv"
  else
    tail -n +2 "$run_dir/submission.csv" >> "$output_dir/submission-rows.csv"
    printf '%d,%s\n' "$sample" "$run_id" >> "$output_dir/submission-run-map.csv"
  fi
done

python3 - "$output_dir/unloaded-latency.csv" "$family" "$block_id" <<'PY' > "$output_dir/block-summary.txt"
import csv, sys
path, family, block_id = sys.argv[1:]
with open(path, newline="", encoding="utf-8") as stream:
    rows = [row for row in csv.DictReader(stream) if row["family"] == family and row["block_id"] == block_id]
valid = sum(row["valid"] == "true" for row in rows)
print(f"family={family}")
print(f"block_id={block_id}")
print(f"samples={len(rows)}")
print(f"valid_samples={valid}")
print(f"invalid_samples={len(rows)-valid}")
PY
echo "DUR050 unloaded block complete: $output_dir"
