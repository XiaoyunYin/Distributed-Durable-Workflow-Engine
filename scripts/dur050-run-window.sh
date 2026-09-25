#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: $0 CONFIG RUN_ID RATE count|duration VALUE OUTPUT_DIR" >&2
  echo "       duration VALUE is an integer number of seconds" >&2
}

if [[ $# -ne 6 ]]; then
  usage
  exit 2
fi

config_path=$1
run_id=$2
rate=$3
mode=$4
value=$5
output_dir=$6

[[ -s "$config_path" ]] || { echo "campaign config is missing: $config_path" >&2; exit 2; }
[[ "$run_id" =~ ^[A-Za-z0-9-]{1,48}$ ]] || { echo "invalid run ID" >&2; exit 2; }
[[ "$rate" =~ ^[0-9]+([.][0-9]+)?$ ]] || { echo "rate must be a positive decimal" >&2; exit 2; }
awk -v rate="$rate" 'BEGIN { exit !(rate > 0) }' || { echo "rate must be positive" >&2; exit 2; }
[[ "$mode" == count || "$mode" == duration ]] || { usage; exit 2; }
[[ "$value" =~ ^[1-9][0-9]*$ ]] || { echo "count/duration value must be a positive integer" >&2; exit 2; }
[[ ! -e "$output_dir" ]] || { echo "refusing to overwrite existing output directory: $output_dir" >&2; exit 2; }
[[ -n "${DUR050_OBSERVER_SECRET_PARAMETER:-}" ]] || { echo "DUR050_OBSERVER_SECRET_PARAMETER is required" >&2; exit 2; }
command -v aws >/dev/null || { echo "aws CLI is required" >&2; exit 2; }
command -v pidstat >/dev/null || { echo "pidstat is required for the independent CPU cross-check" >&2; exit 2; }
command -v jq >/dev/null || { echo "jq is required" >&2; exit 2; }
command -v python3 >/dev/null || { echo "python3 is required to safely encode the observer password" >&2; exit 2; }

namespace=$(jq -er '.namespace' "$config_path")
[[ "$namespace" =~ ^dur050-[A-Za-z0-9-]+$ ]] || { echo "config namespace must be dur050-*" >&2; exit 2; }

if [[ "$mode" == count ]]; then
  count=$value
  (( count >= 2 && count % 2 == 0 )) || { echo "count mode requires an even count >= 2" >&2; exit 2; }
  load_window_args=(-count "$count")
else
  duration_seconds=$value
  count=$(awk -v rate="$rate" -v seconds="$duration_seconds" 'BEGIN { printf "%d", int(rate * seconds + 1e-9) }')
  (( count >= 2 )) || { echo "duration produces fewer than two scheduled arrivals" >&2; exit 2; }
  load_window_args=(-duration "${duration_seconds}s")
fi

mkdir -m 0700 -- "$output_dir"
expected_ids="$output_dir/scheduled-workflow-ids.txt"
accepted_ids="$output_dir/accepted-workflow-ids.txt"
observer_csv="$output_dir/observer.csv"
load_csv="$output_dir/submissions.csv"
submission_reconciliation_csv="$output_dir/submission-reconciliation.csv"
window_validation_json="$output_dir/window-reconciliation.json"
done_file="$output_dir/submissions.done"

for ((sequence = 1; sequence <= count; sequence++)); do
  printf '%s-%s-%06d\n' "$namespace" "$run_id" "$sequence" >> "$expected_ids"
done

observer_password=$(aws ssm get-parameter \
  --name "$DUR050_OBSERVER_SECRET_PARAMETER" \
  --with-decryption \
  --region us-west-1 \
  --query 'Parameter.Value' \
  --output text)
[[ -n "$observer_password" ]] || { echo "observer secret is empty" >&2; exit 2; }
encoded_password=$(python3 -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1], safe=""))' "$observer_password")
unset observer_password
database_host=${DUR050_DATABASE_PRIVATE_IP:?DUR050_DATABASE_PRIVATE_IP is required}
database_name=${DUR050_DATABASE_NAME:?DUR050_DATABASE_NAME is required}
[[ "$database_host" =~ ^[0-9.]+$ && "$database_name" =~ ^[A-Za-z0-9_]+$ ]] || { echo "database target is malformed" >&2; exit 2; }
export DUR050_OBSERVER_DATABASE_URL="postgresql://dur050_observer:${encoded_password}@${database_host}:5432/${database_name}?sslmode=disable"
unset encoded_password

observer_args=(
  -mode batch
  -workflow-ids-file "$expected_ids"
  -done-file "$done_file"
  -output "$observer_csv"
  -timeout 30m
)
"$(dirname "$0")/../bin/dur050-observer" "${observer_args[@]}" > "$output_dir/observer.log" 2>&1 &
observer_pid=$!
pidstat_pid=
cleanup() {
  if [[ -n "$pidstat_pid" ]] && kill -0 "$pidstat_pid" 2>/dev/null; then kill "$pidstat_pid" 2>/dev/null || true; fi
  if kill -0 "$observer_pid" 2>/dev/null; then kill "$observer_pid" 2>/dev/null || true; fi
}
trap cleanup EXIT

observer_ready=0
for _ in $(seq 1 300); do
  if ! kill -0 "$observer_pid" 2>/dev/null; then
    cat "$output_dir/observer.log" >&2
    echo "observer exited before the load window started" >&2
    exit 1
  fi
  if [[ -s "$observer_csv" ]] && (( $(wc -l < "$observer_csv") >= 2 )); then observer_ready=1; break; fi
  sleep 0.1
done
(( observer_ready == 1 )) || { echo "observer did not complete its initial sample in 30 seconds" >&2; exit 1; }

cat > "$output_dir/run-metadata.txt" <<EOF
run_id=$run_id
namespace=$namespace
offered_rate_per_second=$rate
mode=$mode
mode_value=$value
scheduled_count=$count
observer_host_clock=Linux CLOCK_MONOTONIC shared with load generator
EOF

loadgen=(
  "$(dirname "$0")/../bin/dur050-loadgen"
  -config "$config_path"
  -run-id "$run_id"
  -rate "$rate"
  "${load_window_args[@]}"
  -output "$load_csv"
  -workflow-ids-output "$accepted_ids"
)
"${loadgen[@]}" > "$output_dir/loadgen.log" 2>&1 &
loadgen_pid=$!
pidstat -I -h -r -u -p "$loadgen_pid" 1 > "$output_dir/pidstat.txt" 2>&1 &
pidstat_pid=$!

set +e
wait "$loadgen_pid"
loadgen_status=$?
set -e

touch -- "$done_file"
set +e
wait "$observer_pid"
observer_status=$?
set -e
if [[ -n "$pidstat_pid" ]]; then
  kill "$pidstat_pid" 2>/dev/null || true
  wait "$pidstat_pid" 2>/dev/null || true
  pidstat_pid=
fi

reconciliation_status=1
if [[ -s "$load_csv" ]]; then
  set +e
  "$(dirname "$0")/../bin/dur050-observer" \
    -mode reconcile \
    -submissions-file "$load_csv" \
    -output "$submission_reconciliation_csv" \
    -timeout 5m > "$output_dir/submission-reconciliation.log" 2>&1
  reconciliation_status=$?
  set -e
else
  echo "load-generator submission CSV is missing; cannot reconcile uncertain outcomes" \
    > "$output_dir/submission-reconciliation.log"
fi

validation_status=1
set +e
python3 "$(dirname "$0")/dur050-reconcile-window.py" \
  "$load_csv" "$accepted_ids" "$observer_csv" \
  "$submission_reconciliation_csv" "$window_validation_json" \
  > "$output_dir/window-validation.log" 2>&1
validation_status=$?
set -e

printf 'loadgen_exit=%d\nobserver_exit=%d\nsubmission_reconciliation_exit=%d\nwindow_validation_exit=%d\n' \
  "$loadgen_status" "$observer_status" "$reconciliation_status" "$validation_status" \
  >> "$output_dir/run-metadata.txt"
if (( loadgen_status != 0 || observer_status != 0 || reconciliation_status != 0 || validation_status != 0 )); then
  cat "$output_dir/loadgen.log" >&2 || true
  cat "$output_dir/observer.log" >&2 || true
  cat "$output_dir/submission-reconciliation.log" >&2 || true
  cat "$output_dir/window-validation.log" >&2 || true
  exit 1
fi

echo "DUR050 window artifacts: $output_dir"
