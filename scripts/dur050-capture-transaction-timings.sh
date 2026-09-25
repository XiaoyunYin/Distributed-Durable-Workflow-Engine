#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 3 ]]; then
  echo "usage: $0 OUTPUT_FILE NAMESPACE RUN_ID [RUN_ID ...]" >&2
  exit 2
fi

output_file=$1
namespace=$2
shift 2
[[ "$namespace" =~ ^dur050-[A-Za-z0-9-]+$ ]] || { echo "namespace must be dur050-*" >&2; exit 2; }
for run_id in "$@"; do
  [[ "$run_id" =~ ^[A-Za-z0-9-]{1,48}$ ]] || { echo "invalid run ID: $run_id" >&2; exit 2; }
done
[[ ! -e "$output_file" ]] || { echo "refusing to overwrite timing rows: $output_file" >&2; exit 2; }
command -v docker >/dev/null || { echo "docker CLI is required" >&2; exit 2; }
command -v python3 >/dev/null || { echo "python3 is required" >&2; exit 2; }

cd /opt/durable-agent-execution-engine
docker compose --env-file deploy/aws/.env \
  -f deploy/aws/app-compose.yaml \
  -f deploy/aws/dur050-app-compose.yaml \
  logs --no-color runtime |
  python3 scripts/dur050-filter-txn-logs.py "$output_file" "$namespace" "$@"
