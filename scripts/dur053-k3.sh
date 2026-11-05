#!/usr/bin/env bash
set -euo pipefail
: "${1:?scenario required}" "${2:?fresh output required}" "${3:?scratch root required}"
export TMP="$3" TEMP="$3" TMPDIR="$3" PYTHONDONTWRITEBYTECODE=1
if [[ "$1" == "pilot" || "$1" == "pilot-smoke" ]]; then
  job="${4:-}"
  if [[ "$1" == "pilot-smoke" ]]; then job=pilot-smoke; fi
  : "${job:?registered pilot job required}"
  exec python3 "$(dirname "$0")/dur053_campaign.py" --job "$job" --output "$2" --scratch-root "$3"
fi
exec python3 "$(dirname "$0")/dur053_k3.py" --scenario "$1" --output "$2" --scratch-root "$3"
