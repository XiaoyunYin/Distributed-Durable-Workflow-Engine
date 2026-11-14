#!/usr/bin/env bash
set -euo pipefail
: "${1:?scenario required}" "${2:?fresh output required}" "${3:?scratch root required}"
export TMP="$3" TEMP="$3" TMPDIR="$3" PYTHONDONTWRITEBYTECODE=1
if [[ "$1" == "lifecycle" || "$1" == "lifecycle-smoke" || "$1" == "verify-evidence" || ( "$1" == "cleanup" && "${4:-}" == "k4" ) ]]; then
  extra=()
  if [[ "$1" == "lifecycle" ]]; then
    : "${4:?registered K4 job required}"
    extra+=(--job "$4" --replacement-for "${5:-}")
  elif [[ "$1" == "verify-evidence" ]]; then
    : "${4:?offline checker binary required}"
    extra+=(--checker "$4")
  fi
  exec python3 "$(dirname "$0")/dur053_k4_driver.py" --scenario "$1" --output "$2" --scratch-root "$3" "${extra[@]}"
fi
if [[ "$1" == "pilot" || "$1" == "pilot-smoke" ]]; then
  job="${4:-}"
  if [[ "$1" == "pilot-smoke" ]]; then job=pilot-smoke; fi
  : "${job:?registered pilot job required}"
  exec python3 "$(dirname "$0")/dur053_campaign.py" --job "$job" --output "$2" --scratch-root "$3"
fi
exec python3 "$(dirname "$0")/dur053_k3.py" --scenario "$1" --output "$2" --scratch-root "$3"
