#!/usr/bin/env bash
set -euo pipefail
: "${1:?scenario required}" "${2:?fresh output required}" "${3:?scratch root required}"
export TMP="$3" TEMP="$3" TMPDIR="$3" PYTHONDONTWRITEBYTECODE=1
exec python3 "$(dirname "$0")/dur053_k3.py" --scenario "$1" --output "$2" --scratch-root "$3"
