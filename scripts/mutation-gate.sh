#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
"${PYTHON:-python3}" scripts/mutation-runner.py --working-tree --output "${1:-mutations/results-bash.json}"
echo "DUR-046 Linux mutation gate passed."
