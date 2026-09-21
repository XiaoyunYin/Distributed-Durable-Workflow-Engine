#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
python3 scripts/mutation-runner.py --output mutations/results-bash.json
echo "DUR-046 Linux mutation gate passed."
