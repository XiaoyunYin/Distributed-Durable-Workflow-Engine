#!/usr/bin/env bash
set -euo pipefail

# cloud-init may invoke this under a sparse environment. Keep the Go module and
# build caches explicit instead of relying on the caller's HOME/GOPATH.
go_home=/root
go_path="$go_home/go"
module_cache="$go_path/pkg/mod"
build_cache="$go_home/.cache/go-build"
export HOME="$go_home"
export GOPATH="$go_path"
export GOMODCACHE="$module_cache"
export GOCACHE="$build_cache"
export GOFLAGS=-mod=readonly

command -v go >/dev/null 2>&1 || { echo "go is not on PATH" >&2; exit 127; }
test -f go.mod || { echo "run this script from the repository root" >&2; exit 2; }
mkdir -p "$go_path" "$module_cache" "$build_cache" bin

go build -trimpath -o bin/dur050-observer ./cmd/dur050-observer
go build -trimpath -o bin/dur050-loadgen ./cmd/dur050-loadgen
go build -trimpath -o bin/dur050-sink ./cmd/dur050-sink
chmod 0755 bin/dur050-observer bin/dur050-loadgen bin/dur050-sink
