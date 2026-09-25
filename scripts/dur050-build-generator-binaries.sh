#!/usr/bin/env bash
set -euo pipefail

# cloud-init may invoke this under a sparse environment. Keep the Go module and
# build caches explicit instead of relying on the caller's HOME/GOPATH.
export HOME=/root
export GOPATH=/root/go
export GOMODCACHE=/root/go/pkg/mod
export GOCACHE=/root/.cache/go-build
export GOFLAGS=-mod=readonly

command -v go >/dev/null 2>&1 || { echo "go is not on PATH" >&2; exit 127; }
test -f go.mod || { echo "run this script from the repository root" >&2; exit 2; }
mkdir -p "$GOPATH" "$GOMODCACHE" "$GOCACHE" bin

go build -trimpath -o bin/dur050-observer ./cmd/dur050-observer
go build -trimpath -o bin/dur050-loadgen ./cmd/dur050-loadgen
go build -trimpath -o bin/dur050-sink ./cmd/dur050-sink
chmod 0755 bin/dur050-observer bin/dur050-loadgen bin/dur050-sink
