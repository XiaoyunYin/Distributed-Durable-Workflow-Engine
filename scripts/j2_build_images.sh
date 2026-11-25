#!/usr/bin/env bash
set -euo pipefail

source_tree=$1
evidence=$2
script_dir=$(cd -- "$(dirname -- "$0")" && pwd)
registry=localhost:5001
mkdir -p "$evidence"
exec > >(tee "$evidence/build.log") 2>&1

cd "$source_tree"
git rev-parse HEAD 'HEAD^{tree}' > "$evidence/source.txt"
sha256sum deploy/k8s/Dockerfile .dockerignore go.mod go.sum requirements-worker.txt > "$evidence/inputs.sha256"
docker version > "$evidence/docker.txt"
docker buildx version >> "$evidence/docker.txt"
printf '%s\n' "$ImageOS" "$ImageVersion" > "$evidence/runner.txt"
cp deploy/k8s/tools.lock.json "$evidence/dependencies.json"
docker run -d --name j2-image-registry -p 127.0.0.1:5001:5000 "$(jq -r .registry deploy/k8s/tools.lock.json)"
trap 'docker rm -f j2-image-registry' EXIT

set -x
for target in runtime worker; do
    tag="$registry/dur053-$target:candidate"
    docker build --provenance=false --platform=linux/amd64 --target "$target" -t "$tag" -f deploy/k8s/Dockerfile .
    docker push "$tag"
    docker inspect --format '{{json .RepoDigests}}' "$tag" > "$evidence/$target.json"
done
set +x

PYTHONPATH="$script_dir" python3 - "$evidence" <<'PY'
import json
import sys
from pathlib import Path

from j2_oci import retain

root = Path(sys.argv[1])
registered = {
    "runtime": "sha256:df0360c87c5b7b3f16d3912416e6e55ed49781cb1c391da63485fdc43dc7f507",
    "worker": "sha256:6224788bfdceda2d9954e838ebb6f35970d342ad7adbd8283cc3b984f7f84eb7",
}
images = {}
for target, digest in registered.items():
    (ref,) = json.loads((root / f"{target}.json").read_bytes())
    actual = ref.split("@")[1]
    images[target] = {"registered": digest, "rebuilt": ref, "exact": digest == actual}

exact = all(image["exact"] for image in images.values())
(root / "images.json").write_text(json.dumps({
    "profile": "registered profile" if exact else "rebuilt registered profile",
    "images": images,
}, indent=2) + "\n")

for target, image in images.items():
    retain(root / target, image["rebuilt"])
PY
