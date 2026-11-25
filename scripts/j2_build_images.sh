#!/usr/bin/env bash
set -euo pipefail

source_tree=$1
evidence=$2
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

python3 - "$evidence" <<'PY'
import hashlib
import json
import shutil
import sys
import urllib.request
from pathlib import Path

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
    layout = root / target
    blobs = layout / "blobs/sha256"
    blobs.mkdir(parents=True)
    repository, digest = image["rebuilt"].split("@")
    host, name = repository.split("/", 1)
    base = f"http://{host}/v2/{name}"
    manifest = urllib.request.Request(f"{base}/manifests/{digest}", headers={
        "Accept": "application/vnd.docker.distribution.manifest.v2+json, application/vnd.oci.image.manifest.v1+json",
    })
    with urllib.request.urlopen(manifest, timeout=30) as response:
        content = response.read()
        media_type = response.headers.get_content_type()
    if "sha256:" + hashlib.sha256(content).hexdigest() != digest:
        raise ValueError(f"{target} manifest digest mismatch")
    (blobs / digest.removeprefix("sha256:")).write_bytes(content)
    manifest = json.loads(content)
    for blob in [manifest["config"], *manifest["layers"]]:
        path = blobs / blob["digest"].removeprefix("sha256:")
        with urllib.request.urlopen(f'{base}/blobs/{blob["digest"]}', timeout=30) as response:
            with path.open("wb") as stream:
                shutil.copyfileobj(response, stream)
        with path.open("rb") as stream:
            actual = "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()
        if actual != blob["digest"] or path.stat().st_size != blob["size"]:
            raise ValueError(f"{target} blob digest or size mismatch")
    (layout / "oci-layout").write_text('{"imageLayoutVersion":"1.0.0"}\n')
    (layout / "index.json").write_text(json.dumps({"schemaVersion": 2, "manifests": [{
        "mediaType": media_type, "digest": digest, "size": len(content),
    }]}) + "\n")
PY
