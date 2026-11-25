"""Restore a digest-verified image layout to the private hosted-runner registry."""

import hashlib
import json
import shutil
from pathlib import Path
from urllib.parse import urlencode, urljoin
from urllib.request import Request, urlopen


def retain(layout: Path, reference: str) -> None:
    repo, digest = reference.split("@")
    host, name = repo.split("/", 1)
    base = f"http://{host}/v2/{name}"
    blobs = layout / "blobs/sha256"
    blobs.mkdir(parents=True)
    request = Request(
        f"{base}/manifests/{digest}",
        headers={
            "Accept": "application/vnd.docker.distribution.manifest.v2+json, application/vnd.oci.image.manifest.v1+json",
        },
    )
    with urlopen(request, timeout=30) as response:
        raw = response.read()
        media_type = response.headers.get_content_type()
    if "sha256:" + hashlib.sha256(raw).hexdigest() != digest:
        raise ValueError("retained registry manifest digest changed")
    (blobs / digest.removeprefix("sha256:")).write_bytes(raw)
    manifest = json.loads(raw)
    for entry in [manifest["config"], *manifest["layers"]]:
        path = blobs / entry["digest"].removeprefix("sha256:")
        with (
            urlopen(f"{base}/blobs/{entry['digest']}", timeout=30) as response,
            path.open("wb") as stream,
        ):
            shutil.copyfileobj(response, stream)
        with path.open("rb") as stream:
            actual = "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()
        if actual != entry["digest"] or path.stat().st_size != entry["size"]:
            raise ValueError("retained registry blob digest or length changed")
    (layout / "oci-layout").write_text('{"imageLayoutVersion":"1.0.0"}\n')
    (layout / "index.json").write_text(
        json.dumps(
            {
                "schemaVersion": 2,
                "manifests": [
                    {
                        "mediaType": media_type,
                        "digest": digest,
                        "size": len(raw),
                    }
                ],
            }
        )
        + "\n"
    )


def restore(layout: Path, reference: str) -> None:
    repo, digest = reference.split("@")
    host, name = repo.split("/", 1)
    if host != "localhost:5001":
        raise ValueError("J2 image restore requires the loopback registry")
    (descriptor,) = json.loads((layout / "index.json").read_bytes())["manifests"]
    if descriptor["digest"] != digest:
        raise ValueError("OCI manifest differs from the image lock")

    def read_blob(entry):
        path = layout / "blobs/sha256" / entry["digest"].removeprefix("sha256:")
        data = path.read_bytes()
        if (
            len(data) != entry["size"]
            or "sha256:" + hashlib.sha256(data).hexdigest() != entry["digest"]
        ):
            raise ValueError(f"OCI retained blob mismatch: {entry['digest']}")
        return data

    manifest = read_blob(descriptor)
    image = json.loads(manifest)
    base = f"http://{host}/v2/{name}"
    for entry in [image["config"], *image["layers"]]:
        data = read_blob(entry)
        with urlopen(
            Request(f"{base}/blobs/uploads/", method="POST", data=b""), timeout=30
        ) as response:
            location = urljoin(base + "/", response.headers["Location"])
        if not location.startswith(f"http://{host}/v2/{name}/blobs/uploads/"):
            raise ValueError("registry returned an unexpected upload endpoint")
        uri = location + ("&" if "?" in location else "?") + urlencode({"digest": entry["digest"]})
        with urlopen(
            Request(
                uri, method="PUT", data=data, headers={"Content-Type": "application/octet-stream"}
            ),
            timeout=60,
        ):
            pass
    request = Request(
        f"{base}/manifests/{digest}",
        method="PUT",
        data=manifest,
        headers={"Content-Type": descriptor["mediaType"]},
    )
    with urlopen(request, timeout=30) as response:
        if response.headers["Docker-Content-Digest"] != digest:
            raise ValueError("restored registry manifest digest changed")
