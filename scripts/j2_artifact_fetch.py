"""Fetch the metadata and ZIP for one J2 verification receipt."""

import argparse
import json
import re
import shutil
import subprocess
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path

WAIT_SECONDS = 120
BACKOFF_SECONDS = 5
STATUS_LINE = re.compile(rb"HTTP/\S+ ([1-5][0-9]{2})(?:\s|$)")


def fetch(endpoint, output, attempts, deadline):
    while (remaining := deadline - time.monotonic()) > 0:
        started = datetime.now(UTC).isoformat()
        with tempfile.TemporaryFile() as response:
            try:
                result = subprocess.run(
                    ["gh", "api", "--include", endpoint],
                    stdout=response,
                    stderr=subprocess.DEVNULL,
                    timeout=remaining,
                    check=False,
                )
            except subprocess.TimeoutExpired as error:
                raise TimeoutError(f"artifact API deadline reached for {endpoint}") from error
            finally:
                response.seek(0)
                header = STATUS_LINE.match(response.readline())
                status = int(header[1]) if header else None
                event = json.dumps({"endpoint": endpoint, "time": started, "http_status": status})
                attempts.write(event + "\n")
                attempts.flush()
                print(event, flush=True)
            if time.monotonic() >= deadline:
                break
            if result.returncode == 0 and status is not None and 200 <= status < 300:
                # gh prefixes the response with HTTP headers when --include is set.
                for line in response:
                    if line in (b"\n", b"\r\n"):
                        with output.open("wb") as body:
                            shutil.copyfileobj(response, body)
                        return
                raise ValueError(f"artifact API response has no header terminator for {endpoint}")
        if status != 404 and not (status is not None and 500 <= status < 600):
            raise ValueError(
                f"artifact API request failed for {endpoint} (HTTP {status or 'unknown'})"
            )
        if time.monotonic() + BACKOFF_SECONDS >= deadline:
            break
        time.sleep(BACKOFF_SECONDS)
    raise TimeoutError(f"artifact API deadline reached for {endpoint}")


def fetch_artifact(endpoint, receipt, archive):
    deadline = time.monotonic() + WAIT_SECONDS
    with (receipt / "artifact-api-attempts.jsonl").open("w", encoding="utf-8") as attempts:
        fetch(endpoint, receipt / "artifact.json", attempts, deadline)
        fetch(endpoint + "/zip", archive, attempts, deadline)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("endpoint")
    parser.add_argument("receipt", type=Path)
    parser.add_argument("archive", type=Path)
    args = parser.parse_args()
    try:
        fetch_artifact(args.endpoint, args.receipt, args.archive)
    except (OSError, ValueError) as error:
        parser.exit(1, f"{error}\n")


if __name__ == "__main__":
    main()
