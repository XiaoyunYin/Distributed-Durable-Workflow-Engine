"""Kind-only worker bootstrap. Installed as a read-only ConfigMap in Phase G."""

import json
import os
import time
from pathlib import Path
from uuid import uuid4


def main() -> None:
    nonce = uuid4().hex
    Path("/tmp/g-boot").write_text(nonce, encoding="ascii")
    path = Path("/g-bootstrap") / nonce
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        if path.exists():
            supplied = json.loads(path.read_bytes())
            os.environ.update(supplied)
            os.execvp("python", ["python", "-m", "workers"])
        time.sleep(0.1)
    raise RuntimeError("G_RUNTIME_BOOTSTRAP_TIMEOUT")


if __name__ == "__main__":
    main()
