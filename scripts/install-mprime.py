#!/usr/bin/env python3
"""Install the official Linux x86-64 mprime and register it with CoreCycler.

Run using the Python environment in which CoreCycler is installed.
"""
from __future__ import annotations

import hashlib
import platform
import subprocess
import tarfile
import tempfile
import urllib.request
from pathlib import Path

from corecycler.config.paths import user_home
from corecycler.config.tools import record_path

VERSION = "30.19b20"
URL = "https://download.mersenne.ca/gimps/v30/30.19/p95v3019b20.linux64.tar.gz"
# Published at https://www.mersenne.org/download/
SHA256 = "4ce2377e03deb4cf189523136e26401ba08f67857a128e420dd030d00cdca601"


def main() -> None:
    if platform.system() != "Linux" or platform.machine() != "x86_64":
        raise SystemExit("This installer supports Linux x86-64 only.")
    destination = user_home() / ".local/share/corecycler/backends" / f"mprime-{VERSION}"
    with tempfile.TemporaryDirectory(prefix="corecycler-mprime-") as temporary:
        archive = Path(temporary) / "mprime.tar.gz"
        print(f"Downloading official mprime {VERSION}…")
        with urllib.request.urlopen(URL, timeout=60) as response:
            archive.write_bytes(response.read())
        if hashlib.sha256(archive.read_bytes()).hexdigest() != SHA256:
            raise SystemExit("Checksum mismatch: nothing installed or configured.")
        staging = Path(temporary) / "unpacked"
        with tarfile.open(archive) as bundle:
            bundle.extractall(staging, filter="data")
        binary = staging / "mprime"
        binary.chmod(0o755)
        result = subprocess.run([str(binary), "-v"], capture_output=True, text=True, timeout=10, check=True)
        destination.mkdir(parents=True, exist_ok=True)
        # Install the executable and its documentation; system libgmp is used.
        for name in ("mprime", "license.txt", "readme.txt", "stress.txt", "undoc.txt", "whatsnew.txt"):
            source = staging / name
            if source.is_file():
                target = destination / name
                target.write_bytes(source.read_bytes())
                target.chmod(0o755 if name == "mprime" else 0o644)
        record_path("mprime", str(destination / "mprime"))
    print(result.stdout.strip())
    print(f"Registered {destination / 'mprime'}")
    print("Restart CoreCycler, then resume your saved session.")


if __name__ == "__main__":
    main()
