#!/usr/bin/env python3
"""Install the pinned, localhost-only runtime on the configured arm64 Android phone.

This installs a binary; no daemon or boot service is created. The background sync and measurement
tools own each short-lived process and SSH tunnel.
"""
from __future__ import annotations

import hashlib
import lzma
import os
from pathlib import Path
import shutil
import subprocess
import urllib.request

from mibandctl.health.phone_exporter import verify_device, _ssh_base, _root_command
from mibandctl.health.settings import Settings

VERSION = "16.7.19"
NAME = f"frida-server-{VERSION}-android-arm64"
URL = f"https://github.com/frida/frida/releases/download/{VERSION}/{NAME}.xz"
XZ_SHA256 = "36ec3d7474b1ac69c4e7ec985612fae771d37ffb71cb94858bc6978f69f5e581"
BIN_SHA256 = "4eebf1fbc66ff54aba9a9124c2ef8b32b566616388c60e2caa65148a529d826a"
REMOTE = f"/data/local/miband-health/frida-server-{VERSION}"


def digest(path: Path) -> str:
    checksum = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            checksum.update(chunk)
    return checksum.hexdigest()


def main() -> None:
    settings = Settings.from_env()
    if settings.backend != "xiaomi_health":
        raise SystemExit("This optional runtime is only for the Xiaomi Health backend")
    verify_device(settings.ssh_host, settings.device_serial, timeout_seconds=15)
    cache = Path(os.environ.get("XDG_CACHE_HOME", str(Path.home() / ".cache"))) / "miband-health/runtime"
    cache.mkdir(parents=True, exist_ok=True)
    packed, binary = cache / (NAME + ".xz"), cache / NAME
    if not packed.exists() or digest(packed) != XZ_SHA256:
        partial = packed.with_suffix(".download")
        try:
            with urllib.request.urlopen(URL, timeout=60) as src, partial.open("wb") as dst:
                shutil.copyfileobj(src, dst)
            if digest(partial) != XZ_SHA256:
                raise RuntimeError("Frida release checksum mismatch")
            partial.replace(packed)
        finally:
            partial.unlink(missing_ok=True)
    if not binary.exists() or digest(binary) != BIN_SHA256:
        with lzma.open(packed, "rb") as src, binary.open("wb") as dst:
            shutil.copyfileobj(src, dst)
        if digest(binary) != BIN_SHA256:
            binary.unlink()
            raise RuntimeError("Frida binary checksum mismatch")
    command = (
        "mkdir -p /data/local/miband-health && chmod 700 /data/local/miband-health && "
        f"cat > {REMOTE}.new && chmod 700 {REMOTE}.new && "
        f"test \"$(sha256sum {REMOTE}.new | cut -d ' ' -f 1)\" = {BIN_SHA256} && "
        f"mv {REMOTE}.new {REMOTE}"
    )
    try:
        with binary.open("rb") as src:
            subprocess.run(_ssh_base(settings.ssh_host, 60) + [_root_command(command)],
                           stdin=src, check=True, timeout=60)
    finally:
        subprocess.run(_ssh_base(settings.ssh_host, 10) + [_root_command(f"rm -f {REMOTE}.new")],
                       timeout=10, check=False)
    print(f"Installed verified {REMOTE}; no service started.")


if __name__ == "__main__":
    main()
