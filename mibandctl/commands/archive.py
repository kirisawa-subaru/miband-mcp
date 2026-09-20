"""archive: history archive operations (sync / info)."""
from __future__ import annotations

from .. import archive as _archive


def run(args) -> dict:
    sub = args.archive_cmd
    if sub == "sync":
        return _archive.sync()
    if sub == "info":
        return _archive.info()
    raise ValueError(f"unknown archive subcommand: {sub}")
