"""Mac-side append-only history archive.

Source DB has a rolling window — Gadgetbridge on phone drops old data
periodically. This archive accumulates rows over time using the source
tables' built-in `ON CONFLICT REPLACE` PK behavior. Idempotent re-sync.

Lives at MIBAND_ARCHIVE_PATH (default: ~/Library/Application Support/
mibandctl/archive.db). Not synced; pure local accumulation.
"""
from __future__ import annotations

import os
import re
import sqlite3
from datetime import datetime
from pathlib import Path

from .db import LOCAL_TZ, db_path, sqlite_ro_uri

ARCHIVE_TABLES = [
    "DEVICE",
    "XIAOMI_ACTIVITY_SAMPLE",
    "XIAOMI_DAILY_SUMMARY_SAMPLE",
    "XIAOMI_MANUAL_SAMPLE",
    "XIAOMI_SLEEP_TIME_SAMPLE",
    "XIAOMI_SLEEP_STAGE_SAMPLE",
    "BATTERY_LEVEL",
]

# Per-table TIMESTAMP unit, for min/max conversion in info().
TS_UNIT = {
    "XIAOMI_ACTIVITY_SAMPLE": "seconds",
    "BATTERY_LEVEL": "seconds",
    "XIAOMI_DAILY_SUMMARY_SAMPLE": "millis",
    "XIAOMI_MANUAL_SAMPLE": "millis",
    "XIAOMI_SLEEP_TIME_SAMPLE": "millis",
    "XIAOMI_SLEEP_STAGE_SAMPLE": "millis",
    "DEVICE": None,
}

DEFAULT_ARCHIVE_PATH = "~/Library/Application Support/mibandctl/archive.db"


def archive_path() -> Path:
    return Path(os.environ.get("MIBAND_ARCHIVE_PATH", DEFAULT_ARCHIVE_PATH)).expanduser()


def _ensure_if_not_exists(create_sql: str) -> str:
    """Insert IF NOT EXISTS into a CREATE TABLE statement when missing."""
    return re.sub(
        r"^CREATE\s+TABLE\s+(?!IF\s+NOT\s+EXISTS)",
        "CREATE TABLE IF NOT EXISTS ",
        create_sql,
        count=1,
        flags=re.IGNORECASE,
    )


def _create_sql(con: sqlite3.Connection, table: str) -> str | None:
    row = con.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    return row[0] if row else None


def _columns(con: sqlite3.Connection, table: str) -> list[str]:
    return [r[1] for r in con.execute(f'PRAGMA table_info("{table}")').fetchall()]


def _table_exists(con: sqlite3.Connection, table: str) -> bool:
    return (
        con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone()
        is not None
    )


def _iso(ts_seconds: int) -> str:
    return datetime.fromtimestamp(ts_seconds, tz=LOCAL_TZ).isoformat(timespec="seconds")


def sync() -> dict:
    """UPSERT all v0 tables from source into archive. Idempotent."""
    src = db_path()
    if not src.exists():
        raise FileNotFoundError(f"source DB not found at {src}")
    arc = archive_path()
    arc.parent.mkdir(parents=True, exist_ok=True)

    src_con = sqlite3.connect(sqlite_ro_uri(src), uri=True)
    arc_con = sqlite3.connect(arc)

    src_uv = src_con.execute("PRAGMA user_version").fetchone()[0]
    arc_uv = arc_con.execute("PRAGMA user_version").fetchone()[0]

    schema_drift: dict[str, dict] = {}
    table_stats: dict[str, dict] = {}

    try:
        arc_con.execute("BEGIN")
        if arc_uv == 0:
            arc_con.execute(f"PRAGMA user_version = {src_uv}")
        elif arc_uv != src_uv:
            schema_drift["__user_version__"] = {"archive": arc_uv, "source": src_uv}

        for table in ARCHIVE_TABLES:
            create = _create_sql(src_con, table)
            if not create:
                table_stats[table] = {"skipped": "missing in source"}
                continue

            arc_con.execute(_ensure_if_not_exists(create))

            src_cols = set(_columns(src_con, table))
            arc_cols = set(_columns(arc_con, table))
            if src_cols != arc_cols:
                schema_drift[table] = {
                    "src_only": sorted(src_cols - arc_cols),
                    "arc_only": sorted(arc_cols - src_cols),
                }
                # Insert only the columns both sides know about.
                shared = sorted(src_cols & arc_cols)
            else:
                shared = sorted(src_cols)

            collist = ",".join(f'"{c}"' for c in shared)
            placeholders = ",".join("?" * len(shared))

            before = arc_con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
            rows = src_con.execute(f'SELECT {collist} FROM "{table}"').fetchall()
            arc_con.executemany(
                f'INSERT OR REPLACE INTO "{table}" ({collist}) VALUES ({placeholders})',
                rows,
            )
            after = arc_con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]

            table_stats[table] = {
                "src_rows": len(rows),
                "archive_before": before,
                "archive_after": after,
                "delta": after - before,
            }

        arc_con.commit()
    except Exception:
        arc_con.rollback()
        raise
    finally:
        src_con.close()
        arc_con.close()

    return {
        "archive_path": str(arc),
        "source_path": str(src),
        "user_version": src_uv,
        "schema_drift": schema_drift or None,
        "tables": table_stats,
    }


def info() -> dict:
    arc = archive_path()
    if not arc.exists():
        return {"archive_path": str(arc), "exists": False}

    con = sqlite3.connect(sqlite_ro_uri(arc), uri=True)
    try:
        size = arc.stat().st_size
        mtime = _iso(int(arc.stat().st_mtime))
        uv = con.execute("PRAGMA user_version").fetchone()[0]

        tables: dict[str, dict] = {}
        for table in ARCHIVE_TABLES:
            if not _table_exists(con, table):
                tables[table] = {"exists": False}
                continue
            count = con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
            entry: dict = {"rows": count}

            unit = TS_UNIT.get(table)
            if unit and count > 0:
                row = con.execute(
                    f'SELECT MIN(TIMESTAMP), MAX(TIMESTAMP) FROM "{table}"'
                ).fetchone()
                mn, mx = row[0], row[1]
                if unit == "millis":
                    mn, mx = mn // 1000, mx // 1000
                entry["oldest"] = _iso(mn)
                entry["newest"] = _iso(mx)
            tables[table] = entry

        return {
            "archive_path": str(arc),
            "exists": True,
            "db_size_bytes": size,
            "db_mtime": mtime,
            "user_version": uv,
            "tables": tables,
        }
    finally:
        con.close()
