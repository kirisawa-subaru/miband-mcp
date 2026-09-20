"""Freshness signals for the LLM to reason about staleness.

Only reports raw numbers; does not classify into status strings.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Any

from .db import LOCAL_TZ, _candidate_paths, _quick_latest_ts, db_path, iso_from_millis, iso_from_seconds


def _scalar(con: sqlite3.Connection, sql: str) -> Any:
    cur = con.execute(sql)
    row = cur.fetchone()
    if row is None:
        return None
    return row[0]


def latest_data_ts_seconds(con: sqlite3.Connection) -> int | None:
    """Max timestamp across all v0 tables, normalized to unix seconds."""
    candidates: list[int] = []

    sec_sql = [
        "SELECT MAX(TIMESTAMP) FROM XIAOMI_ACTIVITY_SAMPLE",
        "SELECT MAX(TIMESTAMP) FROM BATTERY_LEVEL",
    ]
    ms_sql = [
        "SELECT MAX(TIMESTAMP) FROM XIAOMI_DAILY_SUMMARY_SAMPLE",
        "SELECT MAX(TIMESTAMP) FROM XIAOMI_MANUAL_SAMPLE",
        "SELECT MAX(TIMESTAMP) FROM XIAOMI_SLEEP_TIME_SAMPLE",
        "SELECT MAX(TIMESTAMP) FROM XIAOMI_SLEEP_STAGE_SAMPLE",
        "SELECT MAX(WAKEUP_TIME) FROM XIAOMI_SLEEP_TIME_SAMPLE",
    ]
    for sql in sec_sql:
        v = _scalar(con, sql)
        if v is not None:
            candidates.append(int(v))
    for sql in ms_sql:
        v = _scalar(con, sql)
        if v is not None:
            candidates.append(int(v) // 1000)

    return max(candidates) if candidates else None


def freshness_block(con: sqlite3.Connection) -> dict:
    p = db_path()
    mtime_unix = int(p.stat().st_mtime)
    db_mtime_iso = iso_from_seconds(mtime_unix)
    size = p.stat().st_size

    latest_sec = latest_data_ts_seconds(con)
    data_latest_iso = iso_from_seconds(latest_sec) if latest_sec else None

    now_unix = int(datetime.now(LOCAL_TZ).timestamp())
    now_iso = iso_from_seconds(now_unix)

    if latest_sec is None:
        data_age_min = None
    else:
        data_age_min = max(0, (now_unix - latest_sec) // 60)

    block = {
        "now": now_iso,
        "db_mtime": db_mtime_iso,
        "db_size_bytes": size,
        "data_latest": data_latest_iso,
        "data_age_min": data_age_min,
        "db_source": str(p),
    }

    # When more than one candidate is available, surface what the rejected
    # alternative looked like — useful for catching a sync source that's
    # silently lagging.
    candidates = _candidate_paths()
    if len(candidates) > 1:
        seen = []
        for cp in candidates:
            entry: dict = {"path": str(cp), "exists": cp.exists()}
            if cp.exists():
                ts = _quick_latest_ts(cp)
                if ts > 0:
                    age_min = max(0, (now_unix - ts) // 60)
                    entry["data_age_min"] = age_min
                else:
                    entry["data_age_min"] = None
            entry["chosen"] = cp.resolve() == p.resolve()
            seen.append(entry)
        block["candidates"] = seen

    return block


def row_counts(con: sqlite3.Connection) -> dict:
    return {
        "activity": _scalar(con, "SELECT COUNT(*) FROM XIAOMI_ACTIVITY_SAMPLE") or 0,
        "daily": _scalar(con, "SELECT COUNT(*) FROM XIAOMI_DAILY_SUMMARY_SAMPLE") or 0,
        "manual": _scalar(con, "SELECT COUNT(*) FROM XIAOMI_MANUAL_SAMPLE") or 0,
        "sleep_time": _scalar(con, "SELECT COUNT(*) FROM XIAOMI_SLEEP_TIME_SAMPLE") or 0,
        "sleep_stage": _scalar(con, "SELECT COUNT(*) FROM XIAOMI_SLEEP_STAGE_SAMPLE") or 0,
        "battery": _scalar(con, "SELECT COUNT(*) FROM BATTERY_LEVEL") or 0,
    }
