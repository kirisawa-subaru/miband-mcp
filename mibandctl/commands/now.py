"""now: 当下小快照。最新 HR + 最近 1h 步数 + 电量 + freshness。"""
from __future__ import annotations

from ..db import (
    avg,
    connect,
    iso_from_millis,
    iso_from_seconds,
    normalize_hr,
)
from ..freshness import freshness_block, latest_data_ts_seconds


def _latest_hr(con) -> dict | None:
    """Pick the freshest HR reading from activity (auto) or manual (user-triggered)."""
    auto = con.execute(
        "SELECT TIMESTAMP, HEART_RATE FROM XIAOMI_ACTIVITY_SAMPLE "
        "WHERE HEART_RATE > 0 ORDER BY TIMESTAMP DESC LIMIT 1"
    ).fetchone()
    manual = con.execute(
        "SELECT TIMESTAMP, VALUE FROM XIAOMI_MANUAL_SAMPLE "
        "WHERE TYPE = 17 ORDER BY TIMESTAMP DESC LIMIT 1"
    ).fetchone()

    candidates = []
    if auto:
        candidates.append((auto["TIMESTAMP"], auto["HEART_RATE"], "auto"))
    if manual:
        candidates.append((manual["TIMESTAMP"] // 1000, manual["VALUE"], "manual"))

    if not candidates:
        return None

    ts, val, source = max(candidates, key=lambda x: x[0])
    return {"value": val, "at": iso_from_seconds(ts), "source": source}


def _latest_battery(con) -> dict | None:
    row = con.execute(
        "SELECT TIMESTAMP, LEVEL FROM BATTERY_LEVEL ORDER BY TIMESTAMP DESC LIMIT 1"
    ).fetchone()
    if row is None:
        return None
    return {"value": row["LEVEL"], "at": iso_from_seconds(row["TIMESTAMP"])}


def _last_hour_window(con) -> dict | None:
    latest = latest_data_ts_seconds(con)
    if latest is None:
        return None
    start = latest - 3600
    rows = con.execute(
        "SELECT STEPS, DISTANCE_CM, ACTIVE_CALORIES, HEART_RATE "
        "FROM XIAOMI_ACTIVITY_SAMPLE WHERE TIMESTAMP > ? AND TIMESTAMP <= ?",
        (start, latest),
    ).fetchall()

    steps = sum(r["STEPS"] or 0 for r in rows)
    distance_m = sum(r["DISTANCE_CM"] or 0 for r in rows) / 100.0
    active_calories = sum(r["ACTIVE_CALORIES"] or 0 for r in rows)
    hrs = [r["HEART_RATE"] for r in rows if r["HEART_RATE"] and r["HEART_RATE"] > 0]

    return {
        "window_start": iso_from_seconds(start),
        "window_end": iso_from_seconds(latest),
        "steps": steps,
        "distance_m": round(distance_m, 1),
        "active_calories": active_calories,
        "hr_samples": len(hrs),
        "hr_min": min(hrs) if hrs else None,
        "hr_max": max(hrs) if hrs else None,
        "hr_avg": avg(hrs),
    }


def run(_args) -> dict:
    with connect() as con:
        return {
            "schema": {
                "hr": "bpm",
                "battery": "percent",
                "steps": "count",
                "distance": "m",
                "calories": "kcal",
            },
            "tz": "+08:00",
            "freshness": freshness_block(con),
            "data": {
                "latest_hr": _latest_hr(con),
                "latest_battery": _latest_battery(con),
                "last_hour": _last_hour_window(con),
            },
        }
