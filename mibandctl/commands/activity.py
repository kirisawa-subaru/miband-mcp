"""activity: 最近 N 天 步数 / 距离 / 活动卡路里，默认小时桶，--minute 下钻。"""
from __future__ import annotations

from collections import defaultdict

from ..db import (
    connect,
    hour_floor_local,
    iso_from_seconds,
    resolve_window_seconds,
)
from ..freshness import freshness_block


def _resolve_window(args) -> tuple[int, int]:
    return resolve_window_seconds(args)


def _query_rows(con, start: int, end: int):
    return con.execute(
        "SELECT TIMESTAMP, STEPS, DISTANCE_CM, ACTIVE_CALORIES "
        "FROM XIAOMI_ACTIVITY_SAMPLE "
        "WHERE TIMESTAMP >= ? AND TIMESTAMP <= ? "
        "ORDER BY TIMESTAMP",
        (start, end),
    ).fetchall()


def _bucket_hourly(rows) -> list[dict]:
    buckets: dict[int, dict] = defaultdict(
        lambda: {"steps": 0, "distance_cm": 0, "active_calories": 0, "samples": 0}
    )
    for r in rows:
        h = hour_floor_local(r["TIMESTAMP"])
        b = buckets[h]
        b["steps"] += r["STEPS"] or 0
        b["distance_cm"] += r["DISTANCE_CM"] or 0
        b["active_calories"] += r["ACTIVE_CALORIES"] or 0
        b["samples"] += 1

    return [
        {
            "hour": iso_from_seconds(h),
            "samples": b["samples"],
            "steps": b["steps"],
            "distance_m": round(b["distance_cm"] / 100.0, 1),
            "active_calories": b["active_calories"],
        }
        for h, b in sorted(buckets.items())
    ]


def run(args) -> dict:
    start, end = _resolve_window(args)
    with connect() as con:
        rows = _query_rows(con, start, end)

        if args.minute:
            data = {
                "samples": [
                    {
                        "at": iso_from_seconds(r["TIMESTAMP"]),
                        "steps": r["STEPS"],
                        "distance_m": round((r["DISTANCE_CM"] or 0) / 100.0, 2),
                        "active_calories": r["ACTIVE_CALORIES"],
                    }
                    for r in rows
                ]
            }
        else:
            data = {"buckets": _bucket_hourly(rows)}

        return {
            "schema": {
                "steps": "count",
                "distance": "m",
                "calories": "kcal",
            },
            "tz": "+08:00",
            "window": {
                "start": iso_from_seconds(start),
                "end": iso_from_seconds(end),
            },
            "freshness": freshness_block(con),
            "data": data,
        }
