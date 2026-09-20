"""hr: 最近 N 天心率，默认小时桶（min/max/avg/samples），--minute 下钻。"""
from __future__ import annotations

from collections import defaultdict

from ..db import (
    avg,
    connect,
    hour_floor_local,
    iso_from_seconds,
    resolve_window_seconds,
)
from ..freshness import freshness_block


def _resolve_window(args) -> tuple[int, int]:
    return resolve_window_seconds(args)


def _query_hr_rows(con, start: int, end: int):
    auto = con.execute(
        "SELECT TIMESTAMP, HEART_RATE FROM XIAOMI_ACTIVITY_SAMPLE "
        "WHERE TIMESTAMP >= ? AND TIMESTAMP <= ? AND HEART_RATE > 0 "
        "ORDER BY TIMESTAMP",
        (start, end),
    ).fetchall()
    manual = con.execute(
        "SELECT TIMESTAMP, VALUE FROM XIAOMI_MANUAL_SAMPLE "
        "WHERE TYPE = 17 AND TIMESTAMP >= ? AND TIMESTAMP <= ?",
        (start * 1000, end * 1000),
    ).fetchall()
    return auto, manual


def _bucket_hourly(auto_rows) -> list[dict]:
    buckets: dict[int, list[int]] = defaultdict(list)
    for r in auto_rows:
        h = hour_floor_local(r["TIMESTAMP"])
        buckets[h].append(r["HEART_RATE"])

    out = []
    for h in sorted(buckets):
        hrs = buckets[h]
        out.append(
            {
                "hour": iso_from_seconds(h),
                "samples": len(hrs),
                "hr_min": min(hrs),
                "hr_max": max(hrs),
                "hr_avg": avg(hrs),
            }
        )
    return out


def run(args) -> dict:
    start, end = _resolve_window(args)
    with connect() as con:
        auto, manual = _query_hr_rows(con, start, end)
        manual_data = [
            {"at": iso_from_seconds(r["TIMESTAMP"] // 1000), "hr": r["VALUE"]}
            for r in manual
        ]

        if args.minute:
            data = {
                "samples": [
                    {"at": iso_from_seconds(r["TIMESTAMP"]), "hr": r["HEART_RATE"]}
                    for r in auto
                ],
                "manual": manual_data,
            }
        else:
            data = {"buckets": _bucket_hourly(auto), "manual": manual_data}

        return {
            "schema": {"hr": "bpm"},
            "tz": "+08:00",
            "window": {
                "start": iso_from_seconds(start),
                "end": iso_from_seconds(end),
            },
            "freshness": freshness_block(con),
            "data": data,
        }
