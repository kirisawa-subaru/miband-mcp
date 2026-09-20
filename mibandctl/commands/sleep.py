"""sleep: 最近 N 夜，sleep_time + 阶段转换。"""
from __future__ import annotations

from ..db import (
    connect,
    decode_session_state,
    decode_stage,
    iso_from_millis,
    resolve_window_millis,
)
from ..freshness import freshness_block


def _resolve_window_ms(args) -> tuple[int, int]:
    return resolve_window_millis(args)


def _stages_for_night(con, night_start_ms: int, wakeup_ms: int) -> list[dict]:
    """Stage transitions strictly within [night_start, wakeup]."""
    rows = con.execute(
        "SELECT TIMESTAMP, STAGE FROM XIAOMI_SLEEP_STAGE_SAMPLE "
        "WHERE TIMESTAMP >= ? AND TIMESTAMP <= ? ORDER BY TIMESTAMP",
        (night_start_ms, wakeup_ms),
    ).fetchall()
    return [
        {"at": iso_from_millis(r["TIMESTAMP"]), "stage": decode_stage(r["STAGE"])}
        for r in rows
    ]


def run(args) -> dict:
    start_ms, end_ms = _resolve_window_ms(args)

    with connect() as con:
        # Pick rows whose wakeup falls in the requested range. A previous-evening
        # bedtime is still included because the range applies to WAKEUP_TIME.
        rows = con.execute(
            "SELECT TIMESTAMP, WAKEUP_TIME, IS_AWAKE, TOTAL_DURATION, "
            "DEEP_SLEEP_DURATION, LIGHT_SLEEP_DURATION, REM_SLEEP_DURATION, "
            "AWAKE_DURATION FROM XIAOMI_SLEEP_TIME_SAMPLE "
            "WHERE WAKEUP_TIME >= ? AND WAKEUP_TIME <= ? "
            "ORDER BY TIMESTAMP DESC",
            (start_ms, end_ms),
        ).fetchall()

        nights = []
        for r in rows:
            bedtime_ms = r["TIMESTAMP"]
            wakeup_ms = r["WAKEUP_TIME"]
            nights.append(
                {
                    "sleep_start": iso_from_millis(bedtime_ms),
                    "wakeup_time": iso_from_millis(wakeup_ms),
                    "session_state": decode_session_state(r["IS_AWAKE"]),
                    "total_min": r["TOTAL_DURATION"],
                    "deep_min": r["DEEP_SLEEP_DURATION"],
                    "light_min": r["LIGHT_SLEEP_DURATION"],
                    "rem_min": r["REM_SLEEP_DURATION"],
                    "awake_min": r["AWAKE_DURATION"],
                    "stages": _stages_for_night(con, bedtime_ms, wakeup_ms),
                }
            )

        return {
            "schema": {"duration": "min"},
            "tz": "+08:00",
            "freshness": freshness_block(con),
            "data": {"nights": nights},
        }
