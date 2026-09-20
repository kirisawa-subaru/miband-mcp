"""daily: 最近 N 天 XIAOMI_DAILY_SUMMARY_SAMPLE 整行 + STANDING 解码。"""
from __future__ import annotations

from datetime import datetime

from ..db import (
    LOCAL_TZ,
    connect,
    decode_standing_bitmask,
    decode_tz_offset,
    normalize_hr,
    normalize_spo2,
    normalize_stress,
    resolve_window_millis,
)
from ..freshness import freshness_block


def _resolve_window_ms(args) -> tuple[int, int]:
    return resolve_window_millis(args)


def _row_to_dict(r) -> dict:
    ts_ms = r["TIMESTAMP"]
    date_str = datetime.fromtimestamp(ts_ms / 1000, tz=LOCAL_TZ).date().isoformat()
    return {
        "date": date_str,
        "tz": decode_tz_offset(r["TIMEZONE"]),
        "steps": r["STEPS"],
        "calories": r["CALORIES"],
        "hr_resting": normalize_hr(r["HR_RESTING"]),
        "hr_min": normalize_hr(r["HR_MIN"]),
        "hr_max": normalize_hr(r["HR_MAX"]),
        "hr_avg": normalize_hr(r["HR_AVG"]),
        "stress_avg": normalize_stress(r["STRESS_AVG"]),
        "stress_min": normalize_stress(r["STRESS_MIN"]),
        "stress_max": normalize_stress(r["STRESS_MAX"]),
        "spo2_avg": normalize_spo2(r["SPO2_AVG"]),
        "spo2_min": normalize_spo2(r["SPO2_MIN"]),
        "spo2_max": normalize_spo2(r["SPO2_MAX"]),
        "standing_hours": decode_standing_bitmask(r["STANDING"]),
        "training_load_day": r["TRAINING_LOAD_DAY"],
        "training_load_week": r["TRAINING_LOAD_WEEK"],
        "training_load_level": r["TRAINING_LOAD_LEVEL"],
        "vitality_increase_light": r["VITALITY_INCREASE_LIGHT"],
        "vitality_increase_moderate": r["VITALITY_INCREASE_MODERATE"],
        "vitality_increase_high": r["VITALITY_INCREASE_HIGH"],
        "vitality_current": r["VITALITY_CURRENT"],
    }


def run(args) -> dict:
    start_ms, end_ms = _resolve_window_ms(args)
    with connect() as con:
        rows = con.execute(
            "SELECT * FROM XIAOMI_DAILY_SUMMARY_SAMPLE "
            "WHERE TIMESTAMP >= ? AND TIMESTAMP <= ? ORDER BY TIMESTAMP DESC",
            (start_ms, end_ms),
        ).fetchall()

        return {
            "schema": {
                "steps": "count",
                "calories": "kcal",
                "hr": "bpm",
                "stress": "0-100",
                "spo2": "percent",
                "standing_hours": "hour-of-day [0..23]",
            },
            "tz": "+08:00",
            "freshness": freshness_block(con),
            "data": {"days": [_row_to_dict(r) for r in rows]},
        }
