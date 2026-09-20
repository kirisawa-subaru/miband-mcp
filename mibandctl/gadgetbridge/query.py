"""Read-only queries over a raw Gadgetbridge SQLite export.

The source database remains a Gadgetbridge database.  This module translates
its confirmed public columns at the query boundary; it does not pretend that
the tables are Xiaomi Health exports or decode opaque workout summary blobs.
"""

from __future__ import annotations

import hashlib
import math
import os
import re
import sqlite3
from collections import defaultdict
from datetime import date as Date
from datetime import datetime, time, timedelta, timezone as dt_timezone
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


SOURCE = "gadgetbridge"
MAX_RANGE_SECONDS = 366 * 24 * 60 * 60
MAX_LIMIT = 500
SESSION_ID_RE = re.compile(r"^(?:slp|wrk)_[0-9a-f]{16}$")

REQUIRED_SCHEMA: dict[str, set[str]] = {
    "DEVICE": {"_id", "NAME", "MANUFACTURER", "IDENTIFIER", "TYPE_NAME", "MODEL", "ALIAS"},
    "XIAOMI_ACTIVITY_SAMPLE": {
        "TIMESTAMP", "DEVICE_ID", "STEPS", "HEART_RATE", "DISTANCE_CM", "ACTIVE_CALORIES"
    },
    "XIAOMI_DAILY_SUMMARY_SAMPLE": {
        "TIMESTAMP", "DEVICE_ID", "TIMEZONE", "STEPS", "HR_MIN", "HR_MAX", "HR_AVG",
        "CALORIES", "ACTIVE_CALORIES",
    },
    "XIAOMI_MANUAL_SAMPLE": {"TIMESTAMP", "DEVICE_ID", "TYPE", "VALUE"},
    "BATTERY_LEVEL": {"TIMESTAMP", "DEVICE_ID", "LEVEL", "BATTERY_INDEX"},
    "XIAOMI_SLEEP_TIME_SAMPLE": {
        "TIMESTAMP", "DEVICE_ID", "WAKEUP_TIME", "IS_AWAKE", "TOTAL_DURATION",
        "DEEP_SLEEP_DURATION", "LIGHT_SLEEP_DURATION", "REM_SLEEP_DURATION", "AWAKE_DURATION",
    },
    "XIAOMI_SLEEP_STAGE_SAMPLE": {"TIMESTAMP", "DEVICE_ID", "STAGE"},
    "BASE_ACTIVITY_SUMMARY": {
        "_id", "NAME", "START_TIME", "END_TIME", "ACTIVITY_KIND", "DEVICE_ID",
        "SUMMARY_DATA", "RAW_SUMMARY_DATA",
    },
}

TIMESERIES_METRICS: dict[str, dict[str, Any]] = {
    "heart_rate.bpm": {
        "table": "XIAOMI_ACTIVITY_SAMPLE", "column": "HEART_RATE", "unit": "bpm",
        "aggregate": "avg", "valid": '("HEART_RATE" > 0 AND "HEART_RATE" != 255)',
    },
    "steps": {
        "table": "XIAOMI_ACTIVITY_SAMPLE", "column": "STEPS", "unit": "count",
        "aggregate": "sum", "valid": '"STEPS" IS NOT NULL',
    },
    "distance": {
        "table": "XIAOMI_ACTIVITY_SAMPLE", "column": '"DISTANCE_CM" / 100.0', "unit": "m",
        "aggregate": "sum", "valid": '"DISTANCE_CM" IS NOT NULL',
    },
    "calories": {
        "table": "XIAOMI_ACTIVITY_SAMPLE", "column": "ACTIVE_CALORIES", "unit": "kcal",
        "aggregate": "sum", "valid": '"ACTIVE_CALORIES" IS NOT NULL',
    },
    "battery.level": {
        "table": "BATTERY_LEVEL", "column": "LEVEL", "unit": "percent",
        "aggregate": "last", "valid": '"LEVEL" BETWEEN 0 AND 100',
    },
}

KNOWN_UNSUPPORTED_METRICS = {
    "stand.valid_hour",
    "training_load.current_day",
    "vitality.latest_accumulated",
    "weight",
    "bmi",
    "vo2_max",
}

CURRENT_UNITS = {
    "heart_rate": "bpm",
    "steps_30m": "count",
    "battery": "percent",
    "latest_sleep.duration": "min",
    "latest_workout.duration": "sec",
}

DAILY_UNITS = {
    "heart_rate.avg": "bpm",
    "steps": "count",
    "distance": "m",
    "calories": "kcal",
    "sleep.duration": "min",
    "workouts.duration": "sec",
}

SLEEP_STAGE_LABELS = {
    0: "not_sleep",
    1: "na",
    2: "deep",
    3: "light",
    4: "rem",
    5: "awake",
}


class GadgetbridgeSchemaError(RuntimeError):
    """The file is SQLite, but not the supported Gadgetbridge raw schema."""


def _zone(name: str) -> ZoneInfo:
    try:
        return ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise ValueError(f"unknown timezone: {name}") from exc


def _settings_zone(settings: Any, override: str | None = None) -> tuple[str, ZoneInfo]:
    name = override or getattr(settings, "timezone", None) or "UTC"
    return name, _zone(name)


def _db_path(settings: Any) -> Path:
    value = getattr(settings, "db_path", None)
    if callable(value):
        value = value()
    if value is None:
        value = Path.home() / ".local/share/miband-gadgetbridge/Gadgetbridge.db"
    return Path(value).expanduser().resolve()


def _public_id(kind: str, device_id: Any, raw_record_id: Any) -> str:
    prefix = "slp" if kind == "sleep" else "wrk"
    digest = hashlib.sha256(
        f"gadgetbridge:{kind}:{device_id}:{raw_record_id}".encode("utf-8")
    ).hexdigest()[:16]
    return f"{prefix}_{digest}"


def _stat_identity(stat: os.stat_result) -> dict[str, int]:
    return {
        "dev": int(stat.st_dev),
        "ino": int(stat.st_ino),
        "size": int(stat.st_size),
        "mtime_ns": int(stat.st_mtime_ns),
    }


def _identity_matches(stored: Any, snapshot: dict[str, int] | None) -> bool:
    if not isinstance(stored, dict) or snapshot is None:
        return False
    return all(stored.get(key) == snapshot[key] for key in snapshot)


def _connect(settings: Any) -> tuple[sqlite3.Connection, dict[str, int]] | None:
    path = _db_path(settings)
    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_CLOEXEC", 0))
    except FileNotFoundError:
        return None
    conn: sqlite3.Connection | None = None
    try:
        identity = _stat_identity(os.fstat(fd))
        fd_path = Path(f"/proc/self/fd/{fd}")
        # Opening through our stable fd means an atomic cache replacement can
        # no longer switch the file between identity capture and SQLite open.
        sqlite_path = fd_path if fd_path.exists() else path
        conn = sqlite3.connect(f"{sqlite_path.as_uri()}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        conn.create_function("public_session_id", 3, _public_id, deterministic=True)
        conn.execute("BEGIN")
        _validate_schema(conn)
        if sqlite_path == path:
            current = _stat_identity(path.stat())
            if current != identity:
                conn.close()
                raise RuntimeError("Gadgetbridge database changed while opening the read snapshot")
    except BaseException:
        if conn is not None:
            conn.close()
        raise
    finally:
        os.close(fd)
    return conn, identity


def _validate_schema(conn: sqlite3.Connection) -> None:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    tables = {str(row[0]) for row in rows}
    missing_tables = sorted(set(REQUIRED_SCHEMA) - tables)
    if missing_tables:
        raise GadgetbridgeSchemaError(
            "unsupported Gadgetbridge schema; missing tables: " + ", ".join(missing_tables)
        )
    missing_columns: list[str] = []
    for table, required in REQUIRED_SCHEMA.items():
        columns = {str(row[1]) for row in conn.execute(f'PRAGMA table_info("{table}")')}
        for column in sorted(required - columns):
            missing_columns.append(f"{table}.{column}")
    if missing_columns:
        raise GadgetbridgeSchemaError(
            "unsupported Gadgetbridge schema; missing columns: " + ", ".join(missing_columns)
        )


def _device_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {
        "id": int(row["_id"]),
        "address": str(row["IDENTIFIER"]),
        "name": str(row["NAME"]),
        "manufacturer": str(row["MANUFACTURER"]),
        "type_name": str(row["TYPE_NAME"]),
        "model": row["MODEL"],
        "alias": row["ALIAS"],
    }


def _data_device_ids(conn: sqlite3.Connection) -> set[int]:
    ids: set[int] = set()
    for table in REQUIRED_SCHEMA:
        if table == "DEVICE":
            continue
        for row in conn.execute(f'SELECT DISTINCT "DEVICE_ID" FROM "{table}"'):
            if row[0] is not None:
                ids.add(int(row[0]))
    return ids


def _select_device(conn: sqlite3.Connection, settings: Any) -> sqlite3.Row | None:
    configured_id = getattr(settings, "gadgetbridge_device_id", None)
    configured_address = getattr(settings, "gadgetbridge_device_address", None)
    if configured_id is not None and (
        isinstance(configured_id, bool) or not isinstance(configured_id, int)
    ):
        raise ValueError("gadgetbridge_device_id must be an integer")
    if configured_address is not None:
        if not isinstance(configured_address, str) or not configured_address.strip():
            raise ValueError("gadgetbridge_device_address must be a non-empty string")
        configured_address = configured_address.strip()

    rows = conn.execute('SELECT * FROM "DEVICE" ORDER BY "_id"').fetchall()
    if not rows:
        if _data_device_ids(conn):
            raise GadgetbridgeSchemaError(
                "Gadgetbridge samples exist but the DEVICE table is empty"
            )
        return None
    if configured_id is not None or configured_address is not None:
        matches = [
            row
            for row in rows
            if (configured_id is None or int(row["_id"]) == configured_id)
            and (
                configured_address is None
                or str(row["IDENTIFIER"]).casefold() == configured_address.casefold()
            )
        ]
        if not matches:
            selector = (
                f"id={configured_id}, address={configured_address!r}"
                if configured_id is not None and configured_address is not None
                else f"id={configured_id}" if configured_id is not None
                else f"address={configured_address!r}"
            )
            raise ValueError(f"configured Gadgetbridge device was not found ({selector})")
        return matches[0]

    data_ids = _data_device_ids(conn)
    known_ids = {int(row["_id"]) for row in rows}
    unknown_ids = sorted(data_ids - known_ids)
    if unknown_ids:
        raise GadgetbridgeSchemaError(
            "Gadgetbridge samples reference missing DEVICE rows: "
            + ", ".join(map(str, unknown_ids))
        )
    candidates = [row for row in rows if not data_ids or int(row["_id"]) in data_ids]
    if len(candidates) > 1:
        raise ValueError(
            "multiple Gadgetbridge devices contain usable data; set "
            "gadgetbridge_device_id or gadgetbridge_device_address"
        )
    return candidates[0] if candidates else None


def _read_sync_status(settings: Any) -> dict[str, Any]:
    try:
        from .sync import read_sync_status
    except ImportError:
        return {}
    result = read_sync_status(settings)
    return result if isinstance(result, dict) else {}


def _sync_fields(
    settings: Any, snapshot_identity: dict[str, int] | None
) -> dict[str, Any]:
    status = _read_sync_status(settings)
    matches = _identity_matches(status.get("cache_identity"), snapshot_identity)
    return {
        "last_pulled_at": status.get("last_pulled_at") if matches else None,
        "last_success_at": status.get("last_success_at") if matches else None,
        "sync_status": status.get("status") if matches else "unknown",
        "cache_identity_matches_snapshot": matches,
    }


def _iso(epoch: int | float | None, zone: ZoneInfo) -> str | None:
    if epoch is None:
        return None
    value = float(epoch)
    timespec = "milliseconds" if not value.is_integer() else "seconds"
    return datetime.fromtimestamp(value, tz=zone).isoformat(timespec=timespec)


def _as_number(value: Any) -> int | float | None:
    if value is None:
        return None
    number = float(value)
    if not math.isfinite(number):
        return None
    return int(number) if number.is_integer() else round(number, 2)


def _valid_hr(value: Any) -> int | float | None:
    number = _as_number(value)
    return None if number is None or number <= 0 or number == 255 else number


def _parse_instant(value: str, field: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be an ISO-8601 timestamp with an explicit offset")
    text = value.strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"{field} must be a valid ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field} must include an explicit UTC offset")
    return parsed.astimezone(dt_timezone.utc)


def _resolve_window(
    kind: str,
    start: str | None,
    end: str | None,
    *,
    now: datetime | None = None,
) -> tuple[float, float]:
    if (start is None) != (end is None):
        raise ValueError("start and end must be provided together")
    if start is None:
        end_dt = (now or datetime.now(dt_timezone.utc)).astimezone(dt_timezone.utc)
        start_dt = end_dt - timedelta(days=1 if kind == "timeseries" else 30)
    else:
        start_dt = _parse_instant(start, "start")
        end_dt = _parse_instant(end or "", "end")
    start_epoch = start_dt.timestamp()
    end_epoch = end_dt.timestamp()
    if start_epoch >= end_epoch:
        raise ValueError("start must be earlier than end")
    if end_epoch - start_epoch > MAX_RANGE_SECONDS:
        raise ValueError("query range must not exceed 366 days")
    return start_epoch, end_epoch


def _validate_page(limit: int, offset: int) -> tuple[int, int]:
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= MAX_LIMIT:
        raise ValueError(f"limit must be between 1 and {MAX_LIMIT}")
    if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
        raise ValueError("offset must be a non-negative integer")
    return limit, offset


def _age_details(
    observed_epoch: int | None,
    *,
    now_epoch: int,
    max_age_seconds: int,
) -> dict[str, Any]:
    if observed_epoch is None:
        return {"age_seconds": None, "within_max_age": False, "status": "no_data"}
    age = now_epoch - observed_epoch
    within = 0 <= age <= max_age_seconds
    return {
        "age_seconds": age,
        "within_max_age": within,
        "status": "fresh" if within else ("future" if age < 0 else "stale"),
    }


def _meta(
    *,
    zone_name: str,
    zone: ZoneInfo,
    sync: dict[str, Any],
    device: sqlite3.Row | None,
    observed_epoch: int | None,
    units: dict[str, str | None],
    missing: Iterable[str],
    truncated: bool,
    pagination: dict[str, Any] | None = None,
    unsupported: Iterable[str] = (),
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "observed_at": _iso(observed_epoch, zone),
        "last_pulled_at": sync.get("last_pulled_at"),
        "last_success_at": sync.get("last_success_at"),
        "sync_status": sync.get("sync_status"),
        "cache_identity_matches_snapshot": sync.get("cache_identity_matches_snapshot", False),
        "timezone": zone_name,
        "source": SOURCE,
        "source_schema": "gadgetbridge_raw_sqlite",
        "device": _device_dict(device),
        "units": units,
        "missing": list(missing),
        "unsupported": list(unsupported),
        "truncated": truncated,
    }
    if pagination is not None:
        result["pagination"] = pagination
    return result


def _pagination(limit: int, offset: int, returned: int, has_more: bool) -> dict[str, Any]:
    return {
        "limit": limit,
        "offset": offset,
        "returned": returned,
        "has_more": has_more,
        "next_offset": offset + returned if has_more else None,
    }


def _stage_label(value: Any) -> str | None:
    if value is None:
        return None
    number = int(value)
    return SLEEP_STAGE_LABELS.get(number, f"unknown:{number}")


def _session_state(value: Any) -> str:
    if value is None:
        return "unspecified"
    return "in_progress" if int(value) == 1 else "final"


def _sleep_item(
    conn: sqlite3.Connection,
    row: sqlite3.Row,
    *,
    device_id: int,
    zone: ZoneInfo,
) -> dict[str, Any]:
    start_ms = int(row["TIMESTAMP"])
    wake_ms = int(row["WAKEUP_TIME"])
    stages = [
        {
            "at": _iso(int(stage["TIMESTAMP"]) / 1000, zone),
            "stage": _stage_label(stage["STAGE"]),
        }
        for stage in conn.execute(
            """
            SELECT "TIMESTAMP", "STAGE" FROM "XIAOMI_SLEEP_STAGE_SAMPLE"
            WHERE "DEVICE_ID" = ? AND "TIMESTAMP" >= ? AND "TIMESTAMP" < ?
            ORDER BY "TIMESTAMP"
            """,
            (device_id, start_ms, wake_ms),
        )
    ]
    return {
        "session_id": _public_id("sleep", device_id, start_ms),
        "start_at": _iso(start_ms / 1000, zone),
        "end_at": _iso(wake_ms / 1000, zone),
        "duration_min": _as_number(row["TOTAL_DURATION"]),
        "deep_min": _as_number(row["DEEP_SLEEP_DURATION"]),
        "light_min": _as_number(row["LIGHT_SLEEP_DURATION"]),
        "rem_min": _as_number(row["REM_SLEEP_DURATION"]),
        "awake_min": _as_number(row["AWAKE_DURATION"]),
        "awake_count": None,
        "heart_rate": {"avg_bpm": None, "min_bpm": None, "max_bpm": None},
        "stages_min": {
            "deep": _as_number(row["DEEP_SLEEP_DURATION"]),
            "light": _as_number(row["LIGHT_SLEEP_DURATION"]),
            "rem": _as_number(row["REM_SLEEP_DURATION"]),
            "awake": _as_number(row["AWAKE_DURATION"]),
        },
        "stages": stages,
        "session_state": _session_state(row["IS_AWAKE"]),
        "observed_at": _iso(wake_ms / 1000, zone),
        "future": wake_ms / 1000 > datetime.now(dt_timezone.utc).timestamp(),
    }


def _workout_item(row: sqlite3.Row, *, device_id: int, zone: ZoneInfo) -> dict[str, Any]:
    start_ms = int(row["START_TIME"])
    end_ms = int(row["END_TIME"])
    duration = (end_ms - start_ms) / 1000 if end_ms >= start_ms else None
    return {
        "session_id": _public_id("workouts", device_id, row["_id"]),
        "start_at": _iso(start_ms / 1000, zone),
        "end_at": _iso(end_ms / 1000, zone),
        "name": row["NAME"],
        "activity_kind": int(row["ACTIVITY_KIND"]),
        "sport_type": int(row["ACTIVITY_KIND"]),
        "category": None,
        "duration_sec": _as_number(duration),
        "distance_m": None,
        "calories_kcal": None,
        "steps": None,
        "avg_bpm": None,
        "observed_at": _iso(end_ms / 1000, zone),
        "future": end_ms / 1000 > datetime.now(dt_timezone.utc).timestamp(),
        "unsupported_fields": ["category", "distance_m", "calories_kcal", "steps", "avg_bpm"],
    }


def _latest_sleep(
    conn: sqlite3.Connection, device_id: int, zone: ZoneInfo
) -> tuple[dict[str, Any] | None, int | None]:
    row = conn.execute(
        """
        SELECT * FROM "XIAOMI_SLEEP_TIME_SAMPLE"
        WHERE "DEVICE_ID" = ? AND "WAKEUP_TIME" IS NOT NULL
        ORDER BY "WAKEUP_TIME" DESC, "TIMESTAMP" DESC LIMIT 1
        """,
        (device_id,),
    ).fetchone()
    if row is None:
        return None, None
    item = _sleep_item(conn, row, device_id=device_id, zone=zone)
    return item, int(row["WAKEUP_TIME"]) // 1000


def _latest_workout(
    conn: sqlite3.Connection, device_id: int, zone: ZoneInfo
) -> tuple[dict[str, Any] | None, int | None]:
    row = conn.execute(
        """
        SELECT "_id", "NAME", "START_TIME", "END_TIME", "ACTIVITY_KIND"
        FROM "BASE_ACTIVITY_SUMMARY" WHERE "DEVICE_ID" = ?
        ORDER BY "START_TIME" DESC, "_id" DESC LIMIT 1
        """,
        (device_id,),
    ).fetchone()
    if row is None:
        return None, None
    item = _workout_item(row, device_id=device_id, zone=zone)
    return item, int(row["END_TIME"]) // 1000


def _empty_current(
    *,
    zone_name: str,
    zone: ZoneInfo,
    max_age_seconds: int,
    sync: dict[str, Any],
    device: sqlite3.Row | None = None,
) -> dict[str, Any]:
    return {
        "status": "no_data",
        "data": {
            "heart_rate": None,
            "steps_30m": None,
            "battery": None,
            "latest_sleep": None,
            "latest_workout": None,
        },
        "freshness": {
            "satisfied": False,
            "max_age_seconds": max_age_seconds,
            "latest_sample_at": None,
            "age_seconds": None,
            "status": "no_data",
        },
        "meta": _meta(
            zone_name=zone_name,
            zone=zone,
            sync=sync,
            device=device,
            observed_epoch=None,
            units=CURRENT_UNITS,
            missing=("heart_rate", "steps_30m", "battery", "sleep", "workouts"),
            truncated=False,
            unsupported=(
                "workout.category", "workout.distance_m", "workout.calories_kcal",
                "workout.steps", "workout.avg_bpm",
            ),
        ),
    }


def get_current_state(settings: Any, *, max_age_seconds: int = 900) -> dict[str, Any]:
    """Return the latest cached Gadgetbridge state without initiating a sync."""

    if isinstance(max_age_seconds, bool) or not isinstance(max_age_seconds, int) or max_age_seconds < 0:
        raise ValueError("max_age_seconds must be a non-negative integer")
    zone_name, zone = _settings_zone(settings)
    opened = _connect(settings)
    if opened is None:
        sync = _sync_fields(settings, None)
        return _empty_current(
            zone_name=zone_name, zone=zone, max_age_seconds=max_age_seconds, sync=sync
        )
    conn, snapshot_identity = opened
    sync = _sync_fields(settings, snapshot_identity)

    try:
        device = _select_device(conn, settings)
        if device is None:
            return _empty_current(
                zone_name=zone_name, zone=zone, max_age_seconds=max_age_seconds, sync=sync
            )
        device_id = int(device["_id"])
        now_epoch = int(datetime.now(dt_timezone.utc).timestamp())
        hr = conn.execute(
            """
            SELECT "TIMESTAMP", "HEART_RATE" FROM "XIAOMI_ACTIVITY_SAMPLE"
            WHERE "DEVICE_ID" = ? AND "HEART_RATE" > 0 AND "HEART_RATE" != 255
            ORDER BY "TIMESTAMP" DESC LIMIT 1
            """,
            (device_id,),
        ).fetchone()
        manual_hr = conn.execute(
            """
            SELECT "TIMESTAMP", "VALUE" FROM "XIAOMI_MANUAL_SAMPLE"
            WHERE "DEVICE_ID" = ? AND "TYPE" = 17 AND "VALUE" > 0 AND "VALUE" != 255
            ORDER BY "TIMESTAMP" DESC LIMIT 1
            """,
            (device_id,),
        ).fetchone()
        steps = conn.execute(
            """
            SELECT SUM("STEPS") AS value, COUNT(*) AS samples, MAX("TIMESTAMP") AS observed_epoch
            FROM "XIAOMI_ACTIVITY_SAMPLE"
            WHERE "DEVICE_ID" = ? AND "TIMESTAMP" >= ? AND "TIMESTAMP" < ?
              AND "STEPS" IS NOT NULL
            """,
            (device_id, now_epoch - 1800, now_epoch),
        ).fetchone()
        battery = conn.execute(
            """
            SELECT "TIMESTAMP", "LEVEL" FROM "BATTERY_LEVEL"
            WHERE "DEVICE_ID" = ? AND "LEVEL" BETWEEN 0 AND 100
            ORDER BY "TIMESTAMP" DESC, "BATTERY_INDEX" LIMIT 1
            """,
            (device_id,),
        ).fetchone()
        sleep_data, sleep_epoch = _latest_sleep(conn, device_id, zone)
        workout_data, workout_epoch = _latest_workout(conn, device_id, zone)
    finally:
        conn.close()

    missing: list[str] = []
    observed: list[int] = []
    hr_data = None
    auto_epoch = int(hr["TIMESTAMP"]) if hr is not None else None
    manual_epoch = int(manual_hr["TIMESTAMP"]) // 1000 if manual_hr is not None else None
    use_manual = manual_hr is not None and manual_epoch is not None and (
        auto_epoch is None or manual_epoch > auto_epoch
    )
    if use_manual:
        hr_epoch = manual_epoch
        latest_hr = _valid_hr(manual_hr["VALUE"])
        hr_series = "XIAOMI_MANUAL_SAMPLE"
        hr_basis = "manual_sample_timestamp"
    elif hr is not None and auto_epoch is not None:
        hr_epoch = auto_epoch
        latest_hr = _valid_hr(hr["HEART_RATE"])
        hr_series = "XIAOMI_ACTIVITY_SAMPLE"
        hr_basis = "sample_timestamp"
    else:
        hr_epoch = None
        latest_hr = None
        hr_series = None
        hr_basis = None
    if latest_hr is not None and hr_epoch is not None:
        details = _age_details(hr_epoch, now_epoch=now_epoch, max_age_seconds=max_age_seconds)
        hr_data = {
            "value": latest_hr if details["within_max_age"] else None,
            "latest_value": latest_hr,
            "unit": "bpm",
            "observed_at": _iso(hr_epoch, zone),
            "source": SOURCE,
            "source_series": hr_series,
            "timestamp_basis": hr_basis,
            **details,
        }
        observed.append(hr_epoch)
    else:
        missing.append("heart_rate")

    steps_data = None
    steps_epoch = int(steps["observed_epoch"]) if steps and steps["observed_epoch"] is not None else None
    if steps and steps["value"] is not None and steps_epoch is not None:
        details = _age_details(steps_epoch, now_epoch=now_epoch, max_age_seconds=max_age_seconds)
        steps_data = {
            "value": _as_number(steps["value"]),
            "unit": "count",
            "window_start": _iso(now_epoch - 1800, zone),
            "window_end": _iso(now_epoch, zone),
            "samples": int(steps["samples"]),
            "observed_at": _iso(steps_epoch, zone),
            "source_series": "XIAOMI_ACTIVITY_SAMPLE",
            **details,
        }
        observed.append(steps_epoch)
    else:
        missing.append("steps_30m")

    battery_data = None
    if battery is not None:
        battery_epoch = int(battery["TIMESTAMP"])
        battery_data = {
            "value": _as_number(battery["LEVEL"]),
            "unit": "percent",
            "observed_at": _iso(battery_epoch, zone),
            "source_series": "BATTERY_LEVEL",
            **_age_details(battery_epoch, now_epoch=now_epoch, max_age_seconds=max_age_seconds),
        }
        observed.append(battery_epoch)
    else:
        missing.append("battery")

    if sleep_data is None:
        missing.append("sleep")
    else:
        assert sleep_epoch is not None
        sleep_data = {
            "observation_role": "historical",
            **sleep_data,
            **_age_details(sleep_epoch, now_epoch=now_epoch, max_age_seconds=max_age_seconds),
        }
        observed.append(sleep_epoch)
    if workout_data is None:
        missing.append("workouts")
    else:
        assert workout_epoch is not None
        workout_data = {
            "observation_role": "historical",
            **workout_data,
            **_age_details(workout_epoch, now_epoch=now_epoch, max_age_seconds=max_age_seconds),
        }
        observed.append(workout_epoch)

    hr_age = _age_details(hr_epoch, now_epoch=now_epoch, max_age_seconds=max_age_seconds)
    return {
        "status": "ok" if observed else "no_data",
        "data": {
            "heart_rate": hr_data,
            "steps_30m": steps_data,
            "battery": battery_data,
            "latest_sleep": sleep_data,
            "latest_workout": workout_data,
        },
        "freshness": {
            "satisfied": hr_age["within_max_age"],
            "max_age_seconds": max_age_seconds,
            "latest_sample_at": _iso(hr_epoch, zone),
            "age_seconds": hr_age["age_seconds"],
            "status": hr_age["status"],
        },
        "meta": _meta(
            zone_name=zone_name,
            zone=zone,
            sync=sync,
            device=device,
            observed_epoch=max(observed, default=None),
            units=CURRENT_UNITS,
            missing=missing,
            truncated=False,
            unsupported=(
                "workout.category", "workout.distance_m", "workout.calories_kcal",
                "workout.steps", "workout.avg_bpm",
            ),
        ),
    }


def _query_timeseries(
    conn: sqlite3.Connection,
    *,
    device_id: int,
    metric: str,
    start_epoch: int,
    end_epoch: int,
    aggregation_minutes: int,
    limit: int,
    offset: int,
    zone: ZoneInfo,
) -> tuple[list[dict[str, Any]], bool, int | None]:
    spec = TIMESERIES_METRICS[metric]
    table = spec["table"]
    column = spec["column"]
    valid = spec["valid"]
    bucket_seconds = aggregation_minutes * 60
    if spec["aggregate"] == "last":
        sql = f"""
            WITH candidates AS (
                SELECT CAST(("TIMESTAMP" - ?) / ? AS INTEGER) AS bucket,
                       {column} AS value, "TIMESTAMP" AS observed_epoch,
                       ROW_NUMBER() OVER (
                           PARTITION BY CAST(("TIMESTAMP" - ?) / ? AS INTEGER)
                           ORDER BY "TIMESTAMP" DESC, "BATTERY_INDEX"
                       ) AS sample_rank
                FROM "{table}"
                WHERE "DEVICE_ID" = ? AND "TIMESTAMP" >= ? AND "TIMESTAMP" < ?
                  AND {valid}
            )
            SELECT bucket, value, 1 AS samples, observed_epoch
            FROM candidates WHERE sample_rank = 1
            ORDER BY bucket LIMIT ? OFFSET ?
        """
        params = (
            start_epoch, bucket_seconds, start_epoch, bucket_seconds,
            device_id, start_epoch, end_epoch, limit + 1, offset,
        )
    else:
        aggregate = "AVG" if spec["aggregate"] == "avg" else "SUM"
        sql = f"""
            SELECT CAST(("TIMESTAMP" - ?) / ? AS INTEGER) AS bucket,
                   {aggregate}({column}) AS value, COUNT(*) AS samples,
                   MAX("TIMESTAMP") AS observed_epoch
            FROM "{table}"
            WHERE "DEVICE_ID" = ? AND "TIMESTAMP" >= ? AND "TIMESTAMP" < ?
              AND {valid}
            GROUP BY bucket ORDER BY bucket LIMIT ? OFFSET ?
        """
        params = (
            start_epoch, bucket_seconds, device_id, start_epoch, end_epoch,
            limit + 1, offset,
        )
    rows = conn.execute(sql, params).fetchall()
    has_more = len(rows) > limit
    rows = rows[:limit]
    now_epoch = int(datetime.now(dt_timezone.utc).timestamp())
    observed_epoch = None
    data: list[dict[str, Any]] = []
    for row in rows:
        bucket_start = start_epoch + int(row["bucket"]) * bucket_seconds
        observed = int(row["observed_epoch"])
        observed_epoch = max(observed_epoch or observed, observed)
        item = {
            "start_at": _iso(bucket_start, zone),
            "end_at": _iso(min(bucket_start + bucket_seconds, end_epoch), zone),
            "value": _as_number(row["value"]),
            "samples": int(row["samples"]),
            "observed_at": _iso(observed, zone),
        }
        if observed > now_epoch:
            item["future"] = True
        data.append(item)
    return data, has_more, observed_epoch


def _validate_session_id(kind: str, session_id: str | None) -> None:
    if session_id is None:
        return
    expected = "slp_" if kind == "sleep" else "wrk_"
    if not isinstance(session_id, str) or not SESSION_ID_RE.fullmatch(session_id) or not session_id.startswith(expected):
        raise ValueError(f"invalid {kind} session_id")


def _query_sleep(
    conn: sqlite3.Connection,
    *,
    device_id: int,
    start_epoch: int,
    end_epoch: int,
    limit: int,
    offset: int,
    session_id: str | None,
    zone: ZoneInfo,
) -> tuple[list[dict[str, Any]], bool, int | None]:
    _validate_session_id("sleep", session_id)
    clause = ""
    params: list[Any] = [device_id, start_epoch * 1000, end_epoch * 1000]
    if session_id is not None:
        clause = " AND public_session_id('sleep', \"DEVICE_ID\", \"TIMESTAMP\") = ?"
        params.append(session_id)
    params.extend((limit + 1, offset))
    rows = conn.execute(
        f"""
        SELECT * FROM "XIAOMI_SLEEP_TIME_SAMPLE"
        WHERE "DEVICE_ID" = ? AND "WAKEUP_TIME" >= ? AND "WAKEUP_TIME" < ?
          AND "WAKEUP_TIME" IS NOT NULL {clause}
        ORDER BY "WAKEUP_TIME" DESC, "TIMESTAMP" DESC LIMIT ? OFFSET ?
        """,
        params,
    ).fetchall()
    has_more = len(rows) > limit
    rows = rows[:limit]
    data = [_sleep_item(conn, row, device_id=device_id, zone=zone) for row in rows]
    observed = max((int(row["WAKEUP_TIME"]) // 1000 for row in rows), default=None)
    return data, has_more, observed


def _query_workouts(
    conn: sqlite3.Connection,
    *,
    device_id: int,
    start_epoch: int,
    end_epoch: int,
    limit: int,
    offset: int,
    session_id: str | None,
    zone: ZoneInfo,
) -> tuple[list[dict[str, Any]], bool, int | None]:
    _validate_session_id("workouts", session_id)
    clause = ""
    params: list[Any] = [device_id, start_epoch * 1000, end_epoch * 1000]
    if session_id is not None:
        clause = " AND public_session_id('workouts', \"DEVICE_ID\", \"_id\") = ?"
        params.append(session_id)
    params.extend((limit + 1, offset))
    rows = conn.execute(
        f"""
        SELECT "_id", "NAME", "START_TIME", "END_TIME", "ACTIVITY_KIND"
        FROM "BASE_ACTIVITY_SUMMARY"
        WHERE "DEVICE_ID" = ? AND "START_TIME" >= ? AND "START_TIME" < ? {clause}
        ORDER BY "START_TIME" DESC, "_id" DESC LIMIT ? OFFSET ?
        """,
        params,
    ).fetchall()
    has_more = len(rows) > limit
    rows = rows[:limit]
    data = [_workout_item(row, device_id=device_id, zone=zone) for row in rows]
    observed = max((int(row["END_TIME"]) // 1000 for row in rows), default=None)
    return data, has_more, observed


def _query_empty(
    *,
    kind: str,
    metric: str | None,
    zone_name: str,
    zone: ZoneInfo,
    sync: dict[str, Any],
    device: sqlite3.Row | None,
    units: dict[str, str | None],
    missing: Iterable[str],
    limit: int,
    offset: int,
    status: str = "no_data",
    unsupported: Iterable[str] = (),
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "status": status,
        "kind": kind,
        "data": [],
        "meta": _meta(
            zone_name=zone_name,
            zone=zone,
            sync=sync,
            device=device,
            observed_epoch=None,
            units=units,
            missing=missing,
            unsupported=unsupported,
            truncated=False,
            pagination=_pagination(limit, offset, 0, False),
        ),
    }
    if metric is not None:
        result["metric"] = metric
    return result


def query_health(
    settings: Any,
    *,
    kind: str,
    start: str | None = None,
    end: str | None = None,
    metric: str | None = None,
    aggregation_minutes: int = 60,
    limit: int = 200,
    offset: int = 0,
    session_id: str | None = None,
    timezone: str | None = None,
) -> dict[str, Any]:
    """Query bounded Gadgetbridge metric buckets, sleep, or workout sessions."""

    if kind not in {"timeseries", "sleep", "workouts"}:
        raise ValueError("kind must be one of: timeseries, sleep, workouts")
    limit, offset = _validate_page(limit, offset)
    if isinstance(aggregation_minutes, bool) or not isinstance(aggregation_minutes, int):
        raise ValueError("aggregation_minutes must be an integer")
    if not 1 <= aggregation_minutes <= 1440:
        raise ValueError("aggregation_minutes must be between 1 and 1440")
    zone_name, zone = _settings_zone(settings, timezone)
    start_epoch, end_epoch = _resolve_window(kind, start, end)

    selected_metric = (metric or "heart_rate.bpm") if kind == "timeseries" else metric
    if kind == "timeseries":
        if session_id is not None:
            raise ValueError("session_id is only valid for sleep or workouts")
        if selected_metric in KNOWN_UNSUPPORTED_METRICS:
            sync = _sync_fields(settings, None)
            return _query_empty(
                kind=kind,
                metric=selected_metric,
                zone_name=zone_name,
                zone=zone,
                sync=sync,
                device=None,
                units={selected_metric: None},
                missing=(),
                unsupported=(selected_metric,),
                limit=limit,
                offset=offset,
                status="unsupported",
            )
        if selected_metric not in TIMESERIES_METRICS:
            allowed = ", ".join(sorted(set(TIMESERIES_METRICS) | KNOWN_UNSUPPORTED_METRICS))
            raise ValueError(f"metric must be one of: {allowed}")
        spec = TIMESERIES_METRICS[selected_metric]
        units = {selected_metric: spec["unit"]}
    elif kind == "sleep":
        if metric is not None:
            raise ValueError("metric is only valid for timeseries")
        _validate_session_id("sleep", session_id)
        units = {"duration": "min", "stages": "min", "heart_rate": "bpm"}
    else:
        if metric is not None:
            raise ValueError("metric is only valid for timeseries")
        _validate_session_id("workouts", session_id)
        units = {
            "duration": "sec", "distance": "m", "calories": "kcal",
            "steps": "count", "heart_rate": "bpm",
        }

    opened = _connect(settings)
    if opened is None:
        sync = _sync_fields(settings, None)
        return _query_empty(
            kind=kind,
            metric=selected_metric,
            zone_name=zone_name,
            zone=zone,
            sync=sync,
            device=None,
            units=units,
            missing=(selected_metric or kind,),
            limit=limit,
            offset=offset,
        )
    conn, snapshot_identity = opened
    sync = _sync_fields(settings, snapshot_identity)
    try:
        device = _select_device(conn, settings)
        if device is None:
            return _query_empty(
                kind=kind,
                metric=selected_metric,
                zone_name=zone_name,
                zone=zone,
                sync=sync,
                device=None,
                units=units,
                missing=(selected_metric or kind,),
                limit=limit,
                offset=offset,
            )
        device_id = int(device["_id"])
        if kind == "timeseries":
            data, has_more, observed_epoch = _query_timeseries(
                conn,
                device_id=device_id,
                metric=str(selected_metric),
                start_epoch=start_epoch,
                end_epoch=end_epoch,
                aggregation_minutes=aggregation_minutes,
                limit=limit,
                offset=offset,
                zone=zone,
            )
        elif kind == "sleep":
            data, has_more, observed_epoch = _query_sleep(
                conn,
                device_id=device_id,
                start_epoch=start_epoch,
                end_epoch=end_epoch,
                limit=limit,
                offset=offset,
                session_id=session_id,
                zone=zone,
            )
        else:
            data, has_more, observed_epoch = _query_workouts(
                conn,
                device_id=device_id,
                start_epoch=start_epoch,
                end_epoch=end_epoch,
                limit=limit,
                offset=offset,
                session_id=session_id,
                zone=zone,
            )
    finally:
        conn.close()

    unsupported = (
        ("workout.category", "workout.distance_m", "workout.calories_kcal", "workout.steps", "workout.avg_bpm")
        if kind == "workouts" else ()
    )
    result: dict[str, Any] = {
        "status": "ok" if data else "no_data",
        "kind": kind,
        "data": data,
        "meta": _meta(
            zone_name=zone_name,
            zone=zone,
            sync=sync,
            device=device,
            observed_epoch=observed_epoch,
            units=units,
            missing=() if data else (selected_metric or kind,),
            unsupported=unsupported,
            truncated=has_more,
            pagination=_pagination(limit, offset, len(data), has_more),
        ),
    }
    if kind == "timeseries":
        result.update(
            {
                "metric": selected_metric,
                "aggregation": TIMESERIES_METRICS[str(selected_metric)]["aggregate"],
                "aggregation_minutes": aggregation_minutes,
            }
        )
        result["meta"]["selected_source_series"] = TIMESERIES_METRICS[str(selected_metric)]["table"]
        if selected_metric == "heart_rate.bpm":
            result["meta"]["series_scope"] = "automatic_activity_samples_only"
    return result


def _day_bounds(day: Date, zone: ZoneInfo) -> tuple[int, int]:
    start = datetime.combine(day, time.min, tzinfo=zone)
    end = datetime.combine(day + timedelta(days=1), time.min, tzinfo=zone)
    return int(start.timestamp()), int(end.timestamp())


def _parse_date(value: str) -> Date:
    try:
        parsed = Date.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("date must be YYYY-MM-DD") from exc
    if parsed.isoformat() != value:
        raise ValueError("date must be YYYY-MM-DD")
    return parsed


def _activity_day(
    conn: sqlite3.Connection, device_id: int, start_epoch: int, end_epoch: int
) -> sqlite3.Row:
    return conn.execute(
        """
        SELECT COUNT(*) AS samples,
               SUM("STEPS") AS steps,
               SUM("DISTANCE_CM") / 100.0 AS distance_m,
               SUM("ACTIVE_CALORIES") AS calories,
               AVG(CASE WHEN "HEART_RATE" > 0 AND "HEART_RATE" != 255 THEN "HEART_RATE" END) AS hr_avg,
               MIN(CASE WHEN "HEART_RATE" > 0 AND "HEART_RATE" != 255 THEN "HEART_RATE" END) AS hr_min,
               MAX(CASE WHEN "HEART_RATE" > 0 AND "HEART_RATE" != 255 THEN "HEART_RATE" END) AS hr_max,
               COUNT(CASE WHEN "HEART_RATE" > 0 AND "HEART_RATE" != 255 THEN 1 END) AS hr_samples,
               MAX("TIMESTAMP") AS observed_epoch
        FROM "XIAOMI_ACTIVITY_SAMPLE"
        WHERE "DEVICE_ID" = ? AND "TIMESTAMP" >= ? AND "TIMESTAMP" < ?
        """,
        (device_id, start_epoch, end_epoch),
    ).fetchone()


def _daily_summary(
    conn: sqlite3.Connection,
    device_id: int,
    start_epoch: int,
    end_epoch: int,
    zone: ZoneInfo,
) -> tuple[sqlite3.Row | None, dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT * FROM "XIAOMI_DAILY_SUMMARY_SAMPLE"
        WHERE "DEVICE_ID" = ? AND "TIMESTAMP" >= ? AND "TIMESTAMP" < ?
        ORDER BY "TIMESTAMP" DESC
        """,
        (device_id, start_epoch * 1000, end_epoch * 1000),
    ).fetchall()
    if not rows:
        return None, {"used": False, "reason": "no_summary"}

    target_day = datetime.fromtimestamp(start_epoch, tz=zone).date()
    requested_start_offset = datetime.fromtimestamp(start_epoch, tz=zone).utcoffset()
    requested_end_offset = datetime.fromtimestamp(end_epoch - 1, tz=zone).utcoffset()
    for row in rows:
        blocks = row["TIMEZONE"]
        if blocks is None or isinstance(blocks, bool):
            continue
        blocks = int(blocks)
        if not -48 <= blocks <= 56:
            continue
        source_offset = timedelta(minutes=blocks * 15)
        if requested_start_offset != source_offset or requested_end_offset != source_offset:
            continue
        source_zone = dt_timezone(source_offset)
        source_day = datetime.fromtimestamp(int(row["TIMESTAMP"]) / 1000, tz=source_zone).date()
        if source_day == target_day:
            return row, {
                "used": True,
                "reason": "source_day_boundary_matches_requested_timezone",
                "timezone_blocks": blocks,
            }
    return None, {
        "used": False,
        "reason": "source_day_boundary_does_not_match_requested_timezone",
    }


def _daily_scalars(
    conn: sqlite3.Connection,
    device_id: int,
    start_epoch: int,
    end_epoch: int,
    zone: ZoneInfo,
) -> tuple[dict[str, Any], dict[str, Any]]:
    activity = _activity_day(conn, device_id, start_epoch, end_epoch)
    summary, summary_selection = _daily_summary(
        conn, device_id, start_epoch, end_epoch, zone
    )
    activity_observed = (
        int(activity["observed_epoch"]) if activity["observed_epoch"] is not None else None
    )
    summary_observed = int(summary["TIMESTAMP"]) // 1000 if summary is not None else None

    summary_hr = None
    if summary is not None:
        avg_hr = _valid_hr(summary["HR_AVG"])
        min_hr = _valid_hr(summary["HR_MIN"])
        max_hr = _valid_hr(summary["HR_MAX"])
        if any(value is not None for value in (avg_hr, min_hr, max_hr)):
            summary_hr = {
                "avg_bpm": avg_hr,
                "min_bpm": min_hr,
                "max_bpm": max_hr,
                "samples": 1,
                "source_series": "XIAOMI_DAILY_SUMMARY_SAMPLE",
                "observed_epoch": summary_observed,
            }
    heart_rate = summary_hr
    if heart_rate is None and int(activity["hr_samples"]) > 0:
        heart_rate = {
            "avg_bpm": _as_number(activity["hr_avg"]),
            "min_bpm": _as_number(activity["hr_min"]),
            "max_bpm": _as_number(activity["hr_max"]),
            "samples": int(activity["hr_samples"]),
            "source_series": "XIAOMI_ACTIVITY_SAMPLE",
            "observed_epoch": activity_observed,
        }

    steps = None
    if summary is not None and summary["STEPS"] is not None:
        steps = {
            "value": _as_number(summary["STEPS"]),
            "samples": 1,
            "source_series": "XIAOMI_DAILY_SUMMARY_SAMPLE",
            "observed_epoch": summary_observed,
        }
    elif int(activity["samples"]) > 0 and activity["steps"] is not None:
        steps = {
            "value": _as_number(activity["steps"]),
            "samples": int(activity["samples"]),
            "source_series": "XIAOMI_ACTIVITY_SAMPLE",
            "observed_epoch": activity_observed,
        }

    calories = None
    if summary is not None and summary["ACTIVE_CALORIES"] is not None:
        calories = {
            "value": _as_number(summary["ACTIVE_CALORIES"]),
            "samples": 1,
            "source_series": "XIAOMI_DAILY_SUMMARY_SAMPLE",
            "observed_epoch": summary_observed,
        }
    elif int(activity["samples"]) > 0 and activity["calories"] is not None:
        calories = {
            "value": _as_number(activity["calories"]),
            "samples": int(activity["samples"]),
            "source_series": "XIAOMI_ACTIVITY_SAMPLE",
            "observed_epoch": activity_observed,
        }

    distance = None
    if int(activity["samples"]) > 0 and activity["distance_m"] is not None:
        distance = {
            "value": _as_number(activity["distance_m"]),
            "samples": int(activity["samples"]),
            "source_series": "XIAOMI_ACTIVITY_SAMPLE",
            "observed_epoch": activity_observed,
        }
    return (
        {
            "heart_rate": heart_rate,
            "steps": steps,
            "distance": distance,
            "calories": calories,
        },
        summary_selection,
    )


def _daily_sleep(
    conn: sqlite3.Connection,
    *,
    device_id: int,
    start_epoch: int,
    end_epoch: int,
    zone: ZoneInfo,
) -> tuple[dict[str, Any] | None, bool]:
    sessions, has_more, observed = _query_sleep(
        conn,
        device_id=device_id,
        start_epoch=start_epoch,
        end_epoch=end_epoch,
        limit=MAX_LIMIT,
        offset=0,
        session_id=None,
        zone=zone,
    )
    if not sessions:
        return None, has_more
    durations = [float(item["duration_min"]) for item in sessions if item["duration_min"] is not None]
    return {
        "duration_min": _as_number(sum(durations)) if durations else None,
        "sessions": len(sessions),
        "items": sessions,
        "observed_epoch": observed,
    }, has_more


def _daily_workouts(
    conn: sqlite3.Connection,
    *,
    device_id: int,
    start_epoch: int,
    end_epoch: int,
    zone: ZoneInfo,
) -> tuple[dict[str, Any] | None, bool]:
    workouts, has_more, observed = _query_workouts(
        conn,
        device_id=device_id,
        start_epoch=start_epoch,
        end_epoch=end_epoch,
        limit=MAX_LIMIT,
        offset=0,
        session_id=None,
        zone=zone,
    )
    if not workouts:
        return None, has_more
    durations = [float(item["duration_sec"]) for item in workouts if item["duration_sec"] is not None]
    return {
        "duration_sec": _as_number(sum(durations)) if durations else None,
        "sessions": len(workouts),
        "items": workouts,
        "observed_epoch": observed,
    }, has_more


def _comparison_metric(
    dates: list[Date], values: dict[Date, float], unit: str | None
) -> dict[str, Any]:
    available = [values[day] for day in dates if day in values]
    return {
        "average": _as_number(sum(available) / len(available)) if available else None,
        "unit": unit,
        "available_days": len(available),
        "missing_dates": [day.isoformat() for day in dates if day not in values],
    }


def _comparison(
    conn: sqlite3.Connection,
    *,
    device_id: int,
    target_day: Date,
    compare_days: int,
    zone: ZoneInfo,
) -> dict[str, Any]:
    dates = [target_day - timedelta(days=offset) for offset in range(compare_days, 0, -1)]
    if not dates:
        return {"start_date": None, "end_date": None, "requested_days": 0, "metrics": {}}
    values: dict[str, dict[Date, float]] = {
        "heart_rate.avg_bpm": {},
        "steps": {},
        "distance": {},
        "calories": {},
        "sleep.duration_min": {},
        "workouts.duration_sec": {},
    }
    for day in dates:
        start_epoch, end_epoch = _day_bounds(day, zone)
        scalars, _ = _daily_scalars(conn, device_id, start_epoch, end_epoch, zone)
        for result_name, key, field in (
            ("heart_rate.avg_bpm", "heart_rate", "avg_bpm"),
            ("steps", "steps", "value"),
            ("distance", "distance", "value"),
            ("calories", "calories", "value"),
        ):
            item = scalars[key]
            if item is not None and item[field] is not None:
                values[result_name][day] = float(item[field])
        sleep = conn.execute(
            """
            SELECT SUM("TOTAL_DURATION") FROM "XIAOMI_SLEEP_TIME_SAMPLE"
            WHERE "DEVICE_ID" = ? AND "WAKEUP_TIME" >= ? AND "WAKEUP_TIME" < ?
              AND "TOTAL_DURATION" IS NOT NULL
            """,
            (device_id, start_epoch * 1000, end_epoch * 1000),
        ).fetchone()[0]
        if sleep is not None:
            values["sleep.duration_min"][day] = float(sleep)
        workout = conn.execute(
            """
            SELECT SUM(CASE WHEN "END_TIME" >= "START_TIME"
                            THEN ("END_TIME" - "START_TIME") / 1000.0 END)
            FROM "BASE_ACTIVITY_SUMMARY"
            WHERE "DEVICE_ID" = ? AND "START_TIME" >= ? AND "START_TIME" < ?
            """,
            (device_id, start_epoch * 1000, end_epoch * 1000),
        ).fetchone()[0]
        if workout is not None:
            values["workouts.duration_sec"][day] = float(workout)
    return {
        "start_date": dates[0].isoformat(),
        "end_date": dates[-1].isoformat(),
        "requested_days": compare_days,
        "metrics": {
            "heart_rate.avg_bpm": _comparison_metric(dates, values["heart_rate.avg_bpm"], "bpm"),
            "steps": _comparison_metric(dates, values["steps"], "count"),
            "distance": _comparison_metric(dates, values["distance"], "m"),
            "calories": _comparison_metric(dates, values["calories"], "kcal"),
            "sleep.duration_min": _comparison_metric(dates, values["sleep.duration_min"], "min"),
            "workouts.duration_sec": _comparison_metric(dates, values["workouts.duration_sec"], "sec"),
        },
    }


def _empty_daily(
    *,
    day: Date,
    zone_name: str,
    zone: ZoneInfo,
    compare_days: int,
    sync: dict[str, Any],
    device: sqlite3.Row | None = None,
) -> dict[str, Any]:
    dates = [day - timedelta(days=offset) for offset in range(compare_days, 0, -1)]
    return {
        "status": "no_data",
        "date": day.isoformat(),
        "data": {
            "heart_rate": None,
            "steps": None,
            "distance": None,
            "calories": None,
            "sleep": None,
            "workouts": None,
        },
        "comparison": {
            "start_date": dates[0].isoformat() if dates else None,
            "end_date": dates[-1].isoformat() if dates else None,
            "requested_days": compare_days,
            "metrics": {},
        },
        "meta": _meta(
            zone_name=zone_name,
            zone=zone,
            sync=sync,
            device=device,
            observed_epoch=None,
            units=DAILY_UNITS,
            missing=("heart_rate", "steps", "distance", "calories", "sleep", "workouts"),
            truncated=False,
            unsupported=(
                "workout.category", "workout.distance_m", "workout.calories_kcal",
                "workout.steps", "workout.avg_bpm",
            ),
        ),
    }


def get_daily_report(
    settings: Any,
    *,
    date: str,
    timezone: str | None = None,
    compare_days: int = 7,
) -> dict[str, Any]:
    """Summarize a natural day, assigning sleep to the local wake-up day."""

    day = _parse_date(date)
    if isinstance(compare_days, bool) or not isinstance(compare_days, int) or not 0 <= compare_days <= 365:
        raise ValueError("compare_days must be between 0 and 365")
    zone_name, zone = _settings_zone(settings, timezone)
    start_epoch, end_epoch = _day_bounds(day, zone)
    opened = _connect(settings)
    if opened is None:
        sync = _sync_fields(settings, None)
        return _empty_daily(
            day=day, zone_name=zone_name, zone=zone, compare_days=compare_days, sync=sync
        )
    conn, snapshot_identity = opened
    sync = _sync_fields(settings, snapshot_identity)
    try:
        device = _select_device(conn, settings)
        if device is None:
            return _empty_daily(
                day=day,
                zone_name=zone_name,
                zone=zone,
                compare_days=compare_days,
                sync=sync,
            )
        device_id = int(device["_id"])
        scalars, summary_selection = _daily_scalars(
            conn, device_id, start_epoch, end_epoch, zone
        )
        sleep, sleep_truncated = _daily_sleep(
            conn, device_id=device_id, start_epoch=start_epoch, end_epoch=end_epoch, zone=zone
        )
        workouts, workout_truncated = _daily_workouts(
            conn, device_id=device_id, start_epoch=start_epoch, end_epoch=end_epoch, zone=zone
        )
        comparison = _comparison(
            conn,
            device_id=device_id,
            target_day=day,
            compare_days=compare_days,
            zone=zone,
        )
    finally:
        conn.close()

    data = {**scalars, "sleep": sleep, "workouts": workouts}
    missing = [name for name, value in data.items() if value is None]
    observed: list[int] = []
    for item in data.values():
        if item is not None and item.get("observed_epoch") is not None:
            observed.append(int(item.pop("observed_epoch")))
    result = {
        "status": "ok" if observed else "no_data",
        "date": day.isoformat(),
        "data": data,
        "comparison": comparison,
        "meta": _meta(
            zone_name=zone_name,
            zone=zone,
            sync=sync,
            device=device,
            observed_epoch=max(observed, default=None),
            units=DAILY_UNITS,
            missing=missing,
            truncated=sleep_truncated or workout_truncated,
            unsupported=(
                "workout.category", "workout.distance_m", "workout.calories_kcal",
                "workout.steps", "workout.avg_bpm",
            ),
        ),
    }
    result["meta"]["daily_summary_selection"] = summary_selection
    return result
