"""Read-only queries over the normalized Xiaomi Health cache.

The sync path owns freshness policy and writes.  This module only reports what
is in the local cache, including the observation time of every result so a
caller cannot confuse an old sample with a live measurement.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import sqlite3
from collections import defaultdict
from datetime import date as Date
from datetime import datetime, time, timedelta, timezone as dt_timezone
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


SOURCE = "xiaomi_health_cache"
MAX_RANGE_SECONDS = 366 * 24 * 60 * 60
MAX_LIMIT = 500
SESSION_ID_RE = re.compile(r"^(?:slp|wrk)_[0-9a-f]{16}$")

# SQL fragments below are selected only through this allowlist.  User input is
# always a bound value and can never become an identifier or expression.
TIMESERIES_METRICS: dict[str, dict[str, Any]] = {
    "heart_rate.bpm": {
        "unit": "bpm",
        "aggregate": "avg",
        "sources": ("hr_record",),
        "canonical_hr": True,
    },
    "steps": {"unit": "count", "aggregate": "sum", "sources": ("step_record",)},
    "calories": {
        "unit": "kcal",
        "aggregate": "sum",
        "sources": ("calorie_record", "step_record"),
        "prefer_source": "calorie_record",
    },
    "distance": {"unit": "m", "aggregate": "sum", "sources": ("step_record",)},
    "stand.valid_hour": {"unit": "count", "aggregate": "sum"},
    "training_load.current_day": {"unit": None, "aggregate": "avg"},
    "vitality.latest_accumulated": {"unit": None, "aggregate": "avg"},
    "weight": {"unit": "kg", "aggregate": "avg"},
    "bmi": {"unit": None, "aggregate": "avg"},
    "vo2_max": {"unit": "ml/kg/min", "aggregate": "avg"},
}

CURRENT_UNITS = {
    "heart_rate": "bpm",
    "steps_30m": "count",
    "latest_sleep.duration": "min",
    "latest_workout.duration": "sec",
    "latest_workout.distance": "m",
    "latest_workout.calories": "kcal",
}

DAILY_UNITS = {
    "heart_rate.avg": "bpm",
    "heart_rate.min": "bpm",
    "heart_rate.max": "bpm",
    "steps": "count",
    "calories": "kcal",
    "sleep.duration": "min",
    "workouts.duration": "sec",
}


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
        data_dir = getattr(settings, "data_dir", None)
        if data_dir is None:
            raise ValueError("settings must provide db_path or data_dir")
        value = Path(data_dir) / "health.sqlite"
    return Path(value).expanduser().resolve()


def _connect(settings: Any) -> sqlite3.Connection | None:
    path = _db_path(settings)
    try:
        path.stat()
    except FileNotFoundError:
        return None
    conn = sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.create_function("public_session_id", 2, _public_id, deterministic=True)
    # Keep metadata and all metric reads on the same WAL snapshot while the
    # scheduled importer may commit a newer batch concurrently.
    conn.execute("BEGIN")
    return conn


def _public_id(kind: str, raw_record_id: Any) -> str:
    prefix = "slp" if kind == "sleep" else "wrk"
    digest = hashlib.sha256(f"{kind}:{raw_record_id}".encode("utf-8")).hexdigest()[:16]
    return f"{prefix}_{digest}"


def _metadata_value(conn: sqlite3.Connection, key: str) -> str | None:
    try:
        row = conn.execute("SELECT value FROM metadata WHERE key = ?", (key,)).fetchone()
    except sqlite3.OperationalError as exc:
        if _missing_schema(exc):
            return None
        raise
    return None if row is None else str(row["value"])


def _missing_schema(exc: sqlite3.Error) -> bool:
    return isinstance(exc, sqlite3.OperationalError) and "no such table" in str(exc).lower()


def _iso(epoch: int | float | None, zone: ZoneInfo) -> str | None:
    if epoch is None:
        return None
    return datetime.fromtimestamp(float(epoch), tz=zone).isoformat(timespec="seconds")


def _as_number(value: Any) -> int | float | None:
    if value is None:
        return None
    number = float(value)
    return int(number) if number.is_integer() else round(number, 2)


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


def _successful_measurement(value: str | None) -> tuple[int | float, int] | None:
    """Parse the latest successful active HR probe, ignoring invalid metadata."""

    if value is None:
        return None
    try:
        payload = json.loads(value)
        if not isinstance(payload, dict):
            return None
        bpm = payload.get("heart_rate_bpm")
        if isinstance(bpm, bool) or not isinstance(bpm, (int, float)):
            return None
        if not math.isfinite(float(bpm)) or not 10 < float(bpm) <= 255:
            return None
        if (
            payload.get("status") != "ok"
            or payload.get("source") != "band_realtime_protocol"
            or payload.get("unit") != "bpm"
            or payload.get("start_confirmed") is not True
            or payload.get("stop_confirmed") is not True
        ):
            return None
        received_at = _parse_instant(payload.get("received_at"), "received_at")
        _parse_instant(payload.get("measurement_requested_at"), "measurement_requested_at")
    except (TypeError, ValueError, OverflowError):
        return None
    value_number = _as_number(bpm)
    if value_number is None:  # Defensive; numeric validation above makes this unreachable.
        return None
    return value_number, int(received_at.timestamp())


def _resolve_window(
    kind: str,
    start: str | None,
    end: str | None,
    *,
    now: datetime | None = None,
) -> tuple[int, int]:
    if (start is None) != (end is None):
        raise ValueError("start and end must be provided together")
    if start is None:
        end_dt = (now or datetime.now(dt_timezone.utc)).astimezone(dt_timezone.utc)
        default_days = 1 if kind == "timeseries" else 30
        start_dt = end_dt - timedelta(days=default_days)
    else:
        start_dt = _parse_instant(start, "start")
        end_dt = _parse_instant(end or "", "end")
    start_epoch = int(start_dt.timestamp())
    end_epoch = int(end_dt.timestamp())
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
    status = "fresh" if within else ("future" if age < 0 else "stale")
    return {"age_seconds": age, "within_max_age": within, "status": status}


def _meta(
    *,
    zone_name: str,
    zone: ZoneInfo,
    last_pulled_at: str | None,
    observed_epoch: int | None,
    units: dict[str, str | None],
    missing: Iterable[str],
    truncated: bool,
    pagination: dict[str, Any] | None = None,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "observed_at": _iso(observed_epoch, zone),
        "last_pulled_at": last_pulled_at,
        "timezone": zone_name,
        "source": SOURCE,
        "units": units,
        "missing": list(missing),
        "truncated": truncated,
    }
    if pagination is not None:
        result["pagination"] = pagination
    return result


def _empty_current(
    zone_name: str,
    zone: ZoneInfo,
    max_age_seconds: int,
    *,
    last_pulled_at: str | None = None,
) -> dict[str, Any]:
    return {
        "status": "no_data",
        "data": {
            "heart_rate": None,
            "steps_30m": None,
            "latest_sleep": None,
            "latest_workout": None,
        },
        "freshness": {
            "satisfied": False,
            "max_age_seconds": max_age_seconds,
            "latest_sample_at": None,
            "age_seconds": None,
        },
        "meta": _meta(
            zone_name=zone_name,
            zone=zone,
            last_pulled_at=last_pulled_at,
            observed_epoch=None,
            units=CURRENT_UNITS,
            missing=("heart_rate", "steps_30m", "sleep", "workouts"),
            truncated=False,
        ),
    }


def _latest_sleep(conn: sqlite3.Connection) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT
            raw_record_id,
            MIN(unixepoch(start_at)) AS start_epoch,
            MAX(unixepoch(end_at)) AS end_epoch,
            MAX(CASE WHEN metric = 'sleep.duration' THEN value_real END) AS duration_min,
            MAX(CASE WHEN metric = 'sleep.deep_duration' THEN value_real END) AS deep_min,
            MAX(CASE WHEN metric = 'sleep.light_duration' THEN value_real END) AS light_min,
            MAX(CASE WHEN metric = 'sleep.rem_duration' THEN value_real END) AS rem_min,
            MAX(CASE WHEN metric = 'sleep.awake_duration' THEN value_real END) AS awake_min
        FROM metric_records
        WHERE source_table = 'sleep_segment'
        GROUP BY raw_record_id
        HAVING end_epoch IS NOT NULL
        ORDER BY end_epoch DESC, raw_record_id DESC
        LIMIT 1
        """
    ).fetchone()


def _latest_workout(conn: sqlite3.Connection) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT
            raw_record_id,
            MIN(unixepoch(start_at)) AS start_epoch,
            MAX(unixepoch(end_at)) AS end_epoch,
            MAX(CASE WHEN metric = 'sport.duration' THEN value_real END) AS duration_sec,
            MAX(CASE WHEN metric = 'sport.distance' THEN value_real END) AS distance_m,
            MAX(CASE WHEN metric = 'sport.calories' THEN value_real END) AS calories_kcal,
            MAX(CASE WHEN metric = 'sport.steps' THEN value_real END) AS steps,
            MAX(CASE WHEN metric = 'sport.heart_rate.avg' THEN value_real END) AS avg_bpm,
            MAX(json_extract(attrs_json, '$.sport_type')) AS sport_type,
            MAX(json_extract(attrs_json, '$.category')) AS category
        FROM metric_records
        WHERE source_table = 'sport_report'
        GROUP BY raw_record_id
        HAVING start_epoch IS NOT NULL
        ORDER BY start_epoch DESC, raw_record_id DESC
        LIMIT 1
        """
    ).fetchone()


def get_current_state(settings: Any, *, max_age_seconds: int = 900) -> dict[str, Any]:
    """Return a cached snapshot without initiating or waiting for a sync."""

    if isinstance(max_age_seconds, bool) or not isinstance(max_age_seconds, int) or max_age_seconds < 0:
        raise ValueError("max_age_seconds must be a non-negative integer")
    zone_name, zone = _settings_zone(settings)
    conn = _connect(settings)
    if conn is None:
        return _empty_current(zone_name, zone, max_age_seconds)

    now_epoch = int(datetime.now(dt_timezone.utc).timestamp())
    last_pulled_at = None
    measurement_raw = None
    try:
        last_pulled_at = _metadata_value(conn, "last_pulled_at")
        measurement_raw = _metadata_value(conn, "measurement.latest")
        hr = conn.execute(
            """
            SELECT value_real, unixepoch(start_at) AS observed_epoch
            FROM metric_records
            WHERE metric = 'heart_rate.bpm'
              AND source_table = 'hr_record'
              AND raw_key = 'heart_rate'
              AND json_extract(attrs_json, '$.item_index') IS NULL
              AND value_real IS NOT NULL
              AND unixepoch(start_at) IS NOT NULL
            ORDER BY observed_epoch DESC, metric_id DESC
            LIMIT 1
            """
        ).fetchone()
        steps = conn.execute(
            """
            SELECT SUM(value_real) AS value, COUNT(*) AS samples,
                   MAX(unixepoch(start_at)) AS observed_epoch
            FROM metric_records
            WHERE metric = 'steps'
              AND source_table = 'step_record'
              AND value_real IS NOT NULL
              AND unixepoch(start_at) >= ?
              AND unixepoch(start_at) <= ?
            """,
            (now_epoch - 1800, now_epoch),
        ).fetchone()
        sleep = _latest_sleep(conn)
        workout = _latest_workout(conn)
    except sqlite3.Error as exc:
        if _missing_schema(exc):
            return _empty_current(
                zone_name,
                zone,
                max_age_seconds,
                last_pulled_at=last_pulled_at,
            )
        raise
    finally:
        try:
            conn.close()
        except sqlite3.Error:
            pass

    observed_epochs: list[int] = []
    missing: list[str] = []

    hr_data = None
    db_hr_epoch = int(hr["observed_epoch"]) if hr is not None else None
    measurement = _successful_measurement(measurement_raw)
    use_measurement = measurement is not None and (
        db_hr_epoch is None or measurement[1] > db_hr_epoch
    )
    if use_measurement:
        latest_value, hr_epoch = measurement
        hr_source = "band_realtime_protocol"
        timestamp_basis = "received_at"
    elif hr is not None and db_hr_epoch is not None:
        latest_value = _as_number(hr["value_real"])
        hr_epoch = db_hr_epoch
        hr_source = "xiaomi_health_record"
        timestamp_basis = "recorded_at"
    else:
        latest_value = None
        hr_epoch = None
        hr_source = None
        timestamp_basis = None

    if latest_value is not None and hr_epoch is not None:
        details = _age_details(hr_epoch, now_epoch=now_epoch, max_age_seconds=max_age_seconds)
        hr_data = {
            "value": latest_value if details["within_max_age"] else None,
            "latest_value": latest_value,
            "unit": "bpm",
            "observed_at": _iso(hr_epoch, zone),
            "source": hr_source,
            "timestamp_basis": timestamp_basis,
            **details,
        }
        observed_epochs.append(hr_epoch)
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
            **details,
        }
        observed_epochs.append(steps_epoch)
    else:
        missing.append("steps_30m")

    sleep_data = None
    sleep_epoch = int(sleep["end_epoch"]) if sleep and sleep["end_epoch"] is not None else None
    if sleep is not None and sleep_epoch is not None:
        details = _age_details(sleep_epoch, now_epoch=now_epoch, max_age_seconds=max_age_seconds)
        sleep_data = {
            "observation_role": "historical",
            "session_id": _public_id("sleep", sleep["raw_record_id"]),
            "start_at": _iso(sleep["start_epoch"], zone),
            "end_at": _iso(sleep_epoch, zone),
            "duration_min": _as_number(sleep["duration_min"]),
            "stages_min": {
                "deep": _as_number(sleep["deep_min"]),
                "light": _as_number(sleep["light_min"]),
                "rem": _as_number(sleep["rem_min"]),
                "awake": _as_number(sleep["awake_min"]),
            },
            "observed_at": _iso(sleep_epoch, zone),
            **details,
        }
        observed_epochs.append(sleep_epoch)
    else:
        missing.append("sleep")

    workout_data = None
    workout_epoch = int(workout["end_epoch"] or workout["start_epoch"]) if workout else None
    if workout is not None and workout_epoch is not None:
        details = _age_details(workout_epoch, now_epoch=now_epoch, max_age_seconds=max_age_seconds)
        workout_data = {
            "observation_role": "historical",
            "session_id": _public_id("workout", workout["raw_record_id"]),
            "start_at": _iso(workout["start_epoch"], zone),
            "end_at": _iso(workout["end_epoch"], zone),
            "sport_type": workout["sport_type"],
            "category": workout["category"],
            "duration_sec": _as_number(workout["duration_sec"]),
            "distance_m": _as_number(workout["distance_m"]),
            "calories_kcal": _as_number(workout["calories_kcal"]),
            "steps": _as_number(workout["steps"]),
            "avg_bpm": _as_number(workout["avg_bpm"]),
            "observed_at": _iso(workout_epoch, zone),
            **details,
        }
        observed_epochs.append(workout_epoch)
    else:
        missing.append("workouts")

    hr_age = _age_details(hr_epoch, now_epoch=now_epoch, max_age_seconds=max_age_seconds)
    return {
        "status": "ok" if observed_epochs else "no_data",
        "data": {
            "heart_rate": hr_data,
            "steps_30m": steps_data,
            "latest_sleep": sleep_data,
            "latest_workout": workout_data,
        },
        "freshness": {
            "satisfied": hr_age["within_max_age"],
            "max_age_seconds": max_age_seconds,
            "latest_sample_at": _iso(hr_epoch, zone),
            "age_seconds": hr_age["age_seconds"],
        },
        "meta": _meta(
            zone_name=zone_name,
            zone=zone,
            last_pulled_at=last_pulled_at,
            observed_epoch=max(observed_epochs, default=None),
            units=CURRENT_UNITS,
            missing=missing,
            truncated=False,
        ),
    }


def _pagination(limit: int, offset: int, returned: int, has_more: bool) -> dict[str, Any]:
    return {
        "limit": limit,
        "offset": offset,
        "returned": returned,
        "has_more": has_more,
        "next_offset": offset + returned if has_more else None,
    }


def _query_timeseries(
    conn: sqlite3.Connection,
    *,
    metric: str,
    start_epoch: int,
    end_epoch: int,
    aggregation_minutes: int,
    limit: int,
    offset: int,
    zone: ZoneInfo,
) -> tuple[list[dict[str, Any]], bool, int | None, str | None]:
    spec = TIMESERIES_METRICS[metric]
    bucket_seconds = aggregation_minutes * 60
    aggregate_sql = "AVG(value_real)" if spec["aggregate"] == "avg" else "SUM(value_real)"
    source_values = tuple(spec.get("sources", ()))
    source_clause = ""
    source_params: list[Any] = []
    if source_values:
        placeholders = ",".join("?" for _ in source_values)
        source_clause = f" AND source_table IN ({placeholders})"
        source_params.extend(source_values)
    if spec.get("canonical_hr"):
        source_clause += (
            " AND raw_key = 'heart_rate'"
            " AND json_extract(attrs_json, '$.item_index') IS NULL"
        )

    if spec.get("prefer_source"):
        sql = f"""
            WITH source_buckets AS (
                SELECT CAST((unixepoch(start_at) - ?) / ? AS INTEGER) AS bucket,
                       source_table,
                       {aggregate_sql} AS value,
                       COUNT(*) AS samples,
                       MAX(unixepoch(start_at)) AS observed_epoch
                FROM metric_records
                WHERE metric = ?
                  AND value_real IS NOT NULL
                  AND unixepoch(start_at) >= ?
                  AND unixepoch(start_at) < ?
                  {source_clause}
                GROUP BY bucket, source_table
            ), ranked AS (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY bucket
                    ORDER BY CASE WHEN source_table = ? THEN 0 ELSE 1 END, source_table
                ) AS source_rank
                FROM source_buckets
            )
            SELECT bucket, value, samples, observed_epoch, source_table
            FROM ranked
            WHERE source_rank = 1
            ORDER BY bucket ASC
            LIMIT ? OFFSET ?
        """
        params = [
            start_epoch,
            bucket_seconds,
            metric,
            start_epoch,
            end_epoch,
            *source_params,
            spec["prefer_source"],
            limit + 1,
            offset,
        ]
    else:
        sql = f"""
            SELECT CAST((unixepoch(start_at) - ?) / ? AS INTEGER) AS bucket,
                   {aggregate_sql} AS value,
                   COUNT(*) AS samples,
                   MAX(unixepoch(start_at)) AS observed_epoch,
                   NULL AS source_table
            FROM metric_records
            WHERE metric = ?
              AND value_real IS NOT NULL
              AND unixepoch(start_at) >= ?
              AND unixepoch(start_at) < ?
              {source_clause}
            GROUP BY bucket
            ORDER BY bucket ASC
            LIMIT ? OFFSET ?
        """
        params = [
            start_epoch,
            bucket_seconds,
            metric,
            start_epoch,
            end_epoch,
            *source_params,
            limit + 1,
            offset,
        ]

    rows = conn.execute(sql, params).fetchall()
    has_more = len(rows) > limit
    rows = rows[:limit]
    data: list[dict[str, Any]] = []
    observed_epoch = None
    selected_source = None
    for row in rows:
        bucket_start = start_epoch + int(row["bucket"]) * bucket_seconds
        bucket_end = min(bucket_start + bucket_seconds, end_epoch)
        row_observed = int(row["observed_epoch"])
        observed_epoch = max(observed_epoch or row_observed, row_observed)
        item = {
            "start_at": _iso(bucket_start, zone),
            "end_at": _iso(bucket_end, zone),
            "value": _as_number(row["value"]),
            "samples": int(row["samples"]),
            "observed_at": _iso(row_observed, zone),
        }
        if row["source_table"] is not None:
            item["source_series"] = str(row["source_table"])
            selected_source = str(row["source_table"])
        data.append(item)
    return data, has_more, observed_epoch, selected_source


def _validate_session_id(kind: str, session_id: str | None) -> None:
    if session_id is None:
        return
    expected = "slp_" if kind == "sleep" else "wrk_"
    if not SESSION_ID_RE.fullmatch(session_id) or not session_id.startswith(expected):
        raise ValueError(f"invalid {kind} session_id")


def _query_sleep(
    conn: sqlite3.Connection,
    *,
    start_epoch: int,
    end_epoch: int,
    limit: int,
    offset: int,
    session_id: str | None,
    zone: ZoneInfo,
) -> tuple[list[dict[str, Any]], bool, int | None]:
    _validate_session_id("sleep", session_id)
    session_clause = ""
    params: list[Any] = [start_epoch, end_epoch]
    if session_id is not None:
        session_clause = " AND public_session_id('sleep', raw_record_id) = ?"
        params.append(session_id)
    params.extend((limit + 1, offset))
    rows = conn.execute(
        f"""
        WITH sessions AS (
            SELECT
                raw_record_id,
                MIN(unixepoch(start_at)) AS start_epoch,
                MAX(unixepoch(end_at)) AS end_epoch,
                MAX(CASE WHEN metric = 'sleep.duration' THEN value_real END) AS duration_min,
                MAX(CASE WHEN metric = 'sleep.deep_duration' THEN value_real END) AS deep_min,
                MAX(CASE WHEN metric = 'sleep.light_duration' THEN value_real END) AS light_min,
                MAX(CASE WHEN metric = 'sleep.rem_duration' THEN value_real END) AS rem_min,
                MAX(CASE WHEN metric = 'sleep.awake_duration' THEN value_real END) AS awake_min,
                MAX(CASE WHEN metric = 'sleep.awake_count' THEN value_real END) AS awake_count,
                MAX(CASE WHEN metric = 'sleep.heart_rate.avg' THEN value_real END) AS avg_bpm,
                MAX(CASE WHEN metric = 'sleep.heart_rate.min' THEN value_real END) AS min_bpm,
                MAX(CASE WHEN metric = 'sleep.heart_rate.max' THEN value_real END) AS max_bpm
            FROM metric_records
            WHERE source_table = 'sleep_segment'
            GROUP BY raw_record_id
        )
        SELECT * FROM sessions
        WHERE end_epoch >= ? AND end_epoch < ?
        {session_clause}
        ORDER BY end_epoch DESC, raw_record_id DESC
        LIMIT ? OFFSET ?
        """,
        params,
    ).fetchall()
    has_more = len(rows) > limit
    rows = rows[:limit]
    ids = [int(row["raw_record_id"]) for row in rows]
    stages: dict[int, dict[str, int | float]] = defaultdict(dict)
    if ids:
        placeholders = ",".join("?" for _ in ids)
        for stage in conn.execute(
            f"""
            SELECT raw_record_id, label, SUM(value_real) AS duration_min
            FROM metric_records
            WHERE metric = 'sleep.stage.duration'
              AND raw_record_id IN ({placeholders})
              AND value_real IS NOT NULL
            GROUP BY raw_record_id, label
            ORDER BY raw_record_id, label
            """,
            ids,
        ):
            stages[int(stage["raw_record_id"])][str(stage["label"])] = _as_number(
                stage["duration_min"]
            )  # type: ignore[assignment]

    data = []
    observed_epoch = None
    for row in rows:
        row_id = int(row["raw_record_id"])
        end = int(row["end_epoch"])
        observed_epoch = max(observed_epoch or end, end)
        data.append(
            {
                "session_id": _public_id("sleep", row_id),
                "start_at": _iso(row["start_epoch"], zone),
                "end_at": _iso(end, zone),
                "duration_min": _as_number(row["duration_min"]),
                "deep_min": _as_number(row["deep_min"]),
                "light_min": _as_number(row["light_min"]),
                "rem_min": _as_number(row["rem_min"]),
                "awake_min": _as_number(row["awake_min"]),
                "awake_count": _as_number(row["awake_count"]),
                "heart_rate": {
                    "avg_bpm": _as_number(row["avg_bpm"]),
                    "min_bpm": _as_number(row["min_bpm"]),
                    "max_bpm": _as_number(row["max_bpm"]),
                },
                "stages_min": stages.get(row_id, {}),
                "observed_at": _iso(end, zone),
            }
        )
    return data, has_more, observed_epoch


def _query_workouts(
    conn: sqlite3.Connection,
    *,
    start_epoch: int,
    end_epoch: int,
    limit: int,
    offset: int,
    session_id: str | None,
    zone: ZoneInfo,
) -> tuple[list[dict[str, Any]], bool, int | None]:
    _validate_session_id("workouts", session_id)
    session_clause = ""
    params: list[Any] = [start_epoch, end_epoch]
    if session_id is not None:
        session_clause = " AND public_session_id('workout', raw_record_id) = ?"
        params.append(session_id)
    params.extend((limit + 1, offset))
    rows = conn.execute(
        f"""
        WITH workouts AS (
            SELECT
                raw_record_id,
                MIN(unixepoch(start_at)) AS start_epoch,
                MAX(unixepoch(end_at)) AS end_epoch,
                MAX(json_extract(attrs_json, '$.sport_type')) AS sport_type,
                MAX(json_extract(attrs_json, '$.category')) AS category,
                MAX(CASE WHEN metric = 'sport.duration' THEN value_real END) AS duration_sec,
                MAX(CASE WHEN metric = 'sport.valid_duration' THEN value_real END) AS valid_duration_sec,
                MAX(CASE WHEN metric = 'sport.distance' THEN value_real END) AS distance_m,
                MAX(CASE WHEN metric = 'sport.calories' THEN value_real END) AS calories_kcal,
                MAX(CASE WHEN metric = 'sport.steps' THEN value_real END) AS steps,
                MAX(CASE WHEN metric = 'sport.heart_rate.avg' THEN value_real END) AS avg_bpm,
                MAX(CASE WHEN metric = 'sport.heart_rate.min' THEN value_real END) AS min_bpm,
                MAX(CASE WHEN metric = 'sport.heart_rate.max' THEN value_real END) AS max_bpm,
                MAX(CASE WHEN metric = 'sport.training_load' THEN value_real END) AS training_load,
                MAX(CASE WHEN metric = 'sport.train_effect' THEN value_real END) AS train_effect
            FROM metric_records
            WHERE source_table = 'sport_report'
            GROUP BY raw_record_id
        )
        SELECT * FROM workouts
        WHERE start_epoch >= ? AND start_epoch < ?
        {session_clause}
        ORDER BY start_epoch DESC, raw_record_id DESC
        LIMIT ? OFFSET ?
        """,
        params,
    ).fetchall()
    has_more = len(rows) > limit
    rows = rows[:limit]
    data = []
    observed_epoch = None
    for row in rows:
        row_observed = int(row["end_epoch"] or row["start_epoch"])
        observed_epoch = max(observed_epoch or row_observed, row_observed)
        data.append(
            {
                "session_id": _public_id("workout", row["raw_record_id"]),
                "start_at": _iso(row["start_epoch"], zone),
                "end_at": _iso(row["end_epoch"], zone),
                "sport_type": row["sport_type"],
                "category": row["category"],
                "duration_sec": _as_number(row["duration_sec"]),
                "valid_duration_sec": _as_number(row["valid_duration_sec"]),
                "distance_m": _as_number(row["distance_m"]),
                "calories_kcal": _as_number(row["calories_kcal"]),
                "steps": _as_number(row["steps"]),
                "heart_rate": {
                    "avg_bpm": _as_number(row["avg_bpm"]),
                    "min_bpm": _as_number(row["min_bpm"]),
                    "max_bpm": _as_number(row["max_bpm"]),
                },
                "training_load": _as_number(row["training_load"]),
                "train_effect": _as_number(row["train_effect"]),
                "observed_at": _iso(row_observed, zone),
            }
        )
    return data, has_more, observed_epoch


def _query_empty(
    *,
    kind: str,
    metric: str | None,
    zone_name: str,
    zone: ZoneInfo,
    units: dict[str, str | None],
    missing: Iterable[str],
    limit: int,
    offset: int,
    last_pulled_at: str | None = None,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "status": "no_data",
        "kind": kind,
        "data": [],
        "meta": _meta(
            zone_name=zone_name,
            zone=zone,
            last_pulled_at=last_pulled_at,
            observed_epoch=None,
            units=units,
            missing=missing,
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
    """Query bounded time-series, sleep sessions, or workout sessions."""

    if kind not in {"timeseries", "sleep", "workouts"}:
        raise ValueError("kind must be one of: timeseries, sleep, workouts")
    limit, offset = _validate_page(limit, offset)
    if isinstance(aggregation_minutes, bool) or not isinstance(aggregation_minutes, int):
        raise ValueError("aggregation_minutes must be an integer")
    if not 1 <= aggregation_minutes <= 1440:
        raise ValueError("aggregation_minutes must be between 1 and 1440")
    zone_name, zone = _settings_zone(settings, timezone)
    start_epoch, end_epoch = _resolve_window(kind, start, end)

    selected_metric = metric or "heart_rate.bpm" if kind == "timeseries" else metric
    if kind == "timeseries":
        if selected_metric not in TIMESERIES_METRICS:
            allowed = ", ".join(sorted(TIMESERIES_METRICS))
            raise ValueError(f"metric must be one of: {allowed}")
        if session_id is not None:
            raise ValueError("session_id is only valid for sleep or workouts")
        spec = TIMESERIES_METRICS[selected_metric]
        units = {selected_metric: spec["unit"]}
    elif kind == "sleep":
        if metric is not None:
            raise ValueError("metric is only valid for timeseries")
        _validate_session_id("sleep", session_id)
        units = {
            "duration": "min",
            "stages": "min",
            "heart_rate": "bpm",
        }
    else:
        if metric is not None:
            raise ValueError("metric is only valid for timeseries")
        _validate_session_id("workouts", session_id)
        units = {
            "duration": "sec",
            "distance": "m",
            "calories": "kcal",
            "steps": "count",
            "heart_rate": "bpm",
        }

    conn = _connect(settings)
    if conn is None:
        return _query_empty(
            kind=kind,
            metric=selected_metric,
            zone_name=zone_name,
            zone=zone,
            units=units,
            missing=(selected_metric or kind,),
            limit=limit,
            offset=offset,
        )
    last_pulled_at = None
    selected_source = None
    try:
        last_pulled_at = _metadata_value(conn, "last_pulled_at")
        if kind == "timeseries":
            data, has_more, observed_epoch, selected_source = _query_timeseries(
                conn,
                metric=selected_metric,
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
                start_epoch=start_epoch,
                end_epoch=end_epoch,
                limit=limit,
                offset=offset,
                session_id=session_id,
                zone=zone,
            )
    except sqlite3.Error as exc:
        if _missing_schema(exc):
            return _query_empty(
                kind=kind,
                metric=selected_metric,
                zone_name=zone_name,
                zone=zone,
                units=units,
                missing=(selected_metric or kind,),
                limit=limit,
                offset=offset,
                last_pulled_at=last_pulled_at,
            )
        raise
    finally:
        conn.close()

    result: dict[str, Any] = {
        "status": "ok" if data else "no_data",
        "kind": kind,
        "data": data,
        "meta": _meta(
            zone_name=zone_name,
            zone=zone,
            last_pulled_at=last_pulled_at,
            observed_epoch=observed_epoch,
            units=units,
            missing=() if data else (selected_metric or kind,),
            truncated=has_more,
            pagination=_pagination(limit, offset, len(data), has_more),
        ),
    }
    if kind == "timeseries":
        result.update(
            {
                "metric": selected_metric,
                "aggregation": TIMESERIES_METRICS[selected_metric]["aggregate"],
                "aggregation_minutes": aggregation_minutes,
            }
        )
        if selected_source is not None:
            result["meta"]["selected_source_series"] = selected_source
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


def _canonical_calories(rows: Iterable[sqlite3.Row]) -> tuple[float | None, int, str | None, int | None]:
    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        source = str(row["source_table"])
        state = grouped.setdefault(source, {"value": 0.0, "samples": 0, "observed_epoch": None})
        state["value"] += float(row["value_real"])
        state["samples"] += 1
        epoch = int(row["observed_epoch"])
        state["observed_epoch"] = max(state["observed_epoch"] or epoch, epoch)
    source = "calorie_record" if "calorie_record" in grouped else (
        "step_record" if "step_record" in grouped else None
    )
    if source is None:
        return None, 0, None, None
    state = grouped[source]
    return state["value"], state["samples"], source, state["observed_epoch"]


def _daily_scalar_rows(
    conn: sqlite3.Connection, start_epoch: int, end_epoch: int
) -> dict[str, Any]:
    hr = conn.execute(
        """
        SELECT AVG(value_real) AS avg_value, MIN(value_real) AS min_value,
               MAX(value_real) AS max_value, COUNT(*) AS samples,
               MAX(unixepoch(start_at)) AS observed_epoch
        FROM metric_records
        WHERE metric = 'heart_rate.bpm'
          AND source_table = 'hr_record'
          AND raw_key = 'heart_rate'
          AND json_extract(attrs_json, '$.item_index') IS NULL
          AND value_real IS NOT NULL
          AND unixepoch(start_at) >= ? AND unixepoch(start_at) < ?
        """,
        (start_epoch, end_epoch),
    ).fetchone()
    steps = conn.execute(
        """
        SELECT SUM(value_real) AS value, COUNT(*) AS samples,
               MAX(unixepoch(start_at)) AS observed_epoch
        FROM metric_records
        WHERE metric = 'steps' AND source_table = 'step_record'
          AND value_real IS NOT NULL
          AND unixepoch(start_at) >= ? AND unixepoch(start_at) < ?
        """,
        (start_epoch, end_epoch),
    ).fetchone()
    calorie_rows = conn.execute(
        """
        SELECT source_table, value_real, unixepoch(start_at) AS observed_epoch
        FROM metric_records
        WHERE metric = 'calories'
          AND source_table IN ('calorie_record', 'step_record')
          AND value_real IS NOT NULL
          AND unixepoch(start_at) >= ? AND unixepoch(start_at) < ?
        """,
        (start_epoch, end_epoch),
    )
    calories, calorie_samples, calorie_source, calorie_observed = _canonical_calories(calorie_rows)
    return {
        "heart_rate": None
        if hr["samples"] == 0
        else {
            "avg_bpm": _as_number(hr["avg_value"]),
            "min_bpm": _as_number(hr["min_value"]),
            "max_bpm": _as_number(hr["max_value"]),
            "samples": int(hr["samples"]),
            "observed_epoch": int(hr["observed_epoch"]),
        },
        "steps": None
        if steps["samples"] == 0
        else {
            "value": _as_number(steps["value"]),
            "samples": int(steps["samples"]),
            "observed_epoch": int(steps["observed_epoch"]),
        },
        "calories": None
        if calories is None
        else {
            "value": _as_number(calories),
            "samples": calorie_samples,
            "source_series": calorie_source,
            "observed_epoch": calorie_observed,
        },
    }


def _daily_sleep_rows(
    conn: sqlite3.Connection, start_epoch: int, end_epoch: int, zone: ZoneInfo
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    sessions, _, _ = _query_sleep(
        conn,
        start_epoch=start_epoch,
        end_epoch=end_epoch,
        limit=MAX_LIMIT,
        offset=0,
        session_id=None,
        zone=zone,
    )
    if not sessions:
        return None, []
    durations = [float(item["duration_min"]) for item in sessions if item["duration_min"] is not None]
    summary = {
        "duration_min": _as_number(sum(durations)) if durations else None,
        "sessions": len(sessions),
        "observed_epoch": max(_parse_instant(item["observed_at"], "observed_at").timestamp() for item in sessions),
    }
    return summary, sessions


def _daily_workout_rows(
    conn: sqlite3.Connection, start_epoch: int, end_epoch: int, zone: ZoneInfo
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    workouts, _, _ = _query_workouts(
        conn,
        start_epoch=start_epoch,
        end_epoch=end_epoch,
        limit=MAX_LIMIT,
        offset=0,
        session_id=None,
        zone=zone,
    )
    if not workouts:
        return None, []
    durations = [float(item["duration_sec"]) for item in workouts if item["duration_sec"] is not None]
    summary = {
        "duration_sec": _as_number(sum(durations)) if durations else None,
        "sessions": len(workouts),
        "observed_epoch": max(_parse_instant(item["observed_at"], "observed_at").timestamp() for item in workouts),
    }
    return summary, workouts


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
    target_day: Date,
    compare_days: int,
    zone: ZoneInfo,
) -> dict[str, Any]:
    dates = [target_day - timedelta(days=offset) for offset in range(compare_days, 0, -1)]
    if not dates:
        return {
            "start_date": None,
            "end_date": None,
            "requested_days": 0,
            "metrics": {},
        }
    start_epoch, _ = _day_bounds(dates[0], zone)
    end_epoch, _ = _day_bounds(target_day, zone)
    values: dict[str, dict[Date, float]] = {
        "heart_rate.avg_bpm": {},
        "steps": {},
        "calories": {},
        "sleep.duration_min": {},
        "workouts.duration_sec": {},
    }

    scalar_rows = conn.execute(
        """
        SELECT metric, source_table, value_real, unixepoch(start_at) AS observed_epoch
        FROM metric_records
        WHERE metric IN ('heart_rate.bpm', 'steps', 'calories')
          AND value_real IS NOT NULL
          AND unixepoch(start_at) >= ? AND unixepoch(start_at) < ?
          AND (metric NOT IN ('steps', 'calories')
               OR source_table IN ('step_record', 'calorie_record'))
          AND (metric != 'heart_rate.bpm'
               OR (source_table = 'hr_record' AND raw_key = 'heart_rate'
                   AND json_extract(attrs_json, '$.item_index') IS NULL))
        ORDER BY observed_epoch
        """,
        (start_epoch, end_epoch),
    )
    daily_hr: dict[Date, list[float]] = defaultdict(list)
    daily_steps: dict[Date, list[float]] = defaultdict(list)
    daily_calories: dict[Date, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    for row in scalar_rows:
        day = datetime.fromtimestamp(int(row["observed_epoch"]), tz=zone).date()
        value = float(row["value_real"])
        if row["metric"] == "heart_rate.bpm":
            daily_hr[day].append(value)
        elif row["metric"] == "steps" and row["source_table"] == "step_record":
            daily_steps[day].append(value)
        elif row["metric"] == "calories":
            daily_calories[day][str(row["source_table"])].append(value)
    for day, samples in daily_hr.items():
        values["heart_rate.avg_bpm"][day] = sum(samples) / len(samples)
    for day, samples in daily_steps.items():
        values["steps"][day] = sum(samples)
    for day, sources in daily_calories.items():
        source = "calorie_record" if sources.get("calorie_record") else "step_record"
        if sources.get(source):
            values["calories"][day] = sum(sources[source])

    sleep_rows = conn.execute(
        """
        SELECT raw_record_id, MAX(unixepoch(end_at)) AS observed_epoch,
               MAX(CASE WHEN metric = 'sleep.duration' THEN value_real END) AS duration_min
        FROM metric_records
        WHERE source_table = 'sleep_segment'
        GROUP BY raw_record_id
        HAVING observed_epoch >= ? AND observed_epoch < ?
        """,
        (start_epoch, end_epoch),
    )
    for row in sleep_rows:
        if row["duration_min"] is not None:
            day = datetime.fromtimestamp(int(row["observed_epoch"]), tz=zone).date()
            values["sleep.duration_min"][day] = (
                values["sleep.duration_min"].get(day, 0.0) + float(row["duration_min"])
            )

    workout_rows = conn.execute(
        """
        SELECT raw_record_id, MIN(unixepoch(start_at)) AS start_epoch,
               MAX(CASE WHEN metric = 'sport.duration' THEN value_real END) AS duration_sec
        FROM metric_records
        WHERE source_table = 'sport_report'
        GROUP BY raw_record_id
        HAVING start_epoch >= ? AND start_epoch < ?
        """,
        (start_epoch, end_epoch),
    )
    for row in workout_rows:
        if row["duration_sec"] is not None:
            day = datetime.fromtimestamp(int(row["start_epoch"]), tz=zone).date()
            values["workouts.duration_sec"][day] = (
                values["workouts.duration_sec"].get(day, 0.0) + float(row["duration_sec"])
            )

    return {
        "start_date": dates[0].isoformat(),
        "end_date": dates[-1].isoformat(),
        "requested_days": compare_days,
        "metrics": {
            "heart_rate.avg_bpm": _comparison_metric(
                dates, values["heart_rate.avg_bpm"], "bpm"
            ),
            "steps": _comparison_metric(dates, values["steps"], "count"),
            "calories": _comparison_metric(dates, values["calories"], "kcal"),
            "sleep.duration_min": _comparison_metric(
                dates, values["sleep.duration_min"], "min"
            ),
            "workouts.duration_sec": _comparison_metric(
                dates, values["workouts.duration_sec"], "sec"
            ),
        },
    }


def _empty_daily(
    *,
    day: Date,
    zone_name: str,
    zone: ZoneInfo,
    compare_days: int,
    last_pulled_at: str | None = None,
) -> dict[str, Any]:
    dates = [day - timedelta(days=offset) for offset in range(compare_days, 0, -1)]
    comparison = {
        "start_date": dates[0].isoformat() if dates else None,
        "end_date": dates[-1].isoformat() if dates else None,
        "requested_days": compare_days,
        "metrics": {},
    }
    return {
        "status": "no_data",
        "date": day.isoformat(),
        "data": {
            "heart_rate": None,
            "steps": None,
            "calories": None,
            "sleep": None,
            "workouts": None,
        },
        "comparison": comparison,
        "meta": _meta(
            zone_name=zone_name,
            zone=zone,
            last_pulled_at=last_pulled_at,
            observed_epoch=None,
            units=DAILY_UNITS,
            missing=("heart_rate", "steps", "calories", "sleep", "workouts"),
            truncated=False,
        ),
    }


def get_daily_report(
    settings: Any,
    *,
    date: str,
    timezone: str | None = None,
    compare_days: int = 7,
) -> dict[str, Any]:
    """Summarize one natural day and observed prior-day comparison values."""

    day = _parse_date(date)
    if isinstance(compare_days, bool) or not isinstance(compare_days, int) or not 0 <= compare_days <= 365:
        raise ValueError("compare_days must be between 0 and 365")
    zone_name, zone = _settings_zone(settings, timezone)
    start_epoch, end_epoch = _day_bounds(day, zone)
    conn = _connect(settings)
    if conn is None:
        return _empty_daily(
            day=day,
            zone_name=zone_name,
            zone=zone,
            compare_days=compare_days,
        )
    last_pulled_at = None
    try:
        last_pulled_at = _metadata_value(conn, "last_pulled_at")
        scalars = _daily_scalar_rows(conn, start_epoch, end_epoch)
        sleep_summary, sleep_sessions = _daily_sleep_rows(conn, start_epoch, end_epoch, zone)
        workout_summary, workouts = _daily_workout_rows(conn, start_epoch, end_epoch, zone)
        comparison = _comparison(
            conn,
            target_day=day,
            compare_days=compare_days,
            zone=zone,
        )
    except sqlite3.Error as exc:
        if _missing_schema(exc):
            return _empty_daily(
                day=day,
                zone_name=zone_name,
                zone=zone,
                compare_days=compare_days,
                last_pulled_at=last_pulled_at,
            )
        raise
    finally:
        conn.close()

    observed_epochs: list[int] = []
    missing: list[str] = []
    for name in ("heart_rate", "steps", "calories"):
        value = scalars[name]
        if value is None:
            missing.append(name)
        else:
            observed_epochs.append(int(value.pop("observed_epoch")))
    if sleep_summary is None:
        missing.append("sleep")
        sleep_data = None
    else:
        observed_epochs.append(int(sleep_summary.pop("observed_epoch")))
        sleep_data = {**sleep_summary, "items": sleep_sessions}
    if workout_summary is None:
        missing.append("workouts")
        workout_data = None
    else:
        observed_epochs.append(int(workout_summary.pop("observed_epoch")))
        workout_data = {**workout_summary, "items": workouts}

    data = {
        "heart_rate": scalars["heart_rate"],
        "steps": scalars["steps"],
        "calories": scalars["calories"],
        "sleep": sleep_data,
        "workouts": workout_data,
    }
    return {
        "status": "ok" if observed_epochs else "no_data",
        "date": day.isoformat(),
        "data": data,
        "comparison": comparison,
        "meta": _meta(
            zone_name=zone_name,
            zone=zone,
            last_pulled_at=last_pulled_at,
            observed_epoch=max(observed_epochs, default=None),
            units=DAILY_UNITS,
            missing=missing,
            truncated=False,
        ),
    }
