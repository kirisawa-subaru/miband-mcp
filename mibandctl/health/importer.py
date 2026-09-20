#!/usr/bin/env python3
"""Import Xiaomi Health JSONL exports into a local normalized SQLite store."""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - Python 3.8 fallback only.
    ZoneInfo = None  # type: ignore[assignment]


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DB = PROJECT_ROOT / "data" / "private" / "com.mi.health_normalized" / "mi_health.sqlite"
DEFAULT_ZONE = "Asia/Shanghai"
SCHEMA_VERSION = 4

SLEEP_STAGE_LABELS = {
    2: "deep",
    3: "light",
    4: "rem",
    5: "awake",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import Xiaomi Health normalized JSONL into a queryable SQLite store."
    )
    parser.add_argument(
        "--input",
        type=Path,
        nargs="+",
        required=True,
        help="One or more normalized_records.jsonl or delta_records.jsonl files.",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB,
        help=f"Output SQLite DB. Default: {DEFAULT_DB}",
    )
    parser.add_argument(
        "--batch-id",
        help="Import batch id. Defaults to current Asia/Shanghai timestamp.",
    )
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Delete the output DB before importing.",
    )
    parser.add_argument(
        "--include-deleted-metrics",
        action="store_true",
        help="Also create metric rows for records whose is_deleted flag is true.",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=100_000,
        help="Print progress to stderr every N input rows. Set 0 to disable.",
    )
    return parser.parse_args()


def now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def default_batch_id() -> str:
    tz = ZoneInfo(DEFAULT_ZONE) if ZoneInfo else None
    return datetime.now(tz).strftime("%Y%m%d-%H%M%S-cst")


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def bool_to_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(bool(value))


def coerce_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def resolve_timezone(zone_name: str | None, offset_seconds: int | None):
    if zone_name and ZoneInfo:
        try:
            return ZoneInfo(zone_name)
        except Exception:
            pass
    return timezone(timedelta(seconds=offset_seconds or 0))


def iso_from_epoch(value: Any, record: dict[str, Any]) -> str | None:
    if value in (None, ""):
        return None
    try:
        ts = int(value)
    except (TypeError, ValueError):
        return None
    if ts > 10_000_000_000:
        ts //= 1000
    tz = resolve_timezone(record.get("zone_name"), record.get("zone_offset_sec"))
    return datetime.fromtimestamp(ts, tz=tz).isoformat()


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        PRAGMA foreign_keys = ON;

        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS import_batches (
            batch_id TEXT PRIMARY KEY,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            input_paths_json TEXT NOT NULL,
            stats_json TEXT
        );

        CREATE TABLE IF NOT EXISTS raw_records (
            id INTEGER PRIMARY KEY,
            source_app TEXT NOT NULL,
            source_db TEXT,
            source_table TEXT NOT NULL,
            sid TEXT,
            raw_key TEXT NOT NULL,
            raw_time INTEGER NOT NULL,
            raw_json_hash TEXT NOT NULL,
            start_at TEXT,
            end_at TEXT,
            zone_offset_sec INTEGER,
            zone_name TEXT,
            is_upload INTEGER,
            is_deleted INTEGER,
            raw_json TEXT,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            last_batch_id TEXT NOT NULL,
            UNIQUE(source_table, raw_key, raw_time)
        );

        CREATE TABLE IF NOT EXISTS metric_records (
            metric_id TEXT PRIMARY KEY,
            raw_record_id INTEGER NOT NULL,
            source_app TEXT NOT NULL,
            source_db TEXT,
            source_table TEXT NOT NULL,
            sid TEXT,
            raw_key TEXT NOT NULL,
            raw_time INTEGER NOT NULL,
            raw_json_hash TEXT NOT NULL,
            metric TEXT NOT NULL,
            start_at TEXT,
            end_at TEXT,
            value_real REAL,
            value_text TEXT,
            value_json TEXT NOT NULL,
            unit TEXT,
            device_id TEXT,
            label TEXT,
            attrs_json TEXT,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            last_batch_id TEXT NOT NULL,
            FOREIGN KEY(raw_record_id) REFERENCES raw_records(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_raw_source_time
            ON raw_records(source_table, raw_time, raw_key);
        CREATE INDEX IF NOT EXISTS idx_metric_name_time
            ON metric_records(metric, start_at);
        CREATE INDEX IF NOT EXISTS idx_metric_raw_record
            ON metric_records(raw_record_id);
        CREATE INDEX IF NOT EXISTS idx_metric_source_time
            ON metric_records(source_table, raw_time, raw_key);
        CREATE INDEX IF NOT EXISTS idx_metric_table_name_time
            ON metric_records(source_table, metric, start_at);

        DROP VIEW IF EXISTS daily_steps;
        DROP VIEW IF EXISTS heart_rate_daily;
        DROP VIEW IF EXISTS sleep_sessions;
        DROP VIEW IF EXISTS sleep_stage_segments;
        DROP VIEW IF EXISTS workout_summary;

        CREATE VIEW daily_steps AS
        SELECT
            substr(start_at, 1, 10) AS day,
            CAST(SUM(value_real) AS INTEGER) AS steps,
            COUNT(*) AS samples,
            MIN(start_at) AS first_at,
            MAX(start_at) AS last_at
        FROM metric_records
        WHERE metric = 'steps'
            AND start_at IS NOT NULL
            AND value_real IS NOT NULL
        GROUP BY substr(start_at, 1, 10);

        CREATE VIEW heart_rate_daily AS
        SELECT
            substr(start_at, 1, 10) AS day,
            COUNT(*) AS samples,
            ROUND(AVG(value_real), 1) AS avg_bpm,
            MIN(value_real) AS min_bpm,
            MAX(value_real) AS max_bpm,
            MIN(start_at) AS first_at,
            MAX(start_at) AS last_at
        FROM metric_records
        WHERE metric = 'heart_rate.bpm'
            AND start_at IS NOT NULL
            AND value_real IS NOT NULL
        GROUP BY substr(start_at, 1, 10);

        CREATE VIEW sleep_sessions AS
        SELECT
            raw_record_id,
            raw_key AS session_key,
            substr(MIN(start_at), 1, 10) AS sleep_day,
            MIN(start_at) AS start_at,
            MAX(COALESCE(end_at, start_at)) AS end_at,
            MAX(CASE WHEN metric = 'sleep.duration' THEN value_real END) AS duration_min,
            MAX(CASE WHEN metric = 'sleep.deep_duration' THEN value_real END) AS deep_min,
            MAX(CASE WHEN metric = 'sleep.light_duration' THEN value_real END) AS light_min,
            MAX(CASE WHEN metric = 'sleep.rem_duration' THEN value_real END) AS rem_min,
            MAX(CASE WHEN metric = 'sleep.awake_duration' THEN value_real END) AS awake_min,
            MAX(CASE WHEN metric = 'sleep.awake_count' THEN value_real END) AS awake_count,
            MAX(CASE WHEN metric = 'sleep.heart_rate.avg' THEN value_real END) AS avg_bpm,
            MAX(CASE WHEN metric = 'sleep.heart_rate.min' THEN value_real END) AS min_bpm,
            MAX(CASE WHEN metric = 'sleep.heart_rate.max' THEN value_real END) AS max_bpm,
            COUNT(CASE WHEN metric = 'sleep.stage.duration' THEN 1 END) AS stage_segments
        FROM metric_records
        WHERE source_table = 'sleep_segment'
        GROUP BY raw_record_id, raw_key;

        CREATE VIEW sleep_stage_segments AS
        SELECT
            raw_record_id,
            raw_key AS session_key,
            start_at,
            end_at,
            label AS stage,
            CAST(json_extract(attrs_json, '$.state_code') AS INTEGER) AS state_code,
            value_real AS duration_min
        FROM metric_records
        WHERE metric = 'sleep.stage.duration';

        CREATE VIEW workout_summary AS
        SELECT
            raw_record_id,
            raw_key AS workout_key,
            substr(MIN(start_at), 1, 10) AS workout_day,
            MIN(start_at) AS start_at,
            MAX(COALESCE(end_at, start_at)) AS end_at,
            MAX(CASE WHEN metric = 'sport.type' THEN value_text END) AS sport_label,
            MAX(json_extract(attrs_json, '$.sport_type')) AS sport_type,
            MAX(json_extract(attrs_json, '$.category')) AS category,
            MAX(CASE WHEN metric = 'sport.duration' THEN value_real END) AS duration_sec,
            MAX(CASE WHEN metric = 'sport.distance' THEN value_real END) AS distance_m,
            MAX(CASE WHEN metric = 'sport.calories' THEN value_real END) AS calories_kcal,
            MAX(CASE WHEN metric = 'sport.steps' THEN value_real END) AS steps,
            MAX(CASE WHEN metric = 'sport.heart_rate.avg' THEN value_real END) AS avg_bpm,
            MAX(CASE WHEN metric = 'sport.heart_rate.min' THEN value_real END) AS min_bpm,
            MAX(CASE WHEN metric = 'sport.heart_rate.max' THEN value_real END) AS max_bpm,
            MAX(CASE WHEN metric = 'sport.training_load' THEN value_real END) AS training_load,
            MAX(CASE WHEN metric = 'sport.train_effect' THEN value_real END) AS train_effect,
            MAX(CASE WHEN metric = 'sport.speed.avg' THEN value_real END) AS avg_speed_raw,
            MAX(CASE WHEN metric = 'sport.pace.min' THEN value_real END) AS min_pace_raw
        FROM metric_records
        WHERE source_table = 'sport_report'
        GROUP BY raw_record_id, raw_key;
        """
    )
    # Schema v3 included raw_json_hash in the unique identity, which allowed
    # revised payloads for one source row to accumulate. Keep the most recently
    # observed revision and add the corrected logical uniqueness constraint.
    duplicate_groups = conn.execute(
        """
        SELECT source_table, raw_key, raw_time
        FROM raw_records
        GROUP BY source_table, raw_key, raw_time
        HAVING COUNT(*) > 1
        """
    ).fetchall()
    for source_table, raw_key, raw_time in duplicate_groups:
        rows = conn.execute(
            """
            SELECT id FROM raw_records
            WHERE source_table=? AND raw_key=? AND raw_time=?
            ORDER BY last_seen_at DESC, id DESC
            """,
            (source_table, raw_key, raw_time),
        ).fetchall()
        keep_id = int(rows[0][0])
        conn.execute(
            """
            DELETE FROM raw_records
            WHERE source_table=? AND raw_key=? AND raw_time=? AND id<>?
            """,
            (source_table, raw_key, raw_time, keep_id),
        )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_raw_logical_identity
        ON raw_records(source_table, raw_key, raw_time)
        """
    )
    conn.execute(
        """
        INSERT INTO metadata(key, value, updated_at)
        VALUES ('schema_version', ?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
        """,
        (str(SCHEMA_VERSION), now_iso()),
    )


def configure_connection(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA temp_store = MEMORY")
    conn.execute("PRAGMA foreign_keys = ON")


def normalized_raw_parts(record: dict[str, Any]) -> dict[str, Any]:
    value_json = canonical_json(record.get("value"))
    raw_json = record.get("raw_json")
    if raw_json is None:
        raw_json = value_json
    elif not isinstance(raw_json, str):
        raw_json = canonical_json(raw_json)
    return {
        "source_app": str(record.get("source_app") or "com.mi.health"),
        "source_db": record.get("source_db"),
        "source_table": str(record.get("source_table") or ""),
        "sid": record.get("sid"),
        "raw_key": str(record.get("raw_key") or ""),
        "raw_time": coerce_int(record.get("raw_time")),
        "raw_json_hash": sha256_text(raw_json),
        "start_at": record.get("start_at"),
        "end_at": record.get("end_at"),
        "zone_offset_sec": record.get("zone_offset_sec"),
        "zone_name": record.get("zone_name"),
        "is_upload": bool_to_int(record.get("is_upload")),
        "is_deleted": bool_to_int(record.get("is_deleted")),
        "raw_json": raw_json,
    }


def upsert_raw_record(
    conn: sqlite3.Connection,
    record: dict[str, Any],
    *,
    batch_id: str,
    seen_at: str,
) -> tuple[int, str, dict[str, Any]]:
    parts = normalized_raw_parts(record)
    existing = conn.execute(
        """
        SELECT id, source_app, source_db, sid, raw_json_hash, is_upload, is_deleted,
               start_at, end_at, zone_offset_sec, zone_name
        FROM raw_records
        WHERE source_table = ? AND raw_key = ? AND raw_time = ?
        """,
        (parts["source_table"], parts["raw_key"], parts["raw_time"]),
    ).fetchone()
    if existing is None:
        cursor = conn.execute(
            """
            INSERT INTO raw_records (
                source_app, source_db, source_table, sid, raw_key, raw_time, raw_json_hash,
                start_at, end_at, zone_offset_sec, zone_name, is_upload, is_deleted,
                raw_json, first_seen_at, last_seen_at, last_batch_id
            )
            VALUES (
                :source_app, :source_db, :source_table, :sid, :raw_key, :raw_time, :raw_json_hash,
                :start_at, :end_at, :zone_offset_sec, :zone_name, :is_upload, :is_deleted,
                :raw_json, :first_seen_at, :last_seen_at, :last_batch_id
            )
            """,
            {
                **parts,
                "first_seen_at": seen_at,
                "last_seen_at": seen_at,
                "last_batch_id": batch_id,
            },
        )
        return int(cursor.lastrowid), "inserted", parts

    raw_id = int(existing["id"])
    changed = any(
        existing[key] != parts[key]
        for key in (
            "is_upload",
            "is_deleted",
            "source_app",
            "source_db",
            "sid",
            "raw_json_hash",
            "start_at",
            "end_at",
            "zone_offset_sec",
            "zone_name",
        )
    )
    if changed:
        conn.execute(
            """
            UPDATE raw_records
            SET source_app = :source_app,
                source_db = :source_db,
                sid = :sid,
                raw_json_hash = :raw_json_hash,
                is_upload = :is_upload,
                is_deleted = :is_deleted,
                start_at = :start_at,
                end_at = :end_at,
                zone_offset_sec = :zone_offset_sec,
                zone_name = :zone_name,
                raw_json = :raw_json,
                last_seen_at = :last_seen_at,
                last_batch_id = :last_batch_id
            WHERE id = :id
            """,
            {
                **parts,
                "id": raw_id,
                "last_seen_at": seen_at,
                "last_batch_id": batch_id,
            },
        )
        return raw_id, "updated", parts

    conn.execute(
        "UPDATE raw_records SET last_seen_at = ?, last_batch_id = ? WHERE id = ?",
        (seen_at, batch_id, raw_id),
    )
    return raw_id, "existing", parts


def metric_row(
    record: dict[str, Any],
    parts: dict[str, Any],
    metric: str,
    value: Any,
    *,
    unit: str | None = None,
    start_at: str | None = None,
    end_at: str | None = None,
    label: str | None = None,
    attrs: dict[str, Any] | None = None,
    device_id: str | None = None,
    path: str | None = None,
) -> dict[str, Any]:
    value_json = canonical_json(value)
    value_real = float(value) if is_number(value) else None
    value_text = value if isinstance(value, str) else None
    attrs_for_store = dict(attrs or {})
    metric_path = path or f"{metric}:{label or ''}:{canonical_json(attrs_for_store)}"
    attrs_for_store.setdefault("metric_path", metric_path)
    identity = {
        "source_table": parts["source_table"],
        "raw_key": parts["raw_key"],
        "raw_time": parts["raw_time"],
        "metric": metric,
        "metric_path": metric_path,
    }
    return {
        "metric_id": sha256_text(canonical_json(identity)),
        "source_app": parts["source_app"],
        "source_db": parts["source_db"],
        "source_table": parts["source_table"],
        "sid": parts["sid"],
        "raw_key": parts["raw_key"],
        "raw_time": parts["raw_time"],
        "raw_json_hash": parts["raw_json_hash"],
        "metric": metric,
        "start_at": start_at or record.get("start_at"),
        "end_at": end_at,
        "value_real": value_real,
        "value_text": value_text,
        "value_json": value_json,
        "unit": unit,
        "device_id": device_id or parts["sid"],
        "label": label,
        "attrs_json": canonical_json(attrs_for_store),
    }


def emit_number(
    rows: list[dict[str, Any]],
    record: dict[str, Any],
    parts: dict[str, Any],
    value: dict[str, Any],
    key: str,
    metric: str,
    unit: str | None,
    **kwargs: Any,
) -> None:
    number = value.get(key)
    if is_number(number):
        kwargs.setdefault("path", key)
        rows.append(metric_row(record, parts, metric, number, unit=unit, **kwargs))


def map_step_record(record: dict[str, Any], parts: dict[str, Any]) -> list[dict[str, Any]]:
    value = record.get("value")
    if not isinstance(value, dict):
        return []
    rows: list[dict[str, Any]] = []
    emit_number(rows, record, parts, value, "steps", "steps", "count")
    emit_number(rows, record, parts, value, "distance", "distance", "m")
    emit_number(rows, record, parts, value, "calories", "calories", "kcal")
    return rows


def map_hr_record(record: dict[str, Any], parts: dict[str, Any]) -> list[dict[str, Any]]:
    value = record.get("value")
    if not isinstance(value, dict):
        return []
    rows: list[dict[str, Any]] = []
    source_key = parts["raw_key"]
    attrs = {"source_key": source_key}
    if is_number(value.get("bpm")):
        start_at = iso_from_epoch(value.get("time"), record) or record.get("start_at")
        rows.append(
            metric_row(
                record,
                parts,
                "heart_rate.bpm",
                value["bpm"],
                unit="bpm",
                start_at=start_at,
                label=source_key,
                attrs=attrs,
                path="bpm",
            )
        )
    for index, item in enumerate(value.get("items") or []):
        if not isinstance(item, dict) or not is_number(item.get("value")):
            continue
        rows.append(
            metric_row(
                record,
                parts,
                "heart_rate.bpm",
                item["value"],
                unit="bpm",
                start_at=iso_from_epoch(item.get("time"), record) or record.get("start_at"),
                label=source_key,
                attrs={
                    **attrs,
                    "item_index": index,
                    "threshold": value.get("threshold"),
                    "event_start_at": iso_from_epoch(value.get("start_time"), record),
                    "event_end_at": iso_from_epoch(value.get("end_time"), record),
                },
                path=f"items.{index}.value",
            )
        )
    return rows


def map_sleep_segment(record: dict[str, Any], parts: dict[str, Any]) -> list[dict[str, Any]]:
    value = record.get("value")
    if not isinstance(value, dict):
        return []
    rows: list[dict[str, Any]] = []
    start_at = iso_from_epoch(value.get("bedtime") or value.get("device_bedtime"), record) or record.get("start_at")
    end_at = iso_from_epoch(value.get("wake_up_time") or value.get("device_wake_up_time"), record) or record.get("end_at")
    summary_attrs = {"source_key": parts["raw_key"], "version": value.get("version")}
    summary_specs = [
        ("sleep_deep_duration", "sleep.deep_duration", "min"),
        ("sleep_light_duration", "sleep.light_duration", "min"),
        ("sleep_rem_duration", "sleep.rem_duration", "min"),
        ("sleep_awake_duration", "sleep.awake_duration", "min"),
        ("awake_count", "sleep.awake_count", "count"),
        ("breath_quality", "sleep.breath_quality", "score"),
        ("avg_hr", "sleep.heart_rate.avg", "bpm"),
        ("min_hr", "sleep.heart_rate.min", "bpm"),
        ("max_hr", "sleep.heart_rate.max", "bpm"),
    ]
    # `sleep_duration` has incompatible semantics in observed Xiaomi payloads
    # (for example 360/1080 while stage totals are 520/563 minutes). The
    # asleep-stage total is the stable source; `duration` is its fallback.
    sleep_parts = [
        value.get("sleep_deep_duration"),
        value.get("sleep_light_duration"),
        value.get("sleep_rem_duration"),
    ]
    numeric_sleep_parts = [part for part in sleep_parts if is_number(part)]
    if len(numeric_sleep_parts) == len(sleep_parts):
        sleep_duration = sum(numeric_sleep_parts)
    elif is_number(value.get("duration")):
        sleep_duration = value["duration"]
    else:
        sleep_duration = sum(numeric_sleep_parts) if numeric_sleep_parts else None
    if is_number(sleep_duration):
        rows.append(
            metric_row(
                record,
                parts,
                "sleep.duration",
                sleep_duration,
                unit="min",
                start_at=start_at,
                end_at=end_at,
                attrs=summary_attrs,
                path="asleep_stage_total",
            )
        )
    for key, metric, unit in summary_specs:
        emit_number(
            rows,
            record,
            parts,
            value,
            key,
            metric,
            unit,
            start_at=start_at,
            end_at=end_at,
            attrs=summary_attrs,
        )

    for index, item in enumerate(value.get("items") or []):
        if not isinstance(item, dict):
            continue
        start = iso_from_epoch(item.get("start_time"), record)
        end = iso_from_epoch(item.get("end_time"), record)
        state_code = coerce_int(item.get("state"), default=-1)
        stage = SLEEP_STAGE_LABELS.get(state_code, f"unknown_{state_code}")
        duration_min = None
        if item.get("start_time") is not None and item.get("end_time") is not None:
            duration_min = (coerce_int(item["end_time"]) - coerce_int(item["start_time"])) / 60
        if duration_min is not None and duration_min >= 0:
            rows.append(
                metric_row(
                    record,
                    parts,
                    "sleep.stage.duration",
                    duration_min,
                    unit="min",
                    start_at=start,
                    end_at=end,
                    label=stage,
                    attrs={
                        "source_key": parts["raw_key"],
                        "segment_index": index,
                        "stage": stage,
                        "state_code": state_code,
                    },
                    path=f"items.{index}.duration",
                )
            )
    return rows


def sport_context(record: dict[str, Any], value: dict[str, Any], parts: dict[str, Any]) -> dict[str, Any]:
    return {
        "source_key": parts["raw_key"],
        "sport_type": value.get("sport_type"),
        "proto_type": value.get("proto_type"),
        "category": value.get("category"),
    }


def map_sport_report(record: dict[str, Any], parts: dict[str, Any]) -> list[dict[str, Any]]:
    value = record.get("value")
    if not isinstance(value, dict):
        return []
    rows: list[dict[str, Any]] = []
    start_at = iso_from_epoch(value.get("start_time") or value.get("entityStartTime"), record) or record.get("start_at")
    end_at = iso_from_epoch(value.get("end_time") or value.get("entityEndTime"), record) or record.get("end_at")
    attrs = sport_context(record, value, parts)
    specs = [
        ("duration", "sport.duration", "sec"),
        ("valid_duration", "sport.valid_duration", "sec"),
        ("distance", "sport.distance", "m"),
        ("corrected_distance", "sport.corrected_distance", "m"),
        ("calories", "sport.calories", "kcal"),
        ("total_cal", "sport.total_calories", "kcal"),
        ("steps", "sport.steps", "count"),
        ("avg_hrm", "sport.heart_rate.avg", "bpm"),
        ("min_hrm", "sport.heart_rate.min", "bpm"),
        ("max_hrm", "sport.heart_rate.max", "bpm"),
        ("hrm_warm_up_duration", "sport.hrm_zone.warm_up", "sec"),
        ("hrm_fat_burning_duration", "sport.hrm_zone.fat_burning", "sec"),
        ("hrm_aerobic_duration", "sport.hrm_zone.aerobic", "sec"),
        ("hrm_anaerobic_duration", "sport.hrm_zone.anaerobic", "sec"),
        ("hrm_extreme_duration", "sport.hrm_zone.extreme", "sec"),
        ("training_load", "sport.training_load", None),
        ("train_load", "sport.training_load", None),
        ("train_effect", "sport.train_effect", None),
        ("anaerobic_train_effect", "sport.anaerobic_train_effect", None),
        ("vitality", "sport.vitality", None),
        ("recover_time", "sport.recover_time", None),
        ("vo2_max", "sport.vo2_max", "ml/kg/min"),
        # Xiaomi's stored values do not carry a unit. Observed cycling
        # avg_speed=17.11072 is plausibly km/h and demonstrably not m/s, while
        # pace values use another unverified encoding. Preserve them as raw.
        ("avg_speed", "sport.speed.avg", None),
        ("max_speed", "sport.speed.max", None),
        ("min_pace", "sport.pace.min", None),
        ("max_pace", "sport.pace.max", None),
        ("max_cadence", "sport.cadence.max", "spm"),
        ("stroke_count", "sport.swimming.stroke_count", "count"),
        ("turn_count", "sport.swimming.turn_count", "count"),
        ("avg_swolf", "sport.swimming.swolf.avg", None),
        ("best_swolf", "sport.swimming.swolf.best", None),
    ]
    for key, metric, unit in specs:
        emit_number(
            rows,
            record,
            parts,
            value,
            key,
            metric,
            unit,
            start_at=start_at,
            end_at=end_at,
            label=parts["raw_key"],
            attrs=attrs,
        )
    rows.append(
        metric_row(
            record,
            parts,
            "sport.type",
            parts["raw_key"],
            start_at=start_at,
            end_at=end_at,
            label=parts["raw_key"],
            attrs=attrs,
            path="sport_type",
        )
    )
    return rows


def map_calorie_record(record: dict[str, Any], parts: dict[str, Any]) -> list[dict[str, Any]]:
    value = record.get("value")
    if not isinstance(value, dict):
        return []
    rows: list[dict[str, Any]] = []
    emit_number(rows, record, parts, value, "calories", "calories", "kcal")
    return rows


def map_stand_record(record: dict[str, Any], parts: dict[str, Any]) -> list[dict[str, Any]]:
    value = record.get("value")
    if not isinstance(value, dict):
        return []
    return [
        metric_row(
            record,
            parts,
            "stand.valid_hour",
            1,
            unit="count",
            start_at=iso_from_epoch(value.get("start_time"), record) or record.get("start_at"),
            end_at=iso_from_epoch(value.get("end_time"), record) or record.get("end_at"),
            attrs={"source_key": parts["raw_key"]},
            path="valid_stand",
        )
    ]


def map_training_load_record(record: dict[str, Any], parts: dict[str, Any]) -> list[dict[str, Any]]:
    value = record.get("value")
    if not isinstance(value, dict):
        return []
    rows: list[dict[str, Any]] = []
    emit_number(rows, record, parts, value, "current_day_train_load", "training_load.current_day", None)
    emit_number(rows, record, parts, value, "current_train_load_level", "training_load.level", None)
    emit_number(rows, record, parts, value, "wtl_sum", "training_load.wtl_sum", None)
    return rows


def map_vitality_record(record: dict[str, Any], parts: dict[str, Any]) -> list[dict[str, Any]]:
    value = record.get("value")
    if not isinstance(value, dict):
        return []
    rows: list[dict[str, Any]] = []
    specs = [
        ("latest_accumulated_vitality", "vitality.latest_accumulated", None),
        ("daily_low_intensity_vitality", "vitality.daily.low", None),
        ("daily_medium_intensity_vitality", "vitality.daily.medium", None),
        ("daily_high_intensity_vitality", "vitality.daily.high", None),
        ("suggested_activity_duration", "vitality.suggested_activity_duration", "min"),
        ("suggested_activity_type", "vitality.suggested_activity_type", None),
    ]
    for key, metric, unit in specs:
        emit_number(rows, record, parts, value, key, metric, unit)
    return rows


def map_weight_item_record(record: dict[str, Any], parts: dict[str, Any]) -> list[dict[str, Any]]:
    value = record.get("value")
    if not isinstance(value, dict):
        return []
    rows: list[dict[str, Any]] = []
    emit_number(rows, record, parts, value, "weight", "weight", "kg")
    emit_number(rows, record, parts, value, "bmi", "bmi", None)
    return rows


def map_v02max_record(record: dict[str, Any], parts: dict[str, Any]) -> list[dict[str, Any]]:
    value = record.get("value")
    if not isinstance(value, dict):
        return []
    rows: list[dict[str, Any]] = []
    emit_number(rows, record, parts, value, "vo2_max", "vo2_max", "ml/kg/min")
    emit_number(rows, record, parts, value, "vo2_max_level", "vo2_max.level", None)
    return rows


MAPPERS = {
    "step_record": map_step_record,
    "hr_record": map_hr_record,
    "sleep_segment": map_sleep_segment,
    "sport_report": map_sport_report,
    "calorie_record": map_calorie_record,
    "stand_record": map_stand_record,
    "training_load_record": map_training_load_record,
    "vitality_record": map_vitality_record,
    "weight_item_record": map_weight_item_record,
    "v02max_record": map_v02max_record,
}


def metrics_for_record(record: dict[str, Any], parts: dict[str, Any]) -> list[dict[str, Any]]:
    mapper = MAPPERS.get(parts["source_table"])
    if mapper is None:
        return []
    return mapper(record, parts)


def upsert_metric(
    conn: sqlite3.Connection,
    raw_record_id: int,
    row: dict[str, Any],
    *,
    batch_id: str,
    seen_at: str,
) -> str:
    existing = conn.execute(
        """
        SELECT raw_record_id, source_app, source_db, source_table, sid, raw_key,
               raw_time, raw_json_hash, metric, start_at, end_at, value_real,
               value_text, value_json, unit, device_id, label, attrs_json
        FROM metric_records
        WHERE metric_id = ?
        """,
        (row["metric_id"],),
    ).fetchone()
    if existing is None:
        conn.execute(
            """
            INSERT INTO metric_records (
                metric_id, raw_record_id, source_app, source_db, source_table, sid,
                raw_key, raw_time, raw_json_hash, metric, start_at, end_at,
                value_real, value_text, value_json, unit, device_id, label,
                attrs_json, first_seen_at, last_seen_at, last_batch_id
            )
            VALUES (
                :metric_id, :raw_record_id, :source_app, :source_db, :source_table, :sid,
                :raw_key, :raw_time, :raw_json_hash, :metric, :start_at, :end_at,
                :value_real, :value_text, :value_json, :unit, :device_id, :label,
                :attrs_json, :first_seen_at, :last_seen_at, :last_batch_id
            )
            """,
            {
                **row,
                "raw_record_id": raw_record_id,
                "first_seen_at": seen_at,
                "last_seen_at": seen_at,
                "last_batch_id": batch_id,
            },
        )
        return "inserted"
    changed = any(
        existing[key] != row[key]
        for key in (
            "start_at",
            "end_at",
            "source_app",
            "source_db",
            "source_table",
            "sid",
            "raw_key",
            "raw_time",
            "raw_json_hash",
            "metric",
            "value_real",
            "value_text",
            "value_json",
            "unit",
            "device_id",
            "label",
            "attrs_json",
        )
    ) or existing["raw_record_id"] != raw_record_id
    if changed:
        conn.execute(
            """
            UPDATE metric_records
            SET raw_record_id = :raw_record_id,
                source_app = :source_app,
                source_db = :source_db,
                source_table = :source_table,
                sid = :sid,
                raw_key = :raw_key,
                raw_time = :raw_time,
                raw_json_hash = :raw_json_hash,
                metric = :metric,
                start_at = :start_at,
                end_at = :end_at,
                value_real = :value_real,
                value_text = :value_text,
                value_json = :value_json,
                unit = :unit,
                device_id = :device_id,
                label = :label,
                attrs_json = :attrs_json,
                last_seen_at = :last_seen_at,
                last_batch_id = :last_batch_id
            WHERE metric_id = :metric_id
            """,
            {
                **row,
                "raw_record_id": raw_record_id,
                "last_seen_at": seen_at,
                "last_batch_id": batch_id,
            },
        )
        return "updated"
    conn.execute(
        "UPDATE metric_records SET last_seen_at = ?, last_batch_id = ? WHERE metric_id = ?",
        (seen_at, batch_id, row["metric_id"]),
    )
    return "existing"


def iter_jsonl(paths: Iterable[Path]) -> Iterable[tuple[Path, int, dict[str, Any]]]:
    for path in paths:
        with path.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                if not line.strip():
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise ValueError(f"{path}:{line_number}: invalid JSONL: {exc}") from exc
                if not isinstance(record, dict):
                    raise ValueError(f"{path}:{line_number}: JSONL record must be an object")
                yield path, line_number, record


def import_paths(
    conn: sqlite3.Connection,
    paths: list[Path],
    *,
    batch_id: str,
    include_deleted_metrics: bool,
    progress_every: int,
    deadline_monotonic: float | None = None,
) -> dict[str, Any]:
    stats: dict[str, Any] = {
        "batch_id": batch_id,
        "input_paths": [str(path) for path in paths],
        "records_seen": 0,
        "raw": Counter(),
        "metrics": Counter(),
        "records_by_table": Counter(),
        "metrics_by_name": Counter(),
        "metrics_by_table": Counter(),
        "watermarks": {},
        "errors": [],
    }
    seen_at = now_iso()

    for path, line_number, record in iter_jsonl(paths):
        if deadline_monotonic is not None and time.monotonic() >= deadline_monotonic:
            raise TimeoutError("health import timed out")
        if not record.get("source_table"):
            raise ValueError(f"{path}:{line_number}: source_table is required")
        if record.get("raw_time") in (None, ""):
            raise ValueError(f"{path}:{line_number}: raw_time is required")
        try:
            int(record["raw_time"])
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{path}:{line_number}: raw_time must be an integer") from exc
        stats["records_seen"] += 1
        table = str(record.get("source_table") or "")
        stats["records_by_table"][table] += 1
        raw_time = coerce_int(record.get("raw_time"))
        raw_key = str(record.get("raw_key") or "")
        current_mark = stats["watermarks"].setdefault(table, {"time": 0, "key": ""})
        if (raw_time, raw_key) > (current_mark["time"], current_mark["key"]):
            current_mark.update({"time": raw_time, "key": raw_key})
        raw_record_id, raw_status, parts = upsert_raw_record(
            conn,
            record,
            batch_id=batch_id,
            seen_at=seen_at,
        )
        stats["raw"][raw_status] += 1

        if record.get("is_deleted"):
            deleted = conn.execute(
                "DELETE FROM metric_records WHERE raw_record_id = ?", (raw_record_id,)
            ).rowcount
            stats["metrics"]["skipped_deleted"] += 1
            stats["metrics"]["deleted_obsolete"] += deleted
            continue

        metric_rows = metrics_for_record(record, parts)
        if not metric_rows:
            stats["metrics"]["unmapped_records"] += 1
        for metric in metric_rows:
            status = upsert_metric(
                conn,
                raw_record_id,
                metric,
                batch_id=batch_id,
                seen_at=seen_at,
            )
            stats["metrics"][status] += 1
            stats["metrics_by_name"][metric["metric"]] += 1
            stats["metrics_by_table"][metric["source_table"]] += 1

        active_metric_ids = [row["metric_id"] for row in metric_rows]
        if active_metric_ids:
            placeholders = ",".join("?" for _ in active_metric_ids)
            deleted = conn.execute(
                f"""
                DELETE FROM metric_records
                WHERE raw_record_id = ? AND metric_id NOT IN ({placeholders})
                """,
                (raw_record_id, *active_metric_ids),
            ).rowcount
        else:
            deleted = conn.execute(
                "DELETE FROM metric_records WHERE raw_record_id = ?", (raw_record_id,)
            ).rowcount
        stats["metrics"]["deleted_obsolete"] += deleted

        if progress_every and stats["records_seen"] % progress_every == 0:
            print(
                f"imported {stats['records_seen']} rows from {path.name}:{line_number}",
                file=sys.stderr,
                flush=True,
            )

    return {
        key: dict(value) if isinstance(value, Counter) else value
        for key, value in stats.items()
    }


def main() -> None:
    args = parse_args()
    db_path = args.db.expanduser().resolve()
    input_paths = [path.expanduser().resolve() for path in args.input]
    missing = [str(path) for path in input_paths if not path.exists()]
    if missing:
        raise FileNotFoundError(", ".join(missing))

    if args.rebuild:
        db_path.unlink(missing_ok=True)
        db_path.with_suffix(db_path.suffix + "-wal").unlink(missing_ok=True)
        db_path.with_suffix(db_path.suffix + "-shm").unlink(missing_ok=True)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    batch_id = args.batch_id or default_batch_id()
    started_at = now_iso()
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    configure_connection(conn)
    try:
        ensure_schema(conn)
        conn.commit()
        with conn:
            conn.execute(
                """
                INSERT INTO import_batches(batch_id, started_at, input_paths_json)
                VALUES (?, ?, ?)
                ON CONFLICT(batch_id) DO UPDATE SET
                    started_at = excluded.started_at,
                    finished_at = NULL,
                    input_paths_json = excluded.input_paths_json,
                    stats_json = NULL
                """,
                (batch_id, started_at, canonical_json([str(path) for path in input_paths])),
            )
            stats = import_paths(
                conn,
                input_paths,
                batch_id=batch_id,
                include_deleted_metrics=args.include_deleted_metrics,
                progress_every=args.progress_every,
            )
            finished_at = now_iso()
            conn.execute(
                """
                UPDATE import_batches
                SET finished_at = ?, stats_json = ?
                WHERE batch_id = ?
                """,
                (finished_at, canonical_json(stats), batch_id),
            )
    finally:
        conn.close()

    print(json.dumps({"db": str(db_path), "stats": stats}, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
