"""SSH transport and phone-side Xiaomi Health JSONL exporter."""

from __future__ import annotations

import base64
import json
import shlex
import subprocess
from pathlib import Path
from typing import Any


DEFAULT_TABLES = [
    "step_record",
    "hr_record",
    "calorie_record",
    "stand_record",
    "sleep_segment",
    "sport_report",
    "training_load_record",
    "vitality_record",
    "weight_item_record",
    "v02max_record",
]

TERMUX_PYTHON = "/data/data/com.termux/files/usr/bin/python"
TERMUX_ENV = (
    "HOME=/data/data/com.termux/files/home "
    "PREFIX=/data/data/com.termux/files/usr "
    "PATH=/data/data/com.termux/files/usr/bin:/system/bin:/system/xbin"
)


# Passed to Termux Python over stdin. Configuration is a small base64 argument,
# so neither the script nor its configuration is ever written to the phone.
PHONE_EXPORTER = r'''
from __future__ import annotations

import base64
import glob
import json
import os
import re
import sqlite3
import sys
from datetime import datetime, timedelta, timezone

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

METRIC_NAMES = {
    "step_record": "step",
    "hr_record": "heart_rate",
    "calorie_record": "calorie",
    "stand_record": "stand",
    "sleep_segment": "sleep_segment",
    "sport_report": "sport",
    "training_load_record": "training_load",
    "vitality_record": "vitality",
    "weight_item_record": "weight",
    "v02max_record": "vo2_max",
}

END_TIME_KEYS = {
    "sleep_segment": ["wake_up_time", "device_wake_up_time", "out_bed_timestamp"],
    "sport_report": ["end_time", "entityEndTime"],
    "stand_record": ["end_time"],
}


def decode_json(value):
    if not value:
        return None
    try:
        return json.loads(value)
    except Exception:
        return value


def resolve_timezone(zone_name, offset_seconds):
    if zone_name and ZoneInfo:
        try:
            return ZoneInfo(zone_name)
        except Exception:
            pass
    return timezone(timedelta(seconds=offset_seconds or 0))


def iso_from_epoch(value, zone_name, offset_seconds):
    if value in (None, ""):
        return None
    try:
        timestamp = int(value)
    except Exception:
        return None
    if timestamp > 10_000_000_000:
        timestamp //= 1000
    return datetime.fromtimestamp(
        timestamp, tz=resolve_timezone(zone_name, offset_seconds)
    ).isoformat()


def pick_end_at(table, value, zone_name, offset_seconds):
    if not isinstance(value, dict):
        return None
    for key in END_TIME_KEYS.get(table, []):
        end_at = iso_from_epoch(value.get(key), zone_name, offset_seconds)
        if end_at:
            return end_at
    return None


def table_columns(conn, table):
    return {row[1] for row in conn.execute('PRAGMA table_info("%s")' % table)}


def table_exists(conn, table):
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone() is not None


def find_db():
    fixed = "/data/user/0/com.mi.health/databases/2165204593/cn/fitness_data"
    if os.path.exists(fixed):
        return fixed
    matches = sorted(glob.glob(
        "/data/user/0/com.mi.health/databases/*/cn/fitness_data"
    ))
    if not matches:
        raise FileNotFoundError("fitness_data not found")
    return matches[0]


def main():
    config = json.loads(base64.urlsafe_b64decode(sys.argv[1]).decode("utf-8"))
    db_path = find_db()
    conn = sqlite3.connect("file:%s?mode=ro" % db_path, uri=True)
    conn.row_factory = sqlite3.Row
    stats = {"tables": {}, "total_rows": 0}
    try:
        # One read transaction keeps all table deltas on the same WAL snapshot.
        conn.execute("BEGIN")
        for item in config["tables"]:
            table = str(item["name"])
            if not re.fullmatch(r"[a-z0-9_]+", table):
                raise ValueError("invalid table name")
            if not table_exists(conn, table):
                stats["tables"][table] = {"rows": 0, "missing": True}
                continue
            columns = table_columns(conn, table)
            if "time" not in columns:
                stats["tables"][table] = {"rows": 0, "missing_time": True}
                continue

            params = []
            where = []
            since_time = int(item.get("since_time") or 0)
            since_key = str(item.get("since_key") or "")
            lookback_seconds = int(item.get("lookback_seconds") or 0)
            if lookback_seconds > 0:
                where.append("time >= ?")
                params.append(max(0, since_time - lookback_seconds))
            elif since_time > 0:
                if "key" in columns:
                    where.append("(time > ? OR (time = ? AND key > ?))")
                    params.extend([since_time, since_time, since_key])
                else:
                    where.append("time > ?")
                    params.append(since_time)
            where_sql = (" WHERE " + " AND ".join(where)) if where else ""
            order_sql = " ORDER BY time, key" if "key" in columns else " ORDER BY time"

            rows = 0
            query = 'SELECT * FROM "%s"%s%s' % (table, where_sql, order_sql)
            for row in conn.execute(query, params):
                keys = row.keys()
                raw_value = row["value"] if "value" in keys else None
                value = decode_json(raw_value)
                offset = row["zoneOffsetInSec"] if "zoneOffsetInSec" in keys else None
                zone_name = row["zoneName"] if "zoneName" in keys else None
                record = {
                    "source_app": "com.mi.health",
                    "source_db": "fitness_data",
                    "source_table": table,
                    "metric": METRIC_NAMES.get(table, table),
                    "sid": row["sid"] if "sid" in keys else None,
                    "raw_key": row["key"] if "key" in keys else None,
                    "raw_time": row["time"],
                    "start_at": iso_from_epoch(row["time"], zone_name, offset),
                    "end_at": pick_end_at(table, value, zone_name, offset),
                    "zone_offset_sec": offset,
                    "zone_name": zone_name,
                    "is_upload": bool(row["isUpload"]) if "isUpload" in keys else None,
                    "is_deleted": bool(row["isDeleted"]) if "isDeleted" in keys else None,
                    "value": value,
                    "unit": None,
                    "raw_json": raw_value,
                }
                print(json.dumps(record, ensure_ascii=False, separators=(",", ":")))
                rows += 1
            stats["tables"][table] = {"rows": rows}
            stats["total_rows"] += rows
    finally:
        conn.close()
    print(
        "STATS:" + json.dumps(stats, ensure_ascii=False, separators=(",", ":")),
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
'''


def _ssh_base(host: str | None, timeout_seconds: float) -> list[str]:
    if not isinstance(host, str) or not host.strip() or host.startswith("-"):
        raise ValueError("configure ssh_host for the Xiaomi Health backend before contacting a phone")
    connect_timeout = max(1, min(10, int(timeout_seconds)))
    return [
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        "StrictHostKeyChecking=yes",
        "-o",
        f"ConnectTimeout={connect_timeout}",
        "-o",
        "ServerAliveInterval=15",
        "-o",
        "ServerAliveCountMax=2",
        host,
    ]


def _root_command(command: str) -> str:
    return f"su -c {shlex.quote(command)}"


def _bounded_error(value: str, limit: int = 1000) -> str:
    compact = " ".join(value.split())
    return compact[-limit:]


def _run_ssh_text(host: str, command: str, *, timeout_seconds: float) -> str:
    try:
        completed = subprocess.run(
            _ssh_base(host, timeout_seconds) + [_root_command(command)],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=max(0.1, timeout_seconds),
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise TimeoutError(f"SSH command timed out after {timeout_seconds:.1f}s") from exc
    if completed.returncode != 0:
        detail = _bounded_error(completed.stderr or completed.stdout)
        raise ConnectionError(f"SSH command failed ({completed.returncode}): {detail}")
    return completed.stdout.strip()


def verify_device(host: str | None, expected_serial: str | None, *, timeout_seconds: float) -> dict[str, str]:
    """Verify the remote Android device before any app action or DB export."""
    if not isinstance(expected_serial, str) or not expected_serial.strip():
        raise ValueError("configure device_serial for the Xiaomi Health backend before contacting a phone")
    observed = _run_ssh_text(host, "getprop ro.serialno", timeout_seconds=timeout_seconds)
    serials = [line.strip() for line in observed.splitlines() if line.strip()]
    if serials != [expected_serial]:
        shown = ", ".join(serials) if serials else "<empty>"
        raise RuntimeError(f"unexpected Android serial: expected {expected_serial}, got {shown}")
    return {"host": host, "serial": expected_serial}


def refresh_health_app(host: str, *, timeout_seconds: float) -> dict[str, Any]:
    """Request a full device sync through the existing background app process."""
    if timeout_seconds < 30:
        raise TimeoutError("Background device sync needs at least 30s including cleanup")
    # Imported lazily to avoid phone_exporter -> background_sync -> measurement
    # -> phone_exporter initialization cycles.
    from .background_sync import MAX_ACTIVE_SECONDS, sync_device_in_background

    active_budget = min(MAX_ACTIVE_SECONDS, float(timeout_seconds) - 10.0)
    return sync_device_in_background(host, timeout_seconds=active_budget)


def export_jsonl(
    host: str,
    config: dict[str, Any],
    output_path: Path,
    stderr_path: Path,
    *,
    timeout_seconds: float,
) -> dict[str, Any]:
    """Stream phone JSONL directly into a local file and return bounded stats."""
    encoded = base64.urlsafe_b64encode(
        json.dumps(config, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    ).decode("ascii")
    command = f"{TERMUX_ENV} {TERMUX_PYTHON} - {shlex.quote(encoded)}"
    try:
        with output_path.open("w", encoding="utf-8") as output_handle, stderr_path.open(
            "w", encoding="utf-8"
        ) as stderr_handle:
            completed = subprocess.run(
                _ssh_base(host, timeout_seconds) + [_root_command(command)],
                input=PHONE_EXPORTER,
                text=True,
                stdout=output_handle,
                stderr=stderr_handle,
                timeout=max(0.1, timeout_seconds),
                check=False,
            )
    except subprocess.TimeoutExpired as exc:
        raise TimeoutError(f"health export timed out after {timeout_seconds:.1f}s") from exc

    stderr = stderr_path.read_text(encoding="utf-8", errors="replace")
    if completed.returncode != 0:
        raise ConnectionError(
            f"health export failed ({completed.returncode}): {_bounded_error(stderr)}"
        )

    stats: dict[str, Any] = {}
    unexpected: list[str] = []
    for line in stderr.splitlines():
        if line.startswith("STATS:"):
            try:
                candidate = json.loads(line.removeprefix("STATS:"))
            except json.JSONDecodeError as exc:
                raise RuntimeError("phone exporter returned malformed stats") from exc
            if isinstance(candidate, dict):
                stats = candidate
        elif line.strip():
            unexpected.append(line.strip())
    if unexpected:
        stats["stderr"] = _bounded_error("\n".join(unexpected), 500)
    stats.setdefault("total_rows", 0)
    return stats
