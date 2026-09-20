"""Fresh or cached Band/device-state snapshot assembly."""

from __future__ import annotations

import json
import math
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .device_transport import DeviceTransportError, device_session
from .importer import canonical_json, configure_connection, ensure_schema
from .measurement import _now_iso


SNAPSHOT_KEY = "device.snapshot"


def _read_varint(data: bytes, offset: int) -> tuple[int, int]:
    value = 0
    shift = 0
    while offset < len(data) and shift < 70:
        octet = data[offset]
        offset += 1
        value |= (octet & 0x7F) << shift
        if not octet & 0x80:
            return value, offset
        shift += 7
    raise ValueError("malformed protobuf varint")


def parse_protobuf(data: bytes) -> dict[int, list[Any]]:
    fields: dict[int, list[Any]] = {}
    offset = 0
    while offset < len(data):
        tag, offset = _read_varint(data, offset)
        field, wire = tag >> 3, tag & 7
        if field == 0:
            raise ValueError("invalid protobuf field")
        if wire == 0:
            value, offset = _read_varint(data, offset)
        elif wire == 2:
            length, offset = _read_varint(data, offset)
            end = offset + length
            if end > len(data):
                raise ValueError("truncated protobuf field")
            value, offset = data[offset:end], end
        elif wire == 1:
            end = offset + 8
            if end > len(data):
                raise ValueError("truncated fixed64 field")
            value, offset = data[offset:end], end
        elif wire == 5:
            end = offset + 4
            if end > len(data):
                raise ValueError("truncated fixed32 field")
            value, offset = data[offset:end], end
        else:
            raise ValueError(f"unsupported protobuf wire type {wire}")
        fields.setdefault(field, []).append(value)
    return fields


def _one(fields: dict[int, list[Any]], field: int) -> Any | None:
    values = fields.get(field)
    return None if not values else values[-1]


def _nested(fields: dict[int, list[Any]], field: int) -> dict[int, list[Any]] | None:
    value = _one(fields, field)
    return parse_protobuf(value) if isinstance(value, bytes) else None


def _boolean(fields: dict[int, list[Any]], field: int) -> bool | None:
    value = _one(fields, field)
    return bool(value) if isinstance(value, int) else None


def _integer(fields: dict[int, list[Any]], field: int) -> int | None:
    value = _one(fields, field)
    return int(value) if isinstance(value, int) and not isinstance(value, bool) else None


def _outer_payload(packet: bytes, field: int) -> dict[int, list[Any]]:
    outer = parse_protobuf(packet)
    payload = _nested(outer, field)
    if payload is None:
        raise ValueError(f"protobuf response is missing outer field {field}")
    return payload


def _decode_basic_state(packet: bytes) -> dict[str, Any]:
    system = _outer_payload(packet, 4)
    state = _nested(system, 48)
    if state is None:
        raise ValueError("device-state response is missing basicDeviceState")
    activity = _nested(state, 5)
    activity_value = None
    if activity is not None:
        activity_value = {
            "activity_type": _integer(activity, 1),
            "current_state": _integer(activity, 2),
        }
        if all(value is None for value in activity_value.values()):
            activity_value = None
    return {
        "charging": _boolean(state, 1),
        "battery_percent": _integer(state, 2),
        "wearing": _boolean(state, 3),
        "sleeping": _boolean(state, 4),
        "activity_state": activity_value,
    }


def _decode_battery(packet: bytes) -> dict[str, Any]:
    system = _outer_payload(packet, 4)
    power = _nested(system, 2)
    battery = _nested(power or {}, 1)
    if battery is None:
        raise ValueError("battery response is missing battery data")
    state = _integer(battery, 2)
    charging = True if state == 1 else False if state in {2, 3} else None
    return {"battery_percent": _integer(battery, 1), "charging": charging}


def _decode_heart_rate(packet: bytes) -> dict[str, Any]:
    health = _outer_payload(packet, 10)
    value = _nested(health, 8)
    if value is None:
        raise ValueError("heart-rate config is missing")
    disabled = _boolean(value, 1)
    interval = _integer(value, 2)
    advanced = _nested(value, 5)
    breathing = _integer(value, 9)
    high_enabled = _boolean(value, 3)
    low = _nested(value, 8)
    low_enabled = _boolean(low or {}, 1)
    return {
        "enabled": None if disabled is None else not disabled,
        "mode": None if disabled is None else "disabled" if disabled else "smart" if interval == 0 else "interval",
        "interval_minutes": None if interval == 0 else interval,
        "raw_interval_minutes": interval,
        "sleep_detection_enabled": _boolean(advanced or {}, 1),
        "breathing_quality_enabled": True if breathing == 1 else False if breathing == 2 else None,
        "high_alert_enabled": high_enabled,
        "high_alert_bpm": _integer(value, 4) if high_enabled is True else None,
        "low_alert_enabled": low_enabled,
        "low_alert_bpm": _integer(low or {}, 2) if low_enabled is True else None,
    }


def _decode_spo2(packet: bytes) -> dict[str, Any]:
    health = _outer_payload(packet, 10)
    value = _nested(health, 7)
    if value is None:
        raise ValueError("blood-oxygen config is missing")
    alarm = _nested(value, 4)
    alarm_enabled = _boolean(alarm or {}, 1)
    return {
        "enabled": _boolean(value, 2),
        "low_alert_enabled": alarm_enabled,
        "low_alert_percent": _integer(alarm or {}, 2) if alarm_enabled is True else None,
    }


def _decode_stress(packet: bytes) -> dict[str, Any]:
    health = _outer_payload(packet, 10)
    value = _nested(health, 10)
    if value is None:
        raise ValueError("stress config is missing")
    relax = _nested(value, 2)
    return {
        "enabled": _boolean(value, 1),
        "relaxation_reminder_enabled": _boolean(relax or {}, 1),
    }


def _missing(snapshot: dict[str, Any]) -> list[str]:
    missing = []
    for section, keys in {
        "band": ("connected", "battery_percent", "charging"),
        "person": ("wearing", "sleeping", "activity_state"),
    }.items():
        for key in keys:
            if snapshot[section].get(key) is None:
                missing.append(f"{section}.{key}")
    for name in ("heart_rate", "blood_oxygen", "stress"):
        if snapshot["monitoring"].get(name, {}).get("enabled") is None:
            missing.append(f"monitoring.{name}.enabled")
    return missing


def _persist(settings: Any, snapshot: dict[str, Any]) -> None:
    path = Path(settings.db_path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.parent.chmod(0o700)
    conn = sqlite3.connect(path, timeout=10)
    path.chmod(0o600)
    configure_connection(conn)
    try:
        ensure_schema(conn)
        conn.commit()
        with conn:
            conn.execute(
                """INSERT INTO metadata(key,value,updated_at) VALUES(?,?,?)
                   ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at""",
                (SNAPSHOT_KEY, canonical_json(snapshot), snapshot["observed_at"]),
            )
    finally:
        conn.close()


def _load(settings: Any) -> dict[str, Any] | None:
    path = Path(settings.db_path)
    if not path.exists():
        return None
    try:
        conn = sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True, timeout=2)
        try:
            row = conn.execute("SELECT value FROM metadata WHERE key=?", (SNAPSHOT_KEY,)).fetchone()
        finally:
            conn.close()
        value = json.loads(row[0]) if row else None
        return value if isinstance(value, dict) else None
    except (sqlite3.Error, json.JSONDecodeError, TypeError):
        return None


def _age_from_stamp(stamp_value: Any) -> int | None:
    if not isinstance(stamp_value, str):
        return None
    try:
        stamp = datetime.fromisoformat(stamp_value.replace("Z", "+00:00"))
        if stamp.tzinfo is None:
            return None
        return int((datetime.now(timezone.utc) - stamp).total_seconds())
    except ValueError:
        return None


def _age(snapshot: dict[str, Any] | None) -> int | None:
    return None if not snapshot else _age_from_stamp(snapshot.get("observed_at"))


def _freshness_for(
    observed_at: Any, missing: list[str], max_age_seconds: int
) -> dict[str, Any]:
    age = _age_from_stamp(observed_at)
    in_window = age is not None and 0 <= age <= max_age_seconds
    if age is not None and age < 0:
        status = "future"
    elif in_window and not missing:
        status = "fresh"
    elif in_window:
        status = "partial"
    elif age is not None:
        status = "stale"
    else:
        status = "unknown"
    return {"status": status, "age_seconds": age, "missing": missing}


def _with_freshness(snapshot: dict[str, Any], max_age_seconds: int, *, cached: bool) -> dict[str, Any]:
    result = json.loads(json.dumps(snapshot))
    age = _age(snapshot)
    missing = _missing(result)
    fresh = age is not None and 0 <= age <= max_age_seconds
    result["source"] = "device_snapshot_cache" if cached else "xiaomi_band_live"
    if result.get("status") != "error":
        result["status"] = "ok" if not missing else "partial"
    overall_status = (
        "fresh" if fresh and not missing else "partial" if fresh else "stale" if age is not None else "unknown"
    )
    result["freshness"] = {
        "status": "future" if age is not None and age < 0 else overall_status,
        "age_seconds": age,
        "max_age_seconds": max_age_seconds,
        "missing": missing,
        "satisfied": fresh and not missing,
    }
    for section in ("band", "person"):
        section_value = result.get(section)
        if not isinstance(section_value, dict):
            continue
        section_missing = [item for item in missing if item.startswith(section + ".")]
        section_value["freshness"] = _freshness_for(
            section_value.get("observed_at"), section_missing, max_age_seconds
        )
    monitoring = result.get("monitoring")
    if isinstance(monitoring, dict):
        child_statuses = []
        child_ages = []
        for name in ("heart_rate", "blood_oxygen", "stress"):
            child = monitoring.get(name)
            if not isinstance(child, dict):
                continue
            child_missing = [item for item in missing if item.startswith(f"monitoring.{name}.")]
            child["freshness"] = _freshness_for(
                child.get("observed_at"), child_missing, max_age_seconds
            )
            child_statuses.append(child["freshness"]["status"])
            if child["freshness"]["age_seconds"] is not None:
                child_ages.append(child["freshness"]["age_seconds"])
        monitoring_missing = [item for item in missing if item.startswith("monitoring.")]
        if child_statuses and all(status == "fresh" for status in child_statuses):
            monitoring_status = "fresh"
        elif any(status in {"fresh", "partial"} for status in child_statuses):
            monitoring_status = "partial"
        elif any(status == "future" for status in child_statuses):
            monitoring_status = "future"
        elif any(status == "stale" for status in child_statuses):
            monitoring_status = "stale"
        else:
            monitoring_status = "unknown"
        monitoring["freshness"] = {
            "status": monitoring_status,
            "age_seconds": max(child_ages) if child_ages else None,
            "missing": monitoring_missing,
        }
    return result


def _live_snapshot(settings: Any, timeout_seconds: int) -> dict[str, Any]:
    observed = _now_iso()
    snapshot: dict[str, Any] = {
        "status": "partial",
        "observed_at": observed,
        "source": "xiaomi_band_live",
        "band": {"connected": True, "battery_percent": None, "charging": None,
                 "observed_at": observed, "source": "xiaomi_band_live"},
        "person": {"wearing": None, "sleeping": None, "activity_state": None,
                   "observed_at": None, "source": None},
        "monitoring": {
            "heart_rate": {"enabled": None, "interval_minutes": None,
                           "observed_at": None, "source": None},
            "blood_oxygen": {"enabled": None, "observed_at": None, "source": None},
            "stress": {"enabled": None, "observed_at": None, "source": None},
            "observed_at": None,
            "source": None,
        },
        "reconnect": {"attempted": False, "succeeded": False},
    }
    errors: list[str] = []
    with device_session(settings, timeout_seconds=timeout_seconds, retry_connect=True) as session:
        snapshot["reconnect"] = {
            "attempted": bool(session.connection.get("reconnect_attempted")),
            "succeeded": bool(session.connection.get("reconnect_succeeded")),
        }
        requests = [
            ("state", 2, 78, _decode_basic_state),
            ("battery", 2, 1, _decode_battery),
            ("heart_rate", 8, 10, _decode_heart_rate),
            ("blood_oxygen", 8, 8, _decode_spo2),
            ("stress", 8, 14, _decode_stress),
        ]
        for name, command_type, subtype, decoder in requests:
            try:
                response = session.request(
                    command_type, subtype, timeout_seconds=min(4, max(1, session._remaining() - 1))
                )
                decoded = decoder(response["response"])
                stamp = response["received_at"]
                snapshot["reconnect"]["attempted"] |= bool(response.get("reconnect_attempted"))
                snapshot["reconnect"]["succeeded"] |= bool(response.get("reconnect_succeeded"))
                if name == "state":
                    for key in ("battery_percent", "charging"):
                        if decoded[key] is not None:
                            snapshot["band"][key] = decoded[key]
                    if any(decoded[key] is not None for key in ("battery_percent", "charging")):
                        snapshot["band"].update({"observed_at": stamp, "source": "xiaomi_band_live"})
                    snapshot["person"].update({k: decoded[k] for k in ("wearing", "sleeping", "activity_state")})
                    snapshot["person"].update({"observed_at": stamp, "source": "xiaomi_band_live"})
                elif name == "battery":
                    for key, value in decoded.items():
                        if value is not None:
                            snapshot["band"][key] = value
                    if any(value is not None for value in decoded.values()):
                        snapshot["band"].update({"observed_at": stamp, "source": "xiaomi_band_live"})
                else:
                    snapshot["monitoring"][name] = {
                        **decoded, "observed_at": stamp, "source": "xiaomi_band_live"
                    }
                    snapshot["monitoring"].update({"observed_at": stamp, "source": "xiaomi_band_live"})
                snapshot["observed_at"] = stamp
            except (DeviceTransportError, ValueError) as exc:
                errors.append(f"{name}: {exc}")
        snapshot["errors"] = errors[:5]
        snapshot["status"] = "ok" if not _missing(snapshot) else "partial"
        session.finalize_transport()
        if session.cleanup_errors:
            raise DeviceTransportError(session.cleanup_errors[0])
        _persist(settings, snapshot)
    return snapshot


def get_device_snapshot(
    settings: Any,
    freshness: str = "prefer_fresh",
    max_age_seconds: int = 120,
    timeout_seconds: int = 30,
) -> dict[str, Any]:
    if freshness not in {"cached", "prefer_fresh", "require_fresh"}:
        raise ValueError("freshness must be cached, prefer_fresh, or require_fresh")
    if not 1 <= max_age_seconds <= 86400:
        raise ValueError("max_age_seconds must be 1..86400")
    cached = _load(settings)
    cached_result = _with_freshness(cached, max_age_seconds, cached=True) if cached else None
    if freshness == "cached":
        return cached_result or {
            "status": "no_data", "observed_at": None, "source": "device_snapshot_cache",
            "band": None, "person": None, "monitoring": None,
            "freshness": {"status": "unknown", "age_seconds": None,
                          "max_age_seconds": max_age_seconds, "missing": ["device_snapshot"]},
        }
    if cached_result:
        cached_age = cached_result["freshness"].get("age_seconds")
        cache_in_window = isinstance(cached_age, int) and 0 <= cached_age <= max_age_seconds
        if cache_in_window and (
            freshness == "prefer_fresh"
            or (freshness == "require_fresh" and cached_result["freshness"]["status"] == "fresh")
        ):
            return cached_result
    try:
        live = _live_snapshot(settings, timeout_seconds)
        result = _with_freshness(live, max_age_seconds, cached=False)
        if freshness == "require_fresh" and result["freshness"]["status"] != "fresh":
            result["status"] = "freshness_unmet"
            result["refresh_error"] = "required device snapshot is incomplete or stale"
        return result
    except (DeviceTransportError, OSError, sqlite3.Error) as exc:
        if cached_result:
            cached_result["refresh_error"] = str(exc)[:400]
            if freshness == "require_fresh":
                cached_result["status"] = "freshness_unmet"
            return cached_result
        return {
            "status": "freshness_unmet" if freshness == "require_fresh" else "error",
            "observed_at": None, "source": "device_snapshot_cache",
            "band": None, "person": None, "monitoring": None,
            "freshness": {"status": "unknown", "age_seconds": None,
                          "max_age_seconds": max_age_seconds, "missing": ["device_snapshot"]},
            "refresh_error": str(exc)[:400],
        }
