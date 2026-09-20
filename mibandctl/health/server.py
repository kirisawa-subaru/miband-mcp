"""MCP interface for the selected local health cache (stdio, including over SSH)."""
from __future__ import annotations

import asyncio
from contextlib import contextmanager
from datetime import datetime, timezone
import json
import time
import sqlite3
from typing import Any, Literal

from mcp.server import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import ToolAnnotations

from . import query, sync
from .settings import Settings
from .backend import modules


READ = ToolAnnotations(read_only_hint=True, destructive_hint=False, open_world_hint=False)
SYNC = ToolAnnotations(read_only_hint=False, destructive_hint=False, open_world_hint=True)
EDIT = ToolAnnotations(read_only_hint=False, destructive_hint=True, open_world_hint=True)


@contextmanager
def _tool_errors():
    """Expose actionable input/storage failures as expected MCP tool errors."""
    try:
        yield
    except (ValueError, OSError, sqlite3.Error, RuntimeError) as exc:
        raise ToolError(str(exc)) from exc


DEVICE_PROFILE = {
    "device": "Xiaomi Smart Band 10",
    "transport": "Xiaomi Health on the configured Android phone; background internal APIs over SSH",
    "tools": {
        "get_band_status": "Connection, battery, charging and monitoring configuration",
        "get_health_status": "Health records plus band-reported wearing, sleep and activity state",
        "measure_heart_rate": "One live heart-rate reading, followed by STOP",
        "get_band_schedule": "Read alarms and reminders",
        "set_band_alarm": "Create or update one alarm and verify by reading it back",
        "set_band_reminder": "Create or update one reminder and verify by reading it back",
        "delete_band_schedule": "Delete one identified alarm or reminder and verify absence",
    },
    "limits": [
        "Missing or unsupported device fields are unknown, never false or zero.",
        "Sleep and activity state are device classifications, not conclusions about the user's intent or mood.",
        "Only heart-rate one-shot measurement is implemented; no one-shot SpO2 or stress measurement.",
        "Monitoring settings are read-only; these tools do not change sampling or power settings.",
        "Remote operations perform bounded automatic reconnect; cached queries do not contact the phone.",
        "An unconfirmed schedule write must be reconciled by reading back, not blindly recreated.",
    ],
}


def _device_snapshot(settings: Settings, **kwargs: Any) -> dict[str, Any]:
    from .device import get_device_snapshot
    return get_device_snapshot(settings, **kwargs)


def _validate_status_request(freshness: str, max_age_seconds: int, timeout_seconds: int) -> None:
    if freshness not in {"cached", "prefer_fresh", "require_fresh"}:
        raise ValueError("freshness must be cached, prefer_fresh, or require_fresh")
    if not 1 <= max_age_seconds <= 86400:
        raise ValueError("max_age_seconds must be 1..86400")
    if not 10 <= timeout_seconds <= 90:
        raise ValueError("timeout_seconds must be 10..90")


async def band_status(
    settings: Settings, freshness: str = "prefer_fresh", max_age_seconds: int = 120,
    timeout_seconds: int = 30,
) -> dict[str, Any]:
    _validate_status_request(freshness, max_age_seconds, timeout_seconds)
    snapshot = await asyncio.to_thread(
        _device_snapshot, settings, freshness=freshness,
        max_age_seconds=max_age_seconds, timeout_seconds=timeout_seconds,
    )
    result = {
        key: value for key, value in snapshot.items() if key not in {"person"}
    }
    # This tool's freshness is about the band and monitoring configuration;
    # unsupported activity classification belongs to get_health_status.
    if isinstance(snapshot.get("band"), dict) and isinstance(snapshot.get("monitoring"), dict):
        details = dict(snapshot.get("freshness") or {})
        details["missing"] = [
            name for name in details.get("missing", []) if not name.startswith("person.")
        ]
        groups = [snapshot[section].get("freshness") or {} for section in ("band", "monitoring")]
        group_fresh = all(group.get("status") == "fresh" for group in groups)
        details["satisfied"] = group_fresh and not details["missing"]
        if details["satisfied"]:
            details["status"] = "fresh"
            if result.get("status") == "partial":
                result["status"] = "ok"
        if freshness == "require_fresh" and not details["satisfied"]:
            result["status"] = "freshness_unmet"
        result["freshness"] = details
    return result


async def health_status(
    settings: Settings, freshness: str = "prefer_fresh", max_age_seconds: int = 900,
    timeout_seconds: int = 60,
) -> dict[str, Any]:
    _validate_status_request(freshness, max_age_seconds, timeout_seconds)
    deadline = time.monotonic() + timeout_seconds
    snapshot = await asyncio.to_thread(
        _device_snapshot, settings, freshness=freshness,
        max_age_seconds=min(max_age_seconds, 120),
        timeout_seconds=min(timeout_seconds, 30),
    )
    remaining = int(deadline - time.monotonic())
    history_policy = freshness if remaining >= 1 else "cached"
    result = await current_state(
        settings, history_policy, max_age_seconds, max(1, min(30, remaining)),
    )
    person = snapshot.get("person") or {}
    snapshot_freshness = dict(person.get("freshness") or snapshot.get("freshness") or {})
    observed_at = person.get("observed_at", snapshot.get("observed_at"))
    person_source = person.get("source", snapshot.get("source"))
    if isinstance(observed_at, str):
        try:
            observed = datetime.fromisoformat(observed_at)
            if observed.tzinfo is None:
                raise ValueError("observation must have a timezone")
            age = (datetime.now(timezone.utc) - observed).total_seconds()
            snapshot_freshness.update(
                age_seconds=max(0, int(age)),
                status="future" if age < 0 else "fresh" if age <= min(max_age_seconds, 120) else "stale",
            )
        except (TypeError, ValueError):
            snapshot_freshness.update(age_seconds=None, status="unknown")
    for name, field in (("wearing", "wearing"), ("sleep_state", "sleeping"),
                        ("activity_state", "activity_state")):
        value = person.get(field)
        result["data"][name] = {
            "value": value,
            "source": person_source,
            "observed_at": observed_at,
            "age_seconds": snapshot_freshness.get("age_seconds"),
            "status": snapshot_freshness.get("status", "unknown") if value is not None else "unknown",
        }
    result["device_state"] = {
        key: value for key, value in snapshot.items() if key not in {"band", "person", "monitoring"}
    }
    result["device_state"]["freshness"] = snapshot_freshness
    health_fresh = result["freshness"].get("satisfied") is True
    missing_state = [field for field in ("wearing", "sleeping", "activity_state") if person.get(field) is None]
    device_fresh = snapshot_freshness.get("status") == "fresh" and not missing_state
    result["freshness"]["health_satisfied"] = health_fresh
    result["freshness"]["device_state_satisfied"] = device_fresh
    result["freshness"]["missing_device_state"] = missing_state
    result["freshness"]["satisfied"] = health_fresh and device_fresh
    result["freshness_policy"] = freshness
    if freshness == "require_fresh" and not result["freshness"]["satisfied"]:
        result["status"] = "freshness_unmet"
    elif snapshot.get("status") in {"error", "in_progress", "no_data"} and result["status"] == "ok":
        result["status"] = "partial"
    if result["status"] == "no_data" and any(person.get(field) is not None for field in ("wearing", "sleeping", "activity_state")):
        result["status"] = "partial"
    meta = result.setdefault("meta", {})
    meta["device_state_observed_at"] = observed_at
    meta["missing_device_state"] = missing_state
    return result


def _recent_success(status: dict, seconds: int = 60) -> bool:
    stamp = status.get("last_success_at")
    if not stamp:
        return False
    try:
        age = (datetime.now(timezone.utc) - datetime.fromisoformat(stamp)).total_seconds()
        return 0 <= age < seconds
    except (ValueError, TypeError):
        return False


async def current_state(
    settings: Settings,
    freshness: Literal["cached", "prefer_fresh", "require_fresh"] = "prefer_fresh",
    max_age_seconds: int = 900,
    wait_seconds: int = 10,
) -> dict[str, Any]:
    if freshness not in {"cached", "prefer_fresh", "require_fresh"}:
        raise ValueError("freshness must be cached, prefer_fresh, or require_fresh")
    if not 1 <= wait_seconds <= 30:
        raise ValueError("wait_seconds must be 1..30")
    if not 1 <= max_age_seconds <= 86400:
        raise ValueError("max_age_seconds must be 1..86400")
    query, sync = modules(settings)
    result = query.get_current_state(settings, max_age_seconds=max_age_seconds)
    sync_result = None
    if freshness != "cached" and not result["freshness"]["satisfied"]:
        previous = sync.read_sync_status(settings)
        if _recent_success(previous):
            sync_result = {"status": "recently_synced", "last_success_at": previous["last_success_at"]}
        else:
            deadline = time.monotonic() + wait_seconds
            sync_result = await asyncio.to_thread(
                sync.sync_health, settings, timeout_seconds=wait_seconds
            )
            # Another process owns the single writer: wait for its committed result.
            if sync_result.get("status") == "in_progress":
                initial_success = previous.get("last_success_at")
                while time.monotonic() < deadline:
                    await asyncio.sleep(min(0.25, max(0, deadline - time.monotonic())))
                    status = sync.read_sync_status(settings)
                    if status.get("last_success_at") != initial_success:
                        sync_result = status
                        break
                    if status.get("status") == "error":
                        sync_result = status
                        break
        result = query.get_current_state(settings, max_age_seconds=max_age_seconds)
    result["freshness_policy"] = freshness
    result["sync"] = sync_result
    if freshness == "require_fresh" and not result["freshness"]["satisfied"]:
        result["status"] = "freshness_unmet"
    return result


def _instructions(settings: Settings) -> str:
    if settings.backend == "gadgetbridge":
        return (
            "Mi Band health records from a local Gadgetbridge database export. "
            "Use get_current_state for recent heart rate, steps, battery and historical sessions, "
            "get_daily_report for a daily summary, and query_health for bounded detail. "
            "cached queries do not contact the phone. prefer_fresh may export and pull phone data; "
            "sync_health(refresh_app=true) first requests recorded-data synchronization from the band. "
            "This backend exposes no live measurement, wearing-state or schedule-control tools. "
            "Observation time differs from export/pull time; missing records are unknown, not zero. "
            "Stale heart rate has value=null and preserves latest_value with its observation time. "
            "Sleep belongs to its waking day. Data alone does not establish mood or a diagnosis. "
            f"Default timezone: {settings.timezone}. Device scope and supported metrics are in health://device-profile."
        )
    return (
            "Xiaomi Smart Band 10 records and device controls through Xiaomi Health on the configured phone. "
            "Use get_health_status for conversation (HR, sleep history, wearing/sleep/activity state); "
            "get_band_status for connection, battery, charging and read-only monitoring settings; "
            "get_daily_report for a daily digest, query_health for detail, and measure_heart_rate to request a live reading. Times default to "
            "Asia/Shanghai unless specified. Observation time is distinct from pull time. Missing "
            "records are unknown, not zero; old HR is not current HR. Sleep is assigned to wake day. "
            "Data alone does not establish mood or a diagnosis. Queries use a local cache. "
            "sync_health(refresh_app=true) requests device synchronization through the running app’s internal API without UI interaction. "
            "Remote queries automatically reconnect and retry within their deadline; no separate reconnect or capability tool is needed. "
            "Use get_band_schedule/set_band_alarm/set_band_reminder/delete_band_schedule for wrist reminders. "
            "Writes require read-back confirmation; if the outcome is unknown, inspect the schedule before attempting another create. "
            "Only HR has a one-shot measurement tool. Missing wearing/sleep/activity fields are unknown, not false. "
            "Fixed device scope and limitations are also available at health://device-profile."
        )


def create_server(settings: Settings | None = None) -> MCPServer:
    settings = settings or Settings.from_env()
    query, sync = modules(settings)
    server = MCPServer(
        "miband-health",
        version="0.2.0",
        instructions=_instructions(settings),
        log_level="WARNING",
    )

    if settings.backend == "xiaomi_health":
        @server.tool(annotations=SYNC, structured_output=True)
        async def get_band_status(
            freshness: Literal["cached", "prefer_fresh", "require_fresh"] = "prefer_fresh",
            max_age_seconds: int = 120, timeout_seconds: int = 30,
        ) -> dict[str, Any]:
            """Band connection, battery, charging and heart-rate/SpO2/stress monitoring settings.

            Monitoring configuration is read-only. Remote reads reconnect automatically within
            the timeout; cached never contacts the phone. Unknown fields remain null.
            max_age_seconds: 1..86400; timeout_seconds: 10..90. No app UI interaction.
            """
            with _tool_errors():
                return await band_status(settings, freshness, max_age_seconds, timeout_seconds)

    if settings.backend == "xiaomi_health":
        @server.tool(annotations=SYNC, structured_output=True)
        async def get_health_status(
            freshness: Literal["cached", "prefer_fresh", "require_fresh"] = "prefer_fresh",
            max_age_seconds: int = 900, timeout_seconds: int = 60,
        ) -> dict[str, Any]:
            """Recent HR, steps, sleep/workout history, wearing, sleep and activity state.

            Each device-state observation has its own age/source. Current sleep_state is the
            band's classification, distinct from latest_sleep (a historical sleep session).
            cached uses local data only; remote reads reconnect automatically. This does not
            start a new measurement or change monitoring settings. max_age_seconds: 1..86400;
            device-state age is capped at 120s. timeout_seconds: 10..90.
            """
            with _tool_errors():
                return await health_status(settings, freshness, max_age_seconds, timeout_seconds)

    @server.tool(annotations=SYNC, structured_output=True)
    async def get_current_state(
        freshness: Literal["cached", "prefer_fresh", "require_fresh"] = "prefer_fresh",
        max_age_seconds: int = 900,
        wait_seconds: int = 10,
    ) -> dict[str, Any]:
        """Recent recorded heart rate, steps and historical sleep/workouts; Gadgetbridge adds battery.

        cached returns immediately. prefer_fresh attempts an ordinary backend pull if HR is old;
        require_fresh returns freshness_unmet if the requested age cannot be met. No app UI
        interaction. A successful recent pull is reused for 60 seconds even when source data
        remains stale. wait_seconds: 1..30; max_age_seconds: 1..86400.
        """
        with _tool_errors():
            return await current_state(settings, freshness, max_age_seconds, wait_seconds)

    @server.tool(annotations=READ, structured_output=True)
    def get_daily_report(date: str, timezone: str | None = None, compare_days: int = 7) -> dict[str, Any]:
        """Cached natural-day summary (YYYY-MM-DD), with sleep assigned to the waking day.

        Includes sample coverage and optional preceding-day comparisons (missing days excluded).
        For a morning digest, query yesterday's activity and today's waking sleep separately.
        Call sync_health first if needed. timezone is an IANA name, default Asia/Shanghai.
        """
        with _tool_errors():
            return query.get_daily_report(settings, date=date, timezone=timezone, compare_days=compare_days)

    @server.tool(annotations=READ, structured_output=True)
    def query_health(
        kind: Literal["timeseries", "sleep", "workouts"],
        start: str | None = None,
        end: str | None = None,
        metric: str | None = None,
        aggregation_minutes: int = 60,
        limit: int = 200,
        offset: int = 0,
        session_id: str | None = None,
        timezone: str | None = None,
    ) -> dict[str, Any]:
        """Bounded cached detail: metric buckets, sleep sessions, or workouts.

        Supply both start (inclusive) and end (exclusive) as offset-aware ISO timestamps,
        or neither for the last 24 hours (timeseries) / 30 days (sessions).
        limit 1..500; use returned pagination for more.
        Session IDs from summaries select a session. Missing buckets remain missing.
        Common metrics: heart_rate.bpm (default), steps, calories, distance.
        Additional metrics depend on the configured backend; see health://device-profile.
        aggregation_minutes: 1..1440. No arbitrary SQL.
        """
        with _tool_errors():
            return query.query_health(
                settings, kind=kind, start=start, end=end, metric=metric,
                aggregation_minutes=aggregation_minutes, limit=limit, offset=offset,
                session_id=session_id, timezone=timezone,
            )

    @server.tool(annotations=SYNC, structured_output=True)
    async def sync_health(refresh_app: bool = False, timeout_seconds: int = 60) -> dict[str, Any]:
        """Update the selected local cache and report actual record-time advancement.

        Gadgetbridge exports and pulls its database over ADB; refresh_app=true first requests
        recorded-data synchronization from the band and waits for completion. Xiaomi Health
        reads through SSH; refresh_app=true invokes its internal background synchronization.
        No screen interaction or new physiological measurement is requested. Configure the
        transport once; cached queries still work when the phone is unavailable.
        Concurrent syncs share a single writer; timeout_seconds: 1..180.
        """
        with _tool_errors():
            if not 1 <= timeout_seconds <= 180:
                raise ValueError("timeout_seconds must be 1..180")
            return await asyncio.to_thread(
                sync.sync_health, settings, refresh_app=refresh_app, timeout_seconds=timeout_seconds
            )

    if settings.backend == "xiaomi_health":
        @server.tool(annotations=SYNC, structured_output=True)
        async def measure_heart_rate(timeout_seconds: int = 60) -> dict[str, Any]:
            """Request one live heart-rate reading from the connected, worn Mi Band, then stop.

            Uses the app's authenticated Bluetooth connection, not the historical database.
            Reports received_at (host receipt time), start/stop acknowledgments and cleanup errors.
            No reading may be available if the band is not worn or disconnected. Timeout: 30..90s.
            Current-state queries prefer a newer successful live reading; daily aggregates remain
            based on recorded history. Always attempts to stop; check stop_confirmed rather than assuming success.
            Requires Xiaomi Health to be running with its band connection available.
            """
            from .measurement import measure_heart_rate as measure
            with _tool_errors():
                if not 30 <= timeout_seconds <= 90:
                    raise ValueError("timeout_seconds must be 30..90")
                return await asyncio.to_thread(measure, settings, timeout_seconds=timeout_seconds)

    if settings.backend == "xiaomi_health":
        @server.tool(annotations=SYNC, structured_output=True)
        async def get_band_schedule(
            kind: Literal["all", "alarms", "reminders"] = "all",
            timezone: str | None = None, timeout_seconds: int = 30,
        ) -> dict[str, Any]:
            """Read actual band alarms/reminders, their IDs, repeat rules and device capacity.

            Reconnect is automatic and bounded. Alarm time is the band's local wall clock;
            reminder timestamps are converted from UTC for display in the selected IANA timezone.
            timeout_seconds: 5..90. No data is invented if a device response is unavailable. Read again to reconcile an
            earlier write whose outcome was unknown. timezone defaults to the server setting.
            """
            from .schedules import get_band_schedule as read
            with _tool_errors():
                return await asyncio.to_thread(
                    read, settings, kind=kind, timezone=timezone, timeout_seconds=timeout_seconds,
                )

    if settings.backend == "xiaomi_health":
        @server.tool(annotations=EDIT, structured_output=True)
        async def set_band_alarm(
            time: str, weekdays: list[int], enabled: bool = True, alarm_id: int | None = None,
            timezone: str | None = None, timeout_seconds: int = 30,
        ) -> dict[str, Any]:
            """Create or update one band alarm, preserving other alarms and verifying by read-back.

            time is HH:MM in the band's local wall clock. weekdays uses ISO Monday=1..Sunday=7;
            [] means once, all seven means daily. Omit alarm_id to create; provide a listed ID to
            update that existing alarm. Supply the complete desired time/weekdays/enabled values.
            timezone labels the wall clock; it does not change the band's timezone. A lost write
            response is reconciled by reading back, never by blindly repeating the write.
            timeout_seconds: 5..90.
            """
            from .schedules import set_band_alarm as write
            with _tool_errors():
                return await asyncio.to_thread(
                    write, settings, time=time, weekdays=weekdays, enabled=enabled, alarm_id=alarm_id,
                    timezone=timezone, timeout_seconds=timeout_seconds,
                )

    if settings.backend == "xiaomi_health":
        @server.tool(annotations=EDIT, structured_output=True)
        async def set_band_reminder(
            at: str, title: str,
            repeat: Literal["once", "daily", "weekly", "monthly", "yearly"] = "once",
            reminder_id: int | None = None, timezone: str | None = None, timeout_seconds: int = 30,
        ) -> dict[str, Any]:
            """Create or update one wrist reminder, then verify its actual stored fields.

            at is an ISO timestamp with UTC offset; wire dates/times are UTC. title is the text
            shown on the band. Omit reminder_id to create, or use a listed ID to update. Supply
            all desired fields; unrelated entries are preserved. Recurrence follows the band's
            UTC schedule, so it does not automatically track local daylight-saving changes.
            On an unknown result inspect get_band_schedule before attempting another create.
            timeout_seconds: 5..90.
            """
            from .schedules import set_band_reminder as write
            with _tool_errors():
                return await asyncio.to_thread(
                    write, settings, at=at, title=title, repeat=repeat, reminder_id=reminder_id,
                    timezone=timezone, timeout_seconds=timeout_seconds,
                )

    if settings.backend == "xiaomi_health":
        @server.tool(annotations=EDIT, structured_output=True)
        async def delete_band_schedule(
            kind: Literal["alarm", "reminder"], item_id: int,
            timezone: str | None = None, timeout_seconds: int = 30,
        ) -> dict[str, Any]:
            """Delete only the identified band alarm/reminder and verify its absence by read-back.

            Obtain item_id from get_band_schedule. Other entries remain unchanged. Unknown write
            outcomes are reported explicitly; a transport acknowledgment alone is not success.
            timeout_seconds: 5..90.
            """
            from .schedules import delete_band_schedule as delete
            with _tool_errors():
                return await asyncio.to_thread(
                    delete, settings, kind=kind, item_id=item_id,
                    timezone=timezone, timeout_seconds=timeout_seconds,
                )

    @server.resource("health://status")
    def health_status() -> str:
        """Last sync result without contacting the phone."""
        return json.dumps(sync.read_sync_status(settings), ensure_ascii=False)

    @server.resource("health://device-profile")
    def device_profile() -> str:
        """Fixed band capabilities and tool boundaries; no device discovery call required."""
        if settings.backend == "gadgetbridge":
            return json.dumps({
                "backend": "gadgetbridge",
                "transport": "Gadgetbridge exported SQLite; optional unprivileged ADB synchronization",
                "timezone": settings.timezone,
                "device_id": settings.gadgetbridge_device_id,
                "metrics": list(query.TIMESERIES_METRICS),
                "tools": ["get_current_state", "get_daily_report", "query_health", "sync_health"],
                "limits": [
                    "Battery is an exported historical observation, not a live connection check.",
                    "No live measurement, wearing/sleep/activity classification or schedule controls.",
                    "Exported records do not imply complete history or current physiological state.",
                ],
            }, ensure_ascii=False)
        return json.dumps({**DEVICE_PROFILE, "backend": settings.backend,
                           "timezone": settings.timezone,
                           "metrics": list(query.TIMESERIES_METRICS)}, ensure_ascii=False)

    return server


def main() -> None:
    create_server().run(transport="stdio")


if __name__ == "__main__":
    main()
