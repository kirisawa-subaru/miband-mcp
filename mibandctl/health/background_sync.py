"""Background Xiaomi Health device synchronization through its native API."""

from __future__ import annotations

import threading
import time
from pathlib import Path
from typing import Any

from .measurement import (
    STOP_GRACE_SECONDS,
    _PhoneFridaRuntime,
    _bounded_error,
    _frida_cancellable_until,
    _health_pid,
    _now_iso,
)


MAX_ACTIVE_SECONDS = 60.0


class _SyncCollector:
    def __init__(self) -> None:
        self.finished = threading.Event()
        self.cleanup_done = threading.Event()
        self.requested = False
        self.start_seen = False
        self.finish_code: int | None = None
        self.last_progress: int | None = None
        self.completed_at: str | None = None
        self.last_sync_time_before: int | None = None
        self.last_sync_time_after: int | None = None
        self.hook_restored = False
        self.reconnect_attempted = False
        self.reconnect_succeeded = False
        self.errors: list[str] = []

    def _error(self, value: Any) -> None:
        if len(self.errors) < 4:
            self.errors.append(_bounded_error(str(value)))

    def handle(self, message: dict[str, Any], _data: bytes | None = None) -> None:
        if message.get("type") == "error":
            self._error(message.get("stack") or message.get("description") or "sync script error")
            self.finished.set()
            return
        if message.get("type") != "send" or not isinstance(message.get("payload"), dict):
            return
        payload = message["payload"]
        phase = payload.get("phase")
        if phase == "reconnect_start":
            self.reconnect_attempted = True
        elif phase == "reconnect_success":
            self.reconnect_attempted = True
            self.reconnect_succeeded = True
        elif phase == "reconnect_error":
            self.reconnect_attempted = True
            self._error(payload.get("error") or "wearable reconnect failed")
        elif phase == "request":
            self.requested = True
            value = payload.get("last_sync_time_before")
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                self.last_sync_time_before = int(value)
        elif phase == "start":
            self.start_seen = True
        elif phase == "progress":
            value = payload.get("value")
            if isinstance(value, int) and not isinstance(value, bool):
                self.last_progress = value
        elif phase == "finish":
            code = payload.get("code")
            if isinstance(code, int) and not isinstance(code, bool):
                self.finish_code = code
            value = payload.get("last_sync_time_after")
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                self.last_sync_time_after = int(value)
            self.completed_at = _now_iso()
            if not self.start_seen:
                self._error("ignored sync finish that did not follow this request's start")
            self.finished.set()
        elif phase == "cleanup_done":
            self.hook_restored = payload.get("hook_restored") is True
            self.cleanup_done.set()
        elif phase in {"busy", "not_connected", "manager_missing", "setup_error", "cancelled"}:
            self._error(payload.get("error") or phase)
            self.finished.set()


def _script_source() -> str:
    return Path(__file__).with_name("sync_device.js").read_text(encoding="utf-8")


def _run_sync_script(
    device: Any,
    app_pid: int,
    deadline: float,
    *,
    cleanup_seconds: float = STOP_GRACE_SECONDS,
) -> _SyncCollector:
    collector = _SyncCollector()
    session = None
    script = None
    script_loaded = False
    timed_out = False
    active_deadline = max(time.monotonic() + 0.1, deadline - cleanup_seconds)
    try:
        with _frida_cancellable_until(active_deadline):
            session = device.attach(app_pid)
            script = session.create_script(_script_source())
            script.on("message", collector.handle)
            script.load()
            script_loaded = True
            script.exports_sync.begin()
            if not collector.finished.wait(max(0.1, active_deadline - time.monotonic())):
                timed_out = True
    except Exception as exc:
        collector._error(f"background sync transport failed: {exc}")
    finally:
        cleanup_deadline = time.monotonic() + max(0.1, cleanup_seconds)
        if script_loaded:
            try:
                with _frida_cancellable_until(cleanup_deadline):
                    script.exports_sync.cleanup()
            except Exception as exc:
                collector._error(f"sync listener cleanup failed: {exc}")
            collector.cleanup_done.wait(max(0.0, cleanup_deadline - time.monotonic()))
        if not collector.cleanup_done.is_set() or not collector.hook_restored:
            collector._error("sync callback hook restoration was not confirmed")
        if timed_out:
            collector._error("timed out waiting for Xiaomi Health device sync completion")
        if script is not None:
            try:
                with _frida_cancellable_until(cleanup_deadline):
                    script.unload()
            except Exception as exc:
                collector._error(f"sync script unload failed: {exc}")
        if session is not None:
            try:
                with _frida_cancellable_until(cleanup_deadline):
                    session.detach()
            except Exception as exc:
                collector._error(f"sync Frida detach failed: {exc}")
    return collector


def _sync_via_frida(host: str, app_pid: int, deadline: float) -> dict[str, Any]:
    runtime = _PhoneFridaRuntime(host, deadline)
    collector: _SyncCollector | None = None
    transport_error: str | None = None
    try:
        with runtime:
            import frida  # type: ignore[import-not-found]

            manager = frida.get_device_manager()
            remote_added = False
            try:
                with _frida_cancellable_until(deadline - STOP_GRACE_SECONDS):
                    device = manager.add_remote_device(runtime.address)
                remote_added = True
                collector = _run_sync_script(device, app_pid, deadline)
            finally:
                if remote_added:
                    try:
                        with _frida_cancellable_until(time.monotonic() + 3):
                            manager.remove_remote_device(runtime.address)
                    except Exception as exc:
                        runtime.cleanup_errors.append(
                            _bounded_error(f"Frida remote-device cleanup failed: {exc}")
                        )
    except Exception as exc:
        transport_error = _bounded_error(exc)
    if collector is None:
        collector = _SyncCollector()
        collector._error(transport_error or "background sync script did not start")
    return {
        "requested": collector.requested,
        "start_seen": collector.start_seen,
        "finish_code": collector.finish_code,
        "completed_at": collector.completed_at,
        "last_progress": collector.last_progress,
        "last_sync_time_before": collector.last_sync_time_before,
        "last_sync_time_after": collector.last_sync_time_after,
        "hook_restored": collector.hook_restored,
        "errors": collector.errors,
        "cleanup_errors": runtime.cleanup_errors,
        "reconnect_attempted": collector.reconnect_attempted,
        "reconnect_succeeded": collector.reconnect_succeeded,
    }


def sync_device_in_background(host: str, *, timeout_seconds: float) -> dict[str, Any]:
    """Run one explicit full device sync without foregrounding Xiaomi Health."""
    if timeout_seconds < 10:
        raise TimeoutError("Background device sync needs at least 10 seconds")
    started = time.monotonic()
    requested_at = _now_iso()
    deadline = started + min(float(timeout_seconds), MAX_ACTIVE_SECONDS)
    app_pid = _health_pid(host, timeout_seconds=min(10, max(1, deadline - time.monotonic())))
    if app_pid is None:
        raise RuntimeError(
            "Xiaomi Health background process is not running; start its background process first"
        )
    result = _sync_via_frida(host, app_pid, deadline)
    errors = [str(value) for value in result.pop("errors", []) if value]
    cleanup_errors = [str(value) for value in result.get("cleanup_errors", []) if value]
    confirmed = (
        result.get("requested") is True
        and result.get("start_seen") is True
        and result.get("finish_code") == 0
        and result.get("hook_restored") is True
        and not errors
        and not cleanup_errors
    )
    if not confirmed:
        if errors:
            detail = errors[0]
        elif cleanup_errors:
            detail = cleanup_errors[0]
        elif result.get("requested") is not True:
            detail = "background device sync request was not sent"
        elif result.get("start_seen") is not True:
            detail = "background device sync start was not observed"
        elif result.get("finish_code") != 0:
            detail = f"background device sync finished with code {result.get('finish_code')}"
        else:
            detail = "background device sync callback hook restoration was not confirmed"
        raise RuntimeError(_bounded_error(detail))
    return {
        "requested": True,
        "confirmed": True,
        "mode": "background_device_api",
        "ui_interaction": False,
        "confirmation_basis": "SyncObservers.onFinish",
        "completion_code": 0,
        "sync_requested_at": requested_at,
        "completed_at": result["completed_at"],
        "last_progress": result["last_progress"],
        "last_sync_time_before": result["last_sync_time_before"],
        "last_sync_time_after": result["last_sync_time_after"],
        "hook_restored": True,
        "cleanup_errors": [],
        "reconnect": {
            "attempted": bool(result.get("reconnect_attempted")),
            "succeeded": bool(result.get("reconnect_succeeded")),
        },
        "elapsed_seconds": round(time.monotonic() - started, 3),
    }
