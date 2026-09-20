"""Bounded Gadgetbridge database synchronization over an authorized ADB link.

The phone remains the owner of Gadgetbridge's live database.  This module asks
Gadgetbridge to export a consistent copy, waits for the export completion event,
then validates a staged pull before atomically replacing the local read cache.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import queue
import re
import shlex
import sqlite3
import subprocess
import tempfile
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable


ACTIVITY_SYNC = "nodomain.freeyourgadget.gadgetbridge.command.ACTIVITY_SYNC"
DATABASE_EXPORT = "nodomain.freeyourgadget.gadgetbridge.command.TRIGGER_DATABASE_EXPORT"

SNAPSHOT_TABLES = {
    "BASE_ACTIVITY_SUMMARY",
    "XIAOMI_ACTIVITY_SAMPLE",
    "XIAOMI_DAILY_SUMMARY_SAMPLE",
    "XIAOMI_MANUAL_SAMPLE",
    "XIAOMI_SLEEP_TIME_SAMPLE",
    "XIAOMI_SLEEP_STAGE_SAMPLE",
    "BATTERY_LEVEL",
}

_PACKAGE_RE = re.compile(r"[A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)+")
_SERIAL_RE = re.compile(r"[A-Za-z0-9._:-]+")
_SHA256_RE = re.compile(r"[0-9a-fA-F]{64}")
_MAX_ERROR = 800


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _bounded_error(exc: BaseException) -> str:
    return " ".join(f"{type(exc).__name__}: {exc}".split())[:_MAX_ERROR]


def _status_path(settings: Any) -> Path:
    return Path(settings.data_dir) / "sync-state.json"


def _lock_path(settings: Any) -> Path:
    return Path(settings.data_dir) / "sync.lock"


def _canonical_db_path(settings: Any) -> Path:
    return Path(settings.db_path).expanduser().resolve()


def _cache_identity(path: Path, sha256: str | None = None) -> dict[str, Any] | None:
    try:
        stat = path.stat()
    except FileNotFoundError:
        return None
    identity: dict[str, Any] = {
        "dev": int(stat.st_dev),
        "ino": int(stat.st_ino),
        "size": int(stat.st_size),
        "mtime_ns": int(stat.st_mtime_ns),
    }
    if sha256 is not None:
        identity["sha256"] = sha256
    return identity


def _identity_matches(stored: Any, current: dict[str, Any] | None) -> bool:
    if not isinstance(stored, dict) or current is None:
        return False
    return all(stored.get(key) == current.get(key) for key in ("dev", "ino", "size", "mtime_ns"))


def _normalize_status(settings: Any, state: dict[str, Any]) -> dict[str, Any]:
    if not state:
        return state
    db_path = _canonical_db_path(settings)
    stored_path = state.get("db_path")
    stored_identity = state.get("cache_identity")
    current_identity = _cache_identity(db_path)
    path_matches = stored_path == str(db_path)
    identity_matches = path_matches and _identity_matches(stored_identity, current_identity)
    state = {**state, "cache_identity_matches": identity_matches}
    if identity_matches:
        return state
    had_binding = isinstance(stored_path, str) and isinstance(stored_identity, dict)
    state.update(
        status="cache_changed" if had_binding else "unknown",
        last_success_at=None,
        last_pulled_at=None,
        cache_changed=True,
        current_db_path=str(db_path),
        current_cache_identity=current_identity,
    )
    return state


def _load_status(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _write_status(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.parent.chmod(0o700)
    fd, name = tempfile.mkstemp(prefix=".sync-state-", suffix=".json", dir=path.parent)
    tmp = Path(name)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(value, handle, ensure_ascii=False, separators=(",", ":"))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
        path.chmod(0o600)
    finally:
        tmp.unlink(missing_ok=True)


def _lock_is_held(path: Path) -> bool:
    if not path.exists():
        return False
    try:
        with path.open("a+") as handle:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return True
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    except OSError:
        return False
    return False


def read_sync_status(settings: Any) -> dict[str, Any]:
    """Read persisted synchronization state without contacting the phone."""
    state = _normalize_status(settings, _load_status(_status_path(settings)))
    if not state:
        state = {
            "status": "never_synced",
            "last_attempt_at": None,
            "last_success_at": None,
            "last_pulled_at": None,
            "last_error": None,
        }
    locked = _lock_is_held(_lock_path(settings))
    if locked:
        state = {**state, "status": "in_progress"}
    elif state.get("status") == "in_progress":
        state = {
            **state,
            "status": "error",
            "last_error": "previous Gadgetbridge sync ended before completion",
        }
    for key in ("last_attempt_at", "last_success_at", "last_pulled_at", "last_error"):
        state.setdefault(key, None)
    return state


def _remaining(deadline: float, *, reserve: float = 0.0) -> float:
    value = deadline - time.monotonic() - reserve
    if value <= 0:
        raise TimeoutError("Gadgetbridge sync timed out")
    return value


def _validate_settings(settings: Any) -> tuple[str, str, str]:
    adb_path = str(getattr(settings, "adb_path", "adb"))
    package = str(getattr(settings, "gadgetbridge_package", ""))
    remote = getattr(settings, "gadgetbridge_remote_db", None)
    if not adb_path or "\x00" in adb_path:
        raise ValueError("invalid adb_path")
    if not _PACKAGE_RE.fullmatch(package):
        raise ValueError("invalid Gadgetbridge package name")
    if not isinstance(remote, str) or not remote.startswith("/") or "\x00" in remote:
        raise ValueError("gadgetbridge_remote_db must be an absolute Android path")
    if any(ch in remote for ch in "\r\n;&|`$<>"):
        raise ValueError("invalid gadgetbridge_remote_db")
    serial = getattr(settings, "adb_serial", None)
    if serial is not None and not _SERIAL_RE.fullmatch(str(serial)):
        raise ValueError("invalid adb_serial")
    return adb_path, package, remote


class _Adb:
    def __init__(self, executable: str, serial: str | None = None):
        self.executable = executable
        self.serial = serial

    def argv(self, *args: str) -> list[str]:
        base = [self.executable]
        if self.serial is not None:
            base += ["-s", self.serial]
        return base + list(args)

    def run(self, *args: str, deadline: float, check: bool = True) -> subprocess.CompletedProcess[str]:
        try:
            result = subprocess.run(
                self.argv(*args),
                text=True,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=max(0.05, _remaining(deadline, reserve=0.15)),
                check=False,
                start_new_session=True,
            )
        except subprocess.TimeoutExpired as exc:
            raise TimeoutError(f"ADB command timed out: {args[0] if args else 'adb'}") from exc
        if check and result.returncode != 0:
            detail = " ".join((result.stderr or result.stdout).split())[-500:]
            raise ConnectionError(f"ADB command failed ({result.returncode}): {detail}")
        return result

    def shell(self, *args: str, deadline: float, check: bool = True) -> subprocess.CompletedProcess[str]:
        # ADB joins shell arguments on the device.  Quote every value ourselves
        # so spaces stay data and metacharacters never become commands.
        command = shlex.join(args)
        return self.run("shell", command, deadline=deadline, check=check)


def _parse_devices(output: str) -> dict[str, str]:
    devices: dict[str, str] = {}
    for line in output.splitlines()[1:]:
        fields = line.strip().split()
        if len(fields) >= 2:
            devices[fields[0]] = fields[1]
    return devices


def _resolve_serial(adb_path: str, requested: str | None, deadline: float) -> str:
    result = _Adb(adb_path).run("devices", deadline=deadline)
    devices = _parse_devices(result.stdout)
    if requested is not None:
        state = devices.get(str(requested))
        if state != "device":
            shown = state or "not found"
            raise ConnectionError(f"ADB device {requested} is not authorized ({shown})")
        return str(requested)
    authorized = [serial for serial, state in devices.items() if state == "device"]
    if len(authorized) != 1:
        raise RuntimeError(
            "adb_serial is required unless exactly one authorized ADB device is connected"
        )
    return authorized[0]


def _logcat_pid(line: str) -> int | None:
    # Android's epoch formatter right-aligns some records, so a valid log line
    # may begin with padding before the timestamp.  Markers bypass PID parsing,
    # but completion events do not; preserving the padding here would silently
    # discard a real completion event and leave the caller waiting to timeout.
    line = line.lstrip()
    # threadtime / epoch: timestamp, uid, pid, tid, priority, tag, message
    for pattern in (
        r"^\d+\.\d+\s+\d+\s+(\d+)\s+\d+\s+[VDIWEF]\s+",
        r"^\d+\.\d+\s+(\d+)\s+\d+\s+[VDIWEF]\s+",
        r"^\d\d-\d\d\s+\d\d:\d\d:\d\d\.\d+\s+\d+\s+(\d+)\s+\d+\s+[VDIWEF]\s+",
        r"^\d\d-\d\d\s+\d\d:\d\d:\d\d\.\d+\s+(\d+)\s+\d+\s+[VDIWEF]\s+",
    ):
        match = re.match(pattern, line)
        if match:
            return int(match.group(1))
    # legacy time format: I/Tag( 1234): message
    match = re.search(r"[VDIWEF]/[^()]+\(\s*(\d+)\):", line)
    return int(match.group(1)) if match else None


def _is_relevant_logcat_line(
    line: str, pids: set[int], markers: frozenset[str] = frozenset(),
) -> bool:
    lowered = line.lower()
    if any(marker in line for marker in markers):
        return True
    if _logcat_pid(line) not in pids:
        return False
    return any(
        token in lowered
        for token in (
            "activity sync", "activity_sync", "database export", "database_export", "intent api",
        )
    )


class _LogcatWatcher:
    def __init__(self, adb: _Adb, pids: set[int]):
        self.pids = pids
        self.markers: set[str] = set()
        self.marker_lock = threading.Lock()
        self.process = subprocess.Popen(
            adb.argv("logcat", "-v", "epoch", "-T", "1"),
            text=True,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            bufsize=1,
            start_new_session=True,
        )
        self.lines: queue.Queue[str] = queue.Queue(maxsize=2000)
        self.thread = threading.Thread(target=self._read, daemon=True)
        self.thread.start()

    def _read(self) -> None:
        assert self.process.stdout is not None
        for line in self.process.stdout:
            with self.marker_lock:
                markers = frozenset(self.markers)
            if not _is_relevant_logcat_line(line, self.pids, markers):
                continue
            try:
                self.lines.put_nowait(line.rstrip("\n"))
            except queue.Full:
                try:
                    self.lines.get_nowait()
                except queue.Empty:
                    pass
                try:
                    self.lines.put_nowait(line.rstrip("\n"))
                except queue.Full:
                    pass

    def expect_marker(self, marker: str) -> None:
        with self.marker_lock:
            self.markers.add(marker)

    def wait_for(
        self,
        success: Callable[[str], bool],
        failure: Callable[[str], bool],
        deadline: float,
        description: str,
    ) -> str:
        while True:
            if self.process.poll() is not None and self.lines.empty():
                raise ConnectionError("ADB logcat exited before completion event")
            try:
                line = self.lines.get(timeout=min(0.2, _remaining(deadline, reserve=0.15)))
            except TimeoutError as exc:
                raise TimeoutError(
                    f"timed out waiting for Gadgetbridge {description}"
                ) from exc
            except queue.Empty:
                continue
            if failure(line):
                raise RuntimeError(f"Gadgetbridge reported {description} failure")
            if success(line):
                return line

    def close(self, deadline: float) -> None:
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=max(0.01, min(0.3, deadline - time.monotonic())))
            except (subprocess.TimeoutExpired, ValueError):
                self.process.kill()
                try:
                    self.process.wait(timeout=0.1)
                except subprocess.TimeoutExpired:
                    pass
        if self.process.stdout is not None:
            self.process.stdout.close()


def _phase_marker(watcher: _LogcatWatcher, adb: _Adb, deadline: float, label: str) -> None:
    marker = f"miband_{label}_{uuid.uuid4().hex}"
    watcher.expect_marker(marker)
    adb.shell("log", "-t", "MiBandSync", marker, deadline=deadline)
    watcher.wait_for(
        lambda line: marker in line,
        lambda _line: False,
        deadline,
        f"{label} marker",
    )


def _broadcast(adb: _Adb, package: str, action: str, deadline: float) -> None:
    result = adb.shell("am", "broadcast", "-a", action, package, deadline=deadline)
    if "Broadcast completed" not in result.stdout:
        raise RuntimeError(f"Android did not accept {action} broadcast")


def _intent_not_allowed(line: str) -> bool:
    lowered = line.lower()
    return "intent api" in lowered and "not allowed" in lowered


def _request_exports(
    adb: _Adb, package: str, pids: set[int], refresh_app: bool, deadline: float,
) -> dict[str, Any]:
    watcher = _LogcatWatcher(adb, pids)
    evidence: dict[str, Any] = {"activity_sync": {"requested": False}}
    try:
        if refresh_app:
            _phase_marker(watcher, adb, deadline, "activity")
            _broadcast(adb, package, ACTIVITY_SYNC, deadline)
            watcher.wait_for(
                lambda line: "broadcasting activity sync finish" in line.lower()
                or "action.activity_sync_finish" in line.lower(),
                lambda line: _intent_not_allowed(line)
                or ("activity sync" in line.lower() and any(word in line.lower() for word in (" fail", "failed", "error"))),
                deadline,
                "activity sync",
            )
            evidence["activity_sync"] = {"requested": True, "confirmed": True}

        _phase_marker(watcher, adb, deadline, "export")
        _broadcast(adb, package, DATABASE_EXPORT, deadline)
        watcher.wait_for(
            lambda line: "broadcasting database export success=true" in line.lower()
            or "database_export_success" in line.lower(),
            lambda line: _intent_not_allowed(line)
            or "database_export_fail" in line.lower()
            or "broadcasting database export success=false" in line.lower(),
            deadline,
            "database export",
        )
        evidence["database_export"] = {"requested": True, "confirmed": True}
        return evidence
    finally:
        watcher.close(deadline)


def _remote_sha256(adb: _Adb, remote: str, deadline: float) -> str:
    result = adb.shell("sha256sum", "--", remote, deadline=deadline)
    token = result.stdout.strip().split(maxsplit=1)[0] if result.stdout.strip() else ""
    if not _SHA256_RE.fullmatch(token):
        raise RuntimeError("phone did not return a valid SHA-256 for the exported database")
    return token.lower()


def _pull(adb: _Adb, remote: str, local: Path, deadline: float) -> None:
    adb.run("pull", remote, str(local), deadline=deadline)
    local.chmod(0o600)


def _sha256(path: Path, deadline: float | None = None) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            if deadline is not None:
                _remaining(deadline, reserve=0.1)
            digest.update(chunk)
    return digest.hexdigest()


def _db_snapshot(
    path: Path, settings: Any, *, validate: bool, deadline: float | None = None,
) -> dict[str, Any]:
    conn = sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True, timeout=3)
    try:
        if deadline is not None:
            conn.set_progress_handler(lambda: int(time.monotonic() >= deadline - 0.1), 1000)
            _remaining(deadline, reserve=0.1)
        if validate:
            quick = conn.execute("PRAGMA quick_check").fetchone()
            if not quick or quick[0] != "ok":
                raise ValueError("pulled Gadgetbridge database failed SQLite quick_check")
        conn.row_factory = sqlite3.Row
        # Keep the sync acceptance boundary exactly aligned with what the
        # query backend can read.  Imports are local to avoid a module-startup
        # cycle; query only imports read_sync_status lazily.
        from .query import _select_device, _validate_schema
        _validate_schema(conn)
        selected = _select_device(conn, settings)
        if selected is None:
            raise ValueError("Gadgetbridge export contains no selected device")
        selected_id = int(selected["_id"])
        selected_address = str(selected["IDENTIFIER"])
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}

        rows: dict[str, int] = {}
        latest_health: list[int] = []
        latest_battery: list[int] = []
        for table in sorted(SNAPSHOT_TABLES):
            if table not in tables:
                continue
            columns = {row[1] for row in conn.execute(f'PRAGMA table_info("{table}")')}
            if "DEVICE_ID" not in columns:
                if validate:
                    raise ValueError(f"Gadgetbridge table {table} has no DEVICE_ID")
                continue
            rows[table] = int(conn.execute(
                f'SELECT COUNT(*) FROM "{table}" WHERE DEVICE_ID=?', (selected_id,)
            ).fetchone()[0])
        rows["DEVICE"] = 1
        for table, column, divisor in (
            ("XIAOMI_ACTIVITY_SAMPLE", "TIMESTAMP", 1),
            ("BATTERY_LEVEL", "TIMESTAMP", 1),
            ("XIAOMI_DAILY_SUMMARY_SAMPLE", "TIMESTAMP", 1000),
            ("XIAOMI_MANUAL_SAMPLE", "TIMESTAMP", 1000),
            ("XIAOMI_SLEEP_TIME_SAMPLE", "TIMESTAMP", 1000),
            ("XIAOMI_SLEEP_TIME_SAMPLE", "WAKEUP_TIME", 1000),
            ("XIAOMI_SLEEP_STAGE_SAMPLE", "TIMESTAMP", 1000),
            ("BASE_ACTIVITY_SUMMARY", "END_TIME", 1000),
        ):
            if table in tables:
                value = conn.execute(
                    f'SELECT MAX("{column}") FROM "{table}" WHERE DEVICE_ID=?', (selected_id,)
                ).fetchone()[0]
                if value is not None:
                    destination = latest_battery if table == "BATTERY_LEVEL" else latest_health
                    destination.append(int(value) // divisor)
        latest_epoch = max(latest_health) if latest_health else None
        battery_epoch = max(latest_battery) if latest_battery else None
        result = {
            "device": {"id": selected_id, "address": selected_address},
            "rows": rows,
            "total_rows": sum(count for name, count in rows.items() if name != "DEVICE"),
            "latest_observation_epoch": latest_epoch,
            "latest_observation_at": (
                datetime.fromtimestamp(latest_epoch, timezone.utc).isoformat(timespec="seconds")
                if latest_epoch is not None else None
            ),
            "latest_battery_epoch": battery_epoch,
            "latest_battery_at": (
                datetime.fromtimestamp(battery_epoch, timezone.utc).isoformat(timespec="seconds")
                if battery_epoch is not None else None
            ),
        }
        if deadline is not None:
            _remaining(deadline, reserve=0.1)
        return result
    finally:
        if deadline is not None:
            conn.set_progress_handler(None, 0)
        conn.close()


def _safe_snapshot(
    path: Path, settings: Any, deadline: float | None = None,
) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return _db_snapshot(path, settings, validate=False, deadline=deadline)
    except (OSError, sqlite3.Error, ValueError, RuntimeError):
        return None


def sync_health(
    settings: Any,
    *,
    refresh_app: bool = False,
    timeout_seconds: int = 120,
) -> dict[str, Any]:
    """Export Gadgetbridge's DB and atomically refresh the validated local cache."""
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    data_dir = Path(settings.data_dir)
    data_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    data_dir.chmod(0o700)
    lock_path = _lock_path(settings)
    lock_handle = lock_path.open("a+")
    lock_path.chmod(0o600)
    try:
        try:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return {**read_sync_status(settings), "status": "in_progress"}

        deadline = time.monotonic() + float(timeout_seconds)
        attempted_at = _now_iso()
        previous = _normalize_status(settings, _load_status(_status_path(settings)))
        db_path = _canonical_db_path(settings)
        before = _safe_snapshot(db_path, settings, deadline)
        baseline_identity = _cache_identity(db_path)
        in_progress = {
            "status": "in_progress",
            "last_attempt_at": attempted_at,
            "last_success_at": previous.get("last_success_at"),
            "last_pulled_at": previous.get("last_pulled_at"),
            "last_error": None,
            "before": before,
            "db_path": str(db_path),
            "cache_identity": baseline_identity,
        }
        _write_status(_status_path(settings), in_progress)
        staging: Path | None = None
        cache_updated = False
        try:
            adb_path, package, remote = _validate_settings(settings)
            serial = _resolve_serial(adb_path, getattr(settings, "adb_serial", None), deadline)
            adb = _Adb(adb_path, serial)
            pid = adb.shell("pidof", package, deadline=deadline, check=False)
            if pid.returncode != 0 or not pid.stdout.strip():
                raise RuntimeError("Gadgetbridge is not running; open it without starting a sync UI flow")
            try:
                pids = {int(value) for value in pid.stdout.split()}
            except ValueError as exc:
                raise RuntimeError("Gadgetbridge pidof returned malformed process ids") from exc
            if not pids:
                raise RuntimeError("Gadgetbridge is not running")

            evidence = _request_exports(adb, package, pids, refresh_app, deadline)
            remote_hash = _remote_sha256(adb, remote, deadline)

            db_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            fd, name = tempfile.mkstemp(prefix=f".{db_path.name}.sync-", dir=db_path.parent)
            os.close(fd)
            staging = Path(name)
            staging.chmod(0o600)
            _pull(adb, remote, staging, deadline)
            local_hash = _sha256(staging, deadline)
            if local_hash != remote_hash:
                raise ValueError("pulled Gadgetbridge database hash does not match the phone export")
            after = _db_snapshot(staging, settings, validate=True, deadline=deadline)
            _remaining(deadline, reserve=0.05)
            os.replace(staging, db_path)
            staging = None
            cache_updated = True
            db_path.chmod(0o600)
            finished_at = _now_iso()
            published_identity = _cache_identity(db_path, local_hash)
            result = {
                "status": "ok",
                "last_attempt_at": attempted_at,
                "last_success_at": finished_at,
                "last_pulled_at": finished_at,
                "last_error": None,
                "serial": serial,
                "refresh_app": bool(refresh_app),
                "events": evidence,
                "before": before,
                "after": after,
                "database_sha256": local_hash,
                "db_path": str(db_path),
                "cache_identity": published_identity,
                "cache_identity_matches": True,
                "cache_updated": True,
                "status_persisted": True,
            }
            try:
                _write_status(_status_path(settings), result)
            except Exception as exc:
                result.update(
                    status="error",
                    status_persisted=False,
                    last_error=(
                        "cache was updated, but sync status could not be persisted: "
                        + _bounded_error(exc)
                    ),
                )
            return result
        except Exception as exc:
            failure_identity = _cache_identity(db_path) if cache_updated else baseline_identity
            failed = {
                "status": "error",
                "last_attempt_at": attempted_at,
                "last_success_at": previous.get("last_success_at"),
                "last_pulled_at": previous.get("last_pulled_at"),
                "last_error": _bounded_error(exc),
                "before": before,
                "db_path": str(db_path),
                "cache_identity": failure_identity,
                "cache_updated": cache_updated,
                "status_persisted": True,
            }
            if cache_updated:
                failed["last_error"] = "cache was updated before a later failure: " + failed["last_error"]
            try:
                _write_status(_status_path(settings), failed)
            except Exception as state_exc:
                failed["status_persisted"] = False
                failed["last_error"] += "; status persistence also failed: " + _bounded_error(state_exc)
            return failed
        finally:
            if staging is not None:
                staging.unlink(missing_ok=True)
    finally:
        try:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
        finally:
            lock_handle.close()
