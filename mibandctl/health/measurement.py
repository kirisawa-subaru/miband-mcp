"""One-shot heart-rate measurement through Xiaomi Health's live transport."""

from __future__ import annotations

import fcntl
import json
import select
import shlex
import socket
import sqlite3
import subprocess
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from .importer import canonical_json, configure_connection, ensure_schema
from .phone_exporter import (
    TERMUX_ENV,
    TERMUX_PYTHON,
    _root_command,
    _run_ssh_text,
    _ssh_base,
    verify_device,
)


MEASUREMENT_KEY = "measurement.latest"
HEALTH_PACKAGE = "com.mi.health"
FRIDA_SERVER = "/data/local/miband-health/frida-server-16.7.19"
FRIDA_REMOTE_PORT = 27042
FRIDA_SERVER_LIFETIME_SECONDS = 100
MIN_TIMEOUT_SECONDS = 30
MAX_TIMEOUT_SECONDS = 90
STOP_GRACE_SECONDS = 8.0
MAX_ERROR_CHARS = 800


_FRIDA_SERVER_LAUNCHER = r'''
import subprocess
import signal
import sys

server = sys.argv[1]
port = int(sys.argv[2])
lifetime = int(sys.argv[3])
child = None
stop_requested = False

def stop_child(*_args):
    global stop_requested
    stop_requested = True
    if child is None or child.poll() is not None:
        return
    child.terminate()
    try:
        child.wait(timeout=3)
    except subprocess.TimeoutExpired:
        child.kill()
        child.wait(timeout=2)

signal.signal(signal.SIGHUP, stop_child)
signal.signal(signal.SIGINT, stop_child)
signal.signal(signal.SIGTERM, stop_child)
try:
    child = subprocess.Popen(
        [server, "-l", "127.0.0.1:%d" % port],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    if stop_requested:
        stop_child()
    else:
        print("MIBAND_FRIDA_PID=%d" % child.pid, flush=True)
        child.wait(timeout=lifetime)
except subprocess.TimeoutExpired:
    stop_child()
finally:
    stop_child()
'''


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _bounded_error(value: BaseException | str) -> str:
    if isinstance(value, BaseException):
        value = f"{type(value).__name__}: {value}"
    return " ".join(str(value).split())[:MAX_ERROR_CHARS]


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("heart-rate measurement timed out")
    return remaining


def _base_result(requested_at: str) -> dict[str, Any]:
    return {
        "status": "error",
        "source": "band_realtime_protocol",
        "confirmation_basis": "xiaomi_health_transport_result",
        "heart_rate_bpm": None,
        "unit": "bpm",
        "received_at": None,
        "measurement_requested_at": requested_at,
        "start_confirmed": False,
        "stop_confirmed": False,
        "start_code": None,
        "stop_code": None,
        "elapsed_seconds": 0.0,
        "error": None,
        "cleanup_errors": [],
    }


class _MeasurementCollector:
    """Collect the bounded messages emitted by the injected script."""

    def __init__(self, clock: Callable[[], str] = _now_iso) -> None:
        self._clock = clock
        self.finished = threading.Event()
        self.start_confirmed = False
        self.stop_confirmed = False
        self.start_code: int | None = None
        self.stop_code: int | None = None
        self.heart_rate_bpm: int | None = None
        self.received_at: str | None = None
        self.errors: list[str] = []
        self.reconnect_attempted = False
        self.reconnect_succeeded = False
        # Generic terminal failures and STOP completion are deliberately
        # separate: a script error must not let us detach before async STOP.
        self.stop_done = threading.Event()

    def _error(self, value: Any) -> None:
        if len(self.errors) < 4:
            self.errors.append(_bounded_error(str(value)))

    def handle(self, message: dict[str, Any], _data: bytes | None = None) -> None:
        if message.get("type") == "error":
            self._error(message.get("stack") or message.get("description") or "injected script error")
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
        elif phase == "start_ack":
            code = payload.get("code")
            if isinstance(code, int) and not isinstance(code, bool):
                self.start_code = code
                self.start_confirmed = code == 0
                if code != 0:
                    self._error(f"measurement start returned code {code}")
        elif phase == "realtime":
            heart_rate = payload.get("heart_rate")
            if (
                self.heart_rate_bpm is None
                and isinstance(heart_rate, int)
                and not isinstance(heart_rate, bool)
                and 10 < heart_rate <= 255
            ):
                self.heart_rate_bpm = heart_rate
                self.received_at = self._clock()
        elif phase == "stop_ack":
            code = payload.get("code")
            if isinstance(code, int) and not isinstance(code, bool):
                self.stop_code = code
                self.stop_confirmed = code == 0
                if code != 0:
                    self._error(f"measurement stop returned code {code}")
            self.finished.set()
            self.stop_done.set()
        elif phase in {
            "start_error",
            "stop_error",
            "observer_error",
            "setup_error",
            "manager_missing",
            "not_connected",
            "stop_unavailable",
        }:
            code = payload.get("code")
            detail = payload.get("error") or phase
            if phase == "start_error" and isinstance(code, int) and not isinstance(code, bool):
                self.start_code = code
            if phase == "stop_error" and isinstance(code, int) and not isinstance(code, bool):
                self.stop_code = code
            self._error(f"{detail}" + (f" (code {code})" if code is not None else ""))
            if phase in {"stop_error", "setup_error", "manager_missing", "not_connected", "stop_unavailable"}:
                self.finished.set()
            if phase in {"stop_error", "stop_unavailable"}:
                self.stop_done.set()


def _health_pid(host: str, *, timeout_seconds: float) -> int | None:
    output = _run_ssh_text(
        host,
        f"pidof {HEALTH_PACKAGE} 2>/dev/null || true",
        timeout_seconds=timeout_seconds,
    )
    tokens = output.split()
    if not tokens:
        return None
    if len(tokens) != 1 or not tokens[0].isdigit():
        raise RuntimeError("cannot identify a unique Xiaomi Health process")
    return int(tokens[0])


def _local_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _terminate_process(process: subprocess.Popen[Any] | None, label: str, errors: list[str]) -> None:
    if process is None or process.poll() is not None:
        return
    try:
        process.terminate()
        process.wait(timeout=3)
    except subprocess.TimeoutExpired:
        try:
            process.kill()
            process.wait(timeout=2)
        except Exception as exc:  # pragma: no cover - OS-level last resort
            errors.append(_bounded_error(f"{label} kill failed: {exc}"))
    except Exception as exc:
        errors.append(_bounded_error(f"{label} termination failed: {exc}"))


def _read_server_pid(process: subprocess.Popen[str], deadline: float) -> int:
    streams = [stream for stream in (process.stdout, process.stderr) if stream is not None]
    detail: list[str] = []
    while streams:
        if process.poll() is not None and not streams:
            break
        timeout = min(0.25, _remaining(deadline))
        ready, _, _ = select.select(streams, [], [], timeout)
        if not ready:
            if process.poll() is not None:
                break
            continue
        for stream in ready:
            line = stream.readline()
            if not line:
                streams.remove(stream)
                continue
            line = line.strip()
            if line.startswith("MIBAND_FRIDA_PID="):
                value = line.partition("=")[2]
                if value.isdigit():
                    return int(value)
            if line:
                detail.append(line)
    suffix = _bounded_error(" ".join(detail))
    raise RuntimeError(f"temporary Frida server did not start{': ' + suffix if suffix else ''}")


def _remote_cleanup_command(pid: int) -> str:
    # The command checks the exact PID command line before signalling it. This
    # avoids broad process-name kills and protects an unrelated debugger.
    return f"""
pid={pid}
server={FRIDA_SERVER}
if [ ! -d /proc/$pid ]; then exit 0; fi
cmd=$(tr '\\000' ' ' < /proc/$pid/cmdline 2>/dev/null)
case "$cmd" in
  *"$server"*) ;;
  *) exit 3 ;;
esac
children=$(cat /proc/$pid/task/$pid/children 2>/dev/null)
for child in $children; do
  childcmd=$(tr '\\000' ' ' < /proc/$child/cmdline 2>/dev/null)
  case "$childcmd" in *"$server"*) kill "$child" 2>/dev/null || true ;; esac
done
kill "$pid" 2>/dev/null || true
i=0
while [ -d /proc/$pid ] && [ $i -lt 5 ]; do
  sleep 1
  i=$((i + 1))
done
if [ -d /proc/$pid ]; then
  cmd=$(tr '\\000' ' ' < /proc/$pid/cmdline 2>/dev/null)
  case "$cmd" in *"$server"*) kill -9 "$pid" 2>/dev/null || true ;; *) exit 3 ;; esac
  sleep 1
fi
if [ -d /proc/$pid ]; then
  cmd=$(tr '\\000' ' ' < /proc/$pid/cmdline 2>/dev/null)
  case "$cmd" in *"$server"*) exit 4 ;; esac
fi
""".strip()


class _PhoneFridaRuntime:
    """Own a phone-side server and a loopback-only SSH forwarding process."""

    def __init__(self, host: str, deadline: float) -> None:
        self.host = host
        self.deadline = deadline
        self.local_port: int | None = None
        self.remote_pid: int | None = None
        self.server_process: subprocess.Popen[str] | None = None
        self.tunnel_process: subprocess.Popen[str] | None = None
        self.cleanup_errors: list[str] = []
        self._closed = False

    @property
    def address(self) -> str:
        if self.local_port is None:
            raise RuntimeError("Frida tunnel is not started")
        return f"127.0.0.1:{self.local_port}"

    def start(self) -> None:
        remote = (
            f"[ -x {FRIDA_SERVER} ] || {{ echo MIBAND_FRIDA_ERROR=missing; exit 126; }}; "
            f"{TERMUX_ENV} exec {TERMUX_PYTHON} -c {shlex.quote(_FRIDA_SERVER_LAUNCHER)} "
            f"{shlex.quote(FRIDA_SERVER)} {FRIDA_REMOTE_PORT} {FRIDA_SERVER_LIFETIME_SECONDS}"
        )
        self.server_process = subprocess.Popen(
            _ssh_base(self.host, _remaining(self.deadline)) + [_root_command(remote)],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        self.remote_pid = _read_server_pid(self.server_process, self.deadline)

        self.local_port = _local_port()
        base = _ssh_base(self.host, _remaining(self.deadline))
        tunnel_command = base[:-1] + [
            "-o",
            "ExitOnForwardFailure=yes",
            "-N",
            "-L",
            f"127.0.0.1:{self.local_port}:127.0.0.1:{FRIDA_REMOTE_PORT}",
            base[-1],
        ]
        self.tunnel_process = subprocess.Popen(
            tunnel_command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
        while True:
            if self.tunnel_process.poll() is not None:
                detail = ""
                if self.tunnel_process.stderr is not None:
                    detail = self.tunnel_process.stderr.read()
                raise ConnectionError(
                    f"SSH Frida tunnel failed ({self.tunnel_process.returncode}): {_bounded_error(detail)}"
                )
            try:
                with socket.create_connection(("127.0.0.1", self.local_port), timeout=0.2):
                    # The forwarding socket can connect to a stale listener if
                    # our newly started server lost the remote bind race. Give
                    # the owned foreground SSH process time to report that exit.
                    time.sleep(0.2)
                    if self.server_process.poll() is not None:
                        raise RuntimeError("owned temporary Frida server exited after tunnel startup")
                    return
            except OSError:
                time.sleep(min(0.1, _remaining(self.deadline)))

    def close(self) -> list[str]:
        if self._closed:
            return self.cleanup_errors
        self._closed = True
        _terminate_process(self.tunnel_process, "SSH tunnel", self.cleanup_errors)
        if self.remote_pid is not None:
            try:
                _run_ssh_text(
                    self.host,
                    _remote_cleanup_command(self.remote_pid),
                    timeout_seconds=10,
                )
            except Exception as exc:
                self.cleanup_errors.append(_bounded_error(f"remote server cleanup failed: {exc}"))
        _terminate_process(self.server_process, "server SSH", self.cleanup_errors)
        return self.cleanup_errors

    def __enter__(self) -> _PhoneFridaRuntime:
        try:
            self.start()
        except Exception:
            self.close()
            raise
        return self

    def __exit__(self, _type: Any, _value: Any, _traceback: Any) -> None:
        self.close()


def _script_source() -> str:
    return Path(__file__).with_name("measure.js").read_text(encoding="utf-8")


@contextmanager
def _frida_cancellable_until(deadline: float):
    """Bound one or more synchronous Frida calls by a monotonic deadline."""
    import frida  # type: ignore[import-not-found]

    cancellable = frida.Cancellable()
    timer = threading.Timer(max(0.0, deadline - time.monotonic()), cancellable.cancel)
    timer.daemon = True
    timer.start()
    try:
        with cancellable:
            yield
    finally:
        timer.cancel()


def _run_frida_script(
    device: Any,
    app_pid: int,
    deadline: float,
    *,
    stop_grace_seconds: float = STOP_GRACE_SECONDS,
) -> _MeasurementCollector:
    collector = _MeasurementCollector()
    session = None
    script = None
    script_loaded = False
    timed_out = False
    active_deadline = max(time.monotonic() + 0.1, deadline - stop_grace_seconds)
    try:
        with _frida_cancellable_until(active_deadline):
            session = device.attach(app_pid)
            script = session.create_script(_script_source())
            script.on("message", collector.handle)
            script.load()
            script_loaded = True
            measure_for = max(0.1, active_deadline - time.monotonic())
            script.exports_sync.begin(max(100, int(measure_for * 1000)))
            if not collector.finished.wait(measure_for):
                timed_out = True
    except Exception as exc:
        collector._error(f"measurement transport failed: {exc}")
    finally:
        # Use a new cancellation scope. The active scope may have expired, but
        # STOP must still be attempted before unload/detach.
        cleanup_deadline = time.monotonic() + max(0.1, stop_grace_seconds)
        if script_loaded and not collector.stop_done.is_set():
            try:
                with _frida_cancellable_until(cleanup_deadline):
                    script.exports_sync.stop()
            except Exception as exc:
                collector._error(f"STOP request failed: {exc}")
            collector.stop_done.wait(max(0.0, cleanup_deadline - time.monotonic()))
        if timed_out and collector.heart_rate_bpm is None:
            collector._error("timed out waiting for a valid band heart-rate reading")
        if script is not None:
            try:
                with _frida_cancellable_until(cleanup_deadline):
                    script.unload()
            except Exception as exc:
                collector._error(f"script unload failed: {exc}")
        if session is not None:
            try:
                with _frida_cancellable_until(cleanup_deadline):
                    session.detach()
            except Exception as exc:
                collector._error(f"Frida detach failed: {exc}")
    return collector


def _measure_via_frida(host: str, app_pid: int, deadline: float) -> dict[str, Any]:
    runtime = _PhoneFridaRuntime(host, deadline)
    collector: _MeasurementCollector | None = None
    transport_error: str | None = None
    try:
        with runtime:
            # Imported lazily so cached queries and sync do not load Frida.
            import frida  # type: ignore[import-not-found]

            manager = frida.get_device_manager()
            remote_added = False
            try:
                with _frida_cancellable_until(deadline - STOP_GRACE_SECONDS):
                    device = manager.add_remote_device(runtime.address)
                remote_added = True
                collector = _run_frida_script(device, app_pid, deadline)
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
        collector = _MeasurementCollector()
        collector._error(transport_error or "measurement script did not start")
    return {
        "heart_rate_bpm": collector.heart_rate_bpm,
        "received_at": collector.received_at,
        "start_confirmed": collector.start_confirmed,
        "stop_confirmed": collector.stop_confirmed,
        "start_code": collector.start_code,
        "stop_code": collector.stop_code,
        "errors": collector.errors,
        "cleanup_errors": runtime.cleanup_errors,
        "reconnect_attempted": collector.reconnect_attempted,
        "reconnect_succeeded": collector.reconnect_succeeded,
    }


def _persist_latest(settings: Any, result: dict[str, Any]) -> None:
    db_path = Path(settings.db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    db_path.parent.chmod(0o700)
    conn = sqlite3.connect(db_path, timeout=10)
    db_path.chmod(0o600)
    conn.row_factory = sqlite3.Row
    configure_connection(conn)
    try:
        ensure_schema(conn)
        conn.commit()
        with conn:
            conn.execute(
                """
                INSERT INTO metadata(key, value, updated_at) VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE
                SET value=excluded.value, updated_at=excluded.updated_at
                """,
                (MEASUREMENT_KEY, canonical_json(result), result["received_at"]),
            )
    finally:
        conn.close()


def measure_heart_rate(settings: Any, timeout_seconds: int = 60) -> dict[str, Any]:
    """Request one live Band heart-rate reading through Xiaomi Health.

    The operation never launches or foregrounds the app. Xiaomi Health's
    background process must already be running.
    """
    if not MIN_TIMEOUT_SECONDS <= timeout_seconds <= MAX_TIMEOUT_SECONDS:
        raise ValueError(
            f"timeout_seconds must be {MIN_TIMEOUT_SECONDS}..{MAX_TIMEOUT_SECONDS}"
        )

    requested_at = _now_iso()
    started = time.monotonic()
    result = _base_result(requested_at)
    data_dir = Path(settings.data_dir)
    data_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    data_dir.chmod(0o700)
    lock_path = data_dir / "sync.lock"
    lock_handle = lock_path.open("a+")
    lock_path.chmod(0o600)
    try:
        try:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            result["status"] = "in_progress"
            result["error"] = "another health sync or measurement is in progress"
            return result

        deadline = started + timeout_seconds
        try:
            verify_device(
                settings.ssh_host,
                settings.device_serial,
                timeout_seconds=_remaining(deadline),
            )
            app_pid = _health_pid(settings.ssh_host, timeout_seconds=_remaining(deadline))
            if app_pid is None:
                raise RuntimeError(
                    "Xiaomi Health background process is not running; start it before measuring"
                )

            measured = _measure_via_frida(settings.ssh_host, app_pid, deadline)
            for key in (
                "heart_rate_bpm",
                "received_at",
                "start_confirmed",
                "stop_confirmed",
                "start_code",
                "stop_code",
                "cleanup_errors",
                "reconnect_attempted",
                "reconnect_succeeded",
            ):
                result[key] = measured.get(key)
            errors = [str(value) for value in measured.get("errors", []) if value]
            cleanup_errors = [str(value) for value in measured.get("cleanup_errors", []) if value]
            result["cleanup_errors"] = cleanup_errors[:4]

            success = (
                result["start_confirmed"] is True
                and result["stop_confirmed"] is True
                and isinstance(result["heart_rate_bpm"], int)
                and not isinstance(result["heart_rate_bpm"], bool)
                and 10 < result["heart_rate_bpm"] <= 255
                and isinstance(result["received_at"], str)
                and not errors
                and not cleanup_errors
            )
            if not success:
                if errors:
                    raise RuntimeError(errors[0])
                if cleanup_errors:
                    raise RuntimeError(cleanup_errors[0])
                if not result["start_confirmed"]:
                    raise RuntimeError("band measurement START was not confirmed")
                if result["heart_rate_bpm"] is None:
                    raise TimeoutError("no valid band heart-rate reading was received")
                if not result["stop_confirmed"]:
                    raise RuntimeError("band measurement STOP was not confirmed")

            result["status"] = "ok"
            result["error"] = None
            result["elapsed_seconds"] = round(time.monotonic() - started, 3)
            _persist_latest(settings, result)
            return result
        except Exception as exc:
            result["status"] = "error"
            result["error"] = _bounded_error(exc)
            return result
    finally:
        result["elapsed_seconds"] = round(time.monotonic() - started, 3)
        try:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
        finally:
            lock_handle.close()
