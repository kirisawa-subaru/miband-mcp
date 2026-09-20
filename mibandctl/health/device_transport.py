"""Locked Xiaomi protocol transport over Xiaomi Health's authenticated session."""

from __future__ import annotations

import base64
import fcntl
import time
from pathlib import Path
from typing import Any

from .measurement import (
    _PhoneFridaRuntime,
    _bounded_error,
    _frida_cancellable_until,
    _health_pid,
    _now_iso,
)
from .phone_exporter import verify_device


class DeviceTransportError(RuntimeError):
    """A bounded transport failure, including whether a packet may have left the app."""

    def __init__(
        self, message: str, *, sent: bool | None = None, status: str | None = None
    ) -> None:
        super().__init__(message)
        self.sent = sent
        self.status = status


class DeviceBusyError(DeviceTransportError):
    pass


class DeviceUnavailableError(DeviceTransportError):
    pass


class DeviceTimeoutError(DeviceTransportError):
    pass


def _varint(value: int) -> bytes:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("protobuf values must be non-negative integers")
    encoded = bytearray()
    while value > 0x7F:
        encoded.append((value & 0x7F) | 0x80)
        value >>= 7
    encoded.append(value)
    return bytes(encoded)


def encode_command(
    command_type: int,
    command_subtype: int,
    *,
    payload: bytes = b"",
    payload_field: int | None = None,
) -> bytes:
    """Encode the stable outer Xiaomi Command envelope."""
    if not 0 <= command_type <= 0xFFFFFFFF or not 0 <= command_subtype <= 0xFFFFFFFF:
        raise ValueError("command type and subtype must be uint32")
    if not isinstance(payload, bytes):
        raise TypeError("payload must be bytes")
    result = bytearray((0x08,))
    result.extend(_varint(command_type))
    result.append(0x10)
    result.extend(_varint(command_subtype))
    if payload:
        if payload_field is None or not 1 <= payload_field <= 0x1FFFFFFF:
            raise ValueError("payload_field is required for a non-empty payload")
        result.extend(_varint((payload_field << 3) | 2))
        result.extend(_varint(len(payload)))
        result.extend(payload)
    return bytes(result)


class DeviceSession:
    """One lock and one Frida runtime shared by a packet transaction."""

    def __init__(self, settings: Any, *, timeout_seconds: int = 30, retry_connect: bool = True):
        if not 5 <= timeout_seconds <= 90:
            raise ValueError("timeout_seconds must be 5..90")
        self.settings = settings
        self.deadline = time.monotonic() + timeout_seconds
        self.retry_connect = retry_connect
        self.lock_handle = None
        self.runtime = None
        self.manager = None
        self.remote_added = False
        self.session = None
        self.script = None
        self.connection: dict[str, Any] = {}
        self.cleanup_errors: list[str] = []
        self._transport_finalized = False

    def _remaining(self) -> float:
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise DeviceTimeoutError("device operation timed out")
        return remaining

    def __enter__(self) -> DeviceSession:
        data_dir = Path(self.settings.data_dir)
        data_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        data_dir.chmod(0o700)
        lock_path = data_dir / "sync.lock"
        self.lock_handle = lock_path.open("a+")
        lock_path.chmod(0o600)
        try:
            try:
                fcntl.flock(self.lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise DeviceBusyError("another health sync or device operation is in progress") from exc
            verify_device(
                self.settings.ssh_host,
                self.settings.device_serial,
                timeout_seconds=self._remaining(),
            )
            app_pid = _health_pid(self.settings.ssh_host, timeout_seconds=self._remaining())
            if app_pid is None:
                raise DeviceUnavailableError("Xiaomi Health background process is not running")

            self.runtime = _PhoneFridaRuntime(self.settings.ssh_host, self.deadline)
            self.runtime.__enter__()
            import frida  # type: ignore[import-not-found]

            self.manager = frida.get_device_manager()
            attach_budget = max(0.5, self._remaining() - 1.0)
            with _frida_cancellable_until(time.monotonic() + attach_budget):
                device = self.manager.add_remote_device(self.runtime.address)
                self.remote_added = True
                self.session = device.attach(app_pid)
                self.script = self.session.create_script(
                    Path(__file__).with_name("device_transport.js").read_text(encoding="utf-8")
                )
                self.script.load()
                self.connection = dict(
                    self.script.exports_sync.begin(
                        bool(self.retry_connect), max(1000, int(self._remaining() * 1000) - 5000)
                    )
                )
            if self.connection.get("status") != "ok":
                raise DeviceUnavailableError(
                    _bounded_error(self.connection.get("error") or "wearable is unavailable")
                )
            return self
        except Exception as exc:
            self.close(suppress_errors=True)
            if isinstance(exc, DeviceTransportError):
                raise
            raise DeviceUnavailableError(_bounded_error(exc)) from exc

    def _invoke(self, method: str, *args: Any, timeout_seconds: float | None = None) -> dict[str, Any]:
        if self.script is None:
            raise DeviceTransportError("device session is not open")
        allowed = self._remaining() if timeout_seconds is None else min(self._remaining(), timeout_seconds)
        if allowed <= 0:
            raise DeviceTimeoutError("device request timed out")
        call_deadline = time.monotonic() + allowed
        try:
            with _frida_cancellable_until(call_deadline):
                result = getattr(self.script.exports_sync, method)(*args)
            return dict(result)
        except DeviceTransportError:
            raise
        except Exception as exc:
            if time.monotonic() >= call_deadline:
                raise DeviceTimeoutError("device request timed out") from exc
            raise DeviceTransportError(_bounded_error(exc)) from exc

    def request(
        self,
        command_type: int,
        command_subtype: int,
        *,
        payload: bytes = b"",
        payload_field: int | None = None,
        response_type: int | None = None,
        response_subtype: int | None = None,
        timeout_seconds: float | None = None,
        retry: bool = True,
    ) -> dict[str, Any]:
        packet = encode_command(
            command_type, command_subtype, payload=payload, payload_field=payload_field
        )
        expected_type = command_type if response_type is None else response_type
        expected_subtype = command_subtype if response_subtype is None else response_subtype
        allowed = min(self._remaining(), timeout_seconds or 8.0)
        result = self._invoke(
            "request",
            base64.b64encode(packet).decode("ascii"),
            expected_type,
            expected_subtype,
            max(500, int(allowed * 1000)),
            timeout_seconds=allowed + 1,
        )
        first_attempt_sent = result.get("sent") is True
        if result.get("status") == "not_connected" and retry and self.retry_connect:
            reconnect = self._invoke(
                "reconnect", max(1000, int(min(self._remaining(), 10) * 1000))
            )
            if reconnect.get("status") == "ok":
                result = self._invoke(
                    "request",
                    base64.b64encode(packet).decode("ascii"),
                    expected_type,
                    expected_subtype,
                    max(500, int(min(self._remaining(), allowed) * 1000)),
                    timeout_seconds=min(self._remaining(), allowed + 1),
                )
                result["reconnect_attempted"] = True
                result["reconnect_succeeded"] = True
                result["retry_after_sent"] = first_attempt_sent
        if result.get("status") == "timeout":
            raise DeviceTimeoutError(
                "device response timed out", sent=result.get("sent"), status="timeout"
            )
        if result.get("status") != "ok":
            raise DeviceUnavailableError(
                _bounded_error(result.get("error") or "device request failed"),
                sent=result.get("sent"),
                status=str(result.get("status") or "error"),
            )
        response = result.get("response")
        if not isinstance(response, str):
            raise DeviceTransportError("device response payload is missing")
        return {
            **result,
            "response": base64.b64decode(response, validate=True),
            "received_at": _now_iso(),
            "reconnect_attempted": bool(
                result.get("reconnect_attempted") or self.connection.get("reconnect_attempted")
            ),
            "reconnect_succeeded": bool(
                result.get("reconnect_succeeded") or self.connection.get("reconnect_succeeded")
            ),
            "cleanup_errors": self.cleanup_errors,
        }

    def send(
        self,
        command_type: int,
        command_subtype: int,
        *,
        payload: bytes = b"",
        payload_field: int | None = None,
        retry: bool = False,
    ) -> dict[str, Any]:
        packet = encode_command(
            command_type, command_subtype, payload=payload, payload_field=payload_field
        )
        result = self._invoke("send", base64.b64encode(packet).decode("ascii"))
        if result.get("status") == "not_connected" and retry and self.retry_connect:
            reconnect = self._invoke(
                "reconnect", max(1000, int(min(self._remaining(), 10) * 1000))
            )
            if reconnect.get("status") == "ok":
                result = self._invoke("send", base64.b64encode(packet).decode("ascii"))
                result["reconnect_attempted"] = True
                result["reconnect_succeeded"] = True
        if result.get("status") != "ok":
            raise DeviceUnavailableError(
                _bounded_error(result.get("error") or "device send failed"),
                sent=result.get("sent"),
                status=str(result.get("status") or "error"),
            )
        return {**result, "sent_at": _now_iso(), "cleanup_errors": self.cleanup_errors}

    def finalize_transport(self) -> list[str]:
        """Tear down injected resources while retaining the shared operation lock."""
        if self._transport_finalized:
            return self.cleanup_errors
        self._transport_finalized = True
        if self.script is not None:
            try:
                with _frida_cancellable_until(time.monotonic() + 3):
                    result = dict(self.script.exports_sync.cleanup())
                if result.get("hook_restored") is not True:
                    self.cleanup_errors.append("raw packet callback hook restoration was not confirmed")
            except Exception as exc:
                self.cleanup_errors.append(_bounded_error(f"transport cleanup failed: {exc}"))
            try:
                with _frida_cancellable_until(time.monotonic() + 3):
                    self.script.unload()
            except Exception as exc:
                self.cleanup_errors.append(_bounded_error(f"transport script unload failed: {exc}"))
            self.script = None
        if self.session is not None:
            try:
                with _frida_cancellable_until(time.monotonic() + 3):
                    self.session.detach()
            except Exception as exc:
                self.cleanup_errors.append(_bounded_error(f"transport detach failed: {exc}"))
            self.session = None
        if self.remote_added and self.manager is not None and self.runtime is not None:
            try:
                with _frida_cancellable_until(time.monotonic() + 3):
                    self.manager.remove_remote_device(self.runtime.address)
            except Exception as exc:
                self.cleanup_errors.append(_bounded_error(f"remote-device cleanup failed: {exc}"))
            self.remote_added = False
        if self.runtime is not None:
            try:
                self.runtime.__exit__(None, None, None)
            except Exception as exc:
                self.cleanup_errors.append(_bounded_error(f"phone runtime cleanup failed: {exc}"))
            self.cleanup_errors.extend(self.runtime.cleanup_errors)
            self.runtime = None
        return self.cleanup_errors

    def close(self, *, suppress_errors: bool = False) -> list[str]:
        self.finalize_transport()
        if self.lock_handle is not None:
            try:
                fcntl.flock(self.lock_handle.fileno(), fcntl.LOCK_UN)
            finally:
                self.lock_handle.close()
                self.lock_handle = None
        if self.cleanup_errors and not suppress_errors:
            raise DeviceTransportError(self.cleanup_errors[0])
        return self.cleanup_errors

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        # Cleanup diagnostics must not overwrite a confirmed write/read-back
        # outcome. Results retain a reference to cleanup_errors for callers.
        self.close(suppress_errors=True)


def device_session(settings: Any, *, timeout_seconds: int = 30, retry_connect: bool = True) -> DeviceSession:
    return DeviceSession(settings, timeout_seconds=timeout_seconds, retry_connect=retry_connect)


def request_device_packet(settings: Any, **kwargs: Any) -> dict[str, Any]:
    timeout = int(kwargs.pop("timeout_seconds", 30))
    retry = bool(kwargs.pop("retry", True))
    with device_session(settings, timeout_seconds=timeout, retry_connect=retry) as session:
        return session.request(
            timeout_seconds=max(0.5, min(8, timeout - 5)), retry=retry, **kwargs
        )


def send_device_packet(settings: Any, **kwargs: Any) -> dict[str, Any]:
    timeout = int(kwargs.pop("timeout_seconds", 30))
    retry = bool(kwargs.pop("retry", False))
    with device_session(settings, timeout_seconds=timeout, retry_connect=retry) as session:
        return session.send(retry=retry, **kwargs)
