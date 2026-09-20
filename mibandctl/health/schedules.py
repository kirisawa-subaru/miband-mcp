"""Xiaomi schedule protocol and guarded alarm/reminder CRUD.

The wire format and command numbers follow Gadgetbridge's current
``XiaomiScheduleService`` and ``xiaomi.proto``.  Writes are always a single
attempt inside one locked device session, bracketed by list reads.  A timeout
after dispatch is therefore reported as an unknown outcome unless readback can
prove the intended state; it is never retried blindly.
"""

from __future__ import annotations

import re
from contextlib import AbstractContextManager
from contextvars import ContextVar, Token
from dataclasses import dataclass
from datetime import datetime, timezone as dt_timezone
from typing import Any, Callable, Iterable, Mapping
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


COMMAND_TYPE = 17

ALARMS_GET = 0
ALARMS_CREATE = 1
ALARMS_EDIT = 2
ALARMS_DELETE = 4

REMINDERS_GET = 14
REMINDERS_CREATE = 15
REMINDERS_EDIT = 17
REMINDERS_DELETE = 18

SCHEDULE_FIELD = 19

REPEAT_ONCE = 0
REPEAT_DAILY = 1
REPEAT_WEEKLY = 5
REPEAT_MONTHLY = 7
REPEAT_YEARLY = 8

SMART_WAKE = 1
NORMAL_ALARM = 2

MAX_TITLE_UTF16_UNITS = 20
MAX_UINT32 = 0xFFFFFFFF
TIME_RE = re.compile(r"^(?:[01][0-9]|2[0-3]):[0-5][0-9]$")

REPEAT_TO_WIRE = {
    "once": REPEAT_ONCE,
    "daily": REPEAT_DAILY,
    "weekly": REPEAT_WEEKLY,
    "monthly": REPEAT_MONTHLY,
    "yearly": REPEAT_YEARLY,
}
WIRE_TO_REPEAT = {value: key for key, value in REPEAT_TO_WIRE.items()}
_CLEANUP_ERRORS: ContextVar[list[str] | None] = ContextVar(
    "schedule_cleanup_errors", default=None
)


class ScheduleProtocolError(RuntimeError):
    """The device or transport returned an invalid schedule packet."""


class ScheduleTransportError(RuntimeError):
    """The schedule request did not produce a usable device response."""


@dataclass(frozen=True)
class _Field:
    number: int
    wire_type: int
    value: int | bytes
    raw: bytes


@dataclass(frozen=True)
class _Alarm:
    item_id: int
    hour: int
    minute: int
    repeat_mode: int
    repeat_flags: int
    enabled: bool
    smart: int
    item_unknown: bytes = b""
    details_unknown: bytes = b""
    time_unknown: bytes = b""


@dataclass(frozen=True)
class _Reminder:
    item_id: int
    at_utc: datetime
    repeat_mode: int
    repeat_flags: int
    title: str
    item_unknown: bytes = b""
    details_unknown: bytes = b""
    date_unknown: bytes = b""
    time_unknown: bytes = b""


@dataclass(frozen=True)
class _Snapshot:
    items: tuple[_Alarm | _Reminder, ...]
    maximum: int | None
    received_at: str | None


def _encode_varint(value: int) -> bytes:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= MAX_UINT32:
        raise ValueError("protobuf uint32 is out of range")
    encoded = bytearray()
    while value >= 0x80:
        encoded.append((value & 0x7F) | 0x80)
        value >>= 7
    encoded.append(value)
    return bytes(encoded)


def _uint(field: int, value: int) -> bytes:
    return _encode_varint(field << 3) + _encode_varint(value)


def _bytes(field: int, value: bytes) -> bytes:
    return _encode_varint((field << 3) | 2) + _encode_varint(len(value)) + value


def _string(field: int, value: str) -> bytes:
    return _bytes(field, value.encode("utf-8"))


def _read_varint(data: bytes, offset: int) -> tuple[int, int]:
    value = 0
    shift = 0
    start = offset
    while offset < len(data) and shift < 70:
        byte = data[offset]
        offset += 1
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return value, offset
        shift += 7
    if offset >= len(data):
        raise ScheduleProtocolError(f"truncated protobuf varint at byte {start}")
    raise ScheduleProtocolError(f"protobuf varint is too long at byte {start}")


def _decode_fields(data: bytes) -> list[_Field]:
    fields: list[_Field] = []
    offset = 0
    while offset < len(data):
        start = offset
        key, offset = _read_varint(data, offset)
        number = key >> 3
        wire_type = key & 7
        if number == 0:
            raise ScheduleProtocolError(f"invalid protobuf field zero at byte {start}")
        if wire_type == 0:
            value, offset = _read_varint(data, offset)
        elif wire_type == 1:
            if offset + 8 > len(data):
                raise ScheduleProtocolError("truncated fixed64 protobuf field")
            value = data[offset : offset + 8]
            offset += 8
        elif wire_type == 2:
            length, offset = _read_varint(data, offset)
            if offset + length > len(data):
                raise ScheduleProtocolError("truncated length-delimited protobuf field")
            value = data[offset : offset + length]
            offset += length
        elif wire_type == 5:
            if offset + 4 > len(data):
                raise ScheduleProtocolError("truncated fixed32 protobuf field")
            value = data[offset : offset + 4]
            offset += 4
        else:
            raise ScheduleProtocolError(f"unsupported protobuf wire type {wire_type}")
        fields.append(_Field(number, wire_type, value, data[start:offset]))
    return fields


def _last(fields: Iterable[_Field], number: int, wire_type: int) -> int | bytes | None:
    matches = [field for field in fields if field.number == number]
    if not matches:
        return None
    matching_field = matches[-1]
    if matching_field.wire_type != wire_type:
        raise ScheduleProtocolError(f"protobuf field {number} has wrong wire type")
    return matching_field.value


def _varint(fields: Iterable[_Field], number: int, default: int | None = None) -> int | None:
    value = _last(fields, number, 0)
    if value is None:
        return default
    if not isinstance(value, int):
        raise ScheduleProtocolError(f"protobuf field {number} is not a varint")
    if not 0 <= value <= MAX_UINT32:
        raise ScheduleProtocolError(f"protobuf field {number} exceeds uint32")
    return value


def _message(fields: Iterable[_Field], number: int) -> bytes | None:
    value = _last(fields, number, 2)
    if value is None:
        return None
    if not isinstance(value, bytes):
        raise ScheduleProtocolError(f"protobuf field {number} is not a message")
    return value


def _required_varint(fields: Iterable[_Field], number: int, label: str) -> int:
    value = _varint(fields, number)
    if value is None:
        raise ScheduleProtocolError(f"missing {label}")
    return value


def _required_message(fields: Iterable[_Field], number: int, label: str) -> bytes:
    value = _message(fields, number)
    if value is None:
        raise ScheduleProtocolError(f"missing {label}")
    return value


def _unknown(fields: Iterable[_Field], known: set[int]) -> bytes:
    return b"".join(field.raw for field in fields if field.number not in known)


def _decode_text(value: bytes, label: str) -> str:
    try:
        return value.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ScheduleProtocolError(f"invalid UTF-8 in {label}") from exc


def _parse_hour_minute(data: bytes) -> tuple[int, int, bytes]:
    fields = _decode_fields(data)
    hour = _required_varint(fields, 1, "alarm hour")
    minute = _required_varint(fields, 2, "alarm minute")
    if hour > 23 or minute > 59:
        raise ScheduleProtocolError(f"invalid alarm time {hour:02d}:{minute:02d}")
    return hour, minute, _unknown(fields, {1, 2})


def _parse_alarm(data: bytes) -> _Alarm:
    fields = _decode_fields(data)
    item_id = _required_varint(fields, 1, "alarm id")
    details_data = _required_message(fields, 2, "alarm details")
    details = _decode_fields(details_data)
    hour, minute, time_unknown = _parse_hour_minute(
        _required_message(details, 2, "alarm time")
    )
    return _Alarm(
        item_id=item_id,
        hour=hour,
        minute=minute,
        repeat_mode=_varint(details, 3, REPEAT_ONCE) or 0,
        repeat_flags=_varint(details, 4, 0) or 0,
        enabled=bool(_varint(details, 5, 0)),
        smart=_varint(details, 7, 0) or 0,
        item_unknown=_unknown(fields, {1, 2}),
        details_unknown=_unknown(details, {2, 3, 4, 5, 7}),
        time_unknown=time_unknown,
    )


def _parse_alarms(data: bytes) -> tuple[tuple[_Alarm, ...], int | None]:
    fields = _decode_fields(data)
    if any(field.number == 1 and field.wire_type != 2 for field in fields):
        raise ScheduleProtocolError("alarm entry has wrong wire type")
    alarms = tuple(
        _parse_alarm(field.value)
        for field in fields
        if field.number == 1 and field.wire_type == 2 and isinstance(field.value, bytes)
    )
    _ensure_unique_ids(alarms, "alarm")
    return tuple(sorted(alarms, key=lambda item: item.item_id)), _varint(fields, 2)


def _parse_date(data: bytes) -> tuple[int, int, int, bytes]:
    fields = _decode_fields(data)
    return (
        _required_varint(fields, 1, "reminder year"),
        _required_varint(fields, 2, "reminder month"),
        _required_varint(fields, 3, "reminder day"),
        _unknown(fields, {1, 2, 3}),
    )


def _parse_time(data: bytes) -> tuple[int, int, int, int, bytes]:
    fields = _decode_fields(data)
    hour = _varint(fields, 1, 0) or 0
    minute = _varint(fields, 2, 0) or 0
    second = _varint(fields, 3, 0) or 0
    millisecond = _varint(fields, 4, 0) or 0
    if hour > 23 or minute > 59 or second > 59 or millisecond > 999:
        raise ScheduleProtocolError("invalid reminder time")
    return hour, minute, second, millisecond, _unknown(fields, {1, 2, 3, 4})


def _parse_reminder(data: bytes) -> _Reminder:
    fields = _decode_fields(data)
    item_id = _required_varint(fields, 1, "reminder id")
    details_data = _required_message(fields, 2, "reminder details")
    details = _decode_fields(details_data)
    year, month, day, date_unknown = _parse_date(
        _required_message(details, 1, "reminder date")
    )
    hour, minute, second, millisecond, time_unknown = _parse_time(
        _required_message(details, 2, "reminder time")
    )
    try:
        at_utc = datetime(
            year,
            month,
            day,
            hour,
            minute,
            second,
            millisecond * 1000,
            tzinfo=dt_timezone.utc,
        )
    except ValueError as exc:
        raise ScheduleProtocolError("invalid reminder date") from exc
    title_data = _message(details, 5)
    title = _decode_text(title_data, "reminder title") if title_data is not None else ""
    return _Reminder(
        item_id=item_id,
        at_utc=at_utc,
        repeat_mode=_varint(details, 3, REPEAT_ONCE) or 0,
        repeat_flags=_varint(details, 4, 0) or 0,
        title=title,
        item_unknown=_unknown(fields, {1, 2}),
        details_unknown=_unknown(details, {1, 2, 3, 4, 5}),
        date_unknown=date_unknown,
        time_unknown=time_unknown,
    )


def _parse_reminders(data: bytes) -> tuple[tuple[_Reminder, ...], int | None]:
    fields = _decode_fields(data)
    if any(field.number == 1 and field.wire_type != 2 for field in fields):
        raise ScheduleProtocolError("reminder entry has wrong wire type")
    reminders = tuple(
        _parse_reminder(field.value)
        for field in fields
        if field.number == 1 and field.wire_type == 2 and isinstance(field.value, bytes)
    )
    _ensure_unique_ids(reminders, "reminder")
    return tuple(sorted(reminders, key=lambda item: item.item_id)), _varint(fields, 2)


def _ensure_unique_ids(items: Iterable[_Alarm | _Reminder], kind: str) -> None:
    ids = [item.item_id for item in items]
    if len(ids) != len(set(ids)):
        raise ScheduleProtocolError(f"device returned duplicate {kind} ids")


def _parse_command(response: bytes, subtype: int) -> tuple[bytes, int | None]:
    fields = _decode_fields(response)
    command_type = _required_varint(fields, 1, "command type")
    command_subtype = _varint(fields, 2, 0)
    if command_type != COMMAND_TYPE or command_subtype != subtype:
        raise ScheduleProtocolError(
            f"unexpected command {command_type}/{command_subtype}, expected {COMMAND_TYPE}/{subtype}"
        )
    status = _varint(fields, 100)
    if status not in (None, 0):
        raise ScheduleProtocolError(f"device returned schedule status {status}")
    schedule = _required_message(fields, SCHEDULE_FIELD, "schedule payload")
    return schedule, status


def _parse_ack(response: bytes, subtype: int) -> int | None:
    schedule_data, _ = _parse_command(response, subtype)
    return _varint(_decode_fields(schedule_data), 4)


def _encode_hour_minute(hour: int, minute: int, unknown: bytes = b"") -> bytes:
    return _uint(1, hour) + _uint(2, minute) + unknown


def _encode_alarm_details(
    hour: int,
    minute: int,
    repeat_mode: int,
    repeat_flags: int,
    enabled: bool,
    smart: int,
    *,
    details_unknown: bytes = b"",
    time_unknown: bytes = b"",
) -> bytes:
    data = _bytes(2, _encode_hour_minute(hour, minute, time_unknown))
    data += _uint(3, repeat_mode)
    if repeat_mode == REPEAT_WEEKLY:
        data += _uint(4, repeat_flags)
    data += _uint(5, int(enabled))
    data += _uint(7, smart)
    return data + details_unknown


def _encode_alarm_create(
    hour: int, minute: int, repeat_mode: int, repeat_flags: int, enabled: bool
) -> bytes:
    details = _encode_alarm_details(
        hour, minute, repeat_mode, repeat_flags, enabled, NORMAL_ALARM
    )
    return _bytes(2, details)


def _encode_alarm_update(
    existing: _Alarm,
    hour: int,
    minute: int,
    repeat_mode: int,
    repeat_flags: int,
    enabled: bool,
) -> bytes:
    details = _encode_alarm_details(
        hour,
        minute,
        repeat_mode,
        repeat_flags,
        enabled,
        existing.smart,
        details_unknown=existing.details_unknown,
        time_unknown=existing.time_unknown,
    )
    alarm = _uint(1, existing.item_id) + _bytes(2, details) + existing.item_unknown
    return _bytes(3, alarm)


def _encode_delete(schedule_field: int, item_id: int) -> bytes:
    return _bytes(schedule_field, _uint(1, item_id))


def _encode_reminder_details(
    at_utc: datetime,
    title: str,
    repeat_mode: int,
    repeat_flags: int,
    *,
    details_unknown: bytes = b"",
    date_unknown: bytes = b"",
    time_unknown: bytes = b"",
) -> bytes:
    date = (
        _uint(1, at_utc.year)
        + _uint(2, at_utc.month)
        + _uint(3, at_utc.day)
        + date_unknown
    )
    time = (
        _uint(1, at_utc.hour)
        + _uint(2, at_utc.minute)
        + _uint(3, at_utc.second)
        + _uint(4, at_utc.microsecond // 1000)
        + time_unknown
    )
    details = _bytes(1, date) + _bytes(2, time) + _uint(3, repeat_mode)
    if repeat_mode == REPEAT_WEEKLY:
        details += _uint(4, repeat_flags)
    return details + _string(5, title) + details_unknown


def _encode_reminder_create(
    at_utc: datetime, title: str, repeat_mode: int, repeat_flags: int
) -> bytes:
    return _bytes(14, _encode_reminder_details(at_utc, title, repeat_mode, repeat_flags))


def _encode_reminder_update(
    existing: _Reminder,
    at_utc: datetime,
    title: str,
    repeat_mode: int,
    repeat_flags: int,
) -> bytes:
    details = _encode_reminder_details(
        at_utc,
        title,
        repeat_mode,
        repeat_flags,
        details_unknown=existing.details_unknown,
        date_unknown=existing.date_unknown,
        time_unknown=existing.time_unknown,
    )
    reminder = _uint(1, existing.item_id) + _bytes(2, details) + existing.item_unknown
    return _bytes(15, reminder)


def _zone(settings: Any, override: str | None) -> tuple[str, ZoneInfo]:
    name = override or getattr(settings, "timezone", None) or "Asia/Shanghai"
    if not isinstance(name, str):
        raise ValueError("timezone must be an IANA timezone name")
    try:
        return name, ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise ValueError(f"unknown timezone: {name}") from exc


def _validate_timeout(timeout_seconds: int) -> int:
    if (
        isinstance(timeout_seconds, bool)
        or not isinstance(timeout_seconds, int)
        or not 5 <= timeout_seconds <= 90
    ):
        raise ValueError("timeout_seconds must be between 5 and 90")
    return timeout_seconds


def _validate_id(item_id: int, label: str) -> int:
    if isinstance(item_id, bool) or not isinstance(item_id, int) or not 0 <= item_id <= MAX_UINT32:
        raise ValueError(f"{label} must be an integer between 0 and {MAX_UINT32}")
    return item_id


def _parse_alarm_input(time: str, weekdays: list[int], enabled: bool) -> tuple[int, int, int, int]:
    if not isinstance(time, str) or not TIME_RE.fullmatch(time):
        raise ValueError("time must be HH:MM in 24-hour band-local time")
    if not isinstance(enabled, bool):
        raise ValueError("enabled must be a boolean")
    if not isinstance(weekdays, list) or any(
        isinstance(day, bool) or not isinstance(day, int) or not 1 <= day <= 7
        for day in weekdays
    ):
        raise ValueError("weekdays must be a list containing only integers 1..7")
    if len(weekdays) != len(set(weekdays)):
        raise ValueError("weekdays must not contain duplicates")
    hour, minute = (int(part) for part in time.split(":"))
    if not weekdays:
        return hour, minute, REPEAT_ONCE, 0
    flags = sum(1 << (day - 1) for day in weekdays)
    if flags == 0x7F:
        return hour, minute, REPEAT_DAILY, 0
    return hour, minute, REPEAT_WEEKLY, flags


def _parse_at(value: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("at must be an offset-aware ISO-8601 timestamp")
    text = value.strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError("at must be a valid ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("at must include an explicit UTC offset")
    if parsed.microsecond % 1000:
        raise ValueError("at precision must not exceed milliseconds")
    return parsed.astimezone(dt_timezone.utc)


def _parse_reminder_input(at: str, title: str, repeat: str) -> tuple[datetime, int, int]:
    at_utc = _parse_at(at)
    if not isinstance(title, str) or not title.strip():
        raise ValueError("title must not be empty")
    if any(ord(char) < 32 or ord(char) == 127 for char in title):
        raise ValueError("title must not contain control characters")
    utf16_units = len(title.encode("utf-16-le")) // 2
    if utf16_units > MAX_TITLE_UTF16_UNITS:
        raise ValueError(f"title must not exceed {MAX_TITLE_UTF16_UNITS} UTF-16 code units")
    if repeat not in REPEAT_TO_WIRE:
        raise ValueError("repeat must be one of: once, daily, weekly, monthly, yearly")
    repeat_mode = REPEAT_TO_WIRE[repeat]
    if repeat_mode == REPEAT_ONCE and at_utc <= datetime.now(dt_timezone.utc):
        raise ValueError("one-time reminder must be in the future")
    # Current Gadgetbridge encodes a single UTC weekday ordinal (Monday=1),
    # despite the older proto comment describing this field as day flags.
    repeat_flags = at_utc.isoweekday() if repeat_mode == REPEAT_WEEKLY else 0
    return at_utc, repeat_mode, repeat_flags


def _session_context(
    settings: Any,
    timeout_seconds: int,
    factory: Callable[..., AbstractContextManager[Any]] | None,
) -> AbstractContextManager[Any]:
    if factory is None:
        from .device_transport import device_session

        factory = device_session
    return _CleanupPreservingContext(
        factory(settings, timeout_seconds=timeout_seconds, retry_connect=True)
    )


class _CleanupPreservingContext:
    """Keep a completed/unknown write result visible if transport cleanup fails."""

    def __init__(self, context: AbstractContextManager[Any]) -> None:
        self.context = context
        self.session: Any = None
        self.token: Token[list[str] | None] | None = None

    def __enter__(self) -> Any:
        self.session = self.context.__enter__()
        if not isinstance(getattr(self.session, "cleanup_errors", None), list):
            self.session.cleanup_errors = []
        self.token = _CLEANUP_ERRORS.set(self.session.cleanup_errors)
        return self.session

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> bool:
        try:
            try:
                return bool(self.context.__exit__(exc_type, exc, traceback))
            except Exception as cleanup_exc:
                error = _bounded_error(cleanup_exc)
                if not self.session.cleanup_errors:
                    self.session.cleanup_errors.append(error)
                # Never replace a business result or the original body
                # exception with a cleanup-only failure.
                return False
        finally:
            if self.token is not None:
                _CLEANUP_ERRORS.reset(self.token)


def _response(result: Mapping[str, Any], label: str) -> tuple[bytes, str | None]:
    if not isinstance(result, Mapping):
        raise ScheduleTransportError(f"{label} returned no structured result")
    if result.get("status") != "ok":
        detail = result.get("error") or result.get("status") or "unknown error"
        raise ScheduleTransportError(f"{label} failed: {detail}")
    response = result.get("response")
    if not isinstance(response, bytes):
        raise ScheduleTransportError(f"{label} returned no response bytes")
    received_at = result.get("received_at")
    return response, received_at if isinstance(received_at, str) else None


def _read_alarms(session: Any) -> _Snapshot:
    result = session.request(
        command_type=COMMAND_TYPE,
        command_subtype=ALARMS_GET,
        response_type=COMMAND_TYPE,
        response_subtype=ALARMS_GET,
    )
    response, received_at = _response(result, "alarm list")
    schedule_data, _ = _parse_command(response, ALARMS_GET)
    alarms_data = _required_message(_decode_fields(schedule_data), 1, "alarms")
    alarms, maximum = _parse_alarms(alarms_data)
    return _Snapshot(alarms, maximum, received_at)


def _read_reminders(session: Any) -> _Snapshot:
    result = session.request(
        command_type=COMMAND_TYPE,
        command_subtype=REMINDERS_GET,
        response_type=COMMAND_TYPE,
        response_subtype=REMINDERS_GET,
    )
    response, received_at = _response(result, "reminder list")
    schedule_data, _ = _parse_command(response, REMINDERS_GET)
    reminders_data = _required_message(_decode_fields(schedule_data), 10, "reminders")
    reminders, maximum = _parse_reminders(reminders_data)
    return _Snapshot(reminders, maximum, received_at)


def _alarm_public(item: _Alarm, zone_name: str) -> dict[str, Any]:
    if item.repeat_mode == REPEAT_ONCE:
        repeat = "once"
        weekdays: list[int] = []
    elif item.repeat_mode == REPEAT_DAILY:
        repeat = "daily"
        weekdays = list(range(1, 8))
    elif item.repeat_mode == REPEAT_WEEKLY:
        repeat = "weekly"
        weekdays = [day for day in range(1, 8) if item.repeat_flags & (1 << (day - 1))]
    else:
        repeat = "unknown"
        weekdays = []
    return {
        "id": item.item_id,
        "time": f"{item.hour:02d}:{item.minute:02d}",
        "weekdays": weekdays,
        "repeat": repeat,
        "enabled": item.enabled,
        "smart_wake": True if item.smart == SMART_WAKE else False if item.smart == NORMAL_ALARM else None,
        "timezone": zone_name,
        "time_basis": "band_local_wall_clock",
        "raw_repeat_mode": item.repeat_mode,
        "raw_repeat_flags": item.repeat_flags,
    }


def _reminder_public(item: _Reminder, zone_name: str, zone: ZoneInfo) -> dict[str, Any]:
    local = item.at_utc.astimezone(zone)
    timespec = "milliseconds" if item.at_utc.microsecond else "seconds"
    result = {
        "id": item.item_id,
        "at": local.isoformat(timespec=timespec),
        "title": item.title,
        "repeat": WIRE_TO_REPEAT.get(item.repeat_mode, "unknown"),
        "timezone": zone_name,
        "time_basis": "utc_instant",
        "raw_repeat_mode": item.repeat_mode,
        "raw_repeat_flags": item.repeat_flags,
    }
    if item.repeat_mode == REPEAT_WEEKLY:
        result["weekly_day_utc"] = item.repeat_flags if 1 <= item.repeat_flags <= 7 else None
    return result


def _list_result(
    kind: str,
    zone_name: str,
    zone: ZoneInfo,
    alarms: _Snapshot | None,
    reminders: _Snapshot | None,
) -> dict[str, Any]:
    cleanup_errors = _CLEANUP_ERRORS.get()
    result: dict[str, Any] = {
        "status": "ok",
        "kind": kind,
        "source": "band_realtime_protocol",
        "support_status": "device_confirmed",
        "timezone": zone_name,
        "capacities": {},
        "cleanup_errors": cleanup_errors if cleanup_errors is not None else [],
    }
    observed: list[str] = []
    if alarms is not None:
        alarm_items = [_alarm_public(item, zone_name) for item in alarms.items if isinstance(item, _Alarm)]
        result["alarms"] = alarm_items
        result["capacities"]["alarms"] = {
            "used": len(alarm_items),
            "maximum": alarms.maximum,
            "remaining": None if alarms.maximum is None else max(alarms.maximum - len(alarm_items), 0),
        }
        if alarms.received_at:
            observed.append(alarms.received_at)
    if reminders is not None:
        reminder_items = [
            _reminder_public(item, zone_name, zone)
            for item in reminders.items
            if isinstance(item, _Reminder)
        ]
        result["reminders"] = reminder_items
        result["capacities"]["reminders"] = {
            "used": len(reminder_items),
            "maximum": reminders.maximum,
            "remaining": None
            if reminders.maximum is None
            else max(reminders.maximum - len(reminder_items), 0),
        }
        if reminders.received_at:
            observed.append(reminders.received_at)
    result["observed_at"] = observed[-1] if observed else None
    return result


def get_band_schedule(
    settings: Any,
    *,
    kind: str = "all",
    timezone: str | None = None,
    timeout_seconds: int = 30,
    _session_factory: Callable[..., AbstractContextManager[Any]] | None = None,
) -> dict[str, Any]:
    """Read alarms and/or reminders directly from the band."""

    if kind not in {"all", "alarms", "reminders"}:
        raise ValueError("kind must be one of: all, alarms, reminders")
    timeout_seconds = _validate_timeout(timeout_seconds)
    zone_name, zone = _zone(settings, timezone)
    with _session_context(settings, timeout_seconds, _session_factory) as session:
        alarms = _read_alarms(session) if kind in {"all", "alarms"} else None
        reminders = _read_reminders(session) if kind in {"all", "reminders"} else None
        return _list_result(kind, zone_name, zone, alarms, reminders)


def _ensure_capacity(snapshot: _Snapshot, kind: str) -> None:
    if snapshot.maximum is None:
        raise ScheduleProtocolError(f"device did not report {kind} capacity")
    if snapshot.maximum == 0:
        raise ValueError(f"device reports no supported {kind} slots")
    if len(snapshot.items) >= snapshot.maximum:
        raise ValueError(f"device {kind} capacity is full ({snapshot.maximum})")


def _find(items: Iterable[_Alarm | _Reminder], item_id: int) -> _Alarm | _Reminder | None:
    return next((item for item in items if item.item_id == item_id), None)


def _alarm_matches(
    item: _Alarm, hour: int, minute: int, repeat_mode: int, repeat_flags: int, enabled: bool
) -> bool:
    return (
        item.hour == hour
        and item.minute == minute
        and item.repeat_mode == repeat_mode
        and (repeat_mode != REPEAT_WEEKLY or item.repeat_flags == repeat_flags)
        and item.enabled == enabled
    )


def _reminder_matches(
    item: _Reminder, at_utc: datetime, title: str, repeat_mode: int, repeat_flags: int
) -> bool:
    return (
        item.at_utc == at_utc
        and item.title == title
        and item.repeat_mode == repeat_mode
        and (repeat_mode != REPEAT_WEEKLY or item.repeat_flags == repeat_flags)
    )


def _bounded_error(exc: Exception) -> str:
    return " ".join(f"{type(exc).__name__}: {exc}".split())[:500]


def _write_result_error(result: Any) -> str | None:
    if not isinstance(result, Mapping):
        return "transport returned no structured write result"
    if result.get("status") in {"ok", "sent"}:
        return None
    return str(result.get("error") or result.get("status") or "write failed")[:500]


def _readback(
    session: Any,
    reader: Callable[[Any], _Snapshot],
    matcher: Callable[[_Snapshot], Any | None],
) -> tuple[_Snapshot | None, Any | None, list[str]]:
    """Try two read-only confirmations without ever repeating the write."""

    latest = None
    errors: list[str] = []
    for _ in range(2):
        try:
            latest = reader(session)
        except Exception as exc:
            errors.append(_bounded_error(exc))
            continue
        match = matcher(latest)
        if match is not None:
            return latest, match, errors
    return latest, None, errors


def _operation_result(
    *,
    status: str,
    operation: str,
    kind: str,
    zone_name: str,
    item: dict[str, Any] | None,
    observed_at: str | None,
    write_acknowledged: bool | None,
    write_error: str | None,
    matched_existing_ids: list[int] | None = None,
) -> dict[str, Any]:
    cleanup_errors = _CLEANUP_ERRORS.get()
    result: dict[str, Any] = {
        "status": status,
        "operation": operation,
        "kind": kind,
        "source": "band_realtime_protocol",
        "timezone": zone_name,
        "item": item,
        "readback_confirmed": status in {"ok", "unchanged", "already_present", "already_absent"},
        "write_acknowledged": write_acknowledged,
        "write_error": write_error,
        "observed_at": observed_at,
        "retry_safe": status != "outcome_unknown",
        "cleanup_errors": cleanup_errors if cleanup_errors is not None else [],
    }
    if matched_existing_ids is not None:
        result["matched_existing_ids"] = matched_existing_ids
    return result


def set_band_alarm(
    settings: Any,
    *,
    time: str,
    weekdays: list[int],
    enabled: bool = True,
    alarm_id: int | None = None,
    timezone: str | None = None,
    timeout_seconds: int = 30,
    _session_factory: Callable[..., AbstractContextManager[Any]] | None = None,
) -> dict[str, Any]:
    """Create or fully replace one alarm while preserving device-only fields."""

    hour, minute, repeat_mode, repeat_flags = _parse_alarm_input(time, weekdays, enabled)
    if alarm_id is not None:
        alarm_id = _validate_id(alarm_id, "alarm_id")
    timeout_seconds = _validate_timeout(timeout_seconds)
    zone_name, _ = _zone(settings, timezone)

    with _session_context(settings, timeout_seconds, _session_factory) as session:
        before = _read_alarms(session)
        existing = _find(before.items, alarm_id) if alarm_id is not None else None
        if alarm_id is not None and not isinstance(existing, _Alarm):
            raise ValueError(f"alarm_id {alarm_id} does not exist on the band")

        if alarm_id is None:
            matches = [
                item
                for item in before.items
                if isinstance(item, _Alarm)
                and _alarm_matches(item, hour, minute, repeat_mode, repeat_flags, enabled)
            ]
            if matches:
                item = matches[0]
                return _operation_result(
                    status="already_present",
                    operation="create",
                    kind="alarm",
                    zone_name=zone_name,
                    item=_alarm_public(item, zone_name),
                    observed_at=before.received_at,
                    write_acknowledged=None,
                    write_error=None,
                    matched_existing_ids=[match.item_id for match in matches],
                )
            _ensure_capacity(before, "alarm")
            subtype = ALARMS_CREATE
            payload = _encode_alarm_create(
                hour, minute, repeat_mode, repeat_flags, enabled
            )
            operation = "create"
        else:
            assert isinstance(existing, _Alarm)
            if _alarm_matches(existing, hour, minute, repeat_mode, repeat_flags, enabled):
                return _operation_result(
                    status="unchanged",
                    operation="update",
                    kind="alarm",
                    zone_name=zone_name,
                    item=_alarm_public(existing, zone_name),
                    observed_at=before.received_at,
                    write_acknowledged=None,
                    write_error=None,
                )
            subtype = ALARMS_EDIT
            payload = _encode_alarm_update(
                existing, hour, minute, repeat_mode, repeat_flags, enabled
            )
            operation = "update"

        write_error = None
        ack_id = None
        acknowledged: bool | None = None
        try:
            if operation == "create":
                write_result = session.request(
                    command_type=COMMAND_TYPE,
                    command_subtype=subtype,
                    payload=payload,
                    payload_field=SCHEDULE_FIELD,
                    response_type=COMMAND_TYPE,
                    response_subtype=subtype,
                    retry=False,
                )
                response, _ = _response(write_result, "alarm create")
                ack_id = _parse_ack(response, subtype)
                acknowledged = True
            else:
                write_result = session.send(
                    command_type=COMMAND_TYPE,
                    command_subtype=subtype,
                    payload=payload,
                    payload_field=SCHEDULE_FIELD,
                    retry=False,
                )
                write_error = _write_result_error(write_result)
                acknowledged = None
        except Exception as exc:
            write_error = _bounded_error(exc)
            acknowledged = False if operation == "create" else None

        if operation == "update":
            def match_alarm(snapshot: _Snapshot) -> _Alarm | None:
                candidate = _find(snapshot.items, alarm_id) if alarm_id is not None else None
                return (
                    candidate
                    if isinstance(candidate, _Alarm)
                    and _alarm_matches(
                        candidate, hour, minute, repeat_mode, repeat_flags, enabled
                    )
                    else None
                )
        else:
            before_ids = {item.item_id for item in before.items}

            def match_alarm(snapshot: _Snapshot) -> _Alarm | None:
                candidates = [
                    item
                    for item in snapshot.items
                    if isinstance(item, _Alarm)
                    and item.item_id not in before_ids
                    and _alarm_matches(
                        item, hour, minute, repeat_mode, repeat_flags, enabled
                    )
                    and (ack_id is None or item.item_id == ack_id)
                ]
                return candidates[0] if len(candidates) == 1 else None

        after, confirmed, read_errors = _readback(session, _read_alarms, match_alarm)
        if isinstance(confirmed, _Alarm):
            return _operation_result(
                status="ok",
                operation=operation,
                kind="alarm",
                zone_name=zone_name,
                item=_alarm_public(confirmed, zone_name),
                observed_at=after.received_at if after else None,
                write_acknowledged=acknowledged,
                write_error=write_error,
            )
        return _operation_result(
            status="outcome_unknown",
            operation=operation,
            kind="alarm",
            zone_name=zone_name,
            item=None,
            observed_at=after.received_at if after else None,
            write_acknowledged=acknowledged,
            write_error=write_error
            or (read_errors[-1] if read_errors else "write was not confirmed by device readback"),
        )


def set_band_reminder(
    settings: Any,
    *,
    at: str,
    title: str,
    repeat: str = "once",
    reminder_id: int | None = None,
    timezone: str | None = None,
    timeout_seconds: int = 30,
    _session_factory: Callable[..., AbstractContextManager[Any]] | None = None,
) -> dict[str, Any]:
    """Create or fully replace one UTC-encoded reminder."""

    at_utc, repeat_mode, repeat_flags = _parse_reminder_input(at, title, repeat)
    if reminder_id is not None:
        reminder_id = _validate_id(reminder_id, "reminder_id")
    timeout_seconds = _validate_timeout(timeout_seconds)
    zone_name, zone = _zone(settings, timezone)

    with _session_context(settings, timeout_seconds, _session_factory) as session:
        before = _read_reminders(session)
        existing = _find(before.items, reminder_id) if reminder_id is not None else None
        if reminder_id is not None and not isinstance(existing, _Reminder):
            raise ValueError(f"reminder_id {reminder_id} does not exist on the band")

        if reminder_id is None:
            matches = [
                item
                for item in before.items
                if isinstance(item, _Reminder)
                and _reminder_matches(item, at_utc, title, repeat_mode, repeat_flags)
            ]
            if matches:
                item = matches[0]
                return _operation_result(
                    status="already_present",
                    operation="create",
                    kind="reminder",
                    zone_name=zone_name,
                    item=_reminder_public(item, zone_name, zone),
                    observed_at=before.received_at,
                    write_acknowledged=None,
                    write_error=None,
                    matched_existing_ids=[match.item_id for match in matches],
                )
            _ensure_capacity(before, "reminder")
            subtype = REMINDERS_CREATE
            payload = _encode_reminder_create(at_utc, title, repeat_mode, repeat_flags)
            operation = "create"
        else:
            assert isinstance(existing, _Reminder)
            if _reminder_matches(existing, at_utc, title, repeat_mode, repeat_flags):
                return _operation_result(
                    status="unchanged",
                    operation="update",
                    kind="reminder",
                    zone_name=zone_name,
                    item=_reminder_public(existing, zone_name, zone),
                    observed_at=before.received_at,
                    write_acknowledged=None,
                    write_error=None,
                )
            subtype = REMINDERS_EDIT
            payload = _encode_reminder_update(
                existing, at_utc, title, repeat_mode, repeat_flags
            )
            operation = "update"

        write_error = None
        ack_id = None
        acknowledged: bool | None = None
        try:
            if operation == "create":
                write_result = session.request(
                    command_type=COMMAND_TYPE,
                    command_subtype=subtype,
                    payload=payload,
                    payload_field=SCHEDULE_FIELD,
                    response_type=COMMAND_TYPE,
                    response_subtype=subtype,
                    retry=False,
                )
                response, _ = _response(write_result, "reminder create")
                ack_id = _parse_ack(response, subtype)
                acknowledged = True
            else:
                write_result = session.send(
                    command_type=COMMAND_TYPE,
                    command_subtype=subtype,
                    payload=payload,
                    payload_field=SCHEDULE_FIELD,
                    retry=False,
                )
                write_error = _write_result_error(write_result)
        except Exception as exc:
            write_error = _bounded_error(exc)
            acknowledged = False if operation == "create" else None

        if operation == "update":
            def match_reminder(snapshot: _Snapshot) -> _Reminder | None:
                candidate = _find(snapshot.items, reminder_id) if reminder_id is not None else None
                return (
                    candidate
                    if isinstance(candidate, _Reminder)
                    and _reminder_matches(
                        candidate, at_utc, title, repeat_mode, repeat_flags
                    )
                    else None
                )
        else:
            before_ids = {item.item_id for item in before.items}

            def match_reminder(snapshot: _Snapshot) -> _Reminder | None:
                candidates = [
                    item
                    for item in snapshot.items
                    if isinstance(item, _Reminder)
                    and item.item_id not in before_ids
                    and _reminder_matches(
                        item, at_utc, title, repeat_mode, repeat_flags
                    )
                    and (ack_id is None or item.item_id == ack_id)
                ]
                return candidates[0] if len(candidates) == 1 else None

        after, confirmed, read_errors = _readback(
            session, _read_reminders, match_reminder
        )
        if isinstance(confirmed, _Reminder):
            return _operation_result(
                status="ok",
                operation=operation,
                kind="reminder",
                zone_name=zone_name,
                item=_reminder_public(confirmed, zone_name, zone),
                observed_at=after.received_at if after else None,
                write_acknowledged=acknowledged,
                write_error=write_error,
            )
        return _operation_result(
            status="outcome_unknown",
            operation=operation,
            kind="reminder",
            zone_name=zone_name,
            item=None,
            observed_at=after.received_at if after else None,
            write_acknowledged=acknowledged,
            write_error=write_error
            or (read_errors[-1] if read_errors else "write was not confirmed by device readback"),
        )


def delete_band_schedule(
    settings: Any,
    *,
    kind: str,
    item_id: int,
    timezone: str | None = None,
    timeout_seconds: int = 30,
    _session_factory: Callable[..., AbstractContextManager[Any]] | None = None,
) -> dict[str, Any]:
    """Delete exactly one alarm or reminder and confirm its absence."""

    if kind not in {"alarm", "reminder"}:
        raise ValueError("kind must be one of: alarm, reminder")
    item_id = _validate_id(item_id, "item_id")
    timeout_seconds = _validate_timeout(timeout_seconds)
    zone_name, zone = _zone(settings, timezone)
    reader = _read_alarms if kind == "alarm" else _read_reminders
    subtype = ALARMS_DELETE if kind == "alarm" else REMINDERS_DELETE
    schedule_field = 5 if kind == "alarm" else 17

    with _session_context(settings, timeout_seconds, _session_factory) as session:
        before = reader(session)
        existing = _find(before.items, item_id)
        if existing is None:
            return _operation_result(
                status="already_absent",
                operation="delete",
                kind=kind,
                zone_name=zone_name,
                item=None,
                observed_at=before.received_at,
                write_acknowledged=None,
                write_error=None,
            )
        payload = _encode_delete(schedule_field, item_id)
        write_error = None
        try:
            write_result = session.send(
                command_type=COMMAND_TYPE,
                command_subtype=subtype,
                payload=payload,
                payload_field=SCHEDULE_FIELD,
                retry=False,
            )
            write_error = _write_result_error(write_result)
        except Exception as exc:
            write_error = _bounded_error(exc)

        after, absent, read_errors = _readback(
            session,
            reader,
            lambda snapshot: True if _find(snapshot.items, item_id) is None else None,
        )
        if absent is True:
            return _operation_result(
                status="ok",
                operation="delete",
                kind=kind,
                zone_name=zone_name,
                item=None,
                observed_at=after.received_at if after else None,
                write_acknowledged=None,
                write_error=write_error,
            )
        item = (
            _alarm_public(existing, zone_name)
            if isinstance(existing, _Alarm)
            else _reminder_public(existing, zone_name, zone)
        )
        return _operation_result(
            status="outcome_unknown",
            operation="delete",
            kind=kind,
            zone_name=zone_name,
            item=item,
            observed_at=after.received_at if after else None,
            write_acknowledged=None,
            write_error=write_error
            or (read_errors[-1] if read_errors else "item remains after device readback"),
        )
