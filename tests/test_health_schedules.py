from __future__ import annotations

import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

from mibandctl.health import schedules as s


def command(subtype: int, schedule: bytes, *, status: int | None = None) -> bytes:
    result = s._uint(1, s.COMMAND_TYPE) + s._uint(2, subtype) + s._bytes(s.SCHEDULE_FIELD, schedule)
    if status is not None:
        result += s._uint(100, status)
    return result


def alarm_message(
    item_id: int,
    hour: int,
    minute: int,
    *,
    mode: int = s.REPEAT_ONCE,
    flags: int = 0,
    enabled: bool = True,
    smart: int = s.NORMAL_ALARM,
    item_unknown: bytes = b"",
    details_unknown: bytes = b"",
    time_unknown: bytes = b"",
) -> bytes:
    details = s._encode_alarm_details(
        hour,
        minute,
        mode,
        flags,
        enabled,
        smart,
        details_unknown=details_unknown,
        time_unknown=time_unknown,
    )
    return s._uint(1, item_id) + s._bytes(2, details) + item_unknown


def alarms_response(alarms: list[bytes], maximum: int = 10) -> bytes:
    body = b"".join(s._bytes(1, alarm) for alarm in alarms) + s._uint(2, maximum)
    return command(s.ALARMS_GET, s._bytes(1, body))


def reminder_message(
    item_id: int,
    at_utc: datetime,
    title: str,
    *,
    mode: int = s.REPEAT_ONCE,
    flags: int = 0,
    item_unknown: bytes = b"",
    details_unknown: bytes = b"",
    date_unknown: bytes = b"",
    time_unknown: bytes = b"",
) -> bytes:
    details = s._encode_reminder_details(
        at_utc,
        title,
        mode,
        flags,
        details_unknown=details_unknown,
        date_unknown=date_unknown,
        time_unknown=time_unknown,
    )
    return s._uint(1, item_id) + s._bytes(2, details) + item_unknown


def reminders_response(reminders: list[bytes], maximum: int = 50) -> bytes:
    body = b"".join(s._bytes(1, reminder) for reminder in reminders) + s._uint(2, maximum)
    return command(s.REMINDERS_GET, s._bytes(10, body))


def ack_response(subtype: int, item_id: int) -> bytes:
    return command(subtype, s._uint(4, item_id), status=0)


class FakeSession:
    def __init__(
        self,
        request_results: list[Any],
        send_results: list[Any] | None = None,
        *,
        cleanup_failure: bool = False,
    ):
        self.request_results = list(request_results)
        self.send_results = list(send_results or [])
        self.requests: list[dict[str, Any]] = []
        self.sends: list[dict[str, Any]] = []
        self.cleanup_errors: list[str] = []
        self.cleanup_failure = cleanup_failure

    def __enter__(self) -> "FakeSession":
        return self

    def __exit__(self, *_args: Any) -> None:
        if self.cleanup_failure:
            self.cleanup_errors.append("transport detach failed")
            raise RuntimeError("transport detach failed")
        return None

    def request(self, **kwargs: Any) -> dict[str, Any]:
        self.requests.append(kwargs)
        if not self.request_results:
            raise AssertionError("unexpected request")
        result = self.request_results.pop(0)
        if isinstance(result, Exception):
            raise result
        if isinstance(result, bytes):
            return {
                "status": "ok",
                "response": result,
                "received_at": "2026-09-21T01:02:03Z",
            }
        return result

    def send(self, **kwargs: Any) -> dict[str, Any]:
        self.sends.append(kwargs)
        if not self.send_results:
            return {"status": "ok", "sent": True}
        result = self.send_results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


def factory_for(session: FakeSession):
    def factory(_settings: Any, **_kwargs: Any) -> FakeSession:
        return session

    return factory


class ScheduleCodecTests(unittest.TestCase):
    def test_alarm_and_reminder_ids_may_start_at_zero(self) -> None:
        alarm = s._parse_alarm(alarm_message(0, 5, 27))
        reminder = s._parse_reminder(
            reminder_message(0, datetime(2030, 1, 6, 16, 30, tzinfo=timezone.utc), "Temp")
        )
        self.assertEqual(alarm.item_id, 0)
        self.assertEqual(reminder.item_id, 0)
        self.assertEqual(s._validate_id(0, "item_id"), 0)

    def test_parse_command_defaults_omitted_subtype_to_zero(self) -> None:
        response = s._uint(1, s.COMMAND_TYPE) + s._bytes(s.SCHEDULE_FIELD, b"")
        self.assertEqual(s._parse_command(response, s.ALARMS_GET), (b"", None))

    def test_alarm_details_matches_protobuf_golden_bytes(self) -> None:
        encoded = s._encode_alarm_details(
            7, 30, s.REPEAT_WEEKLY, 0x1F, True, s.NORMAL_ALARM
        )
        self.assertEqual(encoded.hex(), "12040807101e1805201f28013802")

    def test_list_decodes_capacity_alarm_wall_time_and_reminder_utc(self) -> None:
        alarm = alarm_message(
            2, 7, 30, mode=s.REPEAT_WEEKLY, flags=0x1F, enabled=True
        )
        reminder = reminder_message(
            9,
            datetime(2030, 1, 6, 16, 30, tzinfo=timezone.utc),
            "Drink",
            mode=s.REPEAT_WEEKLY,
            flags=7,
        )
        session = FakeSession(
            [alarms_response([alarm], 10), reminders_response([reminder], 50)]
        )
        result = s.get_band_schedule(
            SimpleNamespace(timezone="Asia/Shanghai"),
            _session_factory=factory_for(session),
        )

        self.assertEqual(result["alarms"][0]["time"], "07:30")
        self.assertEqual(result["alarms"][0]["weekdays"], [1, 2, 3, 4, 5])
        self.assertEqual(result["alarms"][0]["time_basis"], "band_local_wall_clock")
        self.assertEqual(result["reminders"][0]["at"], "2030-01-07T00:30:00+08:00")
        self.assertEqual(result["reminders"][0]["weekly_day_utc"], 7)
        self.assertEqual(result["reminders"][0]["time_basis"], "utc_instant")
        self.assertEqual(result["capacities"]["alarms"], {"used": 1, "maximum": 10, "remaining": 9})
        self.assertEqual(result["capacities"]["reminders"], {"used": 1, "maximum": 50, "remaining": 49})

    def test_missing_schedule_container_is_protocol_error_not_empty_list(self) -> None:
        session = FakeSession([command(s.ALARMS_GET, b"")])
        with self.assertRaises(s.ScheduleProtocolError):
            s.get_band_schedule(
                SimpleNamespace(timezone="Asia/Shanghai"),
                kind="alarms",
                _session_factory=factory_for(session),
            )


class ScheduleMutationTests(unittest.TestCase):
    settings = SimpleNamespace(timezone="Asia/Shanghai")

    def test_create_alarm_timeout_is_not_retried_and_readback_can_confirm(self) -> None:
        created = alarm_message(
            3, 6, 45, mode=s.REPEAT_WEEKLY, flags=(1 | 4), enabled=True
        )
        session = FakeSession(
            [
                alarms_response([], 10),
                TimeoutError("ack lost"),
                alarms_response([created], 10),
            ]
        )
        result = s.set_band_alarm(
            self.settings,
            time="06:45",
            weekdays=[1, 3],
            _session_factory=factory_for(session),
        )

        self.assertEqual(result["status"], "ok")
        self.assertTrue(result["readback_confirmed"])
        self.assertFalse(result["write_acknowledged"])
        self.assertEqual(len(session.requests), 3)
        self.assertIs(session.requests[1]["retry"], False)
        self.assertEqual(session.requests[1]["command_subtype"], s.ALARMS_CREATE)
        self.assertEqual(session.sends, [])

    def test_unconfirmed_create_reports_unknown_and_is_not_retry_safe(self) -> None:
        session = FakeSession(
            [
                alarms_response([], 10),
                TimeoutError("ack lost"),
                alarms_response([], 10),
                alarms_response([], 10),
            ]
        )
        result = s.set_band_alarm(
            self.settings,
            time="06:45",
            weekdays=[],
            _session_factory=factory_for(session),
        )
        self.assertEqual(result["status"], "outcome_unknown")
        self.assertFalse(result["retry_safe"])
        self.assertEqual(len(session.requests), 4)
        self.assertIs(session.requests[1]["retry"], False)

    def test_cleanup_failure_does_not_hide_confirmed_write(self) -> None:
        created = alarm_message(3, 6, 45)
        session = FakeSession(
            [
                alarms_response([], 10),
                ack_response(s.ALARMS_CREATE, 3),
                alarms_response([created], 10),
            ],
            cleanup_failure=True,
        )
        result = s.set_band_alarm(
            self.settings,
            time="06:45",
            weekdays=[],
            _session_factory=factory_for(session),
        )
        self.assertEqual(result["status"], "ok")
        self.assertTrue(result["readback_confirmed"])
        self.assertEqual(result["cleanup_errors"], ["transport detach failed"])

    def test_create_short_circuits_matching_existing_alarm(self) -> None:
        existing = alarm_message(4, 9, 0, mode=s.REPEAT_DAILY, enabled=True)
        session = FakeSession([alarms_response([existing], 10)])
        result = s.set_band_alarm(
            self.settings,
            time="09:00",
            weekdays=[1, 2, 3, 4, 5, 6, 7],
            _session_factory=factory_for(session),
        )
        self.assertEqual(result["status"], "already_present")
        self.assertEqual(result["matched_existing_ids"], [4])
        self.assertEqual(len(session.requests), 1)
        self.assertEqual(session.sends, [])

    def test_alarm_update_preserves_smart_and_unknown_fields(self) -> None:
        item_unknown = s._uint(9, 42)
        details_unknown = s._bytes(9, b"future")
        time_unknown = s._uint(3, 9)
        before = alarm_message(
            1,
            7,
            30,
            enabled=True,
            smart=s.SMART_WAKE,
            item_unknown=item_unknown,
            details_unknown=details_unknown,
            time_unknown=time_unknown,
        )
        after = alarm_message(
            1,
            8,
            15,
            mode=s.REPEAT_WEEKLY,
            flags=0x1F,
            enabled=False,
            smart=s.SMART_WAKE,
            item_unknown=item_unknown,
            details_unknown=details_unknown,
            time_unknown=time_unknown,
        )
        session = FakeSession(
            [alarms_response([before]), alarms_response([after])]
        )
        result = s.set_band_alarm(
            self.settings,
            alarm_id=1,
            time="08:15",
            weekdays=[1, 2, 3, 4, 5],
            enabled=False,
            _session_factory=factory_for(session),
        )
        self.assertEqual(result["status"], "ok")
        self.assertEqual(len(session.sends), 1)
        sent = session.sends[0]
        self.assertEqual(sent["command_subtype"], s.ALARMS_EDIT)
        self.assertIs(sent["retry"], False)
        schedule = s._decode_fields(sent["payload"])
        alarm = s._required_message(schedule, 3, "edited alarm")
        alarm_fields = s._decode_fields(alarm)
        details = s._required_message(alarm_fields, 2, "alarm details")
        details_fields = s._decode_fields(details)
        time_data = s._required_message(details_fields, 2, "alarm time")
        self.assertIn(item_unknown, alarm)
        self.assertIn(details_unknown, details)
        self.assertIn(time_unknown, time_data)
        self.assertEqual(s._varint(details_fields, 7), s.SMART_WAKE)

    def test_reminder_create_encodes_utc_and_utc_weekday_without_retry(self) -> None:
        at_utc = datetime(2030, 1, 6, 16, 30, tzinfo=timezone.utc)
        created = reminder_message(
            12, at_utc, "Call", mode=s.REPEAT_WEEKLY, flags=7
        )
        session = FakeSession(
            [
                reminders_response([], 50),
                ack_response(s.REMINDERS_CREATE, 12),
                reminders_response([created], 50),
            ]
        )
        result = s.set_band_reminder(
            self.settings,
            at="2030-01-07T00:30:00+08:00",
            title="Call",
            repeat="weekly",
            _session_factory=factory_for(session),
        )
        self.assertEqual(result["status"], "ok")
        create = session.requests[1]
        self.assertIs(create["retry"], False)
        self.assertEqual(create["command_subtype"], s.REMINDERS_CREATE)
        schedule = s._decode_fields(create["payload"])
        details = s._required_message(schedule, 14, "created reminder")
        parsed = s._parse_reminder(s._uint(1, 1) + s._bytes(2, details))
        self.assertEqual(parsed.at_utc, at_utc)
        self.assertEqual(parsed.repeat_flags, 7)

    def test_delete_contains_only_target_id_and_preserves_other_entries(self) -> None:
        first = alarm_message(1, 7, 0)
        second = alarm_message(2, 8, 0)
        session = FakeSession(
            [alarms_response([first, second]), alarms_response([second])]
        )
        result = s.delete_band_schedule(
            self.settings,
            kind="alarm",
            item_id=1,
            _session_factory=factory_for(session),
        )
        self.assertEqual(result["status"], "ok")
        self.assertEqual(len(session.sends), 1)
        sent = session.sends[0]
        self.assertEqual(sent["command_subtype"], s.ALARMS_DELETE)
        self.assertIs(sent["retry"], False)
        schedule = s._decode_fields(sent["payload"])
        deletion = s._required_message(schedule, 5, "alarm delete")
        fields = s._decode_fields(deletion)
        self.assertEqual([field.value for field in fields if field.number == 1], [1])
        self.assertFalse(any(field.number != 1 for field in fields))

    def test_capacity_and_input_validation_happen_before_write(self) -> None:
        full = alarm_message(1, 7, 0)
        session = FakeSession([alarms_response([full], maximum=1)])
        with self.assertRaisesRegex(ValueError, "capacity is full"):
            s.set_band_alarm(
                self.settings,
                time="08:00",
                weekdays=[],
                _session_factory=factory_for(session),
            )
        self.assertEqual(session.sends, [])

        invalid_calls = (
            lambda: s.set_band_alarm(self.settings, time="8:00", weekdays=[]),
            lambda: s.set_band_alarm(self.settings, time="08:00", weekdays=[1, 1]),
            lambda: s.set_band_reminder(
                self.settings, at="2030-01-01T09:00:00", title="x"
            ),
            lambda: s.set_band_reminder(
                self.settings,
                at="2030-01-01T09:00:00+08:00",
                title="😀" * 11,
            ),
        )
        for call in invalid_calls:
            with self.subTest(call=call), self.assertRaises(ValueError):
                call()


if __name__ == "__main__":
    unittest.main()
