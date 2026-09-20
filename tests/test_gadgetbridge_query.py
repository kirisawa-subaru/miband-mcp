from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest import mock
from zoneinfo import ZoneInfo

from mibandctl.gadgetbridge.query import (
    GadgetbridgeSchemaError,
    get_current_state,
    get_daily_report,
    query_health,
)


SCHEMA = """
CREATE TABLE DEVICE (
    _id INTEGER PRIMARY KEY,
    NAME TEXT NOT NULL,
    MANUFACTURER TEXT NOT NULL,
    IDENTIFIER TEXT NOT NULL UNIQUE,
    TYPE INTEGER NOT NULL,
    TYPE_NAME TEXT NOT NULL,
    MODEL TEXT,
    ALIAS TEXT
);
CREATE TABLE XIAOMI_ACTIVITY_SAMPLE (
    TIMESTAMP INTEGER NOT NULL,
    DEVICE_ID INTEGER NOT NULL,
    USER_ID INTEGER NOT NULL DEFAULT 1,
    RAW_INTENSITY INTEGER NOT NULL DEFAULT 0,
    STEPS INTEGER NOT NULL,
    RAW_KIND INTEGER NOT NULL DEFAULT 0,
    HEART_RATE INTEGER NOT NULL,
    DISTANCE_CM INTEGER NOT NULL,
    ACTIVE_CALORIES INTEGER NOT NULL,
    PRIMARY KEY (TIMESTAMP, DEVICE_ID)
);
CREATE TABLE XIAOMI_DAILY_SUMMARY_SAMPLE (
    TIMESTAMP INTEGER NOT NULL,
    DEVICE_ID INTEGER NOT NULL,
    USER_ID INTEGER NOT NULL DEFAULT 1,
    TIMEZONE INTEGER,
    STEPS INTEGER,
    HR_RESTING INTEGER,
    HR_MAX INTEGER,
    HR_MIN INTEGER,
    HR_AVG INTEGER,
    CALORIES INTEGER,
    ACTIVE_CALORIES INTEGER,
    PRIMARY KEY (TIMESTAMP, DEVICE_ID)
);
CREATE TABLE XIAOMI_MANUAL_SAMPLE (
    TIMESTAMP INTEGER NOT NULL,
    DEVICE_ID INTEGER NOT NULL,
    USER_ID INTEGER NOT NULL DEFAULT 1,
    TYPE INTEGER,
    VALUE INTEGER,
    PRIMARY KEY (TIMESTAMP, DEVICE_ID)
);
CREATE TABLE BATTERY_LEVEL (
    TIMESTAMP INTEGER NOT NULL,
    DEVICE_ID INTEGER NOT NULL,
    LEVEL INTEGER NOT NULL,
    BATTERY_INDEX INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (TIMESTAMP, DEVICE_ID, BATTERY_INDEX)
);
CREATE TABLE XIAOMI_SLEEP_TIME_SAMPLE (
    TIMESTAMP INTEGER NOT NULL,
    DEVICE_ID INTEGER NOT NULL,
    USER_ID INTEGER NOT NULL DEFAULT 1,
    WAKEUP_TIME INTEGER,
    IS_AWAKE INTEGER,
    TOTAL_DURATION INTEGER,
    DEEP_SLEEP_DURATION INTEGER,
    LIGHT_SLEEP_DURATION INTEGER,
    REM_SLEEP_DURATION INTEGER,
    AWAKE_DURATION INTEGER,
    PRIMARY KEY (TIMESTAMP, DEVICE_ID)
);
CREATE TABLE XIAOMI_SLEEP_STAGE_SAMPLE (
    TIMESTAMP INTEGER NOT NULL,
    DEVICE_ID INTEGER NOT NULL,
    USER_ID INTEGER NOT NULL DEFAULT 1,
    STAGE INTEGER,
    PRIMARY KEY (TIMESTAMP, DEVICE_ID)
);
CREATE TABLE BASE_ACTIVITY_SUMMARY (
    _id INTEGER PRIMARY KEY,
    NAME TEXT,
    START_TIME INTEGER NOT NULL,
    END_TIME INTEGER NOT NULL,
    ACTIVITY_KIND INTEGER NOT NULL,
    DEVICE_ID INTEGER NOT NULL,
    USER_ID INTEGER NOT NULL DEFAULT 1,
    SUMMARY_DATA TEXT,
    RAW_SUMMARY_DATA BLOB
);
"""


def epoch(value: str) -> int:
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())


class GadgetbridgeQueryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.sync_status = mock.patch(
            "mibandctl.gadgetbridge.query._read_sync_status",
            return_value={
                "status": "never_synced",
                "last_success_at": None,
                "last_pulled_at": None,
            },
        )
        self.sync_status.start()
        self.addCleanup(self.sync_status.stop)
        self.data_dir = Path(self.temp.name)
        self.db_path = self.data_dir / "Gadgetbridge.db"
        self.settings = SimpleNamespace(
            db_path=self.db_path,
            data_dir=self.data_dir,
            timezone="Asia/Shanghai",
            gadgetbridge_device_id=1,
            gadgetbridge_device_address=None,
        )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def create_db(self, *, devices: tuple[int, ...] = (1,)) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.executescript(SCHEMA)
        for device_id in devices:
            conn.execute(
                "INSERT INTO DEVICE VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    device_id,
                    f"Synthetic Band {device_id}",
                    "Synthetic",
                    f"00:00:00:00:00:{device_id:02d}",
                    1,
                    "SYNTHETIC_BAND",
                    "Test Model",
                    None,
                ),
            )
        conn.commit()
        return conn

    def add_activity(
        self,
        conn: sqlite3.Connection,
        at: int,
        *,
        device: int = 1,
        steps: int = 0,
        heart_rate: int = 0,
        distance_cm: int = 0,
        calories: int = 0,
    ) -> None:
        conn.execute(
            """
            INSERT INTO XIAOMI_ACTIVITY_SAMPLE(
                TIMESTAMP, DEVICE_ID, STEPS, HEART_RATE, DISTANCE_CM, ACTIVE_CALORIES
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (at, device, steps, heart_rate, distance_cm, calories),
        )

    def add_sleep(
        self,
        conn: sqlite3.Connection,
        start: int,
        wake: int,
        *,
        device: int = 1,
        total: int = 480,
    ) -> None:
        conn.execute(
            """
            INSERT INTO XIAOMI_SLEEP_TIME_SAMPLE(
                TIMESTAMP, DEVICE_ID, WAKEUP_TIME, IS_AWAKE, TOTAL_DURATION,
                DEEP_SLEEP_DURATION, LIGHT_SLEEP_DURATION, REM_SLEEP_DURATION, AWAKE_DURATION
            ) VALUES (?, ?, ?, 0, ?, 120, 260, 80, 20)
            """,
            (start * 1000, device, wake * 1000, total),
        )

    def test_missing_file_and_empty_supported_database_return_no_data(self) -> None:
        missing = get_current_state(self.settings)
        self.assertEqual(missing["status"], "no_data")
        self.assertEqual(missing["meta"]["source"], "gadgetbridge")
        self.assertIsNone(missing["data"]["heart_rate"])

        conn = self.create_db(devices=())
        conn.close()
        empty = query_health(
            self.settings,
            kind="timeseries",
            start="2026-09-20T00:00:00Z",
            end="2026-09-21T00:00:00Z",
        )
        self.assertEqual(empty["status"], "no_data")
        self.assertEqual(empty["data"], [])

    def test_existing_database_with_missing_table_is_explicit_schema_error(self) -> None:
        conn = sqlite3.connect(self.db_path)
        conn.execute("CREATE TABLE DEVICE (_id INTEGER PRIMARY KEY)")
        conn.close()
        with self.assertRaisesRegex(GadgetbridgeSchemaError, "missing tables"):
            get_current_state(self.settings)

    def test_multi_device_requires_selection_and_every_series_is_isolated(self) -> None:
        conn = self.create_db(devices=(1, 2))
        at = epoch("2026-09-20T12:05:00Z")
        self.add_activity(conn, at, device=1, steps=10, heart_rate=70)
        self.add_activity(conn, at, device=2, steps=900, heart_rate=190)
        conn.commit()
        conn.close()

        unselected = SimpleNamespace(**{**vars(self.settings), "gadgetbridge_device_id": None})
        with self.assertRaisesRegex(ValueError, "multiple Gadgetbridge devices"):
            query_health(
                unselected,
                kind="timeseries",
                metric="steps",
                start="2026-09-20T12:00:00Z",
                end="2026-09-20T13:00:00Z",
            )

        result = query_health(
            self.settings,
            kind="timeseries",
            metric="steps",
            start="2026-09-20T12:00:00Z",
            end="2026-09-20T13:00:00Z",
        )
        self.assertEqual(result["data"][0]["value"], 10)
        self.assertEqual(result["meta"]["device"]["id"], 1)
        self.assertEqual(result["meta"]["source"], "gadgetbridge")

        addressed = SimpleNamespace(
            **{
                **vars(self.settings),
                "gadgetbridge_device_id": None,
                "gadgetbridge_device_address": "00:00:00:00:00:02",
            }
        )
        result = query_health(
            addressed,
            kind="timeseries",
            metric="heart_rate.bpm",
            start="2026-09-20T12:00:00Z",
            end="2026-09-20T13:00:00Z",
        )
        self.assertEqual(result["data"][0]["value"], 190)

    def test_hr_sentinels_staleness_future_and_pull_time_are_distinct(self) -> None:
        conn = self.create_db()
        now = datetime.now(timezone.utc)
        valid_at = int((now - timedelta(hours=2)).timestamp())
        self.add_activity(conn, valid_at, heart_rate=77)
        self.add_activity(conn, valid_at + 1, heart_rate=0)
        self.add_activity(conn, valid_at + 2, heart_rate=255)
        conn.commit()
        conn.close()

        sync = {
            "status": "ok",
            "last_success_at": now.isoformat(timespec="seconds"),
            "last_pulled_at": now.isoformat(timespec="seconds"),
            "cache_identity": {
                "dev": self.db_path.stat().st_dev,
                "ino": self.db_path.stat().st_ino,
                "size": self.db_path.stat().st_size,
                "mtime_ns": self.db_path.stat().st_mtime_ns,
            },
        }
        with mock.patch("mibandctl.gadgetbridge.query._read_sync_status", return_value=sync):
            current = get_current_state(self.settings, max_age_seconds=60)
        hr = current["data"]["heart_rate"]
        self.assertEqual(hr["latest_value"], 77)
        self.assertIsNone(hr["value"])
        self.assertEqual(hr["status"], "stale")
        self.assertFalse(current["freshness"]["satisfied"])
        self.assertEqual(current["meta"]["last_pulled_at"], sync["last_pulled_at"])

        conn = sqlite3.connect(self.db_path)
        future_at = int((now + timedelta(minutes=5)).timestamp())
        self.add_activity(conn, future_at, heart_rate=88)
        conn.commit()
        conn.close()
        current = get_current_state(self.settings, max_age_seconds=900)
        self.assertEqual(current["data"]["heart_rate"]["status"], "future")
        self.assertEqual(current["data"]["heart_rate"]["latest_value"], 88)
        self.assertIsNone(current["data"]["heart_rate"]["value"])

    def test_current_state_includes_battery_and_does_not_invent_zeroes(self) -> None:
        conn = self.create_db()
        now = int(datetime.now(timezone.utc).timestamp())
        conn.execute("INSERT INTO BATTERY_LEVEL VALUES (?, 1, 82, 0)", (now - 30,))
        conn.commit()
        conn.close()

        current = get_current_state(self.settings)
        self.assertEqual(current["data"]["battery"]["value"], 82)
        self.assertIsNone(current["data"]["steps_30m"])
        self.assertIn("steps_30m", current["meta"]["missing"])

    def test_newer_valid_manual_hr_wins_but_manual_sentinels_do_not(self) -> None:
        conn = self.create_db()
        now = int(datetime.now(timezone.utc).timestamp())
        self.add_activity(conn, now - 50, heart_rate=72)
        self.add_activity(conn, now - 40, heart_rate=-9)
        conn.execute(
            "INSERT INTO XIAOMI_MANUAL_SAMPLE(TIMESTAMP, DEVICE_ID, TYPE, VALUE) VALUES (?, 1, 17, 86)",
            ((now - 10) * 1000,),
        )
        conn.execute(
            "INSERT INTO XIAOMI_MANUAL_SAMPLE(TIMESTAMP, DEVICE_ID, TYPE, VALUE) VALUES (?, 1, 17, 255)",
            ((now - 5) * 1000,),
        )
        conn.execute(
            "INSERT INTO XIAOMI_MANUAL_SAMPLE(TIMESTAMP, DEVICE_ID, TYPE, VALUE) VALUES (?, 1, 99, 190)",
            ((now - 1) * 1000,),
        )
        conn.execute(
            "INSERT INTO XIAOMI_MANUAL_SAMPLE(TIMESTAMP, DEVICE_ID, TYPE, VALUE) VALUES (?, 1, 17, -4)",
            ((now - 2) * 1000,),
        )
        conn.commit()
        conn.close()

        current = get_current_state(self.settings, max_age_seconds=60)
        self.assertEqual(current["data"]["heart_rate"]["value"], 86)
        self.assertEqual(
            current["data"]["heart_rate"]["source_series"], "XIAOMI_MANUAL_SAMPLE"
        )

        series = query_health(
            self.settings,
            kind="timeseries",
            metric="heart_rate.bpm",
            start=datetime.fromtimestamp(now - 60, tz=timezone.utc).isoformat(),
            end=datetime.fromtimestamp(now, tz=timezone.utc).isoformat(),
        )
        self.assertEqual(series["data"][0]["value"], 72)
        self.assertEqual(series["meta"]["series_scope"], "automatic_activity_samples_only")

    def test_timeseries_half_open_boundaries_units_and_pagination(self) -> None:
        conn = self.create_db()
        start = epoch("2026-09-20T00:00:00Z")
        for hour, value in enumerate((100, 200, 300)):
            self.add_activity(
                conn,
                start + hour * 3600,
                steps=value,
                distance_cm=value,
                calories=hour + 1,
            )
        self.add_activity(conn, start + 3 * 3600, steps=999)
        conn.commit()
        conn.close()

        kwargs = dict(
            kind="timeseries",
            metric="steps",
            aggregation_minutes=60,
            start="2026-09-20T00:00:00Z",
            end="2026-09-20T03:00:00Z",
        )
        first = query_health(self.settings, limit=2, offset=0, **kwargs)
        second = query_health(self.settings, limit=2, offset=2, **kwargs)
        self.assertEqual([row["value"] for row in first["data"]], [100, 200])
        self.assertEqual([row["value"] for row in second["data"]], [300])
        self.assertTrue(first["meta"]["pagination"]["has_more"])
        self.assertEqual(first["meta"]["pagination"]["next_offset"], 2)

        distance = query_health(self.settings, metric="distance", **{k: v for k, v in kwargs.items() if k != "metric"})
        self.assertEqual([row["value"] for row in distance["data"]], [1, 2, 3])

    def test_fractional_second_windows_keep_exact_half_open_semantics(self) -> None:
        conn = self.create_db()
        base = epoch("2026-09-20T00:00:00Z")
        self.add_activity(conn, base, steps=10)
        self.add_activity(conn, base + 1, steps=20)
        self.add_activity(conn, base + 2, steps=30)
        conn.execute(
            """
            INSERT INTO XIAOMI_SLEEP_TIME_SAMPLE(
                TIMESTAMP, DEVICE_ID, WAKEUP_TIME, IS_AWAKE, TOTAL_DURATION,
                DEEP_SLEEP_DURATION, LIGHT_SLEEP_DURATION, REM_SLEEP_DURATION, AWAKE_DURATION
            ) VALUES (?, 1, ?, 0, 1, 0, 1, 0, 0)
            """,
            ((base - 60) * 1000, base * 1000 + 500),
        )
        conn.commit()
        conn.close()

        series = query_health(
            self.settings,
            kind="timeseries",
            metric="steps",
            aggregation_minutes=1,
            start="2026-09-20T00:00:00.500Z",
            end="2026-09-20T00:00:01.500Z",
        )
        self.assertEqual([row["value"] for row in series["data"]], [20])

        sleep = query_health(
            self.settings,
            kind="sleep",
            start="2026-09-20T00:00:00.500Z",
            end="2026-09-20T00:00:00.501Z",
        )
        self.assertEqual(len(sleep["data"]), 1)

    def test_daily_report_uses_dst_natural_day_half_open_boundaries(self) -> None:
        conn = self.create_db()
        zone = ZoneInfo("Australia/Sydney")
        day_start = int(datetime(2026, 10, 4, 0, 0, tzinfo=zone).timestamp())
        next_start = int(datetime(2026, 10, 5, 0, 0, tzinfo=zone).timestamp())
        self.assertEqual(next_start - day_start, 23 * 3600)
        self.add_activity(conn, day_start, steps=10, heart_rate=60, distance_cm=100, calories=3)
        self.add_activity(conn, next_start - 1, steps=20, heart_rate=80, distance_cm=200, calories=5)
        self.add_activity(conn, next_start, steps=900, heart_rate=200, distance_cm=900, calories=90)
        conn.commit()
        conn.close()

        report = get_daily_report(
            self.settings,
            date="2026-10-04",
            timezone="Australia/Sydney",
            compare_days=0,
        )
        self.assertEqual(report["data"]["steps"]["value"], 30)
        self.assertEqual(report["data"]["steps"]["source_series"], "XIAOMI_ACTIVITY_SAMPLE")
        self.assertEqual(report["data"]["calories"]["value"], 8)
        self.assertEqual(report["data"]["distance"]["value"], 3)
        self.assertEqual(report["data"]["heart_rate"]["avg_bpm"], 70)

    def test_daily_summary_is_used_only_when_source_day_boundary_matches(self) -> None:
        conn = self.create_db()
        shanghai_start = epoch("2026-09-20T00:00:00+08:00")
        self.add_activity(conn, shanghai_start + 12 * 3600, steps=10, heart_rate=60, calories=3)
        self.add_activity(conn, shanghai_start + 13 * 3600, steps=20, heart_rate=80, calories=5)
        conn.execute(
            """
            INSERT INTO XIAOMI_DAILY_SUMMARY_SAMPLE(
                TIMESTAMP, DEVICE_ID, TIMEZONE, STEPS, HR_MAX, HR_MIN, HR_AVG,
                CALORIES, ACTIVE_CALORIES
            ) VALUES (?, 1, 32, 1000, 90, 50, 70, 999, 50)
            """,
            (shanghai_start * 1000,),
        )
        conn.commit()
        conn.close()

        matching = get_daily_report(self.settings, date="2026-09-20", compare_days=0)
        self.assertEqual(matching["data"]["steps"]["value"], 1000)
        self.assertEqual(
            matching["data"]["steps"]["source_series"], "XIAOMI_DAILY_SUMMARY_SAMPLE"
        )
        self.assertTrue(matching["meta"]["daily_summary_selection"]["used"])

        utc = get_daily_report(
            self.settings, date="2026-09-20", timezone="UTC", compare_days=0
        )
        self.assertEqual(utc["data"]["steps"]["value"], 30)
        self.assertEqual(utc["data"]["steps"]["source_series"], "XIAOMI_ACTIVITY_SAMPLE")
        self.assertFalse(utc["meta"]["daily_summary_selection"]["used"])

    def test_sleep_is_assigned_to_wake_day_and_stages_are_device_scoped(self) -> None:
        conn = self.create_db(devices=(1, 2))
        first_start = epoch("2026-09-19T23:00:00+08:00")
        first_wake = epoch("2026-09-20T07:00:00+08:00")
        second_start = epoch("2026-09-20T23:30:00+08:00")
        second_wake = epoch("2026-09-21T00:00:00+08:00")
        self.add_sleep(conn, first_start, first_wake, device=1)
        self.add_sleep(conn, second_start, second_wake, device=1, total=30)
        self.add_sleep(conn, first_start, first_wake, device=2, total=999)
        conn.execute(
            "INSERT INTO XIAOMI_SLEEP_STAGE_SAMPLE VALUES (?, 1, 1, 2)",
            ((first_start + 10) * 1000,),
        )
        conn.execute(
            "INSERT INTO XIAOMI_SLEEP_STAGE_SAMPLE VALUES (?, 1, 1, 4)",
            ((first_start + 20) * 1000,),
        )
        conn.execute(
            "INSERT INTO XIAOMI_SLEEP_STAGE_SAMPLE VALUES (?, 2, 1, 5)",
            ((first_start + 15) * 1000,),
        )
        conn.commit()
        conn.close()

        result = query_health(
            self.settings,
            kind="sleep",
            start="2026-09-20T00:00:00+08:00",
            end="2026-09-21T00:00:00+08:00",
            limit=1,
        )
        self.assertEqual(len(result["data"]), 1)
        self.assertEqual(result["data"][0]["duration_min"], 480)
        self.assertEqual([stage["stage"] for stage in result["data"][0]["stages"]], ["deep", "rem"])
        self.assertFalse(result["meta"]["pagination"]["has_more"])

        session_id = result["data"][0]["session_id"]
        selected = query_health(
            self.settings,
            kind="sleep",
            start="2026-09-19T00:00:00+08:00",
            end="2026-09-22T00:00:00+08:00",
            session_id=session_id,
        )
        self.assertEqual([row["session_id"] for row in selected["data"]], [session_id])

        report = get_daily_report(self.settings, date="2026-09-20", compare_days=0)
        self.assertEqual(report["data"]["sleep"]["duration_min"], 480)
        self.assertEqual(len(report["data"]["sleep"]["items"]), 1)

    def test_session_ids_change_with_device_even_for_same_raw_key(self) -> None:
        conn = self.create_db(devices=(1, 2))
        start = epoch("2026-09-19T23:00:00+08:00")
        wake = epoch("2026-09-20T07:00:00+08:00")
        self.add_sleep(conn, start, wake, device=1)
        self.add_sleep(conn, start, wake, device=2)
        conn.commit()
        conn.close()

        ids = []
        for device_id in (1, 2):
            selected = SimpleNamespace(**{**vars(self.settings), "gadgetbridge_device_id": device_id})
            result = query_health(
                selected,
                kind="sleep",
                start="2026-09-20T00:00:00+08:00",
                end="2026-09-21T00:00:00+08:00",
            )
            ids.append(result["data"][0]["session_id"])
        self.assertNotEqual(ids[0], ids[1])

    def test_workout_only_uses_confirmed_base_fields(self) -> None:
        conn = self.create_db()
        start = epoch("2026-09-20T10:00:00Z")
        conn.execute(
            """
            INSERT INTO BASE_ACTIVITY_SUMMARY(
                _id, NAME, START_TIME, END_TIME, ACTIVITY_KIND, DEVICE_ID,
                SUMMARY_DATA, RAW_SUMMARY_DATA
            ) VALUES (7, 'Synthetic run', ?, ?, 42, 1, 'secret opaque data', X'0102')
            """,
            (start * 1000, (start + 600) * 1000),
        )
        conn.commit()
        conn.close()

        result = query_health(
            self.settings,
            kind="workouts",
            start="2026-09-20T00:00:00Z",
            end="2026-09-21T00:00:00Z",
        )
        item = result["data"][0]
        self.assertEqual(item["activity_kind"], 42)
        self.assertEqual(item["duration_sec"], 600)
        self.assertIsNone(item["distance_m"])
        self.assertIn("workout.distance_m", result["meta"]["unsupported"])
        self.assertNotIn("SUMMARY_DATA", str(result))
        self.assertNotIn("secret opaque data", str(result))

    def test_known_metric_without_confirmed_mapping_is_unsupported(self) -> None:
        result = query_health(
            self.settings,
            kind="timeseries",
            metric="weight",
            start="2026-09-20T00:00:00Z",
            end="2026-09-21T00:00:00Z",
        )
        self.assertEqual(result["status"], "unsupported")
        self.assertEqual(result["meta"]["unsupported"], ["weight"])


if __name__ == "__main__":
    unittest.main()
