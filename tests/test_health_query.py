from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from mibandctl.health.query import get_current_state, get_daily_report, query_health
from mibandctl.health.settings import Settings


SCHEMA = """
CREATE TABLE metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE metric_records (
    metric_id TEXT PRIMARY KEY,
    raw_record_id INTEGER NOT NULL,
    source_table TEXT NOT NULL,
    raw_key TEXT NOT NULL,
    metric TEXT NOT NULL,
    start_at TEXT,
    end_at TEXT,
    value_real REAL,
    value_text TEXT,
    unit TEXT,
    label TEXT,
    attrs_json TEXT
);
"""


class HealthQueryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.data_dir = Path(self.temp.name)
        self.settings = Settings(data_dir=self.data_dir, timezone="Asia/Shanghai")
        self._metric_number = 0

    def tearDown(self) -> None:
        self.temp.cleanup()

    def create_db(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.settings.db_path)
        conn.executescript(SCHEMA)
        conn.execute(
            "INSERT INTO metadata VALUES (?, ?, ?)",
            ("last_pulled_at", "2026-09-20T20:05:00+08:00", "2026-09-20T20:05:00+08:00"),
        )
        conn.commit()
        return conn

    def add_metric(
        self,
        conn: sqlite3.Connection,
        *,
        metric: str,
        value: float | None,
        start: str,
        end: str | None = None,
        source_table: str = "hr_record",
        raw_key: str = "heart_rate",
        raw_record_id: int | None = None,
        attrs: dict | None = None,
        label: str | None = None,
        value_text: str | None = None,
    ) -> None:
        self._metric_number += 1
        conn.execute(
            """
            INSERT INTO metric_records(
                metric_id, raw_record_id, source_table, raw_key, metric,
                start_at, end_at, value_real, value_text, unit, label, attrs_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                f"metric-{self._metric_number:04d}",
                raw_record_id if raw_record_id is not None else self._metric_number,
                source_table,
                raw_key,
                metric,
                start,
                end,
                value,
                value_text,
                None,
                label,
                json.dumps(attrs or {}),
            ),
        )

    def set_metadata(self, conn: sqlite3.Connection, key: str, value: str) -> None:
        conn.execute(
            """
            INSERT INTO metadata(key, value, updated_at) VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
            """,
            (key, value, "2026-09-21T00:00:00Z"),
        )

    def measurement(self, bpm: int, received_at_time: datetime, **changes: object) -> str:
        payload = {
            "status": "ok",
            "source": "band_realtime_protocol",
            "heart_rate_bpm": bpm,
            "unit": "bpm",
            "received_at": received_at_time.isoformat(timespec="seconds"),
            "measurement_requested_at": (received_at_time - timedelta(seconds=8)).isoformat(
                timespec="seconds"
            ),
            "start_confirmed": True,
            "stop_confirmed": True,
            "elapsed_seconds": 8.0,
        }
        payload.update(changes)
        return json.dumps(payload)

    def test_missing_database_returns_no_data_without_fake_zeroes(self) -> None:
        current = get_current_state(self.settings)
        self.assertEqual(current["status"], "no_data")
        self.assertIsNone(current["data"]["heart_rate"])
        self.assertIsNone(current["data"]["steps_30m"])
        self.assertFalse(current["freshness"]["satisfied"])
        self.assertIsNone(current["meta"]["last_pulled_at"])

        series = query_health(
            self.settings,
            kind="timeseries",
            start="2026-09-20T00:00:00+08:00",
            end="2026-09-21T00:00:00+08:00",
            metric="steps",
        )
        self.assertEqual(series["status"], "no_data")
        self.assertEqual(series["data"], [])

        daily = get_daily_report(self.settings, date="2026-09-20")
        self.assertEqual(daily["status"], "no_data")
        self.assertIsNone(daily["data"]["steps"])
        self.assertIsNone(daily["data"]["sleep"])

    def test_corrupt_existing_database_is_not_reported_as_no_data(self) -> None:
        self.settings.db_path.write_bytes(b"this is not sqlite")
        with self.assertRaises(sqlite3.DatabaseError):
            get_current_state(self.settings)

    def test_timeseries_aggregates_and_filters_overlapping_sources(self) -> None:
        conn = self.create_db()
        # Canonical heart-rate points average to 80.  Resting HR and an event
        # item share the stored metric name but must not enter the series.
        self.add_metric(conn, metric="heart_rate.bpm", value=70, start="2026-09-20T00:10:00Z")
        self.add_metric(conn, metric="heart_rate.bpm", value=90, start="2026-09-20T00:40:00Z")
        self.add_metric(
            conn,
            metric="heart_rate.bpm",
            value=40,
            start="2026-09-20T00:20:00Z",
            raw_key="resting_heart_rate",
        )
        self.add_metric(
            conn,
            metric="heart_rate.bpm",
            value=200,
            start="2026-09-20T00:30:00Z",
            attrs={"item_index": 0},
        )
        # A calorie_record and step_record cover the same bucket.  The
        # independent calorie series wins instead of the values being added.
        self.add_metric(
            conn,
            metric="calories",
            value=12,
            start="2026-09-20T00:05:00Z",
            source_table="step_record",
            raw_key="steps",
        )
        self.add_metric(
            conn,
            metric="calories",
            value=8,
            start="2026-09-20T00:15:00Z",
            source_table="calorie_record",
            raw_key="calories",
        )
        self.add_metric(
            conn,
            metric="calories",
            value=9,
            start="2026-09-20T01:15:00Z",
            source_table="step_record",
            raw_key="steps",
        )
        conn.commit()
        conn.close()

        hr = query_health(
            self.settings,
            kind="timeseries",
            metric="heart_rate.bpm",
            aggregation_minutes=60,
            start="2026-09-20T00:00:00Z",
            end="2026-09-20T02:00:00Z",
        )
        self.assertEqual(hr["data"][0]["value"], 80)
        self.assertEqual(hr["data"][0]["samples"], 2)

        calories = query_health(
            self.settings,
            kind="timeseries",
            metric="calories",
            aggregation_minutes=60,
            start="2026-09-20T00:00:00Z",
            end="2026-09-20T02:00:00Z",
        )
        self.assertEqual([item["value"] for item in calories["data"]], [8, 9])
        self.assertEqual(
            [item["source_series"] for item in calories["data"]],
            ["calorie_record", "step_record"],
        )

    def test_daily_report_uses_timezone_midnight_and_sleep_wake_day(self) -> None:
        conn = self.create_db()
        # 16:30Z is 00:30 on Sep 20 in Shanghai and belongs to Sep 20.
        self.add_metric(conn, metric="heart_rate.bpm", value=72, start="2026-09-19T16:30:00Z")
        # Sleep begins on Sep 19 but is assigned to Sep 20 by wake time.
        for name, value in (
            ("sleep.duration", 480),
            ("sleep.deep_duration", 120),
            ("sleep.light_duration", 260),
            ("sleep.rem_duration", 100),
        ):
            self.add_metric(
                conn,
                metric=name,
                value=value,
                start="2026-09-19T23:00:00+08:00",
                end="2026-09-20T07:00:00+08:00",
                source_table="sleep_segment",
                raw_key="sleep-account-key-must-not-leak",
                raw_record_id=91,
            )
        # This sleep wakes exactly at next midnight and is excluded by the
        # half-open natural-day range.
        self.add_metric(
            conn,
            metric="sleep.duration",
            value=30,
            start="2026-09-20T23:30:00+08:00",
            end="2026-09-21T00:00:00+08:00",
            source_table="sleep_segment",
            raw_key="another-private-key",
            raw_record_id=92,
        )
        conn.commit()
        conn.close()

        report = get_daily_report(self.settings, date="2026-09-20", compare_days=0)
        self.assertEqual(report["data"]["heart_rate"]["avg_bpm"], 72)
        self.assertEqual(report["data"]["sleep"]["duration_min"], 480)
        self.assertEqual(len(report["data"]["sleep"]["items"]), 1)
        rendered = json.dumps(report)
        self.assertNotIn("sleep-account-key", rendered)

    def test_daily_report_handles_dst_natural_day(self) -> None:
        conn = self.create_db()
        # Sydney changes from +10 to +11 on 2026-10-04; both observations are
        # in that 23-hour natural day.
        self.add_metric(conn, metric="heart_rate.bpm", value=60, start="2026-10-04T00:30:00+10:00")
        self.add_metric(conn, metric="heart_rate.bpm", value=80, start="2026-10-04T23:30:00+11:00")
        self.add_metric(conn, metric="heart_rate.bpm", value=100, start="2026-10-05T00:00:00+11:00")
        conn.commit()
        conn.close()

        report = get_daily_report(
            self.settings,
            date="2026-10-04",
            timezone="Australia/Sydney",
            compare_days=0,
        )
        self.assertEqual(report["data"]["heart_rate"]["avg_bpm"], 70)
        self.assertEqual(report["data"]["heart_rate"]["samples"], 2)

    def test_comparison_excludes_missing_days_and_reports_coverage(self) -> None:
        conn = self.create_db()
        self.add_metric(
            conn,
            metric="steps",
            value=300,
            start="2026-09-17T12:00:00+08:00",
            source_table="step_record",
            raw_key="steps",
        )
        self.add_metric(
            conn,
            metric="steps",
            value=500,
            start="2026-09-19T12:00:00+08:00",
            source_table="step_record",
            raw_key="steps",
        )
        self.add_metric(
            conn,
            metric="steps",
            value=100,
            start="2026-09-20T12:00:00+08:00",
            source_table="step_record",
            raw_key="steps",
        )
        conn.commit()
        conn.close()

        report = get_daily_report(self.settings, date="2026-09-20", compare_days=3)
        comparison = report["comparison"]["metrics"]["steps"]
        self.assertEqual(comparison["average"], 400)
        self.assertEqual(comparison["available_days"], 2)
        self.assertEqual(comparison["missing_dates"], ["2026-09-18"])

    def test_pagination_is_deterministic_over_buckets(self) -> None:
        conn = self.create_db()
        for hour, value in enumerate((10, 20, 30)):
            self.add_metric(
                conn,
                metric="steps",
                value=value,
                start=f"2026-09-20T{hour:02d}:05:00Z",
                source_table="step_record",
                raw_key="steps",
            )
        conn.commit()
        conn.close()

        kwargs = dict(
            kind="timeseries",
            metric="steps",
            aggregation_minutes=60,
            start="2026-09-20T00:00:00Z",
            end="2026-09-20T04:00:00Z",
        )
        first = query_health(self.settings, limit=2, offset=0, **kwargs)
        second = query_health(self.settings, limit=2, offset=2, **kwargs)
        self.assertEqual([row["value"] for row in first["data"]], [10, 20])
        self.assertTrue(first["meta"]["truncated"])
        self.assertEqual(first["meta"]["pagination"]["next_offset"], 2)
        self.assertEqual([row["value"] for row in second["data"]], [30])
        self.assertFalse(second["meta"]["truncated"])

    def test_stale_latest_hr_is_not_exposed_as_current(self) -> None:
        conn = self.create_db()
        observed = datetime.now(timezone.utc) - timedelta(hours=2)
        self.add_metric(
            conn,
            metric="heart_rate.bpm",
            value=77,
            start=observed.isoformat(timespec="seconds"),
        )
        conn.commit()
        conn.close()

        current = get_current_state(self.settings, max_age_seconds=60)
        hr = current["data"]["heart_rate"]
        self.assertEqual(hr["latest_value"], 77)
        self.assertIsNone(hr["value"])
        self.assertEqual(hr["status"], "stale")
        self.assertFalse(current["freshness"]["satisfied"])
        self.assertEqual(current["meta"]["last_pulled_at"], "2026-09-20T20:05:00+08:00")

    def test_newer_active_measurement_becomes_current_hr(self) -> None:
        conn = self.create_db()
        now = datetime.now(timezone.utc)
        recorded_at = now - timedelta(minutes=5)
        received_at = now - timedelta(seconds=10)
        self.add_metric(
            conn,
            metric="heart_rate.bpm",
            value=72,
            start=recorded_at.isoformat(timespec="seconds"),
        )
        self.set_metadata(conn, "measurement.latest", self.measurement(86, received_at))
        conn.commit()
        conn.close()

        current = get_current_state(self.settings, max_age_seconds=60)
        heart_rate = current["data"]["heart_rate"]
        self.assertEqual(heart_rate["value"], 86)
        self.assertEqual(heart_rate["latest_value"], 86)
        self.assertEqual(heart_rate["source"], "band_realtime_protocol")
        self.assertEqual(heart_rate["timestamp_basis"], "received_at")
        self.assertEqual(
            int(datetime.fromisoformat(heart_rate["observed_at"]).timestamp()),
            int(received_at.timestamp()),
        )
        self.assertTrue(current["freshness"]["satisfied"])
        self.assertEqual(current["freshness"]["latest_sample_at"], heart_rate["observed_at"])
        self.assertEqual(current["meta"]["observed_at"], heart_rate["observed_at"])

    def test_older_active_measurement_does_not_replace_recorded_hr(self) -> None:
        conn = self.create_db()
        now = datetime.now(timezone.utc)
        recorded_at = now - timedelta(seconds=10)
        received_at = now - timedelta(minutes=5)
        self.add_metric(
            conn,
            metric="heart_rate.bpm",
            value=74,
            start=recorded_at.isoformat(timespec="seconds"),
        )
        self.set_metadata(conn, "measurement.latest", self.measurement(86, received_at))
        conn.commit()
        conn.close()

        heart_rate = get_current_state(self.settings, max_age_seconds=60)["data"]["heart_rate"]
        self.assertEqual(heart_rate["value"], 74)
        self.assertEqual(heart_rate["source"], "xiaomi_health_record")
        self.assertEqual(heart_rate["timestamp_basis"], "recorded_at")

    def test_failed_or_invalid_active_measurement_is_ignored(self) -> None:
        conn = self.create_db()
        recorded_at = datetime.now(timezone.utc) - timedelta(seconds=10)
        self.add_metric(
            conn,
            metric="heart_rate.bpm",
            value=71,
            start=recorded_at.isoformat(timespec="seconds"),
        )
        invalid_values = (
            self.measurement(86, datetime.now(timezone.utc), status="error"),
            self.measurement(86, datetime.now(timezone.utc), stop_confirmed=False),
            self.measurement(86, datetime.now(timezone.utc), received_at="2026-09-21T00:00:00"),
            "not-json",
        )
        for value in invalid_values:
            with self.subTest(value=value):
                self.set_metadata(conn, "measurement.latest", value)
                conn.commit()
                heart_rate = get_current_state(self.settings, max_age_seconds=60)["data"][
                    "heart_rate"
                ]
                self.assertEqual(heart_rate["latest_value"], 71)
                self.assertEqual(heart_rate["source"], "xiaomi_health_record")
        conn.close()

    def test_active_measurement_does_not_enter_daily_or_timeseries_history(self) -> None:
        conn = self.create_db()
        self.add_metric(
            conn,
            metric="heart_rate.bpm",
            value=72,
            start="2026-09-20T12:00:00+08:00",
        )
        self.set_metadata(
            conn,
            "measurement.latest",
            self.measurement(86, datetime.fromisoformat("2026-09-20T12:01:00+08:00")),
        )
        conn.commit()
        conn.close()

        daily = get_daily_report(self.settings, date="2026-09-20", compare_days=0)
        self.assertEqual(daily["data"]["heart_rate"]["avg_bpm"], 72)
        self.assertEqual(daily["data"]["heart_rate"]["samples"], 1)
        history = query_health(
            self.settings,
            kind="timeseries",
            metric="heart_rate.bpm",
            start="2026-09-20T00:00:00+08:00",
            end="2026-09-21T00:00:00+08:00",
        )
        self.assertEqual(history["data"][0]["value"], 72)
        self.assertEqual(history["data"][0]["samples"], 1)

    def test_rejects_unbounded_or_ambiguous_inputs(self) -> None:
        bad_calls = (
            {"kind": "timeseries", "start": "2026-09-20T00:00:00+08:00"},
            {
                "kind": "timeseries",
                "start": "2026-09-20T00:00:00",
                "end": "2026-09-21T00:00:00+08:00",
            },
            {
                "kind": "timeseries",
                "start": "2025-01-01T00:00:00Z",
                "end": "2026-09-20T00:00:00Z",
            },
            {"kind": "timeseries", "metric": "heart_rate.bpm; DROP TABLE metadata"},
            {"kind": "timeseries", "limit": 501},
            {"kind": "sleep", "session_id": "raw-account-id"},
        )
        for kwargs in bad_calls:
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                query_health(self.settings, **kwargs)


if __name__ == "__main__":
    unittest.main()
