from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from mibandctl import db
from mibandctl.__main__ import main
from mibandctl.commands import sleep


def _make_sample_db(path: Path, ts: int) -> None:
    con = sqlite3.connect(path)
    try:
        con.execute("CREATE TABLE XIAOMI_ACTIVITY_SAMPLE (TIMESTAMP INTEGER)")
        con.execute("INSERT INTO XIAOMI_ACTIVITY_SAMPLE VALUES (?)", (ts,))
        con.commit()
    finally:
        con.close()


class DbPathTests(unittest.TestCase):
    def tearDown(self) -> None:
        db.db_path.cache_clear()

    def test_sqlite_ro_uri_handles_spaces_and_uri_chars(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "source db #1?.sqlite"
            con = sqlite3.connect(path)
            con.execute("CREATE TABLE sample (id INTEGER PRIMARY KEY)")
            con.execute("INSERT INTO sample VALUES (1)")
            con.commit()
            con.close()

            ro = sqlite3.connect(db.sqlite_ro_uri(path), uri=True)
            try:
                self.assertEqual(ro.execute("SELECT id FROM sample").fetchone()[0], 1)
                with self.assertRaises(sqlite3.OperationalError):
                    ro.execute("INSERT INTO sample VALUES (2)")
            finally:
                ro.close()

    def test_db_path_picks_freshest_existing_default_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            stale = Path(tmp) / "stale.sqlite"
            fresh = Path(tmp) / "fresh.sqlite"
            _make_sample_db(stale, 100)
            _make_sample_db(fresh, 200)

            with mock.patch.object(db, "DEFAULT_DB_PATHS", [str(stale), str(fresh)]):
                with mock.patch.dict(os.environ, {"MIBAND_DB_PATH": ""}):
                    db.db_path.cache_clear()
                    self.assertEqual(db.db_path(), fresh)

    def test_miband_db_path_override_wins(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            stale = Path(tmp) / "override.sqlite"
            fresh = Path(tmp) / "fresh.sqlite"
            _make_sample_db(stale, 100)
            _make_sample_db(fresh, 200)

            with mock.patch.object(db, "DEFAULT_DB_PATHS", [str(fresh)]):
                with mock.patch.dict(os.environ, {"MIBAND_DB_PATH": str(stale)}):
                    db.db_path.cache_clear()
                    self.assertEqual(db.db_path(), stale)

    def test_candidate_paths_expand_user_override(self) -> None:
        with mock.patch.dict(os.environ, {"MIBAND_DB_PATH": "~/Gadgetbridge.db"}):
            self.assertNotIn("~", str(db._candidate_paths()[0]))


class WindowTests(unittest.TestCase):
    def test_one_sided_explicit_window_is_rejected(self) -> None:
        args = SimpleNamespace(start="2026-06-01T00:00:00+08:00", end=None, n=1)
        with self.assertRaisesRegex(ValueError, "--start and --end"):
            db.resolve_window_seconds(args)

    def test_reversed_explicit_window_is_rejected(self) -> None:
        args = SimpleNamespace(
            start="2026-06-02T00:00:00+08:00",
            end="2026-06-01T00:00:00+08:00",
            n=1,
        )
        with self.assertRaisesRegex(ValueError, "--start must be < --end"):
            db.resolve_window_seconds(args)

    def test_empty_explicit_window_is_rejected(self) -> None:
        args = SimpleNamespace(
            start="2026-06-01T00:00:00+08:00",
            end="2026-06-01T00:00:00+08:00",
            n=1,
        )
        with self.assertRaisesRegex(ValueError, "--start must be < --end"):
            db.resolve_window_seconds(args)

    def test_z_suffix_is_accepted(self) -> None:
        args = SimpleNamespace(
            start="2026-06-01T00:00:00Z",
            end="2026-06-01T01:00:00Z",
            n=1,
        )
        start, end = db.resolve_window_seconds(args)
        self.assertEqual(end - start, 3600)

    def test_n_must_be_positive(self) -> None:
        with self.assertRaisesRegex(ValueError, "n must be >= 1"):
            db.days_ago_seconds(0)

    def test_cli_n_zero_returns_json_error(self) -> None:
        out = StringIO()
        with redirect_stdout(out):
            code = main(["hr", "0"])
        self.assertEqual(code, 2)
        self.assertEqual(out.getvalue(), '{"error": "n must be >= 1"}\n')


class SleepWindowTests(unittest.TestCase):
    def test_sleep_window_filters_by_wakeup_range(self) -> None:
        con = sqlite3.connect(":memory:")
        con.row_factory = sqlite3.Row
        try:
            con.execute(
                "CREATE TABLE XIAOMI_SLEEP_TIME_SAMPLE ("
                "TIMESTAMP INTEGER, WAKEUP_TIME INTEGER, IS_AWAKE INTEGER, "
                "TOTAL_DURATION INTEGER, DEEP_SLEEP_DURATION INTEGER, "
                "LIGHT_SLEEP_DURATION INTEGER, REM_SLEEP_DURATION INTEGER, "
                "AWAKE_DURATION INTEGER)"
            )
            con.execute("CREATE TABLE XIAOMI_SLEEP_STAGE_SAMPLE (TIMESTAMP INTEGER, STAGE INTEGER)")

            rows = [
                ("2026-05-30T22:00:00+08:00", "2026-05-31T07:00:00+08:00"),
                ("2026-05-31T22:00:00+08:00", "2026-06-01T07:00:00+08:00"),
                ("2026-06-01T22:00:00+08:00", "2026-06-02T07:00:00+08:00"),
            ]
            for bedtime, wakeup in rows:
                con.execute(
                    "INSERT INTO XIAOMI_SLEEP_TIME_SAMPLE VALUES (?, ?, 0, 480, 120, 300, 45, 15)",
                    (db.parse_iso_to_millis(bedtime), db.parse_iso_to_millis(wakeup)),
                )
            con.execute(
                "INSERT INTO XIAOMI_SLEEP_STAGE_SAMPLE VALUES (?, ?)",
                (db.parse_iso_to_millis("2026-05-31T22:30:00+08:00"), 2),
            )

            args = SimpleNamespace(
                start="2026-06-01T00:00:00+08:00",
                end="2026-06-02T00:00:00+08:00",
                n=1,
            )
            with mock.patch.object(sleep, "connect", return_value=con):
                with mock.patch.object(sleep, "freshness_block", return_value={}):
                    result = sleep.run(args)

            nights = result["data"]["nights"]
            self.assertEqual(len(nights), 1)
            self.assertEqual(nights[0]["wakeup_time"], "2026-06-01T07:00:00+08:00")
            self.assertEqual(
                nights[0]["stages"],
                [{"at": "2026-05-31T22:30:00+08:00", "stage": "deep"}],
            )
        finally:
            con.close()
