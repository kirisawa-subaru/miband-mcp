from __future__ import annotations

import fcntl
import hashlib
import io
import json
import queue
import shutil
import sqlite3
import subprocess
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from mibandctl.gadgetbridge import sync


def settings(root: Path, **updates):
    values = {
        "backend": "gadgetbridge",
        "data_dir": root / "state",
        "db_path": root / "cache" / "Gadgetbridge.db",
        "adb_path": "adb",
        "adb_serial": "test-serial",
        "gadgetbridge_package": "nodomain.freeyourgadget.gadgetbridge",
        "gadgetbridge_remote_db": "/storage/emulated/0/Download/Band Data/Gadgetbridge.db",
        "gadgetbridge_device_id": 1,
        "gadgetbridge_device_address": "AA:BB:CC:DD:EE:FF",
    }
    values.update(updates)
    return SimpleNamespace(**values)


def make_db(path: Path, *, activity_rows: int = 1, schema: bool = True) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    try:
        if not schema:
            conn.execute("CREATE TABLE unrelated(value TEXT)")
            conn.commit()
            return
        conn.execute(
            "CREATE TABLE DEVICE (_id INTEGER, NAME TEXT, MANUFACTURER TEXT, IDENTIFIER TEXT, "
            "TYPE_NAME TEXT, MODEL TEXT, ALIAS TEXT)"
        )
        conn.execute(
            "INSERT INTO DEVICE VALUES (1, 'Band', 'Xiaomi', 'AA:BB:CC:DD:EE:FF', "
            "'XIAOMI_SMART_BAND_10', 'o66cn', NULL)"
        )
        conn.execute(
            "CREATE TABLE XIAOMI_ACTIVITY_SAMPLE (TIMESTAMP INTEGER, DEVICE_ID INTEGER, "
            "STEPS INTEGER, HEART_RATE INTEGER, DISTANCE_CM INTEGER, ACTIVE_CALORIES INTEGER)"
        )
        conn.execute(
            "CREATE TABLE XIAOMI_DAILY_SUMMARY_SAMPLE (TIMESTAMP INTEGER, DEVICE_ID INTEGER, "
            "TIMEZONE INTEGER, STEPS INTEGER, HR_MIN INTEGER, HR_MAX INTEGER, HR_AVG INTEGER, CALORIES INTEGER, "
            "ACTIVE_CALORIES INTEGER)"
        )
        conn.execute(
            "CREATE TABLE XIAOMI_MANUAL_SAMPLE (TIMESTAMP INTEGER, DEVICE_ID INTEGER, "
            "TYPE INTEGER, VALUE INTEGER)"
        )
        conn.execute(
            "CREATE TABLE XIAOMI_SLEEP_TIME_SAMPLE (TIMESTAMP INTEGER, WAKEUP_TIME INTEGER, "
            "DEVICE_ID INTEGER, IS_AWAKE INTEGER, TOTAL_DURATION INTEGER, DEEP_SLEEP_DURATION INTEGER, "
            "LIGHT_SLEEP_DURATION INTEGER, REM_SLEEP_DURATION INTEGER, AWAKE_DURATION INTEGER)"
        )
        conn.execute(
            "CREATE TABLE XIAOMI_SLEEP_STAGE_SAMPLE (TIMESTAMP INTEGER, DEVICE_ID INTEGER, STAGE INTEGER)"
        )
        conn.execute(
            "CREATE TABLE BATTERY_LEVEL (TIMESTAMP INTEGER, DEVICE_ID INTEGER, LEVEL INTEGER, "
            "BATTERY_INDEX INTEGER)"
        )
        conn.execute(
            "CREATE TABLE BASE_ACTIVITY_SUMMARY (_id INTEGER, NAME TEXT, START_TIME INTEGER, "
            "END_TIME INTEGER, ACTIVITY_KIND INTEGER, DEVICE_ID INTEGER, SUMMARY_DATA BLOB, "
            "RAW_SUMMARY_DATA BLOB)"
        )
        for index in range(activity_rows):
            conn.execute(
                "INSERT INTO XIAOMI_ACTIVITY_SAMPLE VALUES (?, 1, 1, 80, 100, 1)",
                (1_700_000_000 + index,),
            )
        conn.execute("INSERT INTO BATTERY_LEVEL VALUES (1700000100, 1, 50, 0)")
        conn.commit()
    finally:
        conn.close()


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class GadgetbridgeSyncTests(unittest.TestCase):
    def run_sync(self, cfg, exported: Path, *, refresh_app=False, remote_hash=None):
        expected_hash = remote_hash or digest(exported)

        def pull(_adb, _remote, local, _deadline):
            shutil.copyfile(exported, local)

        completed = subprocess.CompletedProcess([], 0, "123\n", "")
        events = {
            "activity_sync": {"requested": refresh_app, "confirmed": refresh_app},
            "database_export": {"requested": True, "confirmed": True},
        }
        with mock.patch.object(sync, "_resolve_serial", return_value="test-serial"), mock.patch.object(
            sync._Adb, "shell", return_value=completed
        ), mock.patch.object(sync, "_request_exports", return_value=events) as request, mock.patch.object(
            sync, "_remote_sha256", return_value=expected_hash
        ), mock.patch.object(sync, "_pull", side_effect=pull):
            result = sync.sync_health(cfg, refresh_app=refresh_app, timeout_seconds=10)
        return result, request

    def test_success_validates_and_atomically_replaces_cache(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg = settings(root)
            old = root / "old.db"
            exported = root / "export.db"
            make_db(old, activity_rows=1)
            make_db(exported, activity_rows=3)
            cfg.db_path.parent.mkdir(parents=True)
            shutil.copyfile(old, cfg.db_path)

            result, request = self.run_sync(cfg, exported, refresh_app=True)

            self.assertEqual(result["status"], "ok")
            self.assertEqual(result["before"]["rows"]["XIAOMI_ACTIVITY_SAMPLE"], 1)
            self.assertEqual(result["after"]["rows"]["XIAOMI_ACTIVITY_SAMPLE"], 3)
            self.assertEqual(digest(cfg.db_path), digest(exported))
            self.assertEqual(cfg.db_path.stat().st_mode & 0o777, 0o600)
            request.assert_called_once()
            self.assertTrue(request.call_args.args[3])
            state = sync.read_sync_status(cfg)
            self.assertEqual(state["status"], "ok")
            self.assertIsNotNone(state["last_pulled_at"])
            self.assertTrue(state["cache_identity_matches"])
            self.assertEqual(state["db_path"], str(cfg.db_path.resolve()))
            self.assertEqual(state["cache_identity"]["sha256"], digest(exported))

    def test_default_still_exports_but_does_not_request_activity_sync(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg = settings(root)
            exported = root / "export.db"
            make_db(exported)
            result, request = self.run_sync(cfg, exported)
            self.assertEqual(result["status"], "ok")
            self.assertFalse(request.call_args.args[3])

    def test_snapshot_selects_one_device_and_separates_battery_time(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg = settings(root)
            db = root / "multi.db"
            make_db(db, activity_rows=1)
            conn = sqlite3.connect(db)
            conn.execute(
                "INSERT INTO DEVICE VALUES (2, 'Other', 'Xiaomi', '11:22:33:44:55:66', "
                "'OTHER', NULL, NULL)"
            )
            conn.execute("INSERT INTO XIAOMI_ACTIVITY_SAMPLE VALUES (1800000000, 2, 1, 90, 1, 1)")
            conn.execute("INSERT INTO BATTERY_LEVEL VALUES (1900000000, 1, 49, 0)")
            conn.commit()
            conn.close()
            snap = sync._db_snapshot(db, cfg, validate=True)
            self.assertEqual(snap["rows"]["XIAOMI_ACTIVITY_SAMPLE"], 1)
            self.assertEqual(snap["latest_observation_epoch"], 1_700_000_000)
            self.assertEqual(snap["latest_battery_epoch"], 1_900_000_000)

    def test_multiple_database_devices_without_selector_are_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg = settings(root, gadgetbridge_device_id=None, gadgetbridge_device_address=None)
            db = root / "multi.db"
            make_db(db)
            conn = sqlite3.connect(db)
            conn.execute(
                "INSERT INTO DEVICE VALUES (2, 'Other', 'Xiaomi', '11:22:33:44:55:66', "
                "'OTHER', NULL, NULL)"
            )
            conn.execute("INSERT INTO XIAOMI_ACTIVITY_SAMPLE VALUES (1800000000, 2, 1, 90, 1, 1)")
            conn.commit()
            conn.close()
            with self.assertRaisesRegex(ValueError, "multiple Gadgetbridge devices"):
                sync._db_snapshot(db, cfg, validate=True)

    def test_status_write_failure_after_replace_reports_cache_updated(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg = settings(root)
            exported = root / "export.db"
            make_db(exported, activity_rows=4)
            expected_hash = digest(exported)

            def pull(_adb, _remote, local, _deadline):
                shutil.copyfile(exported, local)

            completed = subprocess.CompletedProcess([], 0, "123\n", "")
            with mock.patch.object(sync, "_resolve_serial", return_value="test-serial"), mock.patch.object(
                sync._Adb, "shell", return_value=completed
            ), mock.patch.object(sync, "_request_exports", return_value={}), mock.patch.object(
                sync, "_remote_sha256", return_value=expected_hash
            ), mock.patch.object(sync, "_pull", side_effect=pull), mock.patch.object(
                sync, "_write_status", side_effect=[None, OSError("state disk failed")]
            ):
                result = sync.sync_health(cfg, timeout_seconds=10)
            self.assertEqual(result["status"], "error")
            self.assertTrue(result["cache_updated"])
            self.assertFalse(result["status_persisted"])
            self.assertEqual(digest(cfg.db_path), expected_hash)

    def test_failed_event_preserves_existing_cache_and_success_timestamp(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg = settings(root)
            make_db(cfg.db_path, activity_rows=2)
            before = cfg.db_path.read_bytes()
            cfg.data_dir.mkdir(parents=True)
            sync._write_status(sync._status_path(cfg), {
                "status": "ok", "last_success_at": "earlier", "last_pulled_at": "earlier",
                "db_path": str(cfg.db_path.resolve()),
                "cache_identity": sync._cache_identity(cfg.db_path, digest(cfg.db_path)),
            })
            completed = subprocess.CompletedProcess([], 0, "123\n", "")
            with mock.patch.object(sync, "_resolve_serial", return_value="test-serial"), mock.patch.object(
                sync._Adb, "shell", return_value=completed
            ), mock.patch.object(sync, "_request_exports", side_effect=TimeoutError("no completion event")):
                result = sync.sync_health(cfg, timeout_seconds=2)
            self.assertEqual(result["status"], "error")
            self.assertIn("completion event", result["last_error"])
            self.assertEqual(cfg.db_path.read_bytes(), before)
            self.assertEqual(result["last_success_at"], "earlier")

    def test_status_success_times_are_cleared_when_cache_or_path_changes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg = settings(root)
            make_db(cfg.db_path)
            cfg.data_dir.mkdir(parents=True)
            bound = {
                "status": "ok", "last_success_at": "earlier", "last_pulled_at": "earlier",
                "db_path": str(cfg.db_path.resolve()),
                "cache_identity": sync._cache_identity(cfg.db_path, digest(cfg.db_path)),
            }
            sync._write_status(sync._status_path(cfg), bound)
            replacement = root / "replacement.db"
            make_db(replacement, activity_rows=2)
            replacement.replace(cfg.db_path)
            changed = sync.read_sync_status(cfg)
            self.assertEqual(changed["status"], "cache_changed")
            self.assertIsNone(changed["last_success_at"])
            self.assertIsNone(changed["last_pulled_at"])
            self.assertFalse(changed["cache_identity_matches"])

            other = root / "other" / "Gadgetbridge.db"
            make_db(other)
            other_cfg = settings(root, db_path=other)
            wrong_path = sync.read_sync_status(other_cfg)
            self.assertEqual(wrong_path["status"], "cache_changed")
            self.assertIsNone(wrong_path["last_success_at"])

    def test_corrupt_wrong_schema_and_hash_mismatch_never_replace_cache(self):
        cases = ("corrupt", "schema", "hash")
        for case in cases:
            with self.subTest(case=case), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                cfg = settings(root)
                make_db(cfg.db_path, activity_rows=2)
                old_hash = digest(cfg.db_path)
                exported = root / "export.db"
                if case == "corrupt":
                    exported.write_bytes(b"not sqlite")
                else:
                    make_db(exported, schema=case != "schema")
                remote_hash = "0" * 64 if case == "hash" else digest(exported)
                result, _ = self.run_sync(cfg, exported, remote_hash=remote_hash)
                self.assertEqual(result["status"], "error")
                self.assertEqual(digest(cfg.db_path), old_hash)
                self.assertEqual(list(cfg.db_path.parent.glob(".Gadgetbridge.db.sync-*")), [])

    def test_query_required_column_or_workout_table_missing_never_replace_cache(self):
        for defect in ("heart_rate", "workout_table"):
            with self.subTest(defect=defect), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                cfg = settings(root)
                make_db(cfg.db_path, activity_rows=2)
                old_hash = digest(cfg.db_path)
                exported = root / "export.db"
                make_db(exported, activity_rows=3)
                conn = sqlite3.connect(exported)
                if defect == "heart_rate":
                    conn.execute("ALTER TABLE XIAOMI_ACTIVITY_SAMPLE DROP COLUMN HEART_RATE")
                else:
                    conn.execute("DROP TABLE BASE_ACTIVITY_SUMMARY")
                conn.commit()
                conn.close()
                result, _ = self.run_sync(cfg, exported)
                self.assertEqual(result["status"], "error")
                self.assertIn("unsupported Gadgetbridge schema", result["last_error"])
                self.assertEqual(digest(cfg.db_path), old_hash)

    def test_valid_export_can_repair_an_existing_wrong_schema_cache(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg = settings(root)
            make_db(cfg.db_path, schema=False)
            exported = root / "export.db"
            make_db(exported, activity_rows=3)
            result, _ = self.run_sync(cfg, exported)
            self.assertEqual(result["status"], "ok")
            self.assertIsNone(result["before"])
            self.assertEqual(digest(cfg.db_path), digest(exported))

    def test_concurrent_sync_returns_in_progress_without_adb(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = settings(Path(tmp))
            cfg.data_dir.mkdir(parents=True)
            lock = sync._lock_path(cfg)
            with lock.open("a+") as held:
                fcntl.flock(held.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                with mock.patch("subprocess.run") as run:
                    result = sync.sync_health(cfg, timeout_seconds=2)
                self.assertEqual(result["status"], "in_progress")
                run.assert_not_called()

    def test_multiple_adb_devices_require_explicit_serial(self):
        output = "List of devices attached\na\tdevice\nb\tdevice\n"
        with mock.patch.object(sync._Adb, "run", return_value=subprocess.CompletedProcess([], 0, output, "")):
            with self.assertRaisesRegex(RuntimeError, "exactly one"):
                sync._resolve_serial("adb", None, time.monotonic() + 1)

    def test_single_authorized_device_is_selected_and_unauthorized_explicit_is_rejected(self):
        output = "List of devices attached\na\tunauthorized\nb\tdevice\n"
        with mock.patch.object(sync._Adb, "run", return_value=subprocess.CompletedProcess([], 0, output, "")):
            self.assertEqual(sync._resolve_serial("adb", None, time.monotonic() + 1), "b")
            with self.assertRaisesRegex(ConnectionError, "unauthorized"):
                sync._resolve_serial("adb", "a", time.monotonic() + 1)

    def test_paths_with_spaces_are_shell_quoted_and_injection_is_rejected(self):
        cfg = settings(Path("/tmp"))
        self.assertEqual(sync._validate_settings(cfg)[2], cfg.gadgetbridge_remote_db)
        completed = subprocess.CompletedProcess([], 0, "ok", "")
        with mock.patch("subprocess.run", return_value=completed) as run:
            sync._Adb("/opt/adb tools/adb", "serial").shell(
                "sha256sum", "--", cfg.gadgetbridge_remote_db,
                deadline=time.monotonic() + 2,
            )
        argv = run.call_args.args[0]
        self.assertEqual(argv[0], "/opt/adb tools/adb")
        self.assertIn("'/storage/emulated/0/Download/Band Data/Gadgetbridge.db'", argv[-1])
        self.assertEqual(run.call_args.kwargs["stdin"], subprocess.DEVNULL)
        bad = settings(Path("/tmp"), gadgetbridge_remote_db="/sdcard/a;touch /data/b")
        with self.assertRaisesRegex(ValueError, "invalid"):
            sync._validate_settings(bad)

    def test_read_status_is_local_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = settings(Path(tmp))
            with mock.patch("subprocess.run") as run, mock.patch("subprocess.Popen") as popen:
                status = sync.read_sync_status(cfg)
            self.assertEqual(status["status"], "never_synced")
            run.assert_not_called()
            popen.assert_not_called()


class FakeProcess:
    def __init__(self, *, running=True):
        self.running = running
        self.terminated = False
        self.killed = False
        self.stdout = io.StringIO("")

    def poll(self):
        return None if self.running else 0

    def terminate(self):
        self.terminated = True

    def kill(self):
        self.killed = True
        self.running = False

    def wait(self, timeout=None):
        if self.running:
            raise subprocess.TimeoutExpired("logcat", timeout)
        return 0


class LogcatEventTests(unittest.TestCase):
    def watcher(self):
        watcher = object.__new__(sync._LogcatWatcher)
        watcher.process = FakeProcess()
        watcher.lines = queue.Queue()
        watcher.thread = mock.Mock()
        return watcher

    def test_stale_success_before_marker_cannot_complete_phase(self):
        watcher = self.watcher()
        watcher.lines.put("Broadcasting database export success=true")
        watcher.lines.put("marker-now")
        watcher.wait_for(lambda line: "marker-now" in line, lambda _line: False,
                         time.monotonic() + 1, "marker")
        with self.assertRaisesRegex(TimeoutError, "database export"):
            watcher.wait_for(lambda line: "export success" in line, lambda _line: False,
                             time.monotonic() + 0.02, "database export")

    def test_failure_event_is_not_success(self):
        watcher = self.watcher()
        watcher.lines.put("Broadcasting database export success=false")
        with self.assertRaisesRegex(RuntimeError, "failure"):
            watcher.wait_for(lambda line: "success=true" in line,
                             lambda line: "success=false" in line,
                             time.monotonic() + 1, "database export")

    def test_close_terminates_then_kills_stuck_logcat(self):
        watcher = self.watcher()
        watcher.close(time.monotonic() + 0.01)
        self.assertTrue(watcher.process.terminated)
        self.assertTrue(watcher.process.killed)

    def test_logcat_never_inherits_mcp_protocol_stdin(self):
        process = FakeProcess()
        with mock.patch("subprocess.Popen", return_value=process) as popen:
            watcher = sync._LogcatWatcher(sync._Adb("adb", "serial"), {4321})
            watcher.close(time.monotonic() + 0.01)
        self.assertEqual(popen.call_args.kwargs["stdin"], subprocess.DEVNULL)

    def test_log_filter_accepts_only_target_pid_events_and_markers(self):
        target = "1758410000.000 10001 4321 4322 I GB: Broadcasting database export success=true"
        other = "1758410000.000 10002 9876 9877 I GB: Broadcasting database export success=true"
        marker = "1758410000.000 2000 5555 5555 I MiBandSync: miband_export_nonce"
        unrelated = "1758410000.000 10001 4321 4322 I GB: ordinary health log"
        self.assertTrue(sync._is_relevant_logcat_line(target, {4321}))
        self.assertFalse(sync._is_relevant_logcat_line(other, {4321}))
        self.assertTrue(sync._is_relevant_logcat_line(marker, {4321}, frozenset({"miband_export_nonce"})))
        self.assertFalse(sync._is_relevant_logcat_line(marker, {4321}))
        self.assertFalse(sync._is_relevant_logcat_line(unrelated, {4321}))
        without_uid = "1758410000.000 4321 4322 I GB: Broadcasting database export success=true"
        self.assertTrue(sync._is_relevant_logcat_line(without_uid, {4321}))

    def test_log_filter_accepts_epoch_padding_before_timestamp(self):
        # Real `adb logcat -v epoch` output can right-align the record with
        # leading whitespace.  The phase marker is accepted independently of
        # PID, while completion events must still resolve to the target PID.
        padded = (
            "  1758410000.000 10001 4321 4322 I "
            "nodomain.freeyourgadget.gadgetbridge.util.GB: "
            "Broadcasting activity sync finish"
        )
        self.assertEqual(sync._logcat_pid(padded), 4321)
        self.assertTrue(sync._is_relevant_logcat_line(padded, {4321}))
        self.assertFalse(sync._is_relevant_logcat_line(padded, {9876}))


if __name__ == "__main__":
    unittest.main()
