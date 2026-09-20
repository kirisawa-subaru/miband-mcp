from __future__ import annotations

import fcntl
import json
import shutil
import sqlite3
import subprocess
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

from mibandctl.health import sync
from mibandctl.health import background_sync
from mibandctl.health.importer import configure_connection, ensure_schema, import_paths
from mibandctl.health.settings import Settings
from mibandctl.health import phone_exporter


def record(
    *,
    raw_time: int = 1_700_000_000,
    value: dict | None = None,
    deleted: bool = False,
    table: str = "step_record",
    key: str = "daily",
) -> dict:
    value = {"steps": 100, "distance": 75} if value is None else value
    return {
        "source_app": "com.mi.health",
        "source_db": "fitness_data",
        "source_table": table,
        "sid": "device",
        "raw_key": key,
        "raw_time": raw_time,
        "start_at": "2026-09-20T10:00:00+08:00",
        "zone_offset_sec": 28800,
        "zone_name": "Asia/Shanghai",
        "is_upload": True,
        "is_deleted": deleted,
        "value": value,
        "raw_json": json.dumps(value, separators=(",", ":")),
    }


def write_jsonl(path: Path, *rows: dict) -> None:
    path.write_text(
        "".join(json.dumps(row, separators=(",", ":")) + "\n" for row in rows),
        encoding="utf-8",
    )


def open_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    configure_connection(conn)
    ensure_schema(conn)
    conn.commit()
    return conn


def import_file(conn: sqlite3.Connection, path: Path, batch_id: str) -> dict:
    with conn:
        return import_paths(
            conn,
            [path],
            batch_id=batch_id,
            include_deleted_metrics=False,
            progress_every=0,
        )


class ImportRevisionTests(unittest.TestCase):
    def test_revision_replaces_logical_row_and_removes_obsolete_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db_path = root / "health.sqlite"
            first = root / "first.jsonl"
            revised = root / "revised.jsonl"
            write_jsonl(first, record(value={"steps": 100, "distance": 75}))
            write_jsonl(revised, record(value={"steps": 140}))

            conn = open_db(db_path)
            try:
                import_file(conn, first, "first")
                stats = import_file(conn, revised, "revised")

                self.assertEqual(conn.execute("SELECT COUNT(*) FROM raw_records").fetchone()[0], 1)
                metrics = conn.execute(
                    "SELECT metric, value_real FROM metric_records ORDER BY metric"
                ).fetchall()
                self.assertEqual([(row[0], row[1]) for row in metrics], [("steps", 140.0)])
                self.assertEqual(stats["raw"]["updated"], 1)
                self.assertEqual(stats["metrics"]["updated"], 1)
                self.assertEqual(stats["metrics"]["deleted_obsolete"], 1)
            finally:
                conn.close()

    def test_tombstone_keeps_raw_audit_row_and_deletes_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db_path = root / "health.sqlite"
            live = root / "live.jsonl"
            deleted = root / "deleted.jsonl"
            write_jsonl(live, record())
            write_jsonl(deleted, record(deleted=True))

            conn = open_db(db_path)
            try:
                import_file(conn, live, "live")
                import_file(conn, deleted, "deleted")
                raw = conn.execute("SELECT is_deleted FROM raw_records").fetchone()
                self.assertEqual(raw[0], 1)
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM metric_records").fetchone()[0], 0)
            finally:
                conn.close()

    def test_malformed_stream_rolls_back_all_rows_in_transaction(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db_path = root / "health.sqlite"
            stream = root / "bad.jsonl"
            stream.write_text(json.dumps(record()) + "\n{broken\n", encoding="utf-8")

            conn = open_db(db_path)
            try:
                with self.assertRaisesRegex(ValueError, "invalid JSONL"):
                    with conn:
                        conn.execute(
                            "INSERT INTO import_batches(batch_id,started_at,input_paths_json) VALUES ('bad','now','[]')"
                        )
                        import_paths(
                            conn,
                            [stream],
                            batch_id="bad",
                            include_deleted_metrics=False,
                            progress_every=0,
                        )
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM raw_records").fetchone()[0], 0)
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM import_batches").fetchone()[0], 0)
            finally:
                conn.close()

    def test_sleep_duration_uses_asleep_stages_not_sleep_duration_field(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            stream = root / "sleep.jsonl"
            write_jsonl(
                stream,
                record(
                    table="sleep_segment",
                    key="sleep",
                    value={
                        "sleep_deep_duration": 203,
                        "sleep_light_duration": 221,
                        "sleep_rem_duration": 96,
                        "sleep_duration": 360,
                    },
                ),
            )
            conn = open_db(root / "health.sqlite")
            try:
                import_file(conn, stream, "sleep")
                duration = conn.execute(
                    "SELECT value_real FROM metric_records WHERE metric='sleep.duration'"
                ).fetchone()[0]
                self.assertEqual(duration, 520.0)
            finally:
                conn.close()


class BackgroundDeviceSyncTests(unittest.TestCase):
    class FakeExports:
        def __init__(self, script, *, finish=True, hook_restored=True):
            self.script = script
            self.finish = finish
            self.hook_restored = hook_restored
            self.cleanup_calls = 0

        def begin(self):
            for payload in (
                {"phase": "request", "last_sync_time_before": 100},
                {"phase": "start"},
            ):
                self.script.callback({"type": "send", "payload": payload}, None)
            if self.finish:
                self.script.callback(
                    {"type": "send", "payload": {
                        "phase": "finish", "code": 0,
                        "last_sync_time_after": 200,
                    }}, None,
                )

        def cleanup(self):
            self.cleanup_calls += 1
            self.script.callback(
                {"type": "send", "payload": {
                    "phase": "cleanup_done", "hook_restored": self.hook_restored,
                }}, None,
            )

    class FakeScript:
        def __init__(self, *, finish=True, hook_restored=True):
            self.callback = None
            self.exports_sync = BackgroundDeviceSyncTests.FakeExports(
                self, finish=finish, hook_restored=hook_restored
            )
            self.unloaded = False

        def on(self, _event, callback):
            self.callback = callback

        def load(self):
            pass

        def unload(self):
            self.unloaded = True

    class FakeSession:
        def __init__(self, *, finish=True, hook_restored=True):
            self.script = BackgroundDeviceSyncTests.FakeScript(
                finish=finish, hook_restored=hook_restored
            )
            self.detached = False

        def create_script(self, _source):
            return self.script

        def detach(self):
            self.detached = True

    class FakeDevice:
        def __init__(self, *, finish=True, hook_restored=True):
            self.session = BackgroundDeviceSyncTests.FakeSession(
                finish=finish, hook_restored=hook_restored
            )

        def attach(self, _pid):
            return self.session

    def test_script_waits_for_full_finish_and_restores_hook_before_detach(self):
        device = self.FakeDevice()
        collector = background_sync._run_sync_script(
            device, 123, time.monotonic() + 1, cleanup_seconds=0.1
        )
        self.assertTrue(collector.requested)
        self.assertTrue(collector.start_seen)
        self.assertEqual(collector.finish_code, 0)
        self.assertTrue(collector.hook_restored)
        self.assertEqual(device.session.script.exports_sync.cleanup_calls, 1)
        self.assertTrue(device.session.script.unloaded)
        self.assertTrue(device.session.detached)

    def test_timeout_still_restores_hook_before_unload(self):
        device = self.FakeDevice(finish=False)
        collector = background_sync._run_sync_script(
            device, 123, time.monotonic() + 0.04, cleanup_seconds=0.01
        )
        self.assertIn("timed out", " ".join(collector.errors))
        self.assertTrue(collector.hook_restored)
        self.assertEqual(device.session.script.exports_sync.cleanup_calls, 1)
        self.assertTrue(device.session.script.unloaded)
        self.assertTrue(device.session.detached)

    def test_success_requires_full_finish_and_clean_runtime(self):
        transport = {
            "requested": True,
            "start_seen": True,
            "finish_code": 0,
            "completed_at": "2026-09-21T13:00:00.000+00:00",
            "last_progress": 100,
            "last_sync_time_before": 100,
            "last_sync_time_after": 200,
            "hook_restored": True,
            "errors": [],
            "cleanup_errors": [],
        }
        with mock.patch.object(background_sync, "_health_pid", return_value=123), mock.patch.object(
            background_sync, "_sync_via_frida", return_value=transport
        ):
            result = background_sync.sync_device_in_background("phone", timeout_seconds=20)
        self.assertTrue(result["confirmed"])
        self.assertEqual(result["mode"], "background_device_api")
        self.assertFalse(result["ui_interaction"])
        self.assertEqual(result["confirmation_basis"], "SyncObservers.onFinish")

    def test_failed_finish_raises_so_outer_sync_cannot_report_ok(self):
        transport = {
            "requested": True,
            "start_seen": True,
            "finish_code": -1,
            "completed_at": "2026-09-21T13:00:00.000+00:00",
            "last_progress": None,
            "last_sync_time_before": 100,
            "last_sync_time_after": 100,
            "hook_restored": True,
            "errors": [],
            "cleanup_errors": [],
        }
        with mock.patch.object(background_sync, "_health_pid", return_value=123), mock.patch.object(
            background_sync, "_sync_via_frida", return_value=transport
        ), self.assertRaisesRegex(RuntimeError, "code -1"):
            background_sync.sync_device_in_background("phone", timeout_seconds=20)

    def test_finish_without_this_requests_start_is_rejected(self):
        transport = {
            "requested": True,
            "start_seen": False,
            "finish_code": 0,
            "completed_at": "2026-09-21T13:00:00.000+00:00",
            "last_progress": None,
            "last_sync_time_before": 100,
            "last_sync_time_after": 200,
            "hook_restored": True,
            "errors": [],
            "cleanup_errors": [],
        }
        with mock.patch.object(background_sync, "_health_pid", return_value=123), mock.patch.object(
            background_sync, "_sync_via_frida", return_value=transport
        ), self.assertRaisesRegex(RuntimeError, "start was not observed"):
            background_sync.sync_device_in_background("phone", timeout_seconds=20)

    def test_hook_callback_exception_is_rejected(self):
        transport = {
            "requested": True,
            "start_seen": True,
            "finish_code": 0,
            "completed_at": "2026-09-21T13:00:00.000+00:00",
            "last_progress": None,
            "last_sync_time_before": 100,
            "last_sync_time_after": 200,
            "hook_restored": True,
            "errors": ["hook callback failed"],
            "cleanup_errors": [],
        }
        with mock.patch.object(background_sync, "_health_pid", return_value=123), mock.patch.object(
            background_sync, "_sync_via_frida", return_value=transport
        ), self.assertRaisesRegex(RuntimeError, "hook callback failed"):
            background_sync.sync_device_in_background("phone", timeout_seconds=20)

    def test_refresh_wrapper_caps_active_budget_and_has_no_gui_fallback(self):
        expected = {"mode": "background_device_api", "confirmed": True}
        with mock.patch(
            "mibandctl.health.background_sync.sync_device_in_background",
            return_value=expected,
        ) as invoke:
            result = phone_exporter.refresh_health_app("phone", timeout_seconds=120)
        self.assertEqual(result, expected)
        invoke.assert_called_once_with("phone", timeout_seconds=60.0)

    def test_unconfirmed_hook_restoration_is_a_hard_failure(self):
        device = self.FakeDevice(hook_restored=False)
        collector = background_sync._run_sync_script(
            device, 123, time.monotonic() + 1, cleanup_seconds=0.1
        )
        self.assertFalse(collector.hook_restored)
        self.assertIn("hook restoration was not confirmed", " ".join(collector.errors))
        # Frida unload is the bounded second restoration mechanism; the
        # operation still cannot claim success without explicit confirmation.
        self.assertTrue(device.session.script.unloaded)
        self.assertTrue(device.session.detached)

    @unittest.skipUnless(shutil.which("node"), "Node is required for the JS race harness")
    def test_cleanup_closes_late_choose_callback_before_it_can_request_sync(self):
        harness = r'''
const fs = require('fs');
const vm = require('vm');
const code = fs.readFileSync(0, 'utf8');
const queued = [];
const messages = [];
let chooseCallbacks = null;
let syncCalls = 0;
const method = {implementation: null, call: () => undefined};
const mockContact = {
  isIDLE: () => true,
  getLastSyncDataTime: () => 100,
  syncData: {overload: () => ({call: () => { syncCalls += 1; }})}
};
const model = {
  isDeviceConnected: () => true,
  getDid: () => ({toString: () => 'private-did'})
};
const manager = {getCurrentDeviceModel: () => model};
global.send = (payload) => messages.push(payload);
global.rpc = {exports: {}};
global.Java = {
  perform: (fn) => queued.push(fn),
  scheduleOnMainThread: (fn) => queued.push(fn),
  cast: (value, _type) => value,
  choose: (_name, callbacks) => { chooseCallbacks = callbacks; },
  use: (name) => {
    if (name.endsWith('.DeviceContact')) return {Companion: {value: {}}};
    if (name.endsWith('.DeviceSyncExtKt')) return {getInstance: () => mockContact};
    if (name.endsWith('.DeviceContactImpl')) return {};
    if (name.endsWith('.DeviceModelClient')) return {};
    if (name.endsWith('.SyncObservers')) {
      return {
        onStart: {overload: () => ({...method})},
        onFinish: {overload: () => ({...method})}
      };
    }
    throw new Error('unexpected Java.use before delayed onMatch: ' + name);
  }
};
vm.runInThisContext(code);
rpc.exports.begin();
queued.shift()();
rpc.exports.cleanup();
const lateResult = chooseCallbacks.onMatch(manager);
while (queued.length) queued.shift()();
console.log(JSON.stringify({
  lateResult,
  syncCalls,
  requested: messages.some((m) => m.phase === 'request'),
  cleanup: messages.find((m) => m.phase === 'cleanup_done')
}));
'''
        completed = subprocess.run(
            ["node", "-e", harness],
            input=(Path(__file__).parents[1] / "mibandctl/health/sync_device.js").read_text(),
            text=True,
            capture_output=True,
            check=True,
        )
        result = json.loads(completed.stdout)
        self.assertEqual(result["lateResult"], "stop")
        self.assertEqual(result["syncCalls"], 0)
        self.assertFalse(result["requested"])
        self.assertEqual(result["cleanup"], {"phase": "cleanup_done", "hook_restored": True})


class SyncTests(unittest.TestCase):
    def make_settings(self, root: Path) -> Settings:
        return Settings(data_dir=root, ssh_host="phone", device_serial="expected")

    def test_concurrent_lock_returns_in_progress_without_export(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            root.mkdir(mode=0o700, exist_ok=True)
            settings = self.make_settings(root)
            lock_path = root / "sync.lock"
            with lock_path.open("a+") as held:
                fcntl.flock(held.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                with mock.patch.object(sync, "verify_device") as verify:
                    result = sync.sync_health(settings, timeout_seconds=2)
            self.assertEqual(result["status"], "in_progress")
            verify.assert_not_called()

    def test_connection_failure_persists_bounded_error_and_cleans_temp_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            settings = self.make_settings(root)
            with mock.patch.object(sync, "verify_device", side_effect=ConnectionError("offline")):
                result = sync.sync_health(settings, timeout_seconds=2)

            self.assertEqual(result["status"], "error")
            self.assertIn("offline", result["last_error"])
            self.assertEqual(list(root.glob(".health-export-*")), [])
            status = sync.read_sync_status(settings)
            self.assertEqual(status["status"], "error")
            self.assertIsNone(status["last_success_at"])

    def test_success_imports_stream_and_commits_watermark_with_success_time(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            settings = self.make_settings(root)

            def fake_export(_host, _config, output_path, stderr_path, *, timeout_seconds):
                write_jsonl(output_path, record(raw_time=1_800_000_000))
                stderr_path.write_text("", encoding="utf-8")
                return {"total_rows": 1}

            with mock.patch.object(sync, "verify_device", return_value={"serial": "expected"}), mock.patch.object(
                sync, "export_jsonl", side_effect=fake_export
            ):
                result = sync.sync_health(settings, timeout_seconds=10)

            self.assertEqual(result["status"], "ok")
            self.assertEqual(result["records_exported"], 1)
            self.assertEqual(list(root.glob(".health-export-*")), [])
            self.assertEqual(root.stat().st_mode & 0o777, 0o700)
            self.assertEqual(settings.db_path.stat().st_mode & 0o777, 0o600)

            conn = sqlite3.connect(settings.db_path)
            try:
                metadata = dict(conn.execute("SELECT key,value FROM metadata"))
                marks = json.loads(metadata[sync.WATERMARKS_KEY])
                self.assertEqual(marks["step_record"], {"time": 1_800_000_000, "key": "daily"})
                self.assertEqual(metadata[sync.LAST_PULLED_KEY], result["last_success_at"])
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM raw_records").fetchone()[0], 1)
            finally:
                conn.close()

    def test_malformed_followup_preserves_previous_data_and_watermark(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            settings = self.make_settings(root)

            def initial_export(_host, _config, output_path, stderr_path, *, timeout_seconds):
                write_jsonl(output_path, record(raw_time=1_800_000_000))
                stderr_path.write_text("", encoding="utf-8")
                return {"total_rows": 1}

            with mock.patch.object(sync, "verify_device", return_value={"serial": "expected"}), mock.patch.object(
                sync, "export_jsonl", side_effect=initial_export
            ):
                first = sync.sync_health(settings, timeout_seconds=10)

            conn = sqlite3.connect(settings.db_path)
            try:
                before_marks = conn.execute(
                    "SELECT value FROM metadata WHERE key=?", (sync.WATERMARKS_KEY,)
                ).fetchone()[0]
            finally:
                conn.close()

            def malformed_export(_host, _config, output_path, stderr_path, *, timeout_seconds):
                output_path.write_text(json.dumps(record(raw_time=1_900_000_000)) + "\n{bad\n")
                stderr_path.write_text("", encoding="utf-8")
                return {"total_rows": 2}

            with mock.patch.object(sync, "verify_device", return_value={"serial": "expected"}), mock.patch.object(
                sync, "export_jsonl", side_effect=malformed_export
            ):
                failed = sync.sync_health(settings, timeout_seconds=10)

            self.assertEqual(first["status"], "ok")
            self.assertEqual(failed["status"], "error")
            self.assertEqual(failed["last_success_at"], first["last_success_at"])
            conn = sqlite3.connect(settings.db_path)
            try:
                after_marks = conn.execute(
                    "SELECT value FROM metadata WHERE key=?", (sync.WATERMARKS_KEY,)
                ).fetchone()[0]
                self.assertEqual(after_marks, before_marks)
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM raw_records").fetchone()[0], 1)
                self.assertEqual(
                    conn.execute("SELECT MAX(raw_time) FROM raw_records").fetchone()[0],
                    1_800_000_000,
                )
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
