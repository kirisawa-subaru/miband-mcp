from __future__ import annotations

import fcntl
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

from mibandctl.health import measurement
from mibandctl.health.measurement import _MeasurementCollector
from mibandctl.health.settings import Settings


class CollectorTests(unittest.TestCase):
    def test_accepts_only_valid_realtime_hr_and_tracks_acknowledgements(self) -> None:
        collector = _MeasurementCollector(clock=lambda: "2026-09-21T12:42:49.000+00:00")
        collector.handle({"type": "send", "payload": {"phase": "start_ack", "code": 0}})
        collector.handle({"type": "send", "payload": {"phase": "realtime", "heart_rate": True}})
        collector.handle({"type": "send", "payload": {"phase": "realtime", "heart_rate": 8}})
        collector.handle({"type": "send", "payload": {"phase": "realtime", "heart_rate": 1000}})
        collector.handle({"type": "send", "payload": {"phase": "realtime", "heart_rate": 86}})
        collector.handle({"type": "send", "payload": {"phase": "stop_ack", "code": 0}})

        self.assertTrue(collector.start_confirmed)
        self.assertTrue(collector.stop_confirmed)
        self.assertEqual(collector.heart_rate_bpm, 86)
        self.assertEqual(collector.received_at, "2026-09-21T12:42:49.000+00:00")
        self.assertTrue(collector.finished.is_set())

    def test_stop_error_is_terminal_but_not_confirmed(self) -> None:
        collector = _MeasurementCollector()
        collector.handle({"type": "send", "payload": {"phase": "stop_error", "code": 17}})
        self.assertFalse(collector.stop_confirmed)
        self.assertEqual(collector.stop_code, 17)
        self.assertTrue(collector.finished.is_set())


class FridaScriptLifecycleTests(unittest.TestCase):
    class FakeExports:
        def __init__(self, script):
            self.script = script
            self.stop_calls = 0

        def begin(self, _timeout_ms):
            self.script.callback(
                {"type": "send", "payload": {"phase": "start_ack", "code": 0}}, None
            )

        def stop(self):
            self.stop_calls += 1
            self.script.callback(
                {"type": "send", "payload": {"phase": "stop_ack", "code": 0}}, None
            )

    class RaisingExports(FakeExports):
        def begin(self, _timeout_ms):
            self.script.callback(
                {"type": "send", "payload": {"phase": "start_ack", "code": 0}}, None
            )
            raise RuntimeError("RPC transport lost after START")

    class FakeScript:
        def __init__(self):
            self.callback = None
            self.exports_sync = FridaScriptLifecycleTests.FakeExports(self)
            self.loaded = False
            self.unloaded = False

        def on(self, _event, callback):
            self.callback = callback

        def load(self):
            self.loaded = True

        def unload(self):
            self.unloaded = True

    class FakeSession:
        def __init__(self):
            self.script = FridaScriptLifecycleTests.FakeScript()
            self.detached = False

        def create_script(self, _source):
            return self.script

        def detach(self):
            self.detached = True

    class FakeDevice:
        def __init__(self):
            self.session = FridaScriptLifecycleTests.FakeSession()

        def attach(self, _pid):
            return self.session

    class RaisingDevice(FakeDevice):
        def __init__(self):
            super().__init__()
            self.session.script.exports_sync = FridaScriptLifecycleTests.RaisingExports(
                self.session.script
            )

    def test_timeout_requests_stop_before_unload_and_detach(self) -> None:
        device = self.FakeDevice()
        collector = measurement._run_frida_script(
            device,
            123,
            time.monotonic() + 0.04,
            stop_grace_seconds=0.01,
        )
        script = device.session.script
        self.assertEqual(script.exports_sync.stop_calls, 1)
        self.assertTrue(collector.stop_confirmed)
        self.assertIn("timed out", " ".join(collector.errors))
        self.assertTrue(script.unloaded)
        self.assertTrue(device.session.detached)

    def test_begin_exception_after_start_still_stops_before_detach(self) -> None:
        device = self.RaisingDevice()
        collector = measurement._run_frida_script(
            device,
            123,
            time.monotonic() + 1,
            stop_grace_seconds=0.05,
        )
        script = device.session.script
        self.assertTrue(collector.start_confirmed)
        self.assertTrue(collector.stop_confirmed)
        self.assertEqual(script.exports_sync.stop_calls, 1)
        self.assertIn("RPC transport lost", " ".join(collector.errors))
        self.assertTrue(script.unloaded)
        self.assertTrue(device.session.detached)

    def test_script_error_does_not_make_stop_wait_use_generic_finished_event(self) -> None:
        device = self.FakeDevice()
        script = device.session.script

        def begin(_timeout_ms):
            script.callback({"type": "error", "description": "boom"}, None)

        def stop():
            script.exports_sync.stop_calls += 1
            threading.Timer(
                0.02,
                lambda: script.callback(
                    {"type": "send", "payload": {"phase": "stop_ack", "code": 0}}, None
                ),
            ).start()

        import threading
        script.exports_sync.begin = begin
        script.exports_sync.stop = stop
        collector = measurement._run_frida_script(
            device,
            123,
            time.monotonic() + 1,
            stop_grace_seconds=0.2,
        )
        self.assertTrue(collector.stop_confirmed)
        self.assertEqual(script.exports_sync.stop_calls, 1)
        self.assertTrue(script.unloaded)
        self.assertTrue(device.session.detached)


class RuntimeCleanupTests(unittest.TestCase):
    def test_phone_launcher_reports_actual_child_pid_and_signal_cleans_it(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            fake_server = Path(tmp) / "fake-frida-server"
            fake_server.write_text("#!/bin/sh\nexec sleep 30\n", encoding="utf-8")
            fake_server.chmod(0o700)
            launcher = subprocess.Popen(
                [
                    sys.executable,
                    "-c",
                    measurement._FRIDA_SERVER_LAUNCHER,
                    str(fake_server),
                    "27042",
                    "30",
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            try:
                marker = launcher.stdout.readline().strip()
                self.assertRegex(marker, r"^MIBAND_FRIDA_PID=\d+$")
                child_pid = int(marker.partition("=")[2])
                self.assertNotEqual(child_pid, launcher.pid)
                os.kill(child_pid, 0)
                launcher.terminate()
                launcher.wait(timeout=5)
                with self.assertRaises(ProcessLookupError):
                    os.kill(child_pid, 0)
            finally:
                if launcher.poll() is None:
                    launcher.kill()
                    launcher.wait(timeout=2)
                if launcher.stdout is not None:
                    launcher.stdout.close()
                if launcher.stderr is not None:
                    launcher.stderr.close()

    def test_cleanup_targets_recorded_remote_pid_and_both_local_processes(self) -> None:
        runtime = measurement._PhoneFridaRuntime("phone", time.monotonic() + 30)
        runtime.remote_pid = 4321
        runtime.tunnel_process = mock.Mock()
        runtime.server_process = mock.Mock()
        with mock.patch.object(measurement, "_terminate_process") as terminate, mock.patch.object(
            measurement, "_run_ssh_text", return_value=""
        ) as ssh:
            self.assertEqual(runtime.close(), [])

        self.assertEqual(terminate.call_count, 2)
        command = ssh.call_args.args[1]
        self.assertIn("pid=4321", command)
        self.assertIn(measurement.FRIDA_SERVER, command)

    def test_start_failure_returns_runtime_cleanup_errors(self) -> None:
        class FailedRuntime:
            cleanup_errors = ["remote server cleanup failed"]
            server_process = None

            def __init__(self, _host, _deadline):
                pass

            def __enter__(self):
                raise ConnectionError("tunnel failed")

            def __exit__(self, *_args):
                return None

        with mock.patch.object(measurement, "_PhoneFridaRuntime", FailedRuntime):
            result = measurement._measure_via_frida("phone", 123, time.monotonic() + 30)
        self.assertIn("tunnel failed", result["errors"][0])
        self.assertEqual(result["cleanup_errors"], ["remote server cleanup failed"])


class MeasureHeartRateTests(unittest.TestCase):
    def settings(self, root: Path) -> Settings:
        return Settings(data_dir=root, ssh_host="phone", device_serial="expected")

    def successful_transport(self) -> dict:
        return {
            "heart_rate_bpm": 86,
            "received_at": "2026-09-21T12:42:49.000+00:00",
            "start_confirmed": True,
            "stop_confirmed": True,
            "start_code": 0,
            "stop_code": 0,
            "errors": [],
            "cleanup_errors": [],
        }

    def test_success_persists_only_confirmed_sample(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = self.settings(Path(tmp))
            with mock.patch.object(measurement, "verify_device"), mock.patch.object(
                measurement, "_health_pid", return_value=123
            ), mock.patch.object(
                measurement, "_measure_via_frida", return_value=self.successful_transport()
            ):
                result = measurement.measure_heart_rate(settings, timeout_seconds=30)

            self.assertEqual(result["status"], "ok")
            self.assertEqual(result["heart_rate_bpm"], 86)
            self.assertTrue(result["start_confirmed"])
            self.assertTrue(result["stop_confirmed"])
            conn = sqlite3.connect(settings.db_path)
            try:
                raw = conn.execute(
                    "SELECT value FROM metadata WHERE key=?", (measurement.MEASUREMENT_KEY,)
                ).fetchone()[0]
            finally:
                conn.close()
            stored = json.loads(raw)
            self.assertEqual(stored["status"], "ok")
            self.assertEqual(stored["source"], "band_realtime_protocol")
            self.assertEqual(stored["confirmation_basis"], "xiaomi_health_transport_result")
            self.assertEqual(stored["heart_rate_bpm"], 86)
            self.assertTrue(stored["start_confirmed"])
            self.assertTrue(stored["stop_confirmed"])
            self.assertEqual(Path(tmp).stat().st_mode & 0o777, 0o700)
            self.assertEqual(settings.db_path.stat().st_mode & 0o777, 0o600)

    def test_failed_stop_does_not_replace_previous_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = self.settings(Path(tmp))
            good = self.successful_transport()
            bad = {**good, "heart_rate_bpm": 91, "stop_confirmed": False,
                   "stop_code": 9, "errors": ["stop failed"]}
            with mock.patch.object(measurement, "verify_device"), mock.patch.object(
                measurement, "_health_pid", return_value=123
            ), mock.patch.object(
                measurement, "_measure_via_frida", side_effect=[good, bad]
            ):
                first = measurement.measure_heart_rate(settings, timeout_seconds=30)
                second = measurement.measure_heart_rate(settings, timeout_seconds=30)

            self.assertEqual(first["status"], "ok")
            self.assertEqual(second["status"], "error")
            self.assertFalse(second["stop_confirmed"])
            conn = sqlite3.connect(settings.db_path)
            try:
                stored = json.loads(conn.execute(
                    "SELECT value FROM metadata WHERE key=?", (measurement.MEASUREMENT_KEY,)
                ).fetchone()[0])
            finally:
                conn.close()
            self.assertEqual(stored["heart_rate_bpm"], 86)

    def test_shared_lock_returns_in_progress_without_phone_access(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            settings = self.settings(root)
            lock_path = root / "sync.lock"
            with lock_path.open("a+") as held:
                fcntl.flock(held.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                with mock.patch.object(measurement, "verify_device") as verify:
                    result = measurement.measure_heart_rate(settings, timeout_seconds=30)
            self.assertEqual(result["status"], "in_progress")
            verify.assert_not_called()

    def test_missing_app_returns_refresh_prerequisite(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = self.settings(Path(tmp))
            with mock.patch.object(measurement, "verify_device"), mock.patch.object(
                measurement, "_health_pid", return_value=None
            ), mock.patch.object(measurement, "_measure_via_frida") as transport:
                result = measurement.measure_heart_rate(settings, timeout_seconds=30)
            self.assertEqual(result["status"], "error")
            self.assertIn("background process is not running", result["error"])
            transport.assert_not_called()

    def test_timeout_range_is_enforced(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = self.settings(Path(tmp))
            for value in (29, 91):
                with self.subTest(value=value), self.assertRaises(ValueError):
                    measurement.measure_heart_rate(settings, timeout_seconds=value)


if __name__ == "__main__":
    unittest.main()
