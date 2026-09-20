from __future__ import annotations

import base64
import fcntl
import json
import shutil
import subprocess
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

from mibandctl.health import device, device_transport
from mibandctl.health.settings import Settings


def vi(value: int) -> bytes:
    return device_transport._varint(value)


def vint(field: int, value: int) -> bytes:
    return vi(field << 3) + vi(value)


def blob(field: int, value: bytes) -> bytes:
    return vi((field << 3) | 2) + vi(len(value)) + value


def command(command_type: int, subtype: int, payload_field: int, payload: bytes) -> bytes:
    return vint(1, command_type) + vint(2, subtype) + blob(payload_field, payload)


class ProtocolTests(unittest.TestCase):
    def test_command_envelope_without_payload(self) -> None:
        self.assertEqual(device_transport.encode_command(8, 45), bytes([8, 8, 16, 45]))

    def test_command_envelope_requires_explicit_payload_field(self) -> None:
        with self.assertRaisesRegex(ValueError, "payload_field"):
            device_transport.encode_command(17, 1, payload=b"x")
        self.assertEqual(
            device_transport.encode_command(17, 1, payload=b"x", payload_field=19),
            bytes([8, 17, 16, 1, 154, 1, 1, 120]),
        )

    def test_decodes_basic_state_with_false_presence_and_raw_activity(self) -> None:
        activity = vint(1, 7) + vint(2, 3)
        state = vint(1, 0) + vint(2, 46) + vint(3, 1) + vint(4, 0) + blob(5, activity)
        packet = command(2, 78, 4, blob(48, state))
        decoded = device._decode_basic_state(packet)
        self.assertEqual(decoded["battery_percent"], 46)
        self.assertFalse(decoded["charging"])
        self.assertTrue(decoded["wearing"])
        self.assertFalse(decoded["sleeping"])
        self.assertEqual(decoded["activity_state"], {"activity_type": 7, "current_state": 3})

    def test_missing_basic_fields_remain_unknown(self) -> None:
        packet = command(2, 78, 4, blob(48, b""))
        decoded = device._decode_basic_state(packet)
        self.assertIsNone(decoded["wearing"])
        self.assertIsNone(decoded["sleeping"])
        self.assertIsNone(decoded["activity_state"])

    def test_monitoring_decoders_preserve_smart_and_disabled_semantics(self) -> None:
        heart = vint(1, 0) + vint(2, 0) + blob(5, vint(1, 1)) + vint(9, 2)
        hr = device._decode_heart_rate(command(8, 10, 10, blob(8, heart)))
        self.assertTrue(hr["enabled"])
        self.assertEqual(hr["mode"], "smart")
        self.assertIsNone(hr["interval_minutes"])
        self.assertEqual(hr["raw_interval_minutes"], 0)
        self.assertTrue(hr["sleep_detection_enabled"])
        self.assertFalse(hr["breathing_quality_enabled"])

    def test_monitoring_alert_switches_preserve_known_disabled_state(self) -> None:
        low = vint(1, 0) + vint(2, 50)
        heart = vint(1, 0) + vint(2, 5) + vint(3, 0) + vint(4, 180) + blob(8, low)
        hr = device._decode_heart_rate(command(8, 10, 10, blob(8, heart)))
        self.assertFalse(hr["high_alert_enabled"])
        self.assertFalse(hr["low_alert_enabled"])
        self.assertIsNone(hr["high_alert_bpm"])
        self.assertIsNone(hr["low_alert_bpm"])

        spo2 = device._decode_spo2(
            command(8, 8, 10, blob(7, vint(2, 1) + blob(4, low)))
        )
        self.assertFalse(spo2["low_alert_enabled"])
        self.assertIsNone(spo2["low_alert_percent"])

    @unittest.skipUnless(shutil.which("node"), "Node.js is required for the JS parser check")
    def test_js_header_decoder_handles_payload_before_command_fields(self) -> None:
        script_path = Path(device_transport.__file__).with_name("device_transport.js")
        harness = """
const fs = require('fs');
const vm = require('vm');
const sandbox = {rpc: {exports: {}}, Java: {}};
vm.createContext(sandbox);
vm.runInContext(fs.readFileSync(process.argv[1], 'utf8'), sandbox);
process.stdout.write(JSON.stringify(sandbox.decodeHeader([26, 1, 0, 8, 2, 16, 78])));
"""
        completed = subprocess.run(
            ["node", "-e", harness, str(script_path)],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
        self.assertEqual(json.loads(completed.stdout), {"type": 2, "subtype": 78})

    @unittest.skipUnless(shutil.which("node"), "Node.js is required for the JS parser check")
    def test_js_header_decoder_defaults_omitted_subtype_to_zero(self) -> None:
        script_path = Path(device_transport.__file__).with_name("device_transport.js")
        harness = """
const fs = require('fs');
const vm = require('vm');
const sandbox = {rpc: {exports: {}}, Java: {}};
vm.createContext(sandbox);
vm.runInContext(fs.readFileSync(process.argv[1], 'utf8'), sandbox);
process.stdout.write(JSON.stringify(sandbox.decodeHeader([8, 17])));
"""
        completed = subprocess.run(
            ["node", "-e", harness, str(script_path)], check=True,
            capture_output=True, text=True, timeout=5,
        )
        self.assertEqual(json.loads(completed.stdout), {"type": 17, "subtype": 0})

    def test_js_uses_native_callback_packet_and_response_request(self) -> None:
        source = Path(device_transport.__file__).with_name("device_transport.js").read_text()
        request_section = source[source.index("function request("):source.index("function sendPacket(")]
        send_section = source[source.index("function sendPacket("):source.index("function cleanup(")]
        self.assertIn("bytesFromBase64(packetBase64), true, callback, timeoutMs", request_section)
        self.assertIn("result.getPacket()", source)
        self.assertIn("PacketSerializer.i(packet)", source)
        self.assertIn("pending.token !== token", source)
        self.assertNotIn(".implementation =", source)
        self.assertIn("bytesFromBase64(packetBase64), false, null, 8000", send_section)

    @unittest.skipUnless(shutil.which("node"), "Node.js is required for the JS token check")
    def test_js_late_callback_cannot_complete_a_newer_request(self) -> None:
        script_path = Path(device_transport.__file__).with_name("device_transport.js")
        harness = """
const fs = require('fs');
const vm = require('vm');
const sandbox = {rpc: {exports: {}}, Java: {}, clearTimeout: function () {}};
vm.createContext(sandbox);
vm.runInContext(fs.readFileSync(process.argv[1], 'utf8'), sandbox);
sandbox.callbackRefs = {1: 'old', 2: 'current'};
sandbox.pending = {token: 2, timer: 17, resolve: function (value) { sandbox.result = value; }};
sandbox.completePending(1, {status: 'late'});
if (sandbox.pending.token !== 2 || sandbox.result !== undefined) process.exit(2);
sandbox.completePending(2, {status: 'ok'});
process.stdout.write(JSON.stringify({pending: sandbox.pending, result: sandbox.result,
  oldRef: sandbox.callbackRefs[1], currentRef: sandbox.callbackRefs[2]}));
"""
        completed = subprocess.run(
            ["node", "-e", harness, str(script_path)], check=True,
            capture_output=True, text=True, timeout=5,
        )
        self.assertEqual(
            json.loads(completed.stdout),
            {"pending": None, "result": {"status": "ok"}, "oldRef": "old"},
        )


class SessionTests(unittest.TestCase):
    def settings(self, root: Path) -> Settings:
        return Settings(data_dir=root, ssh_host="phone", device_serial="expected")

    def test_shared_lock_rejects_concurrent_device_session_before_phone_access(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = self.settings(Path(tmp))
            lock = settings.data_dir / "sync.lock"
            with lock.open("a+") as held:
                fcntl.flock(held.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                with mock.patch.object(device_transport, "verify_device") as verify, self.assertRaises(
                    device_transport.DeviceBusyError
                ):
                    with device_transport.device_session(settings):
                        pass
            verify.assert_not_called()

    def test_read_request_reconnects_once_but_write_does_not_by_default(self) -> None:
        class Exports:
            def __init__(self):
                self.requests = 0
                self.reconnects = 0
                self.sends = 0

            def request(self, *_args):
                self.requests += 1
                if self.requests == 1:
                    return {"status": "not_connected"}
                return {"status": "ok", "response": base64.b64encode(b"response").decode()}

            def reconnect(self, _timeout):
                self.reconnects += 1
                return {"status": "ok"}

            def send(self, _packet):
                self.sends += 1
                return {"status": "not_connected"}

        session = device_transport.DeviceSession(mock.Mock(), timeout_seconds=30)
        session.deadline = time.monotonic() + 30
        exports = Exports()
        session.script = mock.Mock(exports_sync=exports)
        session.connection = {}
        result = session.request(2, 1)
        self.assertEqual(result["response"], b"response")
        self.assertEqual(exports.requests, 2)
        self.assertEqual(exports.reconnects, 1)
        with self.assertRaises(device_transport.DeviceUnavailableError):
            session.send(17, 1)
        self.assertEqual(exports.sends, 1)
        self.assertEqual(exports.reconnects, 1)

    def test_cleanup_diagnostic_does_not_replace_completed_result(self) -> None:
        class Exports:
            def request(self, *_args):
                return {"status": "ok", "response": base64.b64encode(b"response").decode()}

            def cleanup(self):
                return {"status": "ok", "hook_restored": False}

        session = device_transport.DeviceSession(mock.Mock(), timeout_seconds=30)
        session.deadline = time.monotonic() + 30
        script = mock.Mock(exports_sync=Exports())
        session.script = script
        result = session.request(2, 1)
        session.__exit__(None, None, None)
        self.assertEqual(
            result["cleanup_errors"],
            ["raw packet callback hook restoration was not confirmed"],
        )
        script.unload.assert_called_once()


class SnapshotTests(unittest.TestCase):
    def settings(self, root: Path) -> Settings:
        return Settings(data_dir=root, ssh_host="phone", device_serial="expected")

    def snapshot(self) -> dict:
        stamp = device._now_iso()
        return {
            "status": "partial",
            "observed_at": stamp,
            "source": "xiaomi_band_live",
            "band": {"connected": True, "battery_percent": 46, "charging": False,
                     "observed_at": stamp, "source": "xiaomi_band_live"},
            "person": {"wearing": True, "sleeping": None, "activity_state": None,
                       "observed_at": stamp, "source": "xiaomi_band_live"},
            "monitoring": {
                "heart_rate": {"enabled": True, "interval_minutes": 0},
                "blood_oxygen": {"enabled": None},
                "stress": {"enabled": True},
                "observed_at": stamp,
                "source": "xiaomi_band_live",
            },
            "reconnect": {"attempted": False, "succeeded": False},
        }

    def test_cached_never_contacts_phone_and_partial_is_not_fresh(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = self.settings(Path(tmp))
            device._persist(settings, self.snapshot())
            with mock.patch.object(device, "_live_snapshot") as live:
                result = device.get_device_snapshot(settings, freshness="cached")
            live.assert_not_called()
            self.assertEqual(result["freshness"]["status"], "partial")
            self.assertIn("person.sleeping", result["freshness"]["missing"])
            self.assertEqual(result["person"]["freshness"]["status"], "partial")

    def test_prefer_fresh_falls_back_to_cached_with_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = self.settings(Path(tmp))
            snapshot = self.snapshot()
            snapshot["observed_at"] = "2000-01-01T00:00:00+00:00"
            device._persist(settings, snapshot)
            with mock.patch.object(
                device, "_live_snapshot", side_effect=device_transport.DeviceUnavailableError("offline")
            ):
                result = device.get_device_snapshot(settings, freshness="prefer_fresh")
            self.assertEqual(result["source"], "device_snapshot_cache")
            self.assertIn("offline", result["refresh_error"])

    def test_recent_partial_cache_avoids_repeated_device_contact(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = self.settings(Path(tmp))
            device._persist(settings, self.snapshot())
            with mock.patch.object(device, "_live_snapshot") as live:
                result = device.get_device_snapshot(settings, freshness="prefer_fresh")
            live.assert_not_called()
            self.assertEqual(result["status"], "partial")
            self.assertEqual(result["freshness"]["status"], "partial")

    def test_require_fresh_returns_structured_unmet_result(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = self.settings(Path(tmp))
            with mock.patch.object(
                device, "_live_snapshot", side_effect=device_transport.DeviceUnavailableError("offline")
            ):
                result = device.get_device_snapshot(settings, freshness="require_fresh")
            self.assertEqual(result["status"], "freshness_unmet")
            self.assertIn("offline", result["refresh_error"])

    def test_cleanup_failure_prevents_snapshot_cache_promotion(self) -> None:
        class Session:
            connection = {}
            cleanup_errors: list[str] = []

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

            def _remaining(self):
                return 20.0

            def request(self, *_args, **_kwargs):
                raise device_transport.DeviceUnavailableError("no response")

            def finalize_transport(self):
                self.cleanup_errors.append("cleanup failed")
                return self.cleanup_errors

        with tempfile.TemporaryDirectory() as tmp:
            settings = self.settings(Path(tmp))
            with mock.patch.object(device, "device_session", return_value=Session()), mock.patch.object(
                device, "_persist"
            ) as persist, self.assertRaisesRegex(device_transport.DeviceTransportError, "cleanup failed"):
                device._live_snapshot(settings, 30)
            persist.assert_not_called()


if __name__ == "__main__":
    unittest.main()
