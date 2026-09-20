from __future__ import annotations

import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from mibandctl.health.settings import Settings


class SettingsTests(unittest.TestCase):
    def test_backends_use_separate_default_caches(self):
        with tempfile.TemporaryDirectory() as tmp, patch.dict(os.environ, {"XDG_DATA_HOME": tmp}, clear=True):
            xiaomi = Settings.from_env()
            with patch.dict(os.environ, {"MIBAND_HEALTH_BACKEND": "gadgetbridge"}):
                gadgetbridge = Settings.from_env()
        self.assertEqual(xiaomi.db_path, Path(tmp) / "miband-health/health.sqlite")
        self.assertEqual(gadgetbridge.db_path, Path(tmp) / "miband-gadgetbridge/Gadgetbridge.db")

    def test_explicit_config_paths_and_environment_overrides(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            config = base / "client.json"
            config.write_text(json.dumps({
                "backend": "gadgetbridge", "data_dir": "cache", "gadgetbridge_db_path": "export.db",
                "adb_serial": "configured-phone", "gadgetbridge_device_id": 2,
            }))
            with patch.dict(os.environ, {
                "MIBAND_HEALTH_CONFIG": str(config), "MIBAND_HEALTH_ADB_SERIAL": "other-phone",
                "MIBAND_HEALTH_GADGETBRIDGE_DEVICE_ID": "3",
            }, clear=True):
                settings = Settings.from_env()
            self.assertEqual(settings.data_dir, base / "cache")
            self.assertEqual(settings.db_path, base / "export.db")
            self.assertEqual(settings.adb_serial, "other-phone")
            self.assertEqual(settings.gadgetbridge_device_id, 3)

    def test_bad_config_is_not_silently_ignored(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = Path(tmp) / "client.json"
            for value in (["gadgetbridge"], {"backned": "gadgetbridge"}, {"backend": "unknown"},
                          {"gadgetbridge_device_id": True}, {"timezone": "Not/AZone"},
                          {"backend": []}, {"data_dir": 1}):
                config.write_text(json.dumps(value))
                with patch.dict(os.environ, {"MIBAND_HEALTH_CONFIG": str(config)}, clear=True):
                    with self.assertRaises(ValueError):
                        Settings.from_env()

    def test_xiaomi_phone_identity_can_be_configured(self):
        with patch.dict(os.environ, {"MIBAND_HEALTH_DEVICE_SERIAL": "user-phone"}, clear=True):
            self.assertEqual(Settings.from_env().device_serial, "user-phone")

    def test_unconfigured_xiaomi_never_contacts_a_developer_phone(self):
        from mibandctl.health.phone_exporter import verify_device
        with patch.dict(os.environ, {}, clear=True):
            settings = Settings.from_env()
        self.assertIsNone(settings.ssh_host)
        self.assertIsNone(settings.device_serial)
        with patch("mibandctl.health.phone_exporter._run_ssh_text") as transport:
            with self.assertRaisesRegex(ValueError, "configure device_serial"):
                verify_device(settings.ssh_host, settings.device_serial, timeout_seconds=1)
        transport.assert_not_called()


if __name__ == "__main__":
    unittest.main()
