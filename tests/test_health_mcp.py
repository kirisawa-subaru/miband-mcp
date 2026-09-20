from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mcp import Client

from mibandctl.health.server import band_status, create_server, current_state, health_status
from mibandctl.health.settings import Settings


def state(fresh: bool = False) -> dict:
    return {"status": "ok", "data": {}, "freshness": {"satisfied": fresh}}


class FreshnessTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.settings = Settings(Path(self.tmp.name))

    async def test_cached_never_contacts_phone(self):
        with patch("mibandctl.health.server.query.get_current_state", return_value=state()), \
             patch("mibandctl.health.server.sync.sync_health") as pull:
            result = await current_state(self.settings, "cached")
        pull.assert_not_called()
        self.assertFalse(result["freshness"]["satisfied"])

    async def test_freshness_requires_new_observations_not_successful_pull(self):
        with patch("mibandctl.health.server.query.get_current_state", return_value=state()), \
             patch("mibandctl.health.server.sync.read_sync_status", return_value={}), \
             patch("mibandctl.health.server.sync.sync_health", return_value={"status": "ok"}):
            result = await current_state(self.settings, "require_fresh")
        self.assertEqual(result["status"], "freshness_unmet")

    async def test_offline_prefer_fresh_returns_cache(self):
        with patch("mibandctl.health.server.query.get_current_state", return_value=state()), \
             patch("mibandctl.health.server.sync.read_sync_status", return_value={}), \
             patch("mibandctl.health.server.sync.sync_health", return_value={"status": "error", "last_error": "offline"}):
            result = await current_state(self.settings, "prefer_fresh")
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["sync"]["status"], "error")

    async def test_successful_refresh_requeries_cache(self):
        with patch("mibandctl.health.server.query.get_current_state", side_effect=[state(), state(True)]) as read, \
             patch("mibandctl.health.server.sync.read_sync_status", return_value={}), \
             patch("mibandctl.health.server.sync.sync_health", return_value={"status": "ok"}) as pull:
            result = await current_state(self.settings, "require_fresh")
        self.assertTrue(result["freshness"]["satisfied"])
        self.assertEqual(read.call_count, 2)
        self.assertNotIn("refresh_app", pull.call_args.kwargs)

    async def test_mcp_discovery_and_missing_database(self):
        async with Client(create_server(self.settings)) as client:
            tools = await client.list_tools()
            self.assertEqual({t.name for t in tools.tools}, {
                "get_current_state", "get_health_status", "get_band_status",
                "get_daily_report", "query_health", "sync_health", "measure_heart_rate",
                "get_band_schedule", "set_band_alarm", "set_band_reminder", "delete_band_schedule"
            })
            result = await client.call_tool("get_current_state", {"freshness": "cached"})
            self.assertFalse(result.is_error)
            self.assertEqual(result.structured_content["status"], "no_data")
            result = await client.call_tool("query_health", {"kind": "timeseries", "start": "2026-09-19"})
            self.assertTrue(result.is_error)

    async def test_health_status_keeps_stale_sleep_state_distinct_from_history(self):
        snapshot = {
            "status": "ok", "observed_at": "2026-09-20T12:00:00Z", "source": "band_protocol",
            "person": {"wearing": True, "sleeping": True, "activity_state": None},
            "freshness": {"status": "stale", "age_seconds": 200},
        }
        health = state(True)
        health["data"]["latest_sleep"] = {"session_id": "sleep-123"}
        with patch("mibandctl.health.server._device_snapshot", return_value=snapshot), \
             patch("mibandctl.health.server.current_state", return_value=health):
            result = await health_status(self.settings, "require_fresh")
        self.assertEqual(result["status"], "freshness_unmet")
        self.assertEqual(result["data"]["latest_sleep"]["session_id"], "sleep-123")
        self.assertEqual(result["data"]["sleep_state"]["status"], "stale")
        self.assertTrue(result["data"]["sleep_state"]["value"])
        self.assertEqual(result["data"]["activity_state"]["status"], "unknown")
        self.assertTrue(result["freshness"]["health_satisfied"])
        self.assertFalse(result["freshness"]["device_state_satisfied"])

    async def test_fresh_device_state_does_not_make_old_hr_fresh(self):
        snapshot = {"status": "ok", "person": {"wearing": True, "sleeping": False, "activity_state": {"raw": 1}},
                    "freshness": {"status": "fresh", "age_seconds": 0}}
        with patch("mibandctl.health.server._device_snapshot", return_value=snapshot), \
             patch("mibandctl.health.server.current_state", return_value=state(False)):
            result = await health_status(self.settings, "require_fresh")
        self.assertEqual(result["status"], "freshness_unmet")
        self.assertFalse(result["freshness"]["health_satisfied"])
        self.assertTrue(result["freshness"]["device_state_satisfied"])

    async def test_band_status_keeps_monitoring_but_excludes_person(self):
        snapshot = {"status": "ok", "band": {"connected": False}, "person": {"sleeping": True},
                    "monitoring": {"heart_rate": {"interval_minutes": 10}}}
        with patch("mibandctl.health.server._device_snapshot", return_value=snapshot) as read:
            result = await band_status(self.settings, "cached")
        self.assertNotIn("person", result)
        self.assertEqual(result["monitoring"]["heart_rate"]["interval_minutes"], 10)
        self.assertEqual(read.call_args.kwargs["freshness"], "cached")

    async def test_band_status_ignores_person_freshness_failure(self):
        snapshot = {
            "status": "freshness_unmet",
            "band": {
                "connected": True,
                "freshness": {"status": "fresh", "missing": []},
            },
            "monitoring": {
                "heart_rate": {"enabled": True},
                "freshness": {"status": "fresh", "missing": []},
            },
            "person": {"freshness": {"status": "unknown", "missing": ["person.state"]}},
            "errors": ["state: device response timed out"],
            "refresh_error": "required device snapshot is incomplete or stale",
            "freshness": {"status": "partial", "missing": ["person.state"]},
        }
        with patch("mibandctl.health.server._device_snapshot", return_value=snapshot):
            result = await band_status(self.settings, "require_fresh")
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["freshness"]["status"], "fresh")
        self.assertNotIn("refresh_error", result)
        self.assertEqual(result["errors"], ["state: device response timed out"])

    async def test_schedule_unknown_outcome_remains_structured_and_not_retry_safe(self):
        outcome = {"status": "outcome_unknown", "retry_safe": False, "readback_confirmed": False}
        with patch("mibandctl.health.schedules.set_band_alarm", return_value=outcome) as write:
            async with Client(create_server(self.settings)) as client:
                result = await client.call_tool("set_band_alarm", {
                    "time": "08:30", "weekdays": [1, 2, 3, 4, 5], "alarm_id": 7,
                })
        self.assertFalse(result.is_error)
        self.assertFalse(result.structured_content["retry_safe"])
        self.assertEqual(result.structured_content["status"], "outcome_unknown")
        self.assertEqual(write.call_args.kwargs["alarm_id"], 7)

    async def test_reminder_requires_offset_before_contacting_device(self):
        with patch("mibandctl.health.device_transport.device_session") as contact:
            async with Client(create_server(self.settings)) as client:
                result = await client.call_tool("set_band_reminder", {
                    "at": "2026-09-22T08:30:00", "title": "Test reminder",
                })
        self.assertTrue(result.is_error)
        contact.assert_not_called()

    async def test_schedule_ids_reject_boolean_before_contacting_device(self):
        with patch("mibandctl.health.device_transport.device_session") as contact:
            async with Client(create_server(self.settings)) as client:
                deleted = await client.call_tool(
                    "delete_band_schedule", {"kind": "alarm", "item_id": False}
                )
                updated = await client.call_tool(
                    "set_band_alarm",
                    {"time": "08:30", "weekdays": [], "alarm_id": False},
                )
        self.assertTrue(deleted.is_error)
        self.assertTrue(updated.is_error)
        contact.assert_not_called()


if __name__ == "__main__":
    unittest.main()
