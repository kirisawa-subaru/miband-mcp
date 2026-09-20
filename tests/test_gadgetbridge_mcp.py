from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from mcp import Client

from mibandctl.health.server import create_server, current_state
from mibandctl.health.settings import Settings


class GadgetbridgeMCPTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.settings = Settings(Path(self.tmp.name), backend="gadgetbridge")

    async def test_discovery_exposes_only_supported_tools(self):
        async with Client(create_server(self.settings)) as client:
            found = await client.list_tools()
            self.assertEqual({tool.name for tool in found.tools}, {
                "get_current_state", "get_daily_report", "query_health", "sync_health",
            })
            resource = await client.read_resource("health://device-profile")
            profile = json.loads(resource.contents[0].text)
            self.assertEqual(profile["backend"], "gadgetbridge")
            result = await client.call_tool("get_current_state", {"freshness": "cached"})
            self.assertFalse(result.is_error)
            self.assertEqual(result.structured_content["status"], "no_data")

    async def test_cached_query_never_uses_either_phone_transport(self):
        with patch("mibandctl.gadgetbridge.sync.sync_health") as gb_sync, \
             patch("mibandctl.health.sync.sync_health") as xiaomi_sync:
            await current_state(self.settings, "cached")
        gb_sync.assert_not_called()
        xiaomi_sync.assert_not_called()

    async def test_prefer_fresh_uses_gadgetbridge_and_keeps_offline_cache(self):
        state = {"status": "ok", "data": {"heart_rate": {"latest_value": 70}},
                 "freshness": {"satisfied": False}}
        with patch("mibandctl.gadgetbridge.query.get_current_state", return_value=state), \
             patch("mibandctl.gadgetbridge.sync.read_sync_status", return_value={}), \
             patch("mibandctl.gadgetbridge.sync.sync_health", return_value={"status": "error", "last_error": "offline"}) as gb_sync, \
             patch("mibandctl.health.sync.sync_health") as xiaomi_sync:
            result = await current_state(self.settings, "prefer_fresh")
        self.assertEqual(result["data"]["heart_rate"]["latest_value"], 70)
        self.assertEqual(result["sync"]["status"], "error")
        gb_sync.assert_called_once()
        xiaomi_sync.assert_not_called()

    async def test_mcp_sync_dispatch_and_validation(self):
        with patch("mibandctl.gadgetbridge.sync.sync_health", return_value={"status": "ok"}) as sync:
            async with Client(create_server(self.settings)) as client:
                result = await client.call_tool("sync_health", {"refresh_app": True, "timeout_seconds": 90})
                self.assertFalse(result.is_error)
                sync.assert_called_once_with(self.settings, refresh_app=True, timeout_seconds=90)
                result = await client.call_tool("sync_health", {"timeout_seconds": 999})
                self.assertTrue(result.is_error)
                result = await client.call_tool("query_health", {"kind": "timeseries", "start": "2026-09-21"})
                self.assertTrue(result.is_error)


if __name__ == "__main__":
    unittest.main()
