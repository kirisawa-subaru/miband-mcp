"""Non-MCP entry for scheduled sync, operations and the same health queries."""
from __future__ import annotations

import argparse
import asyncio
import json
import sqlite3
import sys

from .backend import modules
from .settings import Settings


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="miband-health")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("serve", help="Run MCP over stdio")
    sub.add_parser("status", help="Read last sync status")
    s = sub.add_parser("sync", help="Pull phone records into local cache")
    s.add_argument("--sync-device", "--refresh-app", dest="refresh_app", action="store_true",
                   help="Request recorded-data synchronization through the configured backend before pulling")
    s.add_argument("--timeout", type=int, default=120)
    m = sub.add_parser("measure", help="Request one live band heart-rate reading, then stop")
    m.add_argument("--timeout", type=int, default=60)
    b = sub.add_parser("band", help="Band connection, battery and monitoring settings")
    b.add_argument("--freshness", choices=["cached", "prefer_fresh", "require_fresh"], default="prefer_fresh")
    b.add_argument("--max-age", type=int, default=120)
    b.add_argument("--timeout", type=int, default=30)
    n = sub.add_parser("now", help="Recent health records from the configured backend")
    n.add_argument("--freshness", choices=["cached", "prefer_fresh", "require_fresh"], default="cached")
    n.add_argument("--max-age", type=int, default=900)
    n.add_argument("--timeout", type=int, default=60)
    d = sub.add_parser("daily", help="Natural day with waking-day sleep")
    d.add_argument("date")
    d.add_argument("--timezone")
    d.add_argument("--compare-days", type=int, default=7)
    args = parser.parse_args(argv)
    try:
        settings = Settings.from_env()
        query, sync = modules(settings)
        if args.command == "serve":
            from .server import main as serve
            serve()
            return 0
        if args.command == "measure":
            if settings.backend != "xiaomi_health":
                raise ValueError("live heart-rate measurement is not supported by the Gadgetbridge backend")
            from .measurement import measure_heart_rate
            result = measure_heart_rate(settings, timeout_seconds=args.timeout)
        elif args.command == "sync":
            if not 1 <= args.timeout <= 180:
                raise ValueError("timeout must be 1..180 seconds")
            result = sync.sync_health(settings, refresh_app=args.refresh_app, timeout_seconds=args.timeout)
        elif args.command == "status":
            result = sync.read_sync_status(settings)
        elif args.command == "band":
            if settings.backend != "xiaomi_health":
                raise ValueError("live band status is not supported by Gadgetbridge; use now for exported battery data")
            from .server import band_status
            result = asyncio.run(band_status(settings, args.freshness, args.max_age, args.timeout))
        elif args.command == "now":
            if settings.backend == "gadgetbridge":
                from .server import current_state
                result = asyncio.run(current_state(settings, args.freshness, args.max_age, min(args.timeout, 30)))
            else:
                from .server import health_status
                result = asyncio.run(health_status(settings, args.freshness, args.max_age, args.timeout))
        else:
            result = query.get_daily_report(settings, date=args.date, timezone=args.timezone,
                                           compare_days=args.compare_days)
        print(json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False))
        return 1 if result.get("status") in {"error", "freshness_unmet"} else 0
    except (OSError, ValueError, RuntimeError, sqlite3.Error) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}, ensure_ascii=False))
        return 1


if __name__ == "__main__":
    sys.exit(main())
