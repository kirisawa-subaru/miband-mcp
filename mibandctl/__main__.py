"""mibandctl entry point.

6 子命令：health / now / sleep / hr / activity / daily。
所有输出走 stdout JSON。
"""
from __future__ import annotations

import argparse
import json
import sys

from .commands import activity, archive, daily, health, hr, now, sleep


def _add_window_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("n", nargs="?", type=int, default=1, help="last N days (default 1)")
    p.add_argument("--start", help="ISO-8601 inclusive start (overrides N)")
    p.add_argument("--end", help="ISO-8601 inclusive end")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="mibandctl")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("health", help="sync/freshness status")
    sub.add_parser("now", help="latest snapshot + last-hour rollup")

    s_sleep = sub.add_parser("sleep", help="last N nights")
    _add_window_args(s_sleep)

    s_hr = sub.add_parser("hr", help="last N days, hourly buckets by default")
    _add_window_args(s_hr)
    s_hr.add_argument("--minute", action="store_true", help="emit raw minute-level rows")

    s_act = sub.add_parser("activity", help="last N days, hourly steps/distance/calories")
    _add_window_args(s_act)
    s_act.add_argument("--minute", action="store_true", help="emit raw minute-level rows")

    s_daily = sub.add_parser("daily", help="last N days of XIAOMI_DAILY_SUMMARY rows")
    _add_window_args(s_daily)

    s_arc = sub.add_parser("archive", help="local append-only history archive")
    arc_sub = s_arc.add_subparsers(dest="archive_cmd", required=True)
    arc_sub.add_parser("sync", help="upsert all v0 tables from source into archive")
    arc_sub.add_parser("info", help="archive path / size / row counts / oldest-newest")

    return p


COMMANDS = {
    "health": health.run,
    "now": now.run,
    "sleep": sleep.run,
    "hr": hr.run,
    "activity": activity.run,
    "daily": daily.run,
    "archive": archive.run,
}


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = COMMANDS[args.cmd](args)
    except (FileNotFoundError, ValueError) as e:
        json.dump({"error": str(e)}, sys.stdout)
        sys.stdout.write("\n")
        return 2
    json.dump(result, sys.stdout, ensure_ascii=False, indent=2)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
