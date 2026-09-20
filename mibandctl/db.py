"""SQLite connection + Gadgetbridge Xiaomi-table normalization.

vendor 怪癖在这里处理掉，上层只看归一化后的值。
"""
from __future__ import annotations

import functools
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

# The legacy CLI shares the default MCP export cache. Explicit source selection
# remains available through MIBAND_DB_PATH; candidate ordering is only fallback.
DEFAULT_DB_PATHS = [
    str(Path(os.environ.get("XDG_DATA_HOME", str(Path.home() / ".local/share")))
        / "miband-gadgetbridge/Gadgetbridge.db"),
]
LOCAL_TZ = timezone(timedelta(hours=8))

STAGE_MAP = {
    0: "not_sleep",
    1: "na",
    2: "deep",
    3: "light",
    4: "rem",
    5: "awake",
}


def _candidate_paths() -> list[Path]:
    """Paths to consider. Env override wins outright (single-source mode)."""
    override = os.environ.get("MIBAND_DB_PATH")
    if override:
        return [Path(override).expanduser()]
    return [Path(p).expanduser() for p in DEFAULT_DB_PATHS]


def sqlite_ro_uri(p: Path) -> str:
    """SQLite read-only file URI for local paths, including spaces and URI chars."""
    return f"{p.expanduser().resolve().as_uri()}?mode=ro"


def _quick_latest_ts(p: Path) -> int:
    """Latest sample timestamp across v0 tables, in unix seconds. -1 if unreadable/empty.

    Used only for picking between candidate DBs — kept inline to avoid
    circular import with freshness.py. Mirrors freshness.latest_data_ts_seconds.
    """
    try:
        con = sqlite3.connect(sqlite_ro_uri(p), uri=True)
    except sqlite3.Error:
        return -1
    try:
        sec_sql = [
            "SELECT MAX(TIMESTAMP) FROM XIAOMI_ACTIVITY_SAMPLE",
            "SELECT MAX(TIMESTAMP) FROM BATTERY_LEVEL",
        ]
        ms_sql = [
            "SELECT MAX(TIMESTAMP) FROM XIAOMI_DAILY_SUMMARY_SAMPLE",
            "SELECT MAX(TIMESTAMP) FROM XIAOMI_MANUAL_SAMPLE",
            "SELECT MAX(TIMESTAMP) FROM XIAOMI_SLEEP_TIME_SAMPLE",
            "SELECT MAX(TIMESTAMP) FROM XIAOMI_SLEEP_STAGE_SAMPLE",
            "SELECT MAX(WAKEUP_TIME) FROM XIAOMI_SLEEP_TIME_SAMPLE",
        ]
        best = -1
        for sql in sec_sql:
            try:
                v = con.execute(sql).fetchone()[0]
                if v is not None:
                    best = max(best, int(v))
            except sqlite3.Error:
                pass
        for sql in ms_sql:
            try:
                v = con.execute(sql).fetchone()[0]
                if v is not None:
                    best = max(best, int(v) // 1000)
            except sqlite3.Error:
                pass
        return best
    finally:
        con.close()


@functools.lru_cache(maxsize=1)
def db_path() -> Path:
    """Pick the candidate DB with the freshest data (largest latest-sample timestamp).

    Cached per-process: the choice is made once on first call, all later
    callers see the same path within a single mibandctl invocation. This
    is important because some commands hit db_path() multiple times
    (freshness reporting + the actual data query).

    Selection rule:
    - If MIBAND_DB_PATH is set, that path is used regardless.
    - Otherwise from DEFAULT_DB_PATHS, keep only paths that exist on disk,
      then pick the one with the most-recent sample TIMESTAMP. Falls back
      to first-existing if all data timestamps are unreadable.
    - If no candidate exists at all, raises FileNotFoundError.
    """
    candidates = [p for p in _candidate_paths() if p.exists()]
    if not candidates:
        attempted = ", ".join(str(p) for p in _candidate_paths())
        raise FileNotFoundError(f"No Gadgetbridge.db found. Tried: {attempted}")
    if len(candidates) == 1:
        return candidates[0]

    scored = [(p, _quick_latest_ts(p)) for p in candidates]
    scored.sort(key=lambda x: x[1], reverse=True)
    return scored[0][0]


def connect() -> sqlite3.Connection:
    p = db_path()
    if not p.exists():
        raise FileNotFoundError(f"DB not found at {p}")
    con = sqlite3.connect(sqlite_ro_uri(p), uri=True)
    con.row_factory = sqlite3.Row
    return con


def iso_from_seconds(ts: int | None) -> str | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=LOCAL_TZ).isoformat(timespec="seconds")


def iso_from_millis(ts: int | None) -> str | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts / 1000, tz=LOCAL_TZ).isoformat(timespec="seconds")


def normalize_hr(v: int | None) -> int | None:
    return None if v in (None, 0) else v


def normalize_stress(v: int | None) -> int | None:
    return None if v in (None, 0, 255) else v


def normalize_spo2(v: int | None) -> int | None:
    return None if v in (None, 0, 255) else v


def normalize_energy(v: int | None) -> int | None:
    return None if v is None or v < 0 else v


def decode_stage(v: int | None) -> str | None:
    """Sleep stage int → string. Unknown values surfaced as 'unknown:N'."""
    if v is None:
        return None
    return STAGE_MAP.get(v, f"unknown:{v}")


def decode_session_state(v: int | None) -> str:
    """IS_AWAKE field is actually !isSleepFinish — three-state."""
    if v is None:
        return "unspecified"
    return "in_progress" if v == 1 else "final"


def decode_standing_bitmask(v: int | None) -> list[int] | None:
    """STANDING is a 24-bit hour-of-day bitmask. bit 0 = 00:00-01:00."""
    if v is None:
        return None
    return [h for h in range(24) if (v >> h) & 1]


def decode_tz_offset(blocks: int | None) -> str | None:
    """TIMEZONE field is in 15-minute blocks. 32 → +08:00."""
    if blocks is None:
        return None
    minutes = blocks * 15
    sign = "+" if minutes >= 0 else "-"
    minutes = abs(minutes)
    return f"{sign}{minutes // 60:02d}:{minutes % 60:02d}"


def parse_iso_to_seconds(s: str) -> int:
    """Parse ISO-8601 string (with or without TZ; assume LOCAL_TZ if naive) → unix seconds."""
    if s.endswith("Z"):
        s = f"{s[:-1]}+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=LOCAL_TZ)
    return int(dt.timestamp())


def parse_iso_to_millis(s: str) -> int:
    return parse_iso_to_seconds(s) * 1000


def days_ago_seconds(n: int) -> tuple[int, int]:
    """Return (start_unix_sec, end_unix_sec) for last N days, where 'today' is included.

    N=1 → just today (00:00 local → now).
    N=7 → 6 days ago 00:00 local → now.
    """
    if n < 1:
        raise ValueError("n must be >= 1")
    now = datetime.now(LOCAL_TZ)
    end = int(now.timestamp())
    start_local = (now - timedelta(days=n - 1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    start = int(start_local.timestamp())
    return start, end


def resolve_window_seconds(args) -> tuple[int, int]:
    """Resolve shared CLI window args to unix seconds."""
    if bool(args.start) != bool(args.end):
        raise ValueError("--start and --end must be provided together")
    if args.start:
        start = parse_iso_to_seconds(args.start)
        end = parse_iso_to_seconds(args.end)
        if start >= end:
            raise ValueError("--start must be < --end")
        return start, end
    return days_ago_seconds(args.n)


def resolve_window_millis(args) -> tuple[int, int]:
    start, end = resolve_window_seconds(args)
    return start * 1000, end * 1000


def hour_floor_local(ts_seconds: int) -> int:
    """Floor a unix-second timestamp to the local-hour boundary."""
    dt = datetime.fromtimestamp(ts_seconds, tz=LOCAL_TZ)
    floored = dt.replace(minute=0, second=0, microsecond=0)
    return int(floored.timestamp())


def avg(values: Iterable[float]) -> float | None:
    vs = [v for v in values if v is not None]
    if not vs:
        return None
    return round(sum(vs) / len(vs), 1)
