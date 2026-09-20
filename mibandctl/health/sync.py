"""Transactional Xiaomi Health synchronization from the authorized phone."""

from __future__ import annotations

import fcntl
import json
import os
import sqlite3
import tempfile
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from .importer import (
    canonical_json,
    configure_connection,
    ensure_schema,
    import_paths,
    now_iso,
)
from .phone_exporter import (
    DEFAULT_TABLES,
    export_jsonl,
    refresh_health_app,
    verify_device,
)


WATERMARKS_KEY = "sync.watermarks"
STATUS_KEY = "sync.last_status"
LAST_PULLED_KEY = "last_pulled_at"
MAX_BATCHES = 100
MAX_ERROR_CHARS = 800


def _metadata(conn: sqlite3.Connection, key: str) -> str | None:
    row = conn.execute("SELECT value FROM metadata WHERE key = ?", (key,)).fetchone()
    return None if row is None else str(row[0])


def _put_metadata(conn: sqlite3.Connection, key: str, value: str, updated_at: str) -> None:
    conn.execute(
        """
        INSERT INTO metadata(key, value, updated_at) VALUES (?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
        """,
        (key, value, updated_at),
    )


def _decode_json_object(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    try:
        decoded = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return {}
    return decoded if isinstance(decoded, dict) else {}


def _db_snapshot(conn: sqlite3.Connection) -> dict[str, Any]:
    raw_count = int(conn.execute("SELECT COUNT(*) FROM raw_records").fetchone()[0])
    metric_count = int(conn.execute("SELECT COUNT(*) FROM metric_records").fetchone()[0])
    latest_row = conn.execute(
        """
        SELECT start_at FROM metric_records
        WHERE start_at IS NOT NULL
        ORDER BY julianday(start_at) DESC
        LIMIT 1
        """
    ).fetchone()
    latest = None if latest_row is None else latest_row[0]
    return {
        "raw_records": raw_count,
        "metric_records": metric_count,
        "latest_sample_at": latest,
    }


def _bounded_error(exc: BaseException) -> str:
    text = " ".join(f"{type(exc).__name__}: {exc}".split())
    return text[:MAX_ERROR_CHARS]


def _lock_path(settings: Any) -> Path:
    return Path(settings.data_dir) / "sync.lock"


def _lock_is_held(path: Path) -> bool:
    if not path.exists():
        return False
    try:
        with path.open("a+") as handle:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return True
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    except OSError:
        return False
    return False


def _load_status_from_db(db_path: Path) -> dict[str, Any]:
    if not db_path.exists():
        return {}
    try:
        conn = sqlite3.connect(f"{db_path.resolve().as_uri()}?mode=ro", uri=True, timeout=2)
        try:
            status = _decode_json_object(_metadata(conn, STATUS_KEY))
            last_success = _metadata(conn, LAST_PULLED_KEY)
        finally:
            conn.close()
    except sqlite3.Error:
        return {}
    if last_success:
        status["last_success_at"] = last_success
        status["last_pulled_at"] = last_success
    return status


def read_sync_status(settings: Any) -> dict[str, Any]:
    """Return bounded persisted status, reflecting a currently held sync lock."""
    status = _load_status_from_db(Path(settings.db_path))
    if _lock_is_held(_lock_path(settings)):
        return {
            **status,
            "status": "in_progress",
            "last_success_at": status.get("last_success_at"),
            "last_pulled_at": status.get("last_pulled_at"),
        }
    if not status:
        return {
            "status": "never_synced",
            "last_success_at": None,
            "last_pulled_at": None,
            "last_attempt_at": None,
            "last_error": None,
        }
    state = status.get("status")
    if state not in {"ok", "error"}:
        status["status"] = "ok" if status.get("last_success_at") else "never_synced"
    status.setdefault("last_success_at", None)
    status.setdefault("last_pulled_at", status["last_success_at"])
    status.setdefault("last_attempt_at", None)
    status.setdefault("last_error", None)
    return status


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("health sync timed out")
    return remaining


def _build_export_config(settings: Any, watermarks: dict[str, Any]) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    zone = ZoneInfo(settings.timezone)
    bootstrap = int((datetime.now(zone) - timedelta(days=settings.bootstrap_days)).timestamp())
    bases: dict[str, dict[str, Any]] = {}
    items = []
    for table in DEFAULT_TABLES:
        mark = watermarks.get(table)
        if not isinstance(mark, dict):
            mark = {"time": bootstrap, "key": ""}
            lookback_seconds = 0
        else:
            mark = {
                "time": max(0, int(mark.get("time") or 0)),
                "key": str(mark.get("key") or ""),
            }
            lookback_seconds = max(0, int(settings.lookback_hours * 3600))
        bases[table] = mark
        items.append(
            {
                "name": table,
                "since_time": mark["time"],
                "since_key": mark["key"],
                "lookback_seconds": lookback_seconds,
            }
        )
    return {"tables": items}, bases


def _merge_watermarks(
    bases: dict[str, dict[str, Any]], imported: dict[str, Any]
) -> dict[str, dict[str, Any]]:
    result = {table: {"time": int(mark["time"]), "key": str(mark["key"])} for table, mark in bases.items()}
    for table, candidate in imported.items():
        if not isinstance(candidate, dict):
            continue
        new = (int(candidate.get("time") or 0), str(candidate.get("key") or ""))
        old = result.get(table, {"time": 0, "key": ""})
        if new > (int(old.get("time") or 0), str(old.get("key") or "")):
            result[table] = {"time": new[0], "key": new[1]}
    return result


def _persist_attempt_status(db_path: Path, status: dict[str, Any]) -> None:
    conn = sqlite3.connect(db_path, timeout=10)
    conn.row_factory = sqlite3.Row
    configure_connection(conn)
    try:
        ensure_schema(conn)
        conn.commit()
        with conn:
            _put_metadata(conn, STATUS_KEY, canonical_json(status), now_iso())
    finally:
        conn.close()


def sync_health(
    settings: Any,
    *,
    refresh_app: bool = False,
    timeout_seconds: int = 120,
) -> dict[str, Any]:
    """Export, import and advance watermarks as one singleton sync operation."""
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    data_dir = Path(settings.data_dir)
    data_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    data_dir.chmod(0o700)
    lock_handle = _lock_path(settings).open("a+")
    _lock_path(settings).chmod(0o600)
    try:
        try:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return {
                **read_sync_status(settings),
                "status": "in_progress",
            }

        deadline = time.monotonic() + timeout_seconds
        attempted_at = now_iso()
        db_path = Path(settings.db_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)

        # A killed process may leave only these local staging files. Holding the
        # singleton lock makes cleanup safe and keeps private exports bounded.
        for stale in data_dir.glob(".health-export-*"):
            try:
                stale.unlink()
            except FileNotFoundError:
                pass

        conn = sqlite3.connect(db_path, timeout=min(30, timeout_seconds))
        db_path.chmod(0o600)
        conn.row_factory = sqlite3.Row
        configure_connection(conn)
        try:
            # executescript may commit implicitly; schema work must precede the
            # explicit import transaction.
            ensure_schema(conn)
            conn.commit()
            before = _db_snapshot(conn)
            watermarks = _decode_json_object(_metadata(conn, WATERMARKS_KEY))
        finally:
            conn.close()

        in_progress = {
            "status": "in_progress",
            "last_attempt_at": attempted_at,
            "last_success_at": read_sync_status(settings).get("last_success_at"),
            "last_pulled_at": read_sync_status(settings).get("last_pulled_at"),
            "last_error": None,
        }
        _persist_attempt_status(db_path, in_progress)

        export_config, base_watermarks = _build_export_config(settings, watermarks)
        output_path: Path | None = None
        stderr_path: Path | None = None
        try:
            output_fd, output_name = tempfile.mkstemp(
                prefix=".health-export-", suffix=".jsonl", dir=data_dir
            )
            os.close(output_fd)
            output_path = Path(output_name)
            stderr_fd, stderr_name = tempfile.mkstemp(
                prefix=".health-export-", suffix=".stderr", dir=data_dir
            )
            os.close(stderr_fd)
            stderr_path = Path(stderr_name)
            verify_device(
                settings.ssh_host,
                settings.device_serial,
                timeout_seconds=_remaining(deadline),
            )
            refresh_result: dict[str, Any] = {"requested": False}
            if refresh_app:
                refresh_result = refresh_health_app(
                    settings.ssh_host,
                    timeout_seconds=_remaining(deadline),
                )
            remote_stats = export_jsonl(
                settings.ssh_host,
                export_config,
                output_path,
                stderr_path,
                timeout_seconds=_remaining(deadline),
            )

            batch_id = datetime.now(ZoneInfo(settings.timezone)).strftime("sync-%Y%m%d-%H%M%S-%f")
            conn = sqlite3.connect(db_path, timeout=min(30, max(1, int(_remaining(deadline)))))
            db_path.chmod(0o600)
            conn.row_factory = sqlite3.Row
            configure_connection(conn)
            conn.set_progress_handler(
                lambda: int(time.monotonic() >= deadline),
                1000,
            )
            try:
                ensure_schema(conn)
                conn.commit()
                with conn:
                    conn.execute(
                        """
                        INSERT INTO import_batches(batch_id, started_at, input_paths_json)
                        VALUES (?, ?, ?)
                        """,
                        (batch_id, attempted_at, canonical_json(["ssh-jsonl-stream"])),
                    )
                    import_stats = import_paths(
                        conn,
                        [output_path],
                        batch_id=batch_id,
                        include_deleted_metrics=False,
                        progress_every=0,
                        deadline_monotonic=deadline,
                    )
                    merged_watermarks = _merge_watermarks(
                        base_watermarks, import_stats.pop("watermarks", {})
                    )
                    finished_at = now_iso()
                    after = _db_snapshot(conn)
                    result = {
                        "status": "ok",
                        "last_attempt_at": attempted_at,
                        "last_success_at": finished_at,
                        "last_pulled_at": finished_at,
                        "last_error": None,
                        "records_exported": int(remote_stats.get("total_rows") or 0),
                        "before": before,
                        "after": after,
                        "import": {
                            "records_seen": import_stats["records_seen"],
                            "raw": import_stats["raw"],
                            "metrics": import_stats["metrics"],
                            "records_by_table": import_stats["records_by_table"],
                        },
                        "refresh": refresh_result,
                    }
                    conn.execute(
                        "UPDATE import_batches SET finished_at=?, stats_json=? WHERE batch_id=?",
                        (finished_at, canonical_json(result["import"]), batch_id),
                    )
                    _put_metadata(conn, WATERMARKS_KEY, canonical_json(merged_watermarks), finished_at)
                    _put_metadata(conn, LAST_PULLED_KEY, finished_at, finished_at)
                    _put_metadata(conn, STATUS_KEY, canonical_json(result), finished_at)
                    conn.execute(
                        """
                        DELETE FROM import_batches
                        WHERE batch_id NOT IN (
                            SELECT batch_id FROM import_batches
                            ORDER BY COALESCE(finished_at, started_at) DESC, batch_id DESC
                            LIMIT ?
                        )
                        """,
                        (MAX_BATCHES,),
                    )
                    _remaining(deadline)
            finally:
                conn.set_progress_handler(None, 0)
                conn.close()
            return result
        finally:
            if output_path is not None:
                output_path.unlink(missing_ok=True)
            if stderr_path is not None:
                stderr_path.unlink(missing_ok=True)
    except Exception as exc:
        failed_at = now_iso()
        previous = _load_status_from_db(Path(settings.db_path))
        error_status = {
            "status": "error",
            "last_attempt_at": attempted_at if "attempted_at" in locals() else failed_at,
            "failed_at": failed_at,
            "last_success_at": previous.get("last_success_at"),
            "last_pulled_at": previous.get("last_pulled_at"),
            "last_error": _bounded_error(exc),
        }
        try:
            _persist_attempt_status(Path(settings.db_path), error_status)
        except Exception:
            pass
        return error_status
    finally:
        try:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
        finally:
            lock_handle.close()
