"""Explicit backend configuration shared by CLI and MCP clients."""
from __future__ import annotations

from dataclasses import dataclass, fields
import json
import os
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    ssh_host: str | None = None
    device_serial: str | None = None
    timezone: str = "Asia/Shanghai"
    bootstrap_days: int = 30
    lookback_hours: int = 72
    backend: str = "xiaomi_health"
    adb_path: str = "adb"
    adb_serial: str | None = None
    gadgetbridge_package: str = "nodomain.freeyourgadget.gadgetbridge"
    gadgetbridge_remote_db: str | None = None
    gadgetbridge_db_path: Path | None = None
    gadgetbridge_device_id: int | None = None
    gadgetbridge_device_address: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.backend, str) or self.backend not in {"xiaomi_health", "gadgetbridge"}:
            raise ValueError("backend must be xiaomi_health or gadgetbridge")
        if not isinstance(self.timezone, str):
            raise ValueError("timezone must be an IANA timezone name")
        try:
            ZoneInfo(self.timezone)
        except (ValueError, ZoneInfoNotFoundError) as exc:
            raise ValueError(f"unknown timezone: {self.timezone}") from exc
        if not isinstance(self.data_dir, (str, Path)):
            raise ValueError("data_dir must be a path")
        object.__setattr__(self, "data_dir", Path(self.data_dir).expanduser().resolve())
        if self.gadgetbridge_db_path is not None:
            if not isinstance(self.gadgetbridge_db_path, (str, Path)):
                raise ValueError("gadgetbridge_db_path must be a path")
            object.__setattr__(self, "gadgetbridge_db_path", Path(self.gadgetbridge_db_path).expanduser().resolve())
        for name in ("bootstrap_days", "lookback_hours", "gadgetbridge_device_id"):
            value = getattr(self, name)
            if value is not None and (isinstance(value, bool) or not isinstance(value, int) or value < 1):
                raise ValueError(f"{name} must be a positive integer")
        for name in ("adb_path", "gadgetbridge_package"):
            if not isinstance(getattr(self, name), str) or not getattr(self, name).strip():
                raise ValueError(f"{name} must be a non-empty string")
        for name in ("ssh_host", "device_serial", "adb_serial", "gadgetbridge_remote_db", "gadgetbridge_device_address"):
            value = getattr(self, name)
            if value is not None and (not isinstance(value, str) or not value.strip()):
                raise ValueError(f"{name} must be a non-empty string when configured")

    @property
    def db_path(self) -> Path:
        if self.backend == "gadgetbridge":
            return self.gadgetbridge_db_path or self.data_dir / "Gadgetbridge.db"
        return self.data_dir / "health.sqlite"

    @classmethod
    def from_env(cls) -> Settings:
        """Load an explicitly selected JSON config, then environment overrides.

        No config file is discovered implicitly: existing Xiaomi Health timers
        keep their backend when a client opts into a Gadgetbridge config.
        """
        values: dict = {}
        config_name = os.environ.get("MIBAND_HEALTH_CONFIG")
        if config_name:
            config_path = Path(config_name).expanduser().resolve()
            values = json.loads(config_path.read_text(encoding="utf-8"))
            if not isinstance(values, dict):
                raise ValueError("MIBAND_HEALTH_CONFIG must contain a JSON object")
            unknown = values.keys() - {f.name for f in fields(cls)}
            if unknown:
                raise ValueError(f"unknown configuration fields: {', '.join(sorted(unknown))}")
            for name in ("data_dir", "gadgetbridge_db_path"):
                if values.get(name) is not None:
                    if not isinstance(values[name], str):
                        raise ValueError(f"{name} must be a path string")
                    path = Path(values[name]).expanduser()
                    values[name] = path if path.is_absolute() else config_path.parent / path

        env_fields = {
            "backend": "MIBAND_HEALTH_BACKEND",
            "data_dir": "MIBAND_HEALTH_DATA_DIR",
            "ssh_host": "MIBAND_HEALTH_SSH_HOST",
            "device_serial": "MIBAND_HEALTH_DEVICE_SERIAL",
            "timezone": "MIBAND_HEALTH_TIMEZONE",
            "adb_path": "MIBAND_HEALTH_ADB_PATH",
            "adb_serial": "MIBAND_HEALTH_ADB_SERIAL",
            "gadgetbridge_package": "MIBAND_HEALTH_GADGETBRIDGE_PACKAGE",
            "gadgetbridge_remote_db": "MIBAND_HEALTH_GADGETBRIDGE_REMOTE_DB",
            "gadgetbridge_db_path": "MIBAND_HEALTH_GADGETBRIDGE_DB",
            "gadgetbridge_device_id": "MIBAND_HEALTH_GADGETBRIDGE_DEVICE_ID",
            "gadgetbridge_device_address": "MIBAND_HEALTH_GADGETBRIDGE_DEVICE_ADDRESS",
        }
        for name, env in env_fields.items():
            if env in os.environ:
                value = os.environ[env]
                values[name] = int(value) if name == "gadgetbridge_device_id" else value
        if "data_dir" not in values:
            base = Path(os.environ.get("XDG_DATA_HOME", str(Path.home() / ".local/share")))
            suffix = "miband-gadgetbridge" if values.get("backend") == "gadgetbridge" else "miband-health"
            values["data_dir"] = base / suffix
        return cls(**values)
