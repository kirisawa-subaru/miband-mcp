"""Choose a data backend without importing optional device-control runtimes."""
from __future__ import annotations

from types import ModuleType
from typing import Any


def modules(settings: Any) -> tuple[ModuleType, ModuleType]:
    backend = getattr(settings, "backend", "xiaomi_health")
    if backend == "gadgetbridge":
        from ..gadgetbridge import query, sync
    elif backend == "xiaomi_health":
        from . import query, sync
    else:
        raise ValueError("backend must be xiaomi_health or gadgetbridge")
    return query, sync
