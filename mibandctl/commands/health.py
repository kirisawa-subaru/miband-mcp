"""health: 链路状态，无业务数据。"""
from __future__ import annotations

from ..db import LOCAL_TZ, connect
from ..freshness import freshness_block, row_counts


def run(_args) -> dict:
    with connect() as con:
        return {
            "tz": "+08:00",
            "freshness": freshness_block(con),
            "row_counts": row_counts(con),
        }
