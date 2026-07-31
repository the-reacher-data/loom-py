"""ClickHouse backend for the SQL subsystem (optional extra ``loom-kernel[clickhouse]``).

Provides :class:`ClickHouseSqlExecutor`, the ClickHouse implementation of the
``SqlExecutor`` port, and :class:`ClickHouseConnectionRegistry`, the async
context manager owning client lifecycle, the per-query role support assertion
and the fail-closed sentinel-role startup probe.
"""

from loom.core.sql.clickhouse.executor import ClickHouseSqlExecutor
from loom.core.sql.clickhouse.registry import ClickHouseConnectionRegistry

__all__ = [
    "ClickHouseConnectionRegistry",
    "ClickHouseSqlExecutor",
]
