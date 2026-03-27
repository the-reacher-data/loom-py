"""SQL helpers and SQL-driven ETL step primitives."""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = ["StepSQL", "resolve_sql", "sql_literal"]

_EXPORTS: dict[str, str] = {
    "StepSQL": "loom.etl.sql._step_sql",
    "resolve_sql": "loom.etl.sql._sql",
    "sql_literal": "loom.etl.sql.literals",
}


def __getattr__(name: str) -> Any:
    module_path = _EXPORTS.get(name)
    if module_path is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(module_path)
    value = getattr(module, name)
    globals()[name] = value
    return value
