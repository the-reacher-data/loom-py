"""Public SQL convenience API.

This package is intentionally a thin compatibility layer. Implementation
lives in:

- :mod:`loom.etl.pipeline._sql` for SQL template resolution
- :mod:`loom.etl.pipeline._step_sql` for SQL steps
- :mod:`loom.etl.backends._predicate_sql` for SQL literal rendering
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = ["StepSQL", "resolve_sql", "sql_literal"]

_EXPORTS: dict[str, str] = {
    "StepSQL": "loom.etl.pipeline._step_sql",
    "resolve_sql": "loom.etl.pipeline._sql",
    "sql_literal": "loom.etl.backends._predicate_sql",
}


def __getattr__(name: str) -> Any:
    module_path = _EXPORTS.get(name)
    if module_path is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(module_path)
    value = getattr(module, name)
    globals()[name] = value
    return value
