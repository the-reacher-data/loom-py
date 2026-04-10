"""User-facing alias for ETL pipeline declaration primitives."""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = ["ETLParams", "ETLStep", "StepSQL", "ETLProcess", "ETLPipeline", "params", "ParamExpr"]

_EXPORTS: dict[str, str] = {
    "ETLParams": "loom.etl.pipeline._params",
    "ETLStep": "loom.etl.pipeline._step",
    "StepSQL": "loom.etl.pipeline._step_sql",
    "ETLProcess": "loom.etl.pipeline._process",
    "ETLPipeline": "loom.etl.pipeline._pipeline",
    "params": "loom.etl.declarative.expr._params",
    "ParamExpr": "loom.etl.declarative.expr._params",
}


def __getattr__(name: str) -> Any:
    module_path = _EXPORTS.get(name)
    if module_path is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(module_path)
    value = getattr(module, name)
    globals()[name] = value
    return value
