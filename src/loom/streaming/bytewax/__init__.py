"""Bytewax runtime adapter for Loom streaming flows.

The adapter is loaded lazily so that importing the package does not require
``bytewax`` to be installed.
"""

from __future__ import annotations

from typing import Any

__all__ = ["build_dataflow"]

build_dataflow: Any = None


def __getattr__(name: str) -> Any:
    if name == "build_dataflow":
        from loom.streaming.bytewax._adapter import build_dataflow as _bf

        globals()["build_dataflow"] = _bf
        return _bf
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
