"""Bytewax runtime adapter for Loom streaming flows.

This package requires ``bytewax`` to be installed.

Usage::

    from loom.streaming.bytewax import build_dataflow

    flow = build_dataflow(compiled_plan)
"""

from __future__ import annotations

from loom.streaming.bytewax._adapter import build_dataflow, build_dataflow_with_shutdown
from loom.streaming.bytewax.runner import (
    BytewaxRecoverySettings,
    BytewaxRuntimeConfig,
    StreamingRunner,
)

__all__ = [
    "build_dataflow",
    "build_dataflow_with_shutdown",
    "BytewaxRecoverySettings",
    "BytewaxRuntimeConfig",
    "StreamingRunner",
]
