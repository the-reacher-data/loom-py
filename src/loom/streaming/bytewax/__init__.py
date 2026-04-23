"""Bytewax runtime adapter for Loom streaming flows.

This package requires ``bytewax`` to be installed.  The public API is
loaded lazily via ``__getattr__`` so that importing
``loom.streaming.bytewax`` does not fail when bytewax is absent - the
error is deferred until the caller actually accesses one of the public
Bytewax adapter symbols.

Usage::

    from loom.streaming.bytewax import build_dataflow

    flow = build_dataflow(compiled_plan)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from loom.streaming.bytewax._adapter import (
        build_dataflow as build_dataflow,
    )
    from loom.streaming.bytewax._adapter import (
        build_dataflow_with_shutdown as build_dataflow_with_shutdown,
    )
    from loom.streaming.bytewax.runner import (
        BytewaxRecoverySettings as BytewaxRecoverySettings,
    )
    from loom.streaming.bytewax.runner import (
        BytewaxRuntimeConfig as BytewaxRuntimeConfig,
    )
    from loom.streaming.bytewax.runner import StreamingRunner as StreamingRunner

__all__ = [
    "build_dataflow",
    "build_dataflow_with_shutdown",
    "BytewaxRecoverySettings",
    "BytewaxRuntimeConfig",
    "StreamingRunner",
]

_LOADED: dict[str, Any] = {}


def __getattr__(name: str) -> Any:
    if name in (
        "build_dataflow",
        "build_dataflow_with_shutdown",
        "BytewaxRecoverySettings",
        "BytewaxRuntimeConfig",
        "StreamingRunner",
    ):
        if name not in _LOADED:
            from loom.streaming.bytewax._adapter import (
                build_dataflow as _build_fn,
            )
            from loom.streaming.bytewax._adapter import (
                build_dataflow_with_shutdown as _build_shutdown_fn,
            )
            from loom.streaming.bytewax.runner import (
                BytewaxRecoverySettings as _recovery_settings,
            )
            from loom.streaming.bytewax.runner import (
                BytewaxRuntimeConfig as _runtime_config,
            )
            from loom.streaming.bytewax.runner import StreamingRunner as _runner

            _LOADED["build_dataflow"] = _build_fn
            _LOADED["build_dataflow_with_shutdown"] = _build_shutdown_fn
            _LOADED["BytewaxRecoverySettings"] = _recovery_settings
            _LOADED["BytewaxRuntimeConfig"] = _runtime_config
            _LOADED["StreamingRunner"] = _runner
        return _LOADED[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
