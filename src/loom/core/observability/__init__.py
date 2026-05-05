"""Unified observability for Loom — config, events, protocol, and runtime."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

import structlog as _structlog

from loom.core.observability.config import (
    LogObservabilityConfig,
    ObservabilityConfig,
    OtelObservabilityConfig,
    PrometheusConfig,
    PrometheusObservabilityConfig,
)
from loom.core.observability.event import EventKind, LifecycleEvent
from loom.core.observability.protocol import LifecycleObserver
from loom.core.observability.runtime import ObservabilityRuntime

_log: Any = _structlog.get_logger("loom.observer")


def safe_observe(fn: Callable[..., None], *args: Any, **kwargs: Any) -> None:
    """Call an observer callback, logging and swallowing on failure.

    Deprecated: use :class:`ObservabilityRuntime` instead. Kept for backward
    compatibility with the streaming composite observer until the legacy
    cleanup commit removes it.
    """
    try:
        fn(*args, **kwargs)
    except Exception as exc:  # noqa: BLE001
        owner = getattr(fn, "__self__", None)
        name = type(owner).__name__ if owner is not None else repr(fn)
        _log.error("observer_error", observer=name, error=repr(exc))


def notify_observers(
    observers: Sequence[Any],
    method: str,
    *args: Any,
    **kwargs: Any,
) -> None:
    """Fan-out one event to all observers with per-observer error isolation.

    Deprecated: use :class:`ObservabilityRuntime` instead. Kept for backward
    compatibility with the streaming composite observer until the legacy
    cleanup commit removes it.
    """
    for obs in observers:
        safe_observe(getattr(obs, method), *args, **kwargs)


__all__ = [
    "EventKind",
    "LifecycleEvent",
    "LifecycleObserver",
    "LogObservabilityConfig",
    "ObservabilityConfig",
    "ObservabilityRuntime",
    "OtelObservabilityConfig",
    "PrometheusConfig",
    "PrometheusObservabilityConfig",
    # Deprecated — removed in the legacy cleanup commit
    "notify_observers",
    "safe_observe",
]
