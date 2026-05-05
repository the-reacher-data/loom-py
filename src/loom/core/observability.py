"""Shared observability utilities for Loom modules.

Provides error-isolated observer dispatch used by ETL, streaming, and
any future module that follows the observer pattern.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

import structlog

_log: Any = structlog.get_logger("loom.observer")


def safe_observe(fn: Callable[..., None], *args: Any, **kwargs: Any) -> None:
    """Call an observer callback, logging and swallowing on failure.

    Observer errors must never break the main execution path. This
    function catches any exception raised by *fn*, logs it, and returns
    normally.

    Args:
        fn: Observer method to invoke.
        *args: Positional arguments forwarded to *fn*.
        **kwargs: Keyword arguments forwarded to *fn*.
    """
    try:
        fn(*args, **kwargs)
    except Exception as exc:  # noqa: BLE001
        owner = getattr(fn, "__self__", None)
        _observer_name = type(owner).__name__ if owner is not None else repr(fn)
        _log.error(
            "observer_error",
            observer=_observer_name,
            error=repr(exc),
        )


def notify_observers(
    observers: Sequence[Any],
    method: str,
    *args: Any,
    **kwargs: Any,
) -> None:
    """Fan-out one event to all observers with per-observer error isolation.

    Each observer's *method* is called independently. A failure in one
    observer does not prevent the remaining observers from executing.

    Args:
        observers: Sequence of observer instances.
        method: Method name to invoke on each observer.
        *args: Positional arguments forwarded to the method.
        **kwargs: Keyword arguments forwarded to the method.
    """
    for obs in observers:
        safe_observe(getattr(obs, method), *args, **kwargs)
