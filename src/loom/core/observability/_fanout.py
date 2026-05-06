"""Internal fan-out helpers for observability observers."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

import structlog as _structlog

_log: Any = _structlog.get_logger("loom.observer")


def safe_observe(fn: Callable[..., None], *args: Any, **kwargs: Any) -> None:
    """Call an observer callback and swallow failures with logging."""
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
    """Fan-out one event to all observers with per-observer error isolation."""
    for obs in observers:
        safe_observe(getattr(obs, method), *args, **kwargs)
