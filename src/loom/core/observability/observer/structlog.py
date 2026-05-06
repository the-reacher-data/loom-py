"""Structlog lifecycle observer — emits LifecycleEvent as structured log entries."""

from __future__ import annotations

from typing import Any

import structlog

from loom.core.observability.event import EventKind, LifecycleEvent, Scope


class StructlogLifecycleObserver:
    """Lifecycle observer that emits every event as a structured log entry.

    Log levels by scope and kind:
    - ``TRANSPORT`` (produce/consume/encode/decode): ``debug`` for END, ``warning`` for ERROR.
      Transport events are high-frequency and must not pollute INFO logs.
    - ``START``: ``debug`` — high-frequency, useful only in verbose mode.
    - ``END``: ``info`` — standard operational visibility.
    - ``ERROR``: ``error`` — always visible.

    The observer relies on the global structlog pipeline configured by
    :func:`~loom.core.logger.config.configure_logging`. It does not configure
    logging itself.
    """

    def __init__(self) -> None:
        self._log = structlog.get_logger("loom.observability")

    def on_event(self, event: LifecycleEvent) -> None:
        """Emit one lifecycle event as a structured log entry.

        Args:
            event: Lifecycle event to log.
        """
        bound = self._log.bind(
            scope=event.scope,
            name=event.name,
            trace_id=event.trace_id,
            correlation_id=event.correlation_id,
        )
        if event.scope is Scope.TRANSPORT:
            _log_transport(bound, event)
            return
        match event.kind:
            case EventKind.START:
                bound.debug(event.kind.value, **event.meta)
            case EventKind.END:
                bound.info(
                    event.kind.value,
                    duration_ms=event.duration_ms,
                    status=event.status.value if event.status is not None else None,
                    **event.meta,
                )
            case EventKind.ERROR:
                bound.error(
                    event.kind.value,
                    duration_ms=event.duration_ms,
                    error=event.error,
                    **event.meta,
                )


def _log_transport(bound: Any, event: LifecycleEvent) -> None:
    """Log a transport-scope event at debug/warning to avoid INFO noise."""
    if event.kind is EventKind.ERROR:
        bound.warning(event.kind.value, error=event.error, **event.meta)
    else:
        bound.debug(event.kind.value, duration_ms=event.duration_ms, **event.meta)


__all__ = ["StructlogLifecycleObserver"]
