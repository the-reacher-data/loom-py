"""Structlog lifecycle observer — emits LifecycleEvent as structured log entries."""

from __future__ import annotations

import structlog

from loom.core.observability.event import EventKind, LifecycleEvent


class StructlogLifecycleObserver:
    """Lifecycle observer that emits every event as a structured log entry.

    Log levels:
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


__all__ = ["StructlogLifecycleObserver"]
