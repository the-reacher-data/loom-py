"""structlog-based implementation of LoggerPort."""

from __future__ import annotations

from typing import Any

import structlog


class StructLogger:
    """structlog-based implementation of ``LoggerPort``.

    Uses the globally configured structlog pipeline. Bound fields are merged
    into every log call without mutating the underlying structlog context.
    """

    def __init__(self, name: str, *, bound_fields: dict[str, Any] | None = None) -> None:
        """Initialise a structlog-backed logger.

        Args:
            name: Logger name forwarded to ``structlog.get_logger``.
            bound_fields: Pre-bound contextual fields included in every message.
        """
        self._name = name
        self._logger = structlog.get_logger(name)
        self._bound_fields: dict[str, Any] = bound_fields or {}

    def bind(self, **fields: Any) -> StructLogger:
        """Return a new ``StructLogger`` with the given fields merged into the bound context.

        Args:
            **fields: Key-value pairs to bind.

        Returns:
            A new ``StructLogger`` instance with merged fields.
        """
        return StructLogger(self._name, bound_fields=self._merge(fields))

    def _merge(self, fields: dict[str, Any]) -> dict[str, Any]:
        return {**self._bound_fields, **fields}

    def debug(self, event: str, **fields: Any) -> None:
        """Emit a DEBUG-level structured log entry.

        Args:
            event: Structured event name.
            **fields: Additional contextual key-value pairs.
        """
        self._logger.debug(event, **self._merge(fields))

    def info(self, event: str, **fields: Any) -> None:
        """Emit an INFO-level structured log entry.

        Args:
            event: Structured event name.
            **fields: Additional contextual key-value pairs.
        """
        self._logger.info(event, **self._merge(fields))

    def warning(self, event: str, **fields: Any) -> None:
        """Emit a WARNING-level structured log entry.

        Args:
            event: Structured event name.
            **fields: Additional contextual key-value pairs.
        """
        self._logger.warning(event, **self._merge(fields))

    def error(self, event: str, **fields: Any) -> None:
        """Emit an ERROR-level structured log entry.

        Args:
            event: Structured event name.
            **fields: Additional contextual key-value pairs.
        """
        self._logger.error(event, **self._merge(fields))

    def exception(self, event: str, **fields: Any) -> None:
        """Emit an ERROR-level structured log entry with exception traceback.

        Args:
            event: Structured event name.
            **fields: Additional contextual key-value pairs.
        """
        self._logger.exception(event, **self._merge(fields))
