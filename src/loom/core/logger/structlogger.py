from __future__ import annotations

import importlib
from typing import Any

from loom.core.logger.abc import LoggerPort


class StructLogger(LoggerPort):
    """structlog-based implementation of LoggerPort."""

    def __init__(self, name: str, *, bound_fields: dict[str, Any] | None = None) -> None:
        """Initialise a structlog-backed logger.

        Args:
            name: Logger name forwarded to ``structlog.get_logger``.
            bound_fields: Pre-bound contextual fields included in every message.
        """
        structlog_module = importlib.import_module("structlog")
        self._name = name
        self._logger = structlog_module.get_logger(name)
        self._bound_fields = bound_fields or {}

    def bind(self, **fields: Any) -> LoggerPort:
        """Return a new ``StructLogger`` with the given fields merged into the bound context.

        Args:
            **fields: Key-value pairs to bind.

        Returns:
            A new ``StructLogger`` instance with merged fields.
        """
        return StructLogger(self._name, bound_fields=self._merge(fields))

    def _merged(self, fields: dict[str, Any]) -> dict[str, Any]:
        return self._merge(fields)

    def _merge(self, fields: dict[str, Any]) -> dict[str, Any]:
        return {**self._bound_fields, **fields}

    def debug(self, event: str, **fields: Any) -> None:
        """Emit a DEBUG-level structured log entry.

        Args:
            event: Structured event name.
            **fields: Additional contextual key-value pairs.
        """
        self._logger.debug(event, **self._merged(fields))

    def info(self, event: str, **fields: Any) -> None:
        """Emit an INFO-level structured log entry.

        Args:
            event: Structured event name.
            **fields: Additional contextual key-value pairs.
        """
        self._logger.info(event, **self._merged(fields))

    def warning(self, event: str, **fields: Any) -> None:
        """Emit a WARNING-level structured log entry.

        Args:
            event: Structured event name.
            **fields: Additional contextual key-value pairs.
        """
        self._logger.warning(event, **self._merged(fields))

    def error(self, event: str, **fields: Any) -> None:
        """Emit an ERROR-level structured log entry.

        Args:
            event: Structured event name.
            **fields: Additional contextual key-value pairs.
        """
        self._logger.error(event, **self._merged(fields))

    def exception(self, event: str, **fields: Any) -> None:
        """Emit an ERROR-level structured log entry with exception traceback.

        Args:
            event: Structured event name.
            **fields: Additional contextual key-value pairs.
        """
        self._logger.exception(event, **self._merged(fields))
