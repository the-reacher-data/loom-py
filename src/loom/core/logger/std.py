from __future__ import annotations

import logging
from typing import Any

from loom.core.logger.abc import LoggerPort


class StdLogger(LoggerPort):
    """Default stdlib-logging implementation of LoggerPort."""

    def __init__(self, name: str, *, bound_fields: dict[str, Any] | None = None) -> None:
        """Initialise a stdlib-backed logger.

        Args:
            name: Logger name forwarded to ``logging.getLogger``.
            bound_fields: Pre-bound contextual fields included in every message.
        """
        self._logger = logging.getLogger(name)
        self._bound_fields = bound_fields or {}

    def bind(self, **fields: Any) -> LoggerPort:
        """Return a new ``StdLogger`` with the given fields merged into the bound context.

        Args:
            **fields: Key-value pairs to bind.

        Returns:
            A new ``StdLogger`` instance with merged fields.
        """
        return StdLogger(self._logger.name, bound_fields=self._merge(fields))

    def _payload(self, event: str, fields: dict[str, Any]) -> str:
        merged = self._merge(fields)
        if not merged:
            return event
        kv = ", ".join(f"{k}={v!r}" for k, v in sorted(merged.items()))
        return f"{event} | {kv}"

    def _merge(self, fields: dict[str, Any]) -> dict[str, Any]:
        return {**self._bound_fields, **fields}

    def debug(self, event: str, **fields: Any) -> None:
        """Emit a DEBUG-level log entry via stdlib logging.

        Args:
            event: Event name or message.
            **fields: Additional contextual key-value pairs.
        """
        self._logger.debug(self._payload(event, fields))

    def info(self, event: str, **fields: Any) -> None:
        """Emit an INFO-level log entry via stdlib logging.

        Args:
            event: Event name or message.
            **fields: Additional contextual key-value pairs.
        """
        self._logger.info(self._payload(event, fields))

    def warning(self, event: str, **fields: Any) -> None:
        """Emit a WARNING-level log entry via stdlib logging.

        Args:
            event: Event name or message.
            **fields: Additional contextual key-value pairs.
        """
        self._logger.warning(self._payload(event, fields))

    def error(self, event: str, **fields: Any) -> None:
        """Emit an ERROR-level log entry via stdlib logging.

        Args:
            event: Event name or message.
            **fields: Additional contextual key-value pairs.
        """
        self._logger.error(self._payload(event, fields))

    def exception(self, event: str, **fields: Any) -> None:
        """Emit an ERROR-level log entry with traceback via stdlib logging.

        Args:
            event: Event name or message.
            **fields: Additional contextual key-value pairs.
        """
        self._logger.exception(self._payload(event, fields))
