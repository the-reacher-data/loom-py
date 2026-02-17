from __future__ import annotations

from typing import Any, Protocol


class LoggerPort(Protocol):
    """Logging port for framework components."""

    def bind(self, **fields: Any) -> LoggerPort:
        """Return a new logger instance with the given fields bound to every future log entry.

        Args:
            **fields: Key-value pairs to include in all subsequent log messages.

        Returns:
            A new ``LoggerPort`` with the merged bound context.
        """
        ...

    def debug(self, event: str, **fields: Any) -> None:
        """Emit a DEBUG-level log entry.

        Args:
            event: Structured event name or message.
            **fields: Additional contextual key-value pairs.
        """
        ...

    def info(self, event: str, **fields: Any) -> None:
        """Emit an INFO-level log entry.

        Args:
            event: Structured event name or message.
            **fields: Additional contextual key-value pairs.
        """
        ...

    def warning(self, event: str, **fields: Any) -> None:
        """Emit a WARNING-level log entry.

        Args:
            event: Structured event name or message.
            **fields: Additional contextual key-value pairs.
        """
        ...

    def error(self, event: str, **fields: Any) -> None:
        """Emit an ERROR-level log entry.

        Args:
            event: Structured event name or message.
            **fields: Additional contextual key-value pairs.
        """
        ...

    def exception(self, event: str, **fields: Any) -> None:
        """Emit an ERROR-level log entry with exception traceback information.

        Args:
            event: Structured event name or message.
            **fields: Additional contextual key-value pairs.
        """
        ...
