"""Global logger factory registry."""

from __future__ import annotations

from collections.abc import Callable

from loom.core.logger.abc import LoggerPort
from loom.core.logger.structlogger import StructLogger

LoggerFactory = Callable[[str], LoggerPort]

_factory: LoggerFactory = StructLogger


def configure_logger_factory(factory: LoggerFactory) -> None:
    """Override the global logger factory.

    Args:
        factory: Callable that accepts a logger name and returns a ``LoggerPort``.
    """
    global _factory
    _factory = factory


def reset_logger_factory() -> None:
    """Reset the global logger factory to the default ``StructLogger``."""
    global _factory
    _factory = StructLogger


def get_logger(name: str) -> LoggerPort:
    """Return a logger from the active global factory.

    Args:
        name: Logger name, typically ``__name__``.

    Returns:
        A ``LoggerPort`` instance from the current factory.
    """
    return _factory(name)
