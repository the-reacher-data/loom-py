from __future__ import annotations

from collections.abc import Callable

from loom.core.logger.abc import LoggerPort
from loom.core.logger.std import StdLogger

LoggerFactory = Callable[[str], LoggerPort]

_factory: LoggerFactory = StdLogger


def configure_logger_factory(factory: LoggerFactory | None) -> None:
    """Set logger factory globally, or reset to stdlib logger when None."""
    global _factory
    _factory = StdLogger if factory is None else factory


def get_logger(name: str) -> LoggerPort:
    """Return a logger from the active global factory."""
    return _factory(name)
