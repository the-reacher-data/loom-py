"""Logging configuration: Environment/Renderer enums, handler configs, and configure_logging."""

from __future__ import annotations

import logging
import logging.handlers
import sys
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Literal

import msgspec
import structlog


class Environment(StrEnum):
    """Known deployment environments.

    Unknown string values default to ``DEV`` via ``from_str``.
    """

    DEV = "dev"
    PROD = "prod"

    @classmethod
    def from_str(cls, value: str) -> Environment:
        """Parse a string into an ``Environment``, defaulting to ``DEV`` for unknown values.

        Args:
            value: Raw environment name (case-insensitive).

        Returns:
            The matching ``Environment``, or ``DEV`` if unrecognised.

        Example:
            >>> Environment.from_str("prod")
            <Environment.PROD: 'prod'>
            >>> Environment.from_str("staging")
            <Environment.DEV: 'dev'>
        """
        try:
            return cls(value.strip().lower())
        except ValueError:
            return cls.DEV


class Renderer(StrEnum):
    """Supported structlog output renderers.

    Adding a new renderer requires only a new entry here and in ``_RENDERER_FACTORIES``.
    """

    CONSOLE = "console"
    JSON = "json"

    @classmethod
    def from_str(cls, value: str) -> Renderer:
        """Parse a string into a ``Renderer``.

        Args:
            value: Renderer name (case-insensitive).

        Returns:
            The matching ``Renderer``.

        Raises:
            ValueError: If ``value`` does not match any known renderer.
        """
        try:
            return cls(value.strip().lower())
        except ValueError:
            valid = [r.value for r in cls]
            raise ValueError(f"Unsupported renderer: {value!r}. Valid values: {valid}") from None


class StreamHandlerConfig(msgspec.Struct, frozen=True, tag="stream", tag_field="type"):
    """Configuration for a stdlib ``StreamHandler``.

    Attributes:
        stream: Target stream — ``"stdout"`` or ``"stderr"``.
    """

    stream: Literal["stdout", "stderr"] = "stdout"


class FileHandlerConfig(msgspec.Struct, frozen=True, tag="file", tag_field="type"):
    """Configuration for a stdlib ``FileHandler``.

    Attributes:
        filename: Absolute or relative path to the log file.
        encoding: File encoding. Defaults to ``"utf-8"``.
    """

    filename: str
    encoding: str = "utf-8"


class RotatingFileHandlerConfig(msgspec.Struct, frozen=True, tag="rotating_file", tag_field="type"):
    """Configuration for a stdlib ``RotatingFileHandler``.

    Attributes:
        filename: Absolute or relative path to the log file.
        max_bytes: Maximum file size in bytes before rotation. Defaults to 10 MB.
        backup_count: Number of rotated backup files to retain.
        encoding: File encoding. Defaults to ``"utf-8"``.
    """

    filename: str
    max_bytes: int = 10 * 1024 * 1024
    backup_count: int = 5
    encoding: str = "utf-8"


HandlerConfig = StreamHandlerConfig | FileHandlerConfig | RotatingFileHandlerConfig


@dataclass(frozen=True)
class LogConfig:
    """Immutable logging configuration for the framework.

    Controls structlog processor chain, stdlib handler attachment, and rendering.

    Attributes:
        environment: Deployment environment. Drives renderer and color defaults.
        renderer: Output renderer. ``None`` means auto-detect from ``environment``.
        colors: Enable ANSI colors. ``None`` means auto-detect from ``environment``.
        level: Minimum log level name (e.g. ``"INFO"``, ``"DEBUG"``).
        name: stdlib logger name to attach handlers to. ``""`` targets the root logger.
        handlers: stdlib handler configurations. Empty tuple falls back to ``basicConfig``.
        extra_processors: Additional structlog processors inserted before the final renderer.

    Example:
        >>> cfg = LogConfig(environment=Environment.PROD, level="WARNING")
        >>> configure_logging(cfg)
    """

    environment: Environment = Environment.DEV
    renderer: Renderer | None = None
    colors: bool | None = None
    level: str = "INFO"
    name: str = ""
    handlers: tuple[HandlerConfig, ...] = ()
    extra_processors: tuple[Any, ...] = ()


_RENDERER_FACTORIES: dict[Renderer, Callable[[bool], Any]] = {
    Renderer.CONSOLE: lambda colors: structlog.dev.ConsoleRenderer(colors=colors),
    Renderer.JSON: lambda _: structlog.processors.JSONRenderer(),
}


def _parse_level(level: str) -> int:
    value = logging.getLevelName(level.upper())
    if not isinstance(value, int):
        raise ValueError(f"Unsupported log level: {level!r}")
    return value


def _resolve_renderer(config: LogConfig) -> Any:
    is_prod = config.environment == Environment.PROD
    renderer = config.renderer or (Renderer.JSON if is_prod else Renderer.CONSOLE)
    colors = config.colors if config.colors is not None else not is_prod
    return _RENDERER_FACTORIES[renderer](colors)


def _build_stdlib_handler(cfg: HandlerConfig) -> logging.Handler:
    match cfg:
        case StreamHandlerConfig(stream="stderr"):
            return logging.StreamHandler(sys.stderr)
        case StreamHandlerConfig():
            return logging.StreamHandler(sys.stdout)
        case FileHandlerConfig():
            return logging.FileHandler(cfg.filename, encoding=cfg.encoding)
        case RotatingFileHandlerConfig():
            return logging.handlers.RotatingFileHandler(
                cfg.filename,
                maxBytes=cfg.max_bytes,
                backupCount=cfg.backup_count,
                encoding=cfg.encoding,
            )
        case _:
            raise TypeError(f"Unhandled handler config type: {type(cfg).__name__!r}")


def _setup_stdlib(config: LogConfig, level: int) -> None:
    if not config.handlers:
        logging.basicConfig(level=level, format="%(message)s", force=True)
        return

    target = logging.getLogger(config.name) if config.name else logging.getLogger()
    target.setLevel(level)
    for handler_cfg in config.handlers:
        handler = _build_stdlib_handler(handler_cfg)
        handler.setLevel(level)
        target.addHandler(handler)


_DEFAULT_LOG_CONFIG = LogConfig()


def configure_logging(config: LogConfig = _DEFAULT_LOG_CONFIG) -> None:
    """Configure structlog and stdlib logging from a ``LogConfig``.

    Sets up the global structlog processor chain and attaches stdlib handlers.
    Call once at application startup before any logger is used.

    Args:
        config: Logging configuration. Defaults to ``dev`` console with colors.

    Raises:
        ValueError: If ``config.level`` is not a recognised log level name.

    Example:
        >>> configure_logging(LogConfig(environment=Environment.PROD, level="WARNING"))
    """
    level = _parse_level(config.level)
    _setup_stdlib(config, level)

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.format_exc_info,
            *config.extra_processors,
            _resolve_renderer(config),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
