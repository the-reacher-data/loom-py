"""Logging configuration: Environment/Renderer enums, handler configs, and configure_logging."""

from __future__ import annotations

import logging
import logging.handlers
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Literal

import msgspec
import structlog

from loom.core.model import LoomFrozenStruct


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


class StreamHandlerConfig(LoomFrozenStruct, frozen=True, tag="stream", tag_field="type"):
    """Configuration for a stdlib ``StreamHandler``.

    Attributes:
        stream: Target stream — ``"stdout"`` or ``"stderr"``.
        renderer: Per-handler output renderer (``"console"`` / ``"json"``).
            When ``None`` the handler inherits the top-level
            :attr:`LogConfig.renderer`.
    """

    stream: Literal["stdout", "stderr"] = "stdout"
    renderer: str | None = None


class FileHandlerConfig(LoomFrozenStruct, frozen=True, tag="file", tag_field="type"):
    """Configuration for a stdlib ``FileHandler``.

    Attributes:
        filename: Absolute or relative path to the log file.
        encoding: File encoding. Defaults to ``"utf-8"``.
        renderer: Per-handler output renderer. See :class:`StreamHandlerConfig`.
    """

    filename: str
    encoding: str = "utf-8"
    renderer: str | None = None


class RotatingFileHandlerConfig(
    LoomFrozenStruct, frozen=True, tag="rotating_file", tag_field="type"
):
    """Configuration for a stdlib ``RotatingFileHandler``.

    Attributes:
        filename: Absolute or relative path to the log file.
        max_bytes: Maximum file size in bytes before rotation. Defaults to 10 MB.
        backup_count: Number of rotated backup files to retain.
        encoding: File encoding. Defaults to ``"utf-8"``.
        renderer: Per-handler output renderer. See :class:`StreamHandlerConfig`.
    """

    filename: str
    max_bytes: int = 10 * 1024 * 1024
    backup_count: int = 5
    encoding: str = "utf-8"
    renderer: str | None = None


HandlerConfig = StreamHandlerConfig | FileHandlerConfig | RotatingFileHandlerConfig


class LoggerConfig(LoomFrozenStruct, frozen=True, kw_only=True):
    """YAML configuration struct for the ``logger:`` section.

    Use with :func:`~loom.core.config.loader.section` to parse the
    ``logger:`` block from an OmegaConf config, then pass directly to
    :func:`configure_logging_from_values`.

    Attributes:
        name: Logger name.  Empty string targets the root logger.
        environment: Deployment environment (``"dev"``, ``"prod"``).
            When empty, :func:`configure_logging_from_values` falls back to
            the ``ENVIRONMENT`` env var, then ``"dev"``.
        renderer: Output renderer (``"json"`` or ``"console"``).
            ``None`` auto-detects from ``environment``.
        colors: Enable ANSI colours.  ``None`` auto-detects from
            ``environment``.
        level: Minimum log level (``"DEBUG"``, ``"INFO"``, etc.).
        named_levels: Per-logger minimum levels applied after the main logger
            setup.  Useful for silencing noisy third-party libraries like
            ``celery`` or ``redis`` from YAML.
        handlers: Additional stdlib handler configurations.
        fields: Static key-value pairs bound globally at startup via
            ``structlog.contextvars``.  Every log entry will carry these
            fields automatically.  Useful for per-deployment metadata such as
            ``service``, ``version``, or ``region``.

    Example::

        cfg = section(raw, "logger", LoggerConfig)
        configure_logging_from_values(
            name=cfg.name,
            environment=cfg.environment,
            renderer=cfg.renderer,
            colors=cfg.colors,
            level=cfg.level,
            named_levels=cfg.named_levels,
            handlers=cfg.handlers,
            fields=cfg.fields,
        )
    """

    name: str = ""
    environment: str = ""
    renderer: str | None = None
    colors: bool | None = None
    level: str = "INFO"
    named_levels: dict[str, str] = msgspec.field(default_factory=dict)
    handlers: list[HandlerConfig] = msgspec.field(default_factory=list)
    fields: dict[str, str] = msgspec.field(default_factory=dict)


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
        named_levels: Per-logger level overrides for specific stdlib logger names.
        handlers: stdlib handler configurations. Empty tuple falls back to ``basicConfig``.
        fields: Static key-value pairs bound globally at startup via
            ``structlog.contextvars``.  Every log entry will carry these fields.
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
    named_levels: tuple[tuple[str, str], ...] = ()
    handlers: tuple[HandlerConfig, ...] = ()
    fields: tuple[tuple[str, str], ...] = ()
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


def _default_renderer(config: LogConfig) -> Renderer:
    is_prod = config.environment == Environment.PROD
    return config.renderer or (Renderer.JSON if is_prod else Renderer.CONSOLE)


def _default_colors(config: LogConfig) -> bool:
    if config.colors is not None:
        return config.colors
    return config.environment != Environment.PROD


def _resolve_renderer(config: LogConfig) -> Any:
    """Top-level renderer (used by ``_setup_structlog`` for the wrapper chain)."""
    return _RENDERER_FACTORIES[_default_renderer(config)](_default_colors(config))


def _handler_renderer(handler_cfg: HandlerConfig, config: LogConfig) -> Any:
    """Build the structlog renderer instance for a single handler.

    Honours the per-handler ``renderer`` field when set; otherwise falls
    back to the top-level :attr:`LogConfig.renderer` / environment default.
    """
    raw = getattr(handler_cfg, "renderer", None)
    renderer = Renderer.from_str(raw) if raw is not None else _default_renderer(config)
    return _RENDERER_FACTORIES[renderer](_default_colors(config))


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


def _setup_stdlib(config: LogConfig, level: int) -> bool:
    """Attach handlers from ``config`` to the target stdlib logger.

    Returns ``True`` when any handler declares its own renderer — in that
    case ``configure_logging`` must route structlog through stdlib so each
    handler can format with its own ``ProcessorFormatter``.
    """
    if not config.handlers:
        logging.basicConfig(level=level, format="%(message)s", force=True)
        return False

    target = logging.getLogger(config.name) if config.name else logging.getLogger()
    target.setLevel(level)
    # Clear any pre-existing handlers so re-configuration doesn't duplicate
    # output (e.g. in tests or repeated bootstraps).
    for existing in list(target.handlers):
        target.removeHandler(existing)

    per_handler_renderer = any(getattr(h, "renderer", None) is not None for h in config.handlers)

    for handler_cfg in config.handlers:
        handler = _build_stdlib_handler(handler_cfg)
        handler.setLevel(level)
        if per_handler_renderer:
            # Each handler gets a ProcessorFormatter rendering with its own
            # configured (or inherited) renderer so a single log record can
            # land on stdout as colour text AND on a file as JSON.
            handler.setFormatter(
                structlog.stdlib.ProcessorFormatter(
                    processor=_handler_renderer(handler_cfg, config),
                    foreign_pre_chain=_foreign_pre_chain(),
                )
            )
        target.addHandler(handler)
    return per_handler_renderer


def _foreign_pre_chain() -> list[Any]:
    """Pre-chain applied to records that did NOT originate from structlog.

    Lets stdlib log records (e.g. third-party libraries) flow through the
    same processors so they render consistently with structlog events.
    """
    return [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.format_exc_info,
    ]


def _resolve_level(level_name: str, logger_name: str) -> int:
    try:
        return _parse_level(level_name)
    except ValueError as exc:
        raise ValueError(f"Invalid level {level_name!r} for logger {logger_name!r}") from exc


def _apply_named_levels(config: LogConfig) -> None:
    for logger_name, level_name in config.named_levels:
        logging.getLogger(logger_name).setLevel(_resolve_level(level_name, logger_name))


_DEFAULT_LOG_CONFIG = LogConfig()


def configure_logging_from_values(
    *,
    name: str = "",
    environment: str = "",
    renderer: str | None = None,
    colors: bool | None = None,
    level: str = "INFO",
    named_levels: Mapping[str, str] | None = None,
    handlers: Sequence[HandlerConfig] = (),
    fields: Mapping[str, str] | None = None,
    extra_processors: Sequence[Any] = (),
) -> None:
    """Configure logging from plain scalar values.

    Intended for bootstrap layers that parse config structs and want to avoid
    duplicating ``Environment``/``Renderer`` conversion logic.

    Args:
        name: stdlib logger name. ``""`` targets the root logger.
        environment: Deployment environment string (``"dev"``, ``"prod"``).
        renderer: Output renderer name (``"json"`` or ``"console"``).
        colors: Enable ANSI colours. ``None`` auto-detects from ``environment``.
        level: Minimum log level name.
        named_levels: Per-logger level overrides.
        handlers: stdlib handler configurations.
        fields: Static key-value pairs bound globally at startup. Every log
            entry will carry these fields automatically via ``structlog.contextvars``.
        extra_processors: Additional structlog processors inserted before the final renderer.
    """
    env = Environment.from_str(environment.strip()) if environment.strip() else Environment.DEV
    configure_logging(
        LogConfig(
            name=name,
            environment=env,
            renderer=Renderer.from_str(renderer) if renderer is not None else None,
            colors=colors,
            level=level,
            named_levels=tuple((named_levels or {}).items()),
            handlers=tuple(handlers),
            fields=tuple((fields or {}).items()),
            extra_processors=tuple(extra_processors),
        )
    )


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
    per_handler_renderer = _setup_stdlib(config, level)
    _apply_named_levels(config)

    if config.fields:
        structlog.contextvars.bind_contextvars(**dict(config.fields))

    if per_handler_renderer:
        # Hand off the final rendering to each stdlib handler's
        # ProcessorFormatter so different sinks can use different renderers
        # (e.g. console+colour on stdout, JSON on a file for OTEL ingestion).
        final_processor: Any = structlog.stdlib.ProcessorFormatter.wrap_for_formatter
    else:
        final_processor = _resolve_renderer(config)

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.format_exc_info,
            *config.extra_processors,
            final_processor,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
