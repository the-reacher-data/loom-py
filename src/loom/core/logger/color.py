from __future__ import annotations

import logging
import os
from typing import Any

from loom.core.logger.abc import LoggerPort


class ColorLogger(LoggerPort):
    """Stdlib logger implementation with ANSI colorized payloads by level."""

    _RESET = "\033[0m"
    _COLORS = {
        "debug": "\033[36m",
        "info": "\033[32m",
        "warning": "\033[33m",
        "error": "\033[31m",
        "exception": "\033[31m",
    }

    def __init__(
        self,
        name: str,
        *,
        bound_fields: dict[str, Any] | None = None,
        enable_colors: bool = True,
    ) -> None:
        self._logger = logging.getLogger(name)
        self._bound_fields = bound_fields or {}
        self._enable_colors = enable_colors and os.getenv("NO_COLOR") is None

    def bind(self, **fields: Any) -> LoggerPort:
        return ColorLogger(
            self._logger.name,
            bound_fields=self._merge(fields),
            enable_colors=self._enable_colors,
        )

    def _merge(self, fields: dict[str, Any]) -> dict[str, Any]:
        return {**self._bound_fields, **fields}

    def _payload(self, event: str, fields: dict[str, Any]) -> str:
        merged = self._merge(fields)
        if not merged:
            return event
        kv = ", ".join(f"{k}={v!r}" for k, v in sorted(merged.items()))
        return f"{event} | {kv}"

    def _colorize(self, level: str, text: str) -> str:
        if not self._enable_colors:
            return text
        return f"{self._COLORS[level]}{text}{self._RESET}"

    def debug(self, event: str, **fields: Any) -> None:
        self._logger.debug(self._colorize("debug", self._payload(event, fields)))

    def info(self, event: str, **fields: Any) -> None:
        self._logger.info(self._colorize("info", self._payload(event, fields)))

    def warning(self, event: str, **fields: Any) -> None:
        self._logger.warning(self._colorize("warning", self._payload(event, fields)))

    def error(self, event: str, **fields: Any) -> None:
        self._logger.error(self._colorize("error", self._payload(event, fields)))

    def exception(self, event: str, **fields: Any) -> None:
        self._logger.exception(self._colorize("exception", self._payload(event, fields)))
