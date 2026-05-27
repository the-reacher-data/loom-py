"""Registry that dispatches SourceSpec reads to per-kind readers."""

from __future__ import annotations

from typing import Any


class ConfigurationError(Exception):
    """Raised when a required connector dependency is absent at construction."""


class ReaderRegistry:
    def __init__(
        self,
        base: Any | None,
        *,
        extra: dict[str, Any] | None = None,
    ) -> None:
        self._base = base
        self._extra: dict[str, Any] = extra or {}

    def read(self, spec: Any, params: Any, /) -> Any:
        handler = self._extra.get(spec.kind)
        if handler is not None:
            return handler.read(spec, params)
        if self._base is not None:
            return self._base.read(spec, params)
        raise ConfigurationError(
            f"No reader registered for spec kind {spec.kind!r} and no base reader."
        )
