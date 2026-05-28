"""Dispatch registries that route specs to per-kind readers and writers."""

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


class WriterRegistry:
    """Dispatch write() calls to a per-kind writer or fall back to the base writer.

    The ``extra`` dict maps ``spec.kind`` strings to writer instances.  Specs
    whose kind is not in ``extra`` are forwarded to ``base``.
    """

    def __init__(
        self,
        base: Any,
        *,
        extra: dict[str, Any] | None = None,
    ) -> None:
        self._base = base
        self._extra: dict[str, Any] = extra or {}

    def write(self, frame: Any, spec: Any, params: Any, /, *, streaming: bool = False) -> None:
        kind = getattr(spec, "kind", None)
        handler = self._extra.get(kind) if kind is not None else None
        if handler is not None:
            handler.write(frame, spec, params, streaming=streaming)
            return
        self._base.write(frame, spec, params, streaming=streaming)
