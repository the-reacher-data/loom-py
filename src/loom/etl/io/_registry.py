"""Dispatch registries that route specs to per-kind readers and writers."""

from __future__ import annotations

from typing import Any

from loom.etl.runtime.contracts import StreamingSourceReader


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
        handler = self._resolve_handler(spec)
        return handler.read(spec, params)

    def read_streaming(self, spec: Any, params: Any, /) -> Any:
        """Dispatch a streaming read to the per-kind handler or base reader.

        Args:
            spec: Source specification with a ``.kind`` attribute.
            params: Concrete params for current run.

        Returns:
            Backend frame produced by the matching reader's
            ``read_streaming``.

        Raises:
            ConfigurationError: When no reader is registered for this kind.
            TypeError: When the matching reader does not implement
                :class:`StreamingSourceReader`.
        """
        handler = self._resolve_handler(spec)
        if not isinstance(handler, StreamingSourceReader):
            raise TypeError(
                f"Reader for spec kind {spec.kind!r} "
                f"({type(handler).__qualname__}) does not implement "
                "StreamingSourceReader; cannot honor streaming=True."
            )
        return handler.read_streaming(spec, params)

    def _resolve_handler(self, spec: Any) -> Any:
        handler = self._extra.get(spec.kind)
        if handler is not None:
            return handler
        if self._base is not None:
            return self._base
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

    def write(
        self,
        frame: Any,
        spec: Any,
        params: Any,
        /,
        *,
        streaming: bool = False,
        write_ctx: Any = None,
    ) -> None:
        """Dispatch write to the matching extra handler or fall back to base."""
        kind = getattr(spec, "kind", None)
        handler = self._extra.get(kind) if kind is not None else None
        if handler is not None:
            handler.write(frame, spec, params, streaming=streaming, write_ctx=write_ctx)
            return
        self._base.write(frame, spec, params, streaming=streaming, write_ctx=write_ctx)
