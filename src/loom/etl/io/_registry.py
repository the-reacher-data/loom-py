"""Dispatch registries that route specs to per-kind readers and writers."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from loom.etl.declarative.source import SourceSpec
from loom.etl.declarative.target import TargetSpec
from loom.etl.runtime.contracts import SourceReader, StreamingSourceReader, TargetWriter

if TYPE_CHECKING:
    from loom.etl.lineage._records import WriteContext


class ReaderRegistry:
    """Read a source spec through one reader, refusing a streaming read it cannot serve.

    Args:
        base: Reader every spec is read through.
    """

    def __init__(self, base: SourceReader) -> None:
        self._base = base

    def read(self, spec: SourceSpec, params: Any, /) -> Any:
        """Read *spec* and return its frame.

        Args:
            spec: Compiled source specification.
            params: Concrete params for current run.

        Returns:
            Backend frame produced by the reader.
        """
        return self._base.read(spec, params)

    def read_streaming(self, spec: SourceSpec, params: Any, /) -> Any:
        """Read *spec* with the reader's memory-bounded strategy.

        Args:
            spec: Compiled source specification.
            params: Concrete params for current run.

        Returns:
            Backend frame produced by the reader's ``read_streaming``.

        Raises:
            TypeError: When the reader does not implement
                :class:`~loom.etl.runtime.contracts.StreamingSourceReader`.
        """
        if not isinstance(self._base, StreamingSourceReader):
            raise TypeError(
                f"Reader {type(self._base).__qualname__!r} does not implement "
                "StreamingSourceReader; cannot honor streaming=True."
            )
        return self._base.read_streaming(spec, params)


class WriterRegistry:
    """Dispatch a write to the writer registered for the spec's kind, or to the base writer.

    Args:
        base: Writer every spec whose kind is not in *extra* is written through.
        extra: Writers by ``spec.kind``.
    """

    def __init__(
        self,
        base: TargetWriter,
        *,
        extra: Mapping[str, TargetWriter] | None = None,
    ) -> None:
        self._base = base
        self._extra: Mapping[str, TargetWriter] = extra or {}

    def write(
        self,
        frame: Any,
        spec: TargetSpec,
        params: Any,
        /,
        *,
        streaming: bool = False,
        write_ctx: WriteContext | None = None,
    ) -> None:
        """Write *frame* into *spec* through its kind's writer, or through the base writer.

        Args:
            frame: Frame returned by the step's ``execute()``.
            spec: Compiled target specification.
            params: Concrete params for current run.
            streaming: Hint for lazy backends to use streaming materialization.
            write_ctx: Execution context for audit-column injection.
        """
        kind = getattr(spec, "kind", None)
        handler = self._extra.get(kind) if kind is not None else None
        writer = handler if handler is not None else self._base
        writer.write(frame, spec, params, streaming=streaming, write_ctx=write_ctx)
