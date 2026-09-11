"""Tests for ReaderRegistry and WriterRegistry dispatch logic."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from loom.etl.io._registry import ReaderRegistry, WriterRegistry


class _FakeSpec:
    """A minimal spec object with a configurable kind."""

    def __init__(self, kind: str) -> None:
        self.kind = kind


_PARAMS = object()


class _StreamingCapableReader:
    """Reader that implements both read and read_streaming."""

    def __init__(self) -> None:
        self.read_calls: list[tuple[Any, Any]] = []
        self.stream_calls: list[tuple[Any, Any]] = []

    def read(self, spec: Any, params: Any, /) -> Any:
        self.read_calls.append((spec, params))
        return "non-streaming-result"

    def read_streaming(self, spec: Any, params: Any, /) -> Any:
        self.stream_calls.append((spec, params))
        return "streaming-result"


class _NonStreamingReader:
    """Reader that only implements read (no streaming capability)."""

    def read(self, spec: Any, params: Any, /) -> Any:
        return "result"


class TestReaderRegistryRead:
    def test_reads_through_its_reader(self) -> None:
        reader = MagicMock()
        reader.read.return_value = "result"
        registry = ReaderRegistry(reader)

        spec = _FakeSpec(kind="table")
        assert registry.read(spec, _PARAMS) == "result"
        reader.read.assert_called_once_with(spec, _PARAMS)


class TestReaderRegistryReadStreaming:
    def test_streams_through_a_capable_reader(self) -> None:
        reader = _StreamingCapableReader()
        registry = ReaderRegistry(reader)
        spec = _FakeSpec(kind="clickhouse")

        assert registry.read_streaming(spec, _PARAMS) == "streaming-result"
        assert reader.stream_calls == [(spec, _PARAMS)]
        assert reader.read_calls == []

    def test_refuses_a_streaming_read_a_reader_cannot_serve(self) -> None:
        # Falling back to a non-streaming read would risk OOM, so it is refused.
        registry = ReaderRegistry(_NonStreamingReader())

        with pytest.raises(TypeError, match="StreamingSourceReader"):
            registry.read_streaming(_FakeSpec(kind="clickhouse"), _PARAMS)


class TestWriterRegistryDispatch:
    def test_writes_through_the_writer_registered_for_the_kind(self) -> None:
        clickhouse_writer = MagicMock()
        base_writer = MagicMock()
        registry = WriterRegistry(base_writer, extra={"clickhouse": clickhouse_writer})
        frame = object()
        spec = _FakeSpec(kind="clickhouse")

        registry.write(frame, spec, _PARAMS)

        clickhouse_writer.write.assert_called_once_with(
            frame, spec, _PARAMS, streaming=False, write_ctx=None
        )
        base_writer.write.assert_not_called()

    def test_falls_back_to_the_base_writer_for_an_unregistered_kind(self) -> None:
        base_writer = MagicMock()
        registry = WriterRegistry(base_writer, extra={"clickhouse": MagicMock()})
        frame = object()
        spec = _FakeSpec(kind="delta")

        registry.write(frame, spec, _PARAMS)

        base_writer.write.assert_called_once_with(
            frame, spec, _PARAMS, streaming=False, write_ctx=None
        )

    def test_falls_back_to_the_base_writer_for_a_spec_without_a_kind(self) -> None:
        base_writer = MagicMock()

        class _KindlessSpec:
            pass

        registry = WriterRegistry(base_writer, extra={"clickhouse": MagicMock()})
        frame = object()
        spec = _KindlessSpec()

        registry.write(frame, spec, _PARAMS)

        base_writer.write.assert_called_once_with(
            frame, spec, _PARAMS, streaming=False, write_ctx=None
        )

    def test_forwards_the_streaming_flag(self) -> None:
        clickhouse_writer = MagicMock()
        registry = WriterRegistry(MagicMock(), extra={"clickhouse": clickhouse_writer})

        registry.write(object(), _FakeSpec(kind="clickhouse"), _PARAMS, streaming=True)

        _, call_kwargs = clickhouse_writer.write.call_args
        assert call_kwargs["streaming"] is True
