"""Tests for WriterRegistry dispatch logic."""

from __future__ import annotations

from unittest.mock import MagicMock

from loom.etl.io._registry import WriterRegistry


class _FakeSpec:
    """A minimal spec object with a configurable kind."""

    def __init__(self, kind: str) -> None:
        self.kind = kind


_PARAMS = object()


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
