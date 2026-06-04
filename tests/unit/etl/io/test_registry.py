"""Tests for ReaderRegistry and WriterRegistry dispatch logic."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from loom.etl.io._registry import (
    ConfigurationError,  # noqa: F401
    ReaderRegistry,  # noqa: F401
    WriterRegistry,  # noqa: F401
)

# ---------------------------------------------------------------------------
# Minimal spec stand-ins — plain objects with a .kind attribute
# ---------------------------------------------------------------------------


class _FakeSpec:
    """A minimal spec object with a configurable kind."""

    def __init__(self, kind: str) -> None:
        self.kind = kind


_PARAMS = object()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestRegistryDispatchesToRegisteredReader:
    def test_registry_dispatches_to_registered_reader(self) -> None:
        """A reader registered for 'clickhouse' kind is called when that spec arrives."""
        ch_reader = MagicMock()
        ch_reader.read.return_value = "ch_result"

        base_reader = MagicMock()

        registry = ReaderRegistry(base_reader, extra={"clickhouse": ch_reader})

        spec = _FakeSpec(kind="clickhouse")
        result = registry.read(spec, _PARAMS)

        ch_reader.read.assert_called_once_with(spec, _PARAMS)
        base_reader.read.assert_not_called()
        assert result == "ch_result"


class TestRegistryFallsBackToBaseReader:
    def test_registry_falls_back_to_base_reader(self) -> None:
        """A spec whose kind is not in extra dispatches to the base reader."""
        base_reader = MagicMock()
        base_reader.read.return_value = "base_result"

        registry = ReaderRegistry(base_reader, extra={"clickhouse": MagicMock()})

        spec = _FakeSpec(kind="table")
        result = registry.read(spec, _PARAMS)

        base_reader.read.assert_called_once_with(spec, _PARAMS)
        assert result == "base_result"


class TestRegistryRaisesOnUnknownKindWithoutBaseFallback:
    def test_registry_raises_on_unknown_kind_without_base_fallback(self) -> None:
        """Without a base reader and an unknown kind, ConfigurationError is raised."""
        registry = ReaderRegistry(None, extra={"clickhouse": MagicMock()})

        spec = _FakeSpec(kind="mongo_lookup")
        with pytest.raises(ConfigurationError):
            registry.read(spec, _PARAMS)


class TestWriterRegistryDispatchesToRegisteredWriter:
    def test_writer_registry_dispatches_to_registered_writer(self) -> None:
        """A writer registered for 'clickhouse' kind is called when that spec arrives."""
        ch_writer = MagicMock()
        base_writer = MagicMock()

        class _CHSpec:
            kind = "clickhouse"

        registry = WriterRegistry(base_writer, extra={"clickhouse": ch_writer})
        frame = object()
        spec = _CHSpec()
        registry.write(frame, spec, _PARAMS)

        ch_writer.write.assert_called_once_with(
            frame, spec, _PARAMS, streaming=False, write_ctx=None
        )
        base_writer.write.assert_not_called()

    def test_writer_registry_falls_back_to_base_writer(self) -> None:
        """A spec without a matching kind falls back to the base writer."""
        base_writer = MagicMock()

        class _DeltaSpec:
            pass  # no .kind attribute

        registry = WriterRegistry(base_writer, extra={"clickhouse": MagicMock()})
        frame = object()
        spec = _DeltaSpec()
        registry.write(frame, spec, _PARAMS)

        base_writer.write.assert_called_once_with(
            frame, spec, _PARAMS, streaming=False, write_ctx=None
        )

    def test_writer_registry_forwards_streaming_flag(self) -> None:
        """The streaming keyword argument is forwarded to the chosen handler."""
        ch_writer = MagicMock()

        class _CHSpec:
            kind = "clickhouse"

        registry = WriterRegistry(MagicMock(), extra={"clickhouse": ch_writer})
        registry.write(object(), _CHSpec(), _PARAMS, streaming=True)

        ch_writer.write.assert_called_once()
        _, call_kwargs = ch_writer.write.call_args
        assert call_kwargs["streaming"] is True


class TestRegistryValidatesPresenceAtConstruction:
    def test_registry_validates_presence_at_construction(self) -> None:
        """A handler without its required dependency raises ConfigurationError at build time.

        The implementation must accept an optional 'validate' callable (or similar
        mechanism) per handler so that missing dependencies (e.g. no Mongo connection)
        are caught at registry construction, not at read() time.
        This test verifies the contract: pass a handler that raises ConfigurationError
        on construction when its dependency is absent.
        """

        def _make_broken_reader() -> Any:
            """Factory that always fails — simulates absent external dependency."""
            raise ConfigurationError("Mongo client not configured")

        with pytest.raises(ConfigurationError, match="Mongo client not configured"):
            # The registry must call the factory (or validate) at construction time.
            ReaderRegistry(
                None,
                extra={"mongo_lookup": _make_broken_reader()},
            )


# ---------------------------------------------------------------------------
# Streaming dispatch
# ---------------------------------------------------------------------------


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


class TestRegistryReadStreaming:
    def test_read_streaming_dispatches_to_capable_handler(self) -> None:
        handler = _StreamingCapableReader()
        registry = ReaderRegistry(None, extra={"clickhouse": handler})
        spec = _FakeSpec(kind="clickhouse")

        result = registry.read_streaming(spec, _PARAMS)

        assert result == "streaming-result"
        assert handler.stream_calls == [(spec, _PARAMS)]
        assert handler.read_calls == []

    def test_read_streaming_rejects_non_streaming_handler(self) -> None:
        registry = ReaderRegistry(None, extra={"clickhouse": _NonStreamingReader()})
        spec = _FakeSpec(kind="clickhouse")

        with pytest.raises(TypeError, match="StreamingSourceReader"):
            registry.read_streaming(spec, _PARAMS)

    def test_read_streaming_falls_back_to_streaming_base(self) -> None:
        base = _StreamingCapableReader()
        registry = ReaderRegistry(base, extra={"clickhouse": _NonStreamingReader()})
        spec = _FakeSpec(kind="table")

        result = registry.read_streaming(spec, _PARAMS)

        assert result == "streaming-result"
        assert base.stream_calls == [(spec, _PARAMS)]

    def test_read_streaming_raises_on_unknown_kind(self) -> None:
        registry = ReaderRegistry(None, extra={})
        spec = _FakeSpec(kind="mystery")

        with pytest.raises(ConfigurationError):
            registry.read_streaming(spec, _PARAMS)
