"""Tests for direct Bytewax runtime I/O builders."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from loom.streaming.bytewax import _runtime_io
from loom.streaming.compiler._plan import CompiledSink, CompiledSource
from loom.streaming.core._errors import ErrorKind
from loom.streaming.core._message import Message
from loom.streaming.kafka._errors import KafkaDeliveryError
from loom.streaming.nodes._boundary import PartitionPolicy
from tests.unit.streaming.kafka.fakes import ConsumerBackendStub, RawProducerStub

pytestmark = pytest.mark.bytewax


class TestRuntimeIOBuilders:
    def test_build_runtime_source_returns_polling_source(
        self,
        monkeypatch: pytest.MonkeyPatch,
        bytewax_runtime_source_factory: Callable[[int], CompiledSource],
    ) -> None:
        closed: list[str] = []

        class _ClosingConsumer(ConsumerBackendStub):
            def __init__(self, settings: object) -> None:
                super().__init__({})
                del settings

            def close(self) -> None:
                closed.append("done")
                super().close()

        monkeypatch.setattr(_runtime_io, "KafkaConsumerClient", _ClosingConsumer)
        source = _runtime_io.build_runtime_source(bytewax_runtime_source_factory(250))

        assert isinstance(source, _runtime_io._KafkaPollingSource)
        with pytest.raises(_runtime_io._KafkaPollingSource.Retry):
            source.next_item()
        source.close()
        assert closed == ["done"]

    def test_build_runtime_sink_returns_sink_and_terminal_mappings(
        self,
        monkeypatch: pytest.MonkeyPatch,
        bytewax_runtime_sink_factory: Callable[
            [PartitionPolicy[Any] | None, str | None], CompiledSink
        ],
    ) -> None:
        fake_raw = RawProducerStub()
        monkeypatch.setattr(_runtime_io, "KafkaProducerClient", lambda settings: fake_raw)

        error_sink = bytewax_runtime_sink_factory(None, "dlq")
        terminal_sink = bytewax_runtime_sink_factory(None, "out")
        sink = _runtime_io.build_runtime_sink(bytewax_runtime_sink_factory(None, None))
        error_sinks = _runtime_io.build_runtime_error_sinks({ErrorKind.WIRE: error_sink})
        terminal_sinks = _runtime_io.build_runtime_terminal_sinks({(0,): terminal_sink})

        assert isinstance(sink, _runtime_io._KafkaMessageSink)
        assert isinstance(error_sinks[ErrorKind.WIRE], _runtime_io._KafkaMessageSink)
        assert isinstance(terminal_sinks[(0,)], _runtime_io._KafkaMessageSink)

        partition = sink.build("step", 0, 1)
        assert isinstance(partition, _runtime_io._KafkaMessageSinkPartition)
        partition.write_batch([])
        partition.close()

    def test_build_inline_sink_partition_can_write_dlq_payloads(
        self,
        monkeypatch: pytest.MonkeyPatch,
        bytewax_runtime_sink_factory: Callable[
            [PartitionPolicy[Any] | None, str | None], CompiledSink
        ],
        bytewax_order_message_factory: Callable[[str, bytes | str | None], Message[Any]],
    ) -> None:
        fake_raw = RawProducerStub()
        monkeypatch.setattr(_runtime_io, "KafkaProducerClient", lambda settings: fake_raw)
        fake_raw.flush_error = KafkaDeliveryError("broker unavailable")

        partition = _runtime_io.build_inline_sink_partition(
            bytewax_runtime_sink_factory(None, "orders.dlq"),
        )
        partition.write_batch([bytewax_order_message_factory("123", None)])

        topics = [record.topic for record in fake_raw.sent]
        assert "orders.out" in topics
        assert "orders.dlq" in topics
