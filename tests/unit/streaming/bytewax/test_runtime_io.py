"""Tests for direct Bytewax runtime I/O builders."""

from __future__ import annotations

from typing import cast

import pytest
from confluent_kafka import TopicPartition

from loom.streaming.bytewax import _runtime_io
from loom.streaming.core._errors import ErrorKind
from loom.streaming.kafka._errors import KafkaDeliveryError
from loom.streaming.kafka._record import KafkaRecord
from tests.unit.streaming.bytewax.cases import (
    build_compiled_sink,
    build_compiled_source,
    build_order_message,
)
from tests.unit.streaming.kafka.fakes import ConsumerBackendStub, RawProducerStub

pytestmark = pytest.mark.bytewax


class TestRuntimeIOBuilders:
    def test_build_runtime_source_returns_polling_source(
        self,
        monkeypatch: pytest.MonkeyPatch,
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
        source = _runtime_io.build_runtime_source(build_compiled_source(250))

        assert isinstance(source, _runtime_io._KafkaPollingSource)
        with pytest.raises(_runtime_io._KafkaPollingSource.Retry):
            source.next_item()
        source.close()
        assert closed == ["done"]

    def test_build_runtime_sink_returns_sink_and_terminal_mappings(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fake_raw = RawProducerStub()
        monkeypatch.setattr(_runtime_io, "KafkaProducerClient", lambda settings: fake_raw)
        source = build_compiled_source(enable_auto_commit=False)
        tracker = _runtime_io.build_commit_tracker(source)
        assert tracker is not None

        sink = _runtime_io.build_runtime_sink(build_compiled_sink(), tracker)
        error_sinks = _runtime_io.build_runtime_error_sinks(
            {ErrorKind.WIRE: build_compiled_sink(topic="orders.dlq")},
            tracker,
        )
        terminal_sinks = _runtime_io.build_runtime_terminal_sinks(
            {(0,): build_compiled_sink(topic="orders.terminal")},
            tracker,
        )

        assert isinstance(sink, _runtime_io._KafkaMessageSink)
        assert isinstance(error_sinks[ErrorKind.WIRE], _runtime_io._KafkaMessageSink)
        assert isinstance(terminal_sinks[(0,)], _runtime_io._KafkaMessageSink)

        partition = sink.build("step", 0, 1)
        assert isinstance(partition, _runtime_io._KafkaMessageSinkPartition)
        partition.write_batch([build_order_message("123", None)])

        error_partition = error_sinks[ErrorKind.WIRE].build("step", 0, 1)
        error_partition.write_batch([build_order_message("456", None)])

        terminal_partition = terminal_sinks[(0,)].build("step", 0, 1)
        terminal_partition.write_batch([build_order_message("789", None)])
        partition.close()
        error_partition.close()
        terminal_partition.close()

        assert [record.topic for record in fake_raw.sent] == [
            "orders.out",
            "orders.dlq",
            "orders.terminal",
        ]

    def test_commit_tracker_commits_after_sink_write(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        source_cfg = build_compiled_source(enable_auto_commit=False)
        tracker = _runtime_io.build_commit_tracker(source_cfg)
        assert tracker is not None

        class _TrackingConsumer(ConsumerBackendStub):
            def __init__(self, settings: object) -> None:
                super().__init__({})
                del settings
                self.next_message = KafkaRecord(
                    topic="orders.in",
                    key=None,
                    value=b"raw",
                    partition=2,
                    offset=9,
                )

        fake_raw = RawProducerStub()
        monkeypatch.setattr(_runtime_io, "KafkaConsumerClient", _TrackingConsumer)
        monkeypatch.setattr(_runtime_io, "KafkaProducerClient", lambda settings: fake_raw)

        source = _runtime_io.build_runtime_source(source_cfg, tracker)
        sink = _runtime_io.build_runtime_sink(build_compiled_sink(), tracker)

        record = source.next_item()
        assert isinstance(record, KafkaRecord)

        partition = sink.build("step", 0, 1)
        partition.write_batch(
            [
                build_order_message(
                    "123",
                    None,
                    partition=2,
                    offset=9,
                )
            ]
        )

        assert cast(ConsumerBackendStub, source._consumer).commit_offset_calls == [
            [TopicPartition("orders.in", 2, 10)]
        ]

    def test_commit_tracker_waits_for_contiguous_offsets(
        self,
    ) -> None:
        tracker = _runtime_io.build_commit_tracker(build_compiled_source(enable_auto_commit=False))
        assert tracker is not None
        consumer = ConsumerBackendStub({})
        tracker.bind(consumer)

        tracker.register_record(
            KafkaRecord(topic="orders.in", key=None, value=b"raw", partition=2, offset=3)
        )
        tracker.register_record(
            KafkaRecord(topic="orders.in", key=None, value=b"raw", partition=2, offset=4)
        )
        tracker.register_record(
            KafkaRecord(topic="orders.in", key=None, value=b"raw", partition=2, offset=5)
        )

        tracker.complete("orders.in", 2, 5)
        tracker.complete("orders.in", 2, 3)
        assert consumer.commit_offset_calls == [[TopicPartition("orders.in", 2, 4)]]

        tracker.complete("orders.in", 2, 4)

        assert consumer.commit_offset_calls == [
            [TopicPartition("orders.in", 2, 4)],
            [TopicPartition("orders.in", 2, 6)],
        ]

    def test_commit_tracker_accounts_for_broadcast_fanout(
        self,
    ) -> None:
        tracker = _runtime_io.build_commit_tracker(build_compiled_source(enable_auto_commit=False))
        assert tracker is not None
        consumer = ConsumerBackendStub({})
        tracker.bind(consumer)

        tracker.register_record(
            KafkaRecord(topic="orders.in", key=None, value=b"raw", partition=2, offset=9)
        )
        tracker.fork("orders.in", 2, 9, 2)

        tracker.complete("orders.in", 2, 9)
        assert consumer.commit_offset_calls == []

        tracker.complete("orders.in", 2, 9)
        assert consumer.commit_offset_calls == []

        tracker.complete("orders.in", 2, 9)
        assert consumer.commit_offset_calls == [[TopicPartition("orders.in", 2, 10)]]

    def test_commit_tracker_propagates_commit_offset_errors(
        self,
    ) -> None:
        tracker = _runtime_io.build_commit_tracker(build_compiled_source(enable_auto_commit=False))
        assert tracker is not None
        consumer = ConsumerBackendStub({})
        consumer.commit_error = RuntimeError("commit-boom")
        tracker.bind(consumer)

        tracker.register_record(
            KafkaRecord(topic="orders.in", key=None, value=b"raw", partition=2, offset=9)
        )

        with pytest.raises(RuntimeError, match="commit-boom"):
            tracker.complete("orders.in", 2, 9)

    def test_build_inline_sink_partition_can_write_dlq_payloads(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fake_raw = RawProducerStub()
        monkeypatch.setattr(_runtime_io, "KafkaProducerClient", lambda settings: fake_raw)
        fake_raw.flush_error = KafkaDeliveryError("broker unavailable")

        partition = _runtime_io.build_inline_sink_partition(
            build_compiled_sink(dlq_topic="orders.dlq"),
        )
        partition.write_batch([build_order_message("123", None)])

        topics = [record.topic for record in fake_raw.sent]
        assert "orders.out" in topics
        assert "orders.dlq" in topics
