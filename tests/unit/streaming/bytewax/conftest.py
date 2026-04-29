"""Shared fixtures for Bytewax unit tests."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from loom.streaming import FromTopic, IntoTopic, Process, StreamFlow
from loom.streaming.compiler._plan import CompiledSink, CompiledSource
from loom.streaming.core._message import Message, MessageMeta
from loom.streaming.kafka._config import ConsumerSettings, ProducerSettings
from loom.streaming.nodes._boundary import PartitionPolicy
from loom.streaming.nodes._shape import StreamShape
from tests.unit.streaming.bytewax.cases import (
    DoubleStep,
    Order,
    Result,
    SuffixStep,
    build_message,
)


@pytest.fixture
def bytewax_order() -> Order:
    """Return the canonical Bytewax order payload."""

    return Order(order_id="A")


@pytest.fixture
def bytewax_result() -> Result:
    """Return the canonical Bytewax result payload."""

    return Result(value="AA")


@pytest.fixture
def bytewax_message(bytewax_order: Order) -> Message[Order]:
    """Return the canonical Bytewax message."""

    return build_message(bytewax_order)


@pytest.fixture
def bytewax_double_step() -> DoubleStep:
    """Return the canonical first record step."""

    return DoubleStep()


@pytest.fixture
def bytewax_suffix_step() -> SuffixStep:
    """Return the canonical second record step."""

    return SuffixStep()


@pytest.fixture
def bytewax_runtime_config_dict() -> dict[str, object]:
    """Return the canonical runtime configuration for Bytewax tests."""

    return {
        "kafka": {
            "consumer": {
                "brokers": ["localhost:9092"],
                "group_id": "test",
                "topics": ["orders.in"],
            },
            "producer": {
                "brokers": ["localhost:9092"],
                "client_id": "test-producer",
                "topic": "orders.out",
            },
        },
        "streaming": {
            "runtime": {
                "workers_per_process": 2,
                "epoch_interval_ms": 5000,
                "addresses": ["127.0.0.1:2101", "127.0.0.1:2102"],
                "process_id": 1,
                "recovery": {
                    "db_dir": "/var/lib/loom/tests/bytewax-recovery",
                    "backup_interval_ms": 30000,
                },
            }
        },
    }


@pytest.fixture
def bytewax_stream_flow() -> StreamFlow[Order, Result]:
    """Return a canonical Bytewax flow used by runner tests."""

    return StreamFlow(
        name="runner_flow",
        source=FromTopic("orders.in", payload=Order),
        process=Process(DoubleStep()),
        output=IntoTopic("orders.out", payload=Result),
    )


@pytest.fixture
def bytewax_runtime_sink_factory() -> Callable[
    [PartitionPolicy[Any] | None, str | None], CompiledSink
]:
    """Return a factory for compiled runtime sink test objects."""

    def _build_sink(
        partitioning: PartitionPolicy[Any] | None,
        dlq_topic: str | None = None,
    ) -> CompiledSink:
        return CompiledSink(
            settings=ProducerSettings(
                brokers=("localhost:9092",),
                client_id="test-producer",
                topic="orders.out",
            ),
            topic="orders.out",
            partition_policy=partitioning,
            dlq_topic=dlq_topic,
        )

    return _build_sink


@pytest.fixture
def bytewax_runtime_source_factory() -> Callable[[int], CompiledSource]:
    """Return a factory for compiled runtime source test objects."""

    def _build_source(poll_timeout_ms: int = 100) -> CompiledSource:
        return CompiledSource(
            settings=ConsumerSettings(
                brokers=("localhost:9092",),
                group_id="test",
                topics=("orders.in",),
                poll_timeout_ms=poll_timeout_ms,
            ),
            topics=("orders.in",),
            payload_type=Order,
            shape=StreamShape.RECORD,
            decode_strategy="record",
        )

    return _build_source


@pytest.fixture
def bytewax_order_message_factory() -> Callable[[str, bytes | str | None], Message[Order]]:
    """Return a factory for order payload messages used by runtime I/O tests."""

    def _build_message(order_id: str, key: bytes | str | None = None) -> Message[Order]:
        return Message(
            payload=Order(order_id=order_id),
            meta=MessageMeta(
                message_id="msg-1",
                topic="orders.in",
                key=key,
            ),
        )

    return _build_message
