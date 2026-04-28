"""Shared fixtures for Bytewax unit tests."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from loom.streaming import FromTopic, IntoTopic, Process, StreamFlow
from loom.streaming.compiler._plan import (
    CompiledNode,
    CompiledPlan,
    CompiledSink,
    CompiledSource,
)
from loom.streaming.core._errors import ErrorKind
from loom.streaming.core._message import Message
from loom.streaming.kafka._config import ConsumerSettings, ProducerSettings
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
def bytewax_plan_factory() -> Callable[
    [
        tuple[object, ...],
        IntoTopic[Any] | None,
        dict[tuple[int, ...], CompiledSink] | None,
        dict[ErrorKind, CompiledSink] | None,
    ],
    CompiledPlan,
]:
    """Return a reusable compiled-plan builder for Bytewax adapter tests."""

    def _build_plan(
        *nodes: object,
        output: IntoTopic[Any] | None = None,
        terminal_sinks: dict[tuple[int, ...], CompiledSink] | None = None,
        error_routes: dict[ErrorKind, CompiledSink] | None = None,
    ) -> CompiledPlan:
        compiled_nodes = [
            CompiledNode(node=node, input_shape=StreamShape.RECORD, output_shape=StreamShape.RECORD)
            for node in nodes
        ]
        compiled_output = None
        if output is not None:
            compiled_output = CompiledSink(
                settings=ProducerSettings(
                    brokers=("localhost:9092",),
                    client_id="test-producer",
                    topic=output.name,
                ),
                topic=output.name,
                partition_policy=None,
            )
        return CompiledPlan(
            name="test_flow",
            source=CompiledSource(
                settings=ConsumerSettings(
                    brokers=("localhost:9092",),
                    group_id="test",
                    topics=("in",),
                ),
                topics=("in",),
                payload_type=Order,
                shape=StreamShape.RECORD,
                decode_strategy="record",
            ),
            nodes=tuple(compiled_nodes),
            output=compiled_output,
            terminal_sinks=terminal_sinks or {},
            error_routes=error_routes or {},
            needs_async_bridge=False,
        )

    return _build_plan
