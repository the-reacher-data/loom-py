"""Tests for the streaming flow compiler."""

from __future__ import annotations

from typing import Any

import pytest
from omegaconf import OmegaConf

from loom.core.model import LoomStruct
from loom.streaming import FromTopic, IntoTopic, Process, StreamFlow, StreamShape, Task
from loom.streaming._message import Message
from loom.streaming.compiler import CompilationError, CompiledSource, compile_flow
from loom.streaming.compiler._compiler import _uses_kafka


class _Order(LoomStruct):
    order_id: str


class _Result(LoomStruct):
    value: str


class _FakeTask(Task[_Order, _Result]):
    def execute(self, message: Message[_Order], **kwargs: Any) -> _Result:
        return _Result(value=message.payload.order_id)


def test_compile_success_with_minimal_flow() -> None:
    flow = StreamFlow(
        name="test",
        source=FromTopic("in", payload=_Order),
        process=Process(IntoTopic("out", payload=_Result)),
    )
    cfg = OmegaConf.create(
        {
            "kafka": {
                "consumer": {"brokers": ["localhost:9092"], "group_id": "test", "topics": ["in"]},
                "producer": {"brokers": ["localhost:9092"], "topic": "out"},
            }
        }
    )

    plan = compile_flow(flow, runtime_config=cfg)

    assert plan.name == "test"
    assert isinstance(plan.source, CompiledSource)
    assert plan.source.payload_type is _Order
    assert plan.source.shape is StreamShape.RECORD
    assert plan.source.decode_strategy == "record"
    assert plan.output is None


def test_compile_fails_when_binding_path_missing() -> None:
    binding = _FakeTask.from_config("tasks.missing")
    flow = StreamFlow(
        name="test",
        source=FromTopic("in", payload=_Order),
        process=Process(binding, IntoTopic("out", payload=_Result)),
    )
    cfg = OmegaConf.create({})

    with pytest.raises(CompilationError) as exc_info:
        compile_flow(flow, runtime_config=cfg)

    assert "tasks.missing" in str(exc_info.value)


def test_compile_fails_when_kafka_missing_for_topic_flow() -> None:
    flow = StreamFlow(
        name="test",
        source=FromTopic("in", payload=_Order),
        process=Process(_FakeTask(), IntoTopic("out", payload=_Result)),
    )
    cfg = OmegaConf.create({})  # no kafka section

    with pytest.raises(CompilationError) as exc_info:
        compile_flow(flow, runtime_config=cfg)

    assert "kafka" in str(exc_info.value)


def test_compile_succeeds_when_kafka_present() -> None:
    flow = StreamFlow(
        name="test",
        source=FromTopic("in", payload=_Order),
        process=Process(_FakeTask(), IntoTopic("out", payload=_Result)),
    )
    cfg = OmegaConf.create(
        {
            "kafka": {
                "consumer": {"brokers": ["localhost:9092"], "group_id": "test", "topics": ["in"]},
                "producer": {"brokers": ["localhost:9092"], "topic": "out"},
            }
        }
    )

    plan = compile_flow(flow, runtime_config=cfg)

    assert plan.name == "test"


def test_compile_fails_on_shape_mismatch() -> None:
    """Task expects RECORD but source is BATCH without CollectBatch."""
    flow = StreamFlow(
        name="test",
        source=FromTopic("in", payload=_Order, shape=StreamShape.BATCH),
        process=Process(_FakeTask(), IntoTopic("out", payload=_Result)),
    )
    cfg = OmegaConf.create(
        {
            "kafka": {
                "consumer": {"brokers": ["localhost:9092"], "group_id": "test", "topics": ["in"]},
                "producer": {"brokers": ["localhost:9092"], "topic": "out"},
            }
        }
    )

    with pytest.raises(CompilationError) as exc_info:
        compile_flow(flow, runtime_config=cfg)

    assert "shape mismatch" in str(exc_info.value)


def test_compile_fails_without_output() -> None:
    flow = StreamFlow(
        name="test",
        source=FromTopic("in", payload=_Order),
        process=Process(_FakeTask()),
        output=None,
    )
    cfg = OmegaConf.create(
        {
            "kafka": {
                "consumer": {"brokers": ["localhost:9092"], "group_id": "test", "topics": ["in"]},
            }
        }
    )

    with pytest.raises(CompilationError) as exc_info:
        compile_flow(flow, runtime_config=cfg)

    assert "no terminal output" in str(exc_info.value)


def test_uses_kafka_detects_kafka_usage() -> None:
    flow_with_output = StreamFlow(
        name="test",
        source=FromTopic("in", payload=_Order),
        process=Process(IntoTopic("out", payload=_Result)),
    )
    flow_without_output = StreamFlow(
        name="test",
        source=FromTopic("in", payload=_Order),
        process=Process(_FakeTask()),
        output=None,
    )
    # Any flow with FromTopic source uses Kafka
    assert _uses_kafka(flow_with_output) is True
    assert _uses_kafka(flow_without_output) is True
