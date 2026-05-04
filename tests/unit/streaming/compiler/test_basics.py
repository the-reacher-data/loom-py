"""Core compiler contracts for streaming flows."""

from __future__ import annotations

import pytest
from omegaconf import DictConfig, OmegaConf

from loom.streaming import Drain, FromTopic, IntoTopic, Process, StreamFlow, StreamShape
from loom.streaming.compiler import CompiledSource, compile_flow
from loom.streaming.compiler._compiler import CompilationError
from tests.unit.streaming.compiler.cases import FakeStep, Order, Result


def test_compile_success_with_minimal_flow(streaming_kafka_config: DictConfig) -> None:
    flow: StreamFlow[Order, Result] = StreamFlow(
        name="test",
        source=FromTopic("in", payload=Order),
        process=Process(IntoTopic("out", payload=Result)),
    )

    plan = compile_flow(flow, runtime_config=streaming_kafka_config)

    assert plan.name == "test"
    assert isinstance(plan.source, CompiledSource)
    assert plan.source.payload_type is Order
    assert plan.source.shape is StreamShape.RECORD
    assert plan.source.decode_strategy == "record"
    assert plan.output is None


def test_compile_fails_when_binding_path_missing() -> None:
    binding = FakeStep.from_config("tasks.missing")
    flow: StreamFlow[Order, Result] = StreamFlow(
        name="test",
        source=FromTopic("in", payload=Order),
        process=Process(binding, IntoTopic("out", payload=Result)),
    )

    with pytest.raises(CompilationError) as exc_info:
        compile_flow(flow, runtime_config=OmegaConf.create({}))

    assert "tasks.missing" in str(exc_info.value)


def test_compile_fails_when_kafka_missing_for_topic_flow() -> None:
    flow: StreamFlow[Order, Result] = StreamFlow(
        name="test",
        source=FromTopic("in", payload=Order),
        process=Process(FakeStep(), IntoTopic("out", payload=Result)),
    )

    with pytest.raises(CompilationError) as exc_info:
        compile_flow(flow, runtime_config=OmegaConf.create({}))

    assert "kafka" in str(exc_info.value)


def test_compile_succeeds_when_kafka_present(streaming_kafka_config: DictConfig) -> None:
    flow: StreamFlow[Order, Result] = StreamFlow(
        name="test",
        source=FromTopic("in", payload=Order),
        process=Process(FakeStep(), IntoTopic("out", payload=Result)),
    )

    plan = compile_flow(flow, runtime_config=streaming_kafka_config)

    assert plan.name == "test"


def test_compile_fails_on_shape_mismatch(streaming_kafka_config: DictConfig) -> None:
    """Step expects RECORD but source is BATCH without CollectBatch."""
    flow: StreamFlow[Order, Result] = StreamFlow(
        name="test",
        source=FromTopic("in", payload=Order, shape=StreamShape.BATCH),
        process=Process(FakeStep(), IntoTopic("out", payload=Result)),
    )

    with pytest.raises(CompilationError) as exc_info:
        compile_flow(flow, runtime_config=streaming_kafka_config)

    assert "shape mismatch" in str(exc_info.value)


def test_compile_fails_without_output(streaming_kafka_config: DictConfig) -> None:
    flow: StreamFlow[Order, Result] = StreamFlow(
        name="test",
        source=FromTopic("in", payload=Order),
        process=Process(FakeStep()),
        output=None,
    )

    with pytest.raises(CompilationError) as exc_info:
        compile_flow(flow, runtime_config=streaming_kafka_config)

    assert "no terminal output" in str(exc_info.value)


def test_compile_drain_outputs_none_shape(streaming_kafka_config: DictConfig) -> None:
    flow: StreamFlow[Order, Result] = StreamFlow(
        name="test",
        source=FromTopic("in", payload=Order),
        process=Process(Drain()),
    )

    plan = compile_flow(flow, runtime_config=streaming_kafka_config)

    assert plan.nodes[-1].output_shape is StreamShape.NONE
    assert plan.output is None
