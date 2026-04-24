"""Tests for the streaming flow compiler."""

from __future__ import annotations

from typing import Any

import pytest
from omegaconf import OmegaConf

from loom.core.model import LoomStruct
from loom.streaming import (
    CollectBatch,
    Drain,
    FromTopic,
    IntoTopic,
    Process,
    RecordStep,
    Route,
    Router,
    StreamFlow,
    StreamShape,
    msg,
)
from loom.streaming.compiler import CompilationError, CompiledSource, compile_flow
from loom.streaming.compiler._compiler import _uses_kafka
from loom.streaming.core._message import Message


class _Order(LoomStruct):
    order_id: str


class _Result(LoomStruct):
    value: str


class _FakeStep(RecordStep[_Order, _Result]):
    def execute(self, message: Message[_Order], **kwargs: object) -> _Result:
        return _Result(value=message.payload.order_id)


def _kafka_config() -> dict[str, Any]:
    return {
        "kafka": {
            "consumer": {"brokers": ["localhost:9092"], "group_id": "test", "topics": ["in"]},
            "producer": {"brokers": ["localhost:9092"], "topic": "out"},
        }
    }


def test_compile_success_with_minimal_flow() -> None:
    flow: StreamFlow[_Order, _Result] = StreamFlow(
        name="test",
        source=FromTopic("in", payload=_Order),
        process=Process(IntoTopic("out", payload=_Result)),
    )
    cfg = OmegaConf.create(_kafka_config())

    plan = compile_flow(flow, runtime_config=cfg)

    assert plan.name == "test"
    assert isinstance(plan.source, CompiledSource)
    assert plan.source.payload_type is _Order
    assert plan.source.shape is StreamShape.RECORD
    assert plan.source.decode_strategy == "record"
    assert plan.output is None


def test_compile_fails_when_binding_path_missing() -> None:
    binding = _FakeStep.from_config("tasks.missing")
    flow: StreamFlow[_Order, _Result] = StreamFlow(
        name="test",
        source=FromTopic("in", payload=_Order),
        process=Process(binding, IntoTopic("out", payload=_Result)),
    )
    cfg = OmegaConf.create({})

    with pytest.raises(CompilationError) as exc_info:
        compile_flow(flow, runtime_config=cfg)

    assert "tasks.missing" in str(exc_info.value)


def test_compile_fails_when_kafka_missing_for_topic_flow() -> None:
    flow: StreamFlow[_Order, _Result] = StreamFlow(
        name="test",
        source=FromTopic("in", payload=_Order),
        process=Process(_FakeStep(), IntoTopic("out", payload=_Result)),
    )
    cfg = OmegaConf.create({})  # no kafka section

    with pytest.raises(CompilationError) as exc_info:
        compile_flow(flow, runtime_config=cfg)

    assert "kafka" in str(exc_info.value)


def test_compile_succeeds_when_kafka_present() -> None:
    flow: StreamFlow[_Order, _Result] = StreamFlow(
        name="test",
        source=FromTopic("in", payload=_Order),
        process=Process(_FakeStep(), IntoTopic("out", payload=_Result)),
    )
    cfg = OmegaConf.create(_kafka_config())

    plan = compile_flow(flow, runtime_config=cfg)

    assert plan.name == "test"


def test_compile_fails_on_shape_mismatch() -> None:
    """Step expects RECORD but source is BATCH without CollectBatch."""
    flow: StreamFlow[_Order, _Result] = StreamFlow(
        name="test",
        source=FromTopic("in", payload=_Order, shape=StreamShape.BATCH),
        process=Process(_FakeStep(), IntoTopic("out", payload=_Result)),
    )
    cfg = OmegaConf.create(_kafka_config())

    with pytest.raises(CompilationError) as exc_info:
        compile_flow(flow, runtime_config=cfg)

    assert "shape mismatch" in str(exc_info.value)


def test_compile_fails_without_output() -> None:
    flow: StreamFlow[_Order, _Result] = StreamFlow(
        name="test",
        source=FromTopic("in", payload=_Order),
        process=Process(_FakeStep()),
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


def test_compile_drain_outputs_none_shape() -> None:
    flow: StreamFlow[_Order, _Result] = StreamFlow(
        name="test",
        source=FromTopic("in", payload=_Order),
        process=Process(Drain()),
    )
    cfg = OmegaConf.create(_kafka_config())

    plan = compile_flow(flow, runtime_config=cfg)

    assert plan.nodes[-1].output_shape is StreamShape.NONE
    assert plan.output is None


def test_compile_validates_router_branch_shapes() -> None:
    flow: StreamFlow[_Order, _Result] = StreamFlow(
        name="test",
        source=FromTopic("in", payload=_Order),
        process=Process(
            Router.when(
                (
                    Route(
                        when=msg.payload.order_id == "batch",
                        process=Process(CollectBatch(max_records=10, timeout_ms=1000)),
                    ),
                ),
                default=Process(_FakeStep()),
            ),
            IntoTopic("out", payload=_Result),
        ),
    )
    cfg = OmegaConf.create(_kafka_config())

    with pytest.raises(CompilationError) as exc_info:
        compile_flow(flow, runtime_config=cfg)

    assert "router branches produce different shapes" in str(exc_info.value)


def test_compile_accepts_router_with_terminal_branch_output() -> None:
    flow: StreamFlow[_Order, _Result] = StreamFlow(
        name="test",
        source=FromTopic("in", payload=_Order),
        process=Process(
            Router.by(
                msg.payload.order_id,
                routes={"vip": Process(_FakeStep(), IntoTopic("out", payload=_Result))},
            )
        ),
        output=None,
    )
    cfg = OmegaConf.create(_kafka_config())

    plan = compile_flow(flow, runtime_config=cfg)

    assert plan.nodes[0].output_shape is StreamShape.RECORD
    assert plan.output is None


def test_uses_kafka_detects_kafka_usage() -> None:
    flow_with_output: StreamFlow[_Order, _Result] = StreamFlow(
        name="test",
        source=FromTopic("in", payload=_Order),
        process=Process(IntoTopic("out", payload=_Result)),
    )
    flow_without_output: StreamFlow[_Order, _Result] = StreamFlow(
        name="test",
        source=FromTopic("in", payload=_Order),
        process=Process(_FakeStep()),
        output=None,
    )
    # Any flow with FromTopic source uses Kafka
    assert _uses_kafka(flow_with_output) is True
    assert _uses_kafka(flow_without_output) is True


def test_compile_fails_on_batch_scope_with_direct_context_manager() -> None:
    """BATCH scope with a direct CM instance must be rejected at compile time."""
    from loom.streaming import ResourceScope, With

    class _FakeSyncCM:
        def __enter__(self) -> _FakeSyncCM:
            return self

        def __exit__(self, *args: object) -> None:
            return None

    flow: StreamFlow[_Order, _Result] = StreamFlow(
        name="test",
        source=FromTopic("in", payload=_Order),
        process=Process(With(step=_FakeStep(), scope=ResourceScope.BATCH, db=_FakeSyncCM())),
        output=IntoTopic("out", payload=_Result),
    )

    with pytest.raises(CompilationError, match="ContextFactory"):
        compile_flow(flow, runtime_config=OmegaConf.create(_kafka_config()))


def test_compile_succeeds_on_batch_scope_with_context_factory() -> None:
    """BATCH scope with a ContextFactory is valid."""
    from loom.streaming import ContextFactory, ResourceScope, With

    class _FakeSyncCM:
        def __enter__(self) -> _FakeSyncCM:
            return self

        def __exit__(self, *args: object) -> None:
            return None

    flow: StreamFlow[_Order, _Result] = StreamFlow(
        name="test",
        source=FromTopic("in", payload=_Order),
        process=Process(
            With(
                step=_FakeStep(),
                scope=ResourceScope.BATCH,
                db=ContextFactory(lambda: _FakeSyncCM()),
            )
        ),
        output=IntoTopic("out", payload=_Result),
    )

    plan = compile_flow(flow, runtime_config=OmegaConf.create(_kafka_config()))
    assert plan.name == "test"
