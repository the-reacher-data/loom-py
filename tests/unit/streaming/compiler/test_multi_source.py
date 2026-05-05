"""Compiler tests for FromMultiTypeTopic and CompiledMultiSource."""

from __future__ import annotations

from typing import ClassVar

import pytest
from omegaconf import DictConfig

from loom.core.model import LoomFrozenStruct
from loom.streaming import FromMultiTypeTopic, IntoTopic, Process, StreamFlow, StreamShape
from loom.streaming.compiler import compile_flow
from loom.streaming.core._errors import ErrorEnvelope
from tests.unit.streaming.compiler.cases import Order, Result


class _OrderEvent(LoomFrozenStruct, frozen=True):
    __loom_message_type__: ClassVar[str] = "order.event"
    order_id: str


class _ProductEvent(LoomFrozenStruct, frozen=True):
    __loom_message_type__: ClassVar[str] = "product.event"
    sku: str


class TestCompileFlowWithMultiSource:
    def test_produces_compiled_multi_source(self, streaming_kafka_config: DictConfig) -> None:
        from loom.streaming.compiler import CompiledMultiSource

        flow: StreamFlow[_OrderEvent | _ProductEvent, Result] = StreamFlow(
            name="multi-consumer",
            source=FromMultiTypeTopic("in", payloads=(_OrderEvent, _ProductEvent)),
            process=Process(IntoTopic("out", payload=Result)),
        )

        plan = compile_flow(flow, runtime_config=streaming_kafka_config)

        assert isinstance(plan.source, CompiledMultiSource)

    def test_compiled_source_has_correct_shape(self, streaming_kafka_config: DictConfig) -> None:
        from loom.streaming.compiler import CompiledMultiSource

        flow: StreamFlow[_OrderEvent | _ProductEvent, Result] = StreamFlow(
            name="multi-consumer",
            source=FromMultiTypeTopic(
                "in",
                payloads=(_OrderEvent, _ProductEvent),
                shape=StreamShape.RECORD,
            ),
            process=Process(IntoTopic("out", payload=Result)),
        )

        plan = compile_flow(flow, runtime_config=streaming_kafka_config)

        assert isinstance(plan.source, CompiledMultiSource)
        assert plan.source.shape is StreamShape.RECORD

    def test_dispatch_table_contains_plain_type_keys(
        self, streaming_kafka_config: DictConfig
    ) -> None:
        from loom.streaming.compiler import CompiledMultiSource

        flow: StreamFlow[_OrderEvent | _ProductEvent, Result] = StreamFlow(
            name="multi-consumer",
            source=FromMultiTypeTopic("in", payloads=(_OrderEvent, _ProductEvent)),
            process=Process(IntoTopic("out", payload=Result)),
        )

        plan = compile_flow(flow, runtime_config=streaming_kafka_config)

        assert isinstance(plan.source, CompiledMultiSource)
        assert "order.event" in plan.source.dispatch.plain
        assert "product.event" in plan.source.dispatch.plain
        assert plan.source.dispatch.plain["order.event"] is _OrderEvent
        assert plan.source.dispatch.plain["product.event"] is _ProductEvent

    def test_dispatch_table_contains_error_envelope_keys(
        self, streaming_kafka_config: DictConfig
    ) -> None:
        from loom.streaming.compiler import CompiledMultiSource

        flow: StreamFlow[_OrderEvent | ErrorEnvelope[_OrderEvent], Result] = StreamFlow(
            name="error-consumer",
            source=FromMultiTypeTopic(
                "in",
                payloads=(_OrderEvent, ErrorEnvelope[_OrderEvent]),
            ),
            process=Process(IntoTopic("out", payload=Result)),
        )

        plan = compile_flow(flow, runtime_config=streaming_kafka_config)

        assert isinstance(plan.source, CompiledMultiSource)
        assert "order.event" in plan.source.dispatch.error

    def test_compile_fails_without_loom_message_type_on_plain_type(
        self, streaming_kafka_config: DictConfig
    ) -> None:
        from loom.streaming.compiler._compiler import CompilationError

        class _Unregistered(LoomFrozenStruct, frozen=True):
            value: str

        flow: StreamFlow[_OrderEvent | _Unregistered, Result] = StreamFlow(
            name="multi-consumer",
            source=FromMultiTypeTopic("in", payloads=(_OrderEvent, _Unregistered)),
            process=Process(IntoTopic("out", payload=Result)),
        )

        with pytest.raises(CompilationError):
            compile_flow(flow, runtime_config=streaming_kafka_config)

    def test_from_topic_compilation_is_unchanged(self, streaming_kafka_config: DictConfig) -> None:
        from loom.streaming import FromTopic
        from loom.streaming.compiler import CompiledSource

        flow: StreamFlow[Order, Result] = StreamFlow(
            name="single-consumer",
            source=FromTopic("in", payload=Order),
            process=Process(IntoTopic("out", payload=Result)),
        )

        plan = compile_flow(flow, runtime_config=streaming_kafka_config)

        assert isinstance(plan.source, CompiledSource)
