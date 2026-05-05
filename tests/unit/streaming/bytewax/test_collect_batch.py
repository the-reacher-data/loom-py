"""Behavioral tests for Bytewax collect batching."""

from __future__ import annotations

import pytest
from omegaconf import DictConfig

from loom.streaming import (
    CollectBatch,
    FromTopic,
    IntoTopic,
    Message,
    Process,
    RecordStep,
    ResourceScope,
    StreamFlow,
    With,
)
from loom.streaming.testing import StreamingTestRunner
from tests.unit.streaming.bytewax.cases import Order, Result, build_message

pytestmark = pytest.mark.bytewax


class TestCollectBatch:
    def test_collect_batch_emits_after_timeout_even_without_reaching_max_records(
        self,
        streaming_kafka_config: DictConfig,
    ) -> None:
        flow: StreamFlow[Order, Result] = StreamFlow(
            name="collect_timeout_flow",
            source=FromTopic("orders.in", payload=Order),
            process=Process(
                CollectBatch(max_records=2, timeout_ms=1),
                With(
                    process=Process(_EchoRecordStep(), IntoTopic("orders.out", payload=Result)),
                    client=object(),
                    scope=ResourceScope.WORKER,
                ),
            ),
        )
        runner = StreamingTestRunner.from_flow(
            flow,
            runtime_config=streaming_kafka_config,
        ).with_messages([build_message(Order(order_id="o-1"))])

        runner.run()

        assert [message.payload.value for message in runner.output] == ["o-1"]
        assert len(runner.output) == 1


class _EchoRecordStep(RecordStep[Order, Result]):
    def execute(self, message: Message[Order], **kwargs: object) -> Result:
        del kwargs
        return Result(value=message.payload.order_id)
