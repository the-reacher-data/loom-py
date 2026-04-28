"""Message contract tests for streaming."""

from __future__ import annotations

from typing import Any, cast

import pytest

from loom.core.model import LoomFrozenStruct
from loom.streaming import CollectBatch, ErrorEnvelope, ErrorKind, Message, MessageMeta
from tests.unit.streaming.contracts.cases import Order


class TestMessageContracts:
    def test_message_and_metadata_are_loom_structs(self) -> None:
        meta = MessageMeta(
            message_id="msg-1",
            correlation_id="corr-1",
            trace_id="trace-1",
            causation_id="cause-1",
            produced_at_ms=42,
            message_type="order.created",
            message_version=2,
            topic="orders.in",
            partition=2,
            offset=9,
            key=b"tenant-a",
            headers={"x": b"1"},
        )
        message = Message(payload=Order(order_id="o-1"), meta=meta)

        assert isinstance(meta, LoomFrozenStruct)
        assert isinstance(message, LoomFrozenStruct)
        assert message.payload.order_id == "o-1"
        assert message.meta.causation_id == "cause-1"
        assert message.meta.produced_at_ms == 42
        assert message.meta.message_type == "order.created"
        assert message.meta.message_version == 2
        assert message.meta.topic == "orders.in"
        assert message.meta.headers == {"x": b"1"}

    def test_value_contracts_are_immutable(self) -> None:
        batch = CollectBatch(max_records=500, timeout_ms=250)

        with pytest.raises(AttributeError):
            cast(Any, batch).max_records = 100

    def test_collect_batch_rejects_non_positive_limits(self) -> None:
        with pytest.raises(ValueError):
            CollectBatch(max_records=0, timeout_ms=1)

    def test_error_envelope_carries_error_kind_and_original_message(self) -> None:
        message = Message(payload=Order(order_id="o-1"), meta=MessageMeta(message_id="msg-1"))
        envelope = ErrorEnvelope(
            kind=ErrorKind.TASK,
            reason="task failed",
            original_message=message,
        )

        assert envelope.kind is ErrorKind.TASK
        assert envelope.reason == "task failed"
        assert envelope.original_message is message
