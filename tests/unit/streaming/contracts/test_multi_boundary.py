"""Boundary contract tests for FromMultiTypeTopic and ErrorEnvelope.payload_type."""

from __future__ import annotations

import pytest

from loom.core.model import LoomFrozenStruct
from loom.core.routing import LogicalRef
from loom.streaming import FromMultiTypeTopic, StreamShape
from loom.streaming.core._errors import ErrorEnvelope, ErrorKind


class _OrderEvent(LoomFrozenStruct, frozen=True):
    order_id: str


class _ProductEvent(LoomFrozenStruct, frozen=True):
    sku: str


class TestFromMultiTypeTopicBoundary:
    def test_holds_payloads_tuple_name_and_logical_ref(self) -> None:
        source: FromMultiTypeTopic[_OrderEvent | _ProductEvent] = FromMultiTypeTopic(
            "events.all", payloads=(_OrderEvent, _ProductEvent)
        )

        assert source.name == "events.all"
        assert source.payloads == (_OrderEvent, _ProductEvent)
        assert source.logical_ref == LogicalRef("events.all")

    @pytest.mark.parametrize(
        ("shape", "expected"),
        [
            (StreamShape.RECORD, StreamShape.RECORD),
            (StreamShape.BATCH, StreamShape.BATCH),
        ],
    )
    def test_shape_contract(self, shape: StreamShape, expected: StreamShape) -> None:
        source: FromMultiTypeTopic[_OrderEvent | _ProductEvent] = FromMultiTypeTopic(
            "events.all",
            payloads=(_OrderEvent, _ProductEvent),
            shape=shape,
        )

        assert source.shape is expected

    def test_rejects_single_payload(self) -> None:
        with pytest.raises((ValueError, TypeError)):
            FromMultiTypeTopic("events.all", payloads=(_OrderEvent,))

    def test_accepts_three_or_more_payloads(self) -> None:
        class _ThirdEvent(LoomFrozenStruct, frozen=True):
            ref: str

        source: FromMultiTypeTopic[_OrderEvent | _ProductEvent | _ThirdEvent] = FromMultiTypeTopic(
            "events.all",
            payloads=(_OrderEvent, _ProductEvent, _ThirdEvent),
        )

        assert len(source.payloads) == 3


class TestErrorEnvelopePayloadTypeField:
    def test_payload_type_defaults_and_can_be_set(self) -> None:
        empty: ErrorEnvelope[LoomFrozenStruct] = ErrorEnvelope(kind=ErrorKind.TASK, reason="failed")
        filled: ErrorEnvelope[LoomFrozenStruct] = ErrorEnvelope(
            kind=ErrorKind.BUSINESS,
            reason="domain rule violated",
            payload_type="order.event",
        )

        assert empty.payload_type is None
        assert filled.payload_type == "order.event"

    def test_kind_and_reason_still_required(self) -> None:
        with pytest.raises(TypeError):
            ErrorEnvelope(payload_type="order.event")  # type: ignore[call-arg]

    def test_payload_type_is_independent_of_original_message(self) -> None:
        envelope: ErrorEnvelope[LoomFrozenStruct] = ErrorEnvelope(
            kind=ErrorKind.WIRE,
            reason="decode failed",
            payload_type="order.event",
            original_message=None,
        )

        assert envelope.payload_type == "order.event"
        assert envelope.original_message is None
