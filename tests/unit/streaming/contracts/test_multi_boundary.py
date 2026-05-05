"""Boundary contract tests for FromMultiTypeTopic and ErrorEnvelope.payload_type."""

from __future__ import annotations

import pytest

from loom.core.model import LoomFrozenStruct
from loom.core.routing import LogicalRef
from loom.streaming import StreamShape
from loom.streaming.core._errors import ErrorEnvelope, ErrorKind


class _OrderEvent(LoomFrozenStruct, frozen=True):
    order_id: str


class _ProductEvent(LoomFrozenStruct, frozen=True):
    sku: str


class TestFromMultiTypeTopicBoundary:
    def test_holds_payloads_tuple_and_name(self) -> None:
        from loom.streaming import FromMultiTypeTopic

        source = FromMultiTypeTopic("events.all", payloads=(_OrderEvent, _ProductEvent))

        assert source.name == "events.all"
        assert source.payloads == (_OrderEvent, _ProductEvent)

    def test_defaults_to_record_shape(self) -> None:
        from loom.streaming import FromMultiTypeTopic

        source = FromMultiTypeTopic("events.all", payloads=(_OrderEvent, _ProductEvent))

        assert source.shape is StreamShape.RECORD

    def test_accepts_explicit_shape(self) -> None:
        from loom.streaming import FromMultiTypeTopic

        source = FromMultiTypeTopic(
            "events.all",
            payloads=(_OrderEvent, _ProductEvent),
            shape=StreamShape.BATCH,
        )

        assert source.shape is StreamShape.BATCH

    def test_logical_ref_is_derived_from_name(self) -> None:
        from loom.streaming import FromMultiTypeTopic

        source = FromMultiTypeTopic("events.all", payloads=(_OrderEvent, _ProductEvent))

        assert source.logical_ref == LogicalRef("events.all")

    def test_rejects_single_payload(self) -> None:
        from loom.streaming import FromMultiTypeTopic

        with pytest.raises((ValueError, TypeError)):
            FromMultiTypeTopic("events.all", payloads=(_OrderEvent,))

    def test_accepts_three_or_more_payloads(self) -> None:

        from loom.streaming import FromMultiTypeTopic

        class _ThirdEvent(LoomFrozenStruct, frozen=True):
            ref: str

        source = FromMultiTypeTopic(
            "events.all",
            payloads=(_OrderEvent, _ProductEvent, _ThirdEvent),
        )

        assert len(source.payloads) == 3


class TestErrorEnvelopePayloadTypeField:
    def test_defaults_to_none_for_backward_compat(self) -> None:
        envelope = ErrorEnvelope(kind=ErrorKind.TASK, reason="failed")

        assert envelope.payload_type is None

    def test_can_be_set_to_inner_message_type_string(self) -> None:
        envelope = ErrorEnvelope(
            kind=ErrorKind.BUSINESS,
            reason="domain rule violated",
            payload_type="order.event",
        )

        assert envelope.payload_type == "order.event"

    def test_kind_and_reason_still_required(self) -> None:
        with pytest.raises(TypeError):
            ErrorEnvelope(payload_type="order.event")  # type: ignore[call-arg]

    def test_payload_type_is_independent_of_original_message(self) -> None:
        envelope = ErrorEnvelope(
            kind=ErrorKind.WIRE,
            reason="decode failed",
            payload_type="order.event",
            original_message=None,
        )

        assert envelope.payload_type == "order.event"
        assert envelope.original_message is None
