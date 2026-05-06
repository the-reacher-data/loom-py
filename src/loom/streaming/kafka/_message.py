"""Standard Kafka message envelope and helpers."""

from __future__ import annotations

import time
from typing import Generic, TypeVar

import msgspec

from loom.core.model import LoomFrozenStruct, LoomStruct
from loom.core.tracing import get_trace_id

PayloadT = TypeVar("PayloadT", bound=LoomStruct | LoomFrozenStruct)

HEADER_CORRELATION_ID = "x-correlation-id"
HEADER_CAUSATION_ID = "x-causation-id"
HEADER_TRACE_ID = "x-trace-id"
HEADER_PARENT_TRACE_ID = "x-parent-trace-id"


class ContentType(LoomFrozenStruct, frozen=True):
    """Describe the logical wire content type of one Kafka payload.

    Attributes:
        media_type: Stable media type string for the payload bytes.
        encoding: Optional encoding qualifier.
    """

    media_type: str
    encoding: str | None = None

    @classmethod
    def msgpack(cls) -> ContentType:
        """Return the default Loom MessagePack content type.

        Returns:
            Standard MessagePack content-type descriptor for Loom-native
            payloads.
        """

        return cls(media_type="application/x-loom-msgpack")

    @classmethod
    def avro(cls) -> ContentType:
        """Return the standard Avro content type.

        Returns:
            Standard Avro content-type descriptor.
        """

        return cls(media_type="application/avro")


class SchemaRef(LoomFrozenStruct, frozen=True):
    """Reference a payload schema contract without forcing one registry.

    Attributes:
        namespace: Schema authority or logical owner.
        name: Stable schema or message name.
        version: Schema version identifier.
        format: Schema representation identifier.
    """

    namespace: str
    name: str
    version: str
    format: str


class MessageDescriptor(LoomFrozenStruct, frozen=True):
    """Describe the stable identity of a Kafka message contract.

    Attributes:
        message_type: Stable logical message type name.
        message_version: Logical message version.
        content_type: Wire content descriptor.
        schema_ref: Optional schema reference.
    """

    message_type: str
    message_version: int
    content_type: ContentType = msgspec.field(default_factory=ContentType.msgpack)
    schema_ref: SchemaRef | None = None


class MessageMetadata(LoomFrozenStruct, frozen=True):
    """Transport metadata for the standard Kafka message envelope.

    Attributes:
        descriptor: Stable message contract descriptor.
        trace_id: Trace identifier propagated across process boundaries.
        parent_trace_id: Optional upstream trace identifier.
        correlation_id: Correlation identifier shared across related messages.
        causation_id: Optional upstream message identifier.
        produced_at_ms: Producer timestamp in epoch milliseconds.
    """

    descriptor: MessageDescriptor
    trace_id: str | None = None
    parent_trace_id: str | None = None
    correlation_id: str | None = None
    causation_id: str | None = None
    produced_at_ms: int = msgspec.field(default_factory=lambda: int(time.time() * 1000))


class MessageEnvelope(LoomFrozenStruct, Generic[PayloadT], frozen=True):
    """Standard Kafka message envelope for Loom streaming.

    Attributes:
        meta: Envelope metadata.
        payload: Typed payload body.
    """

    meta: MessageMetadata
    payload: PayloadT


def build_message(
    payload: PayloadT,
    descriptor: MessageDescriptor,
    *,
    correlation_id: str | None = None,
    parent_trace_id: str | None = None,
    causation_id: str | None = None,
    trace_id: str | None = None,
    produced_at_ms: int | None = None,
) -> MessageEnvelope[PayloadT]:
    """Build the standard Kafka message envelope.

    The caller usually provides only the payload and descriptor. Trace context
    is taken from the active Loom tracing context when not supplied.

    Args:
        payload: Typed message payload.
        descriptor: Stable message contract descriptor.
        correlation_id: Optional correlation identifier.
        parent_trace_id: Optional upstream trace identifier.
        causation_id: Optional upstream message identifier.
        trace_id: Optional explicit trace identifier.
        produced_at_ms: Optional producer timestamp in epoch milliseconds.

    Returns:
        A typed message envelope ready for serialization.
    """

    active_trace_id = trace_id if trace_id is not None else get_trace_id()
    timestamp_ms = produced_at_ms if produced_at_ms is not None else int(time.time() * 1000)
    return MessageEnvelope(
        meta=MessageMetadata(
            descriptor=descriptor,
            trace_id=active_trace_id,
            parent_trace_id=parent_trace_id,
            correlation_id=correlation_id,
            causation_id=causation_id,
            produced_at_ms=timestamp_ms,
        ),
        payload=payload,
    )
