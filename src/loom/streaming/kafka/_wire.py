"""Kafka wire decoding helpers for streaming adapters."""

from __future__ import annotations

from typing import Generic, TypeAlias, TypeVar

from loom.core.model import LoomFrozenStruct, LoomStruct
from loom.streaming.core._errors import ErrorEnvelope, ErrorKind
from loom.streaming.core._message import Message, MessageMeta
from loom.streaming.kafka._codec import KafkaCodec
from loom.streaming.kafka._message import MessageEnvelope
from loom.streaming.kafka._record import KafkaRecord

PayloadT = TypeVar("PayloadT", bound=LoomStruct | LoomFrozenStruct)


class DecodeOk(LoomFrozenStruct, Generic[PayloadT], frozen=True):
    """Successful Kafka wire decode result.

    Args:
        message: Transport-neutral message ready for DSL execution.
    """

    message: Message[PayloadT]


class DecodeError(LoomFrozenStruct, frozen=True):
    """Failed Kafka wire decode result with raw dead-letter context.

    Args:
        error: Structured WIRE error for DSL error routing.
        raw: Original record bytes that failed to decode.
        topic: Source Kafka topic.
        key: Source Kafka key.
        headers: Source Kafka headers.
        partition: Source Kafka partition when available.
        offset: Source Kafka offset when available.
        timestamp_ms: Source Kafka timestamp when available.
    """

    error: ErrorEnvelope[LoomStruct | LoomFrozenStruct]
    raw: bytes
    topic: str
    key: bytes | str | None
    headers: dict[str, bytes]
    partition: int | None = None
    offset: int | None = None
    timestamp_ms: int | None = None


DecodeResult: TypeAlias = DecodeOk[PayloadT] | DecodeError
"""Result of decoding one Kafka wire record without raising decode errors."""


def envelope_to_message(
    envelope: MessageEnvelope[PayloadT],
    record: KafkaRecord[bytes],
) -> Message[PayloadT]:
    """Convert a Kafka wire envelope and record context to a DSL message.

    Args:
        envelope: Decoded standard Kafka message envelope.
        record: Original Kafka transport record.

    Returns:
        Transport-neutral streaming message with envelope metadata preserved
        where it can influence DSL routing or user logic.
    """

    descriptor = envelope.meta.descriptor
    return Message(
        payload=envelope.payload,
        meta=MessageMeta(
            message_id=_message_id(record),
            correlation_id=envelope.meta.correlation_id,
            trace_id=envelope.meta.trace_id,
            causation_id=envelope.meta.causation_id,
            produced_at_ms=envelope.meta.produced_at_ms,
            message_type=descriptor.message_type,
            message_version=descriptor.message_version,
            topic=record.topic,
            partition=record.partition,
            offset=record.offset,
            key=record.key,
            headers=record.headers,
        ),
    )


def try_decode_record(
    record: KafkaRecord[bytes],
    payload_type: type[PayloadT],
    codec: KafkaCodec[PayloadT],
) -> DecodeResult[PayloadT]:
    """Decode one Kafka record to a DSL message without raising decode errors.

    Args:
        record: Raw Kafka record whose value contains a Loom message envelope.
        payload_type: Expected payload model type.
        codec: Codec used to decode the envelope bytes.

    Returns:
        ``DecodeOk`` when decoding succeeds, otherwise ``DecodeError`` carrying
        the original raw bytes and Kafka context needed by a DLQ sink.
    """

    try:
        envelope = codec.decode(record.value, payload_type)
    except Exception as exc:
        return DecodeError(
            error=ErrorEnvelope(kind=ErrorKind.WIRE, reason=str(exc), original_message=None),
            raw=record.value,
            topic=record.topic,
            key=record.key,
            headers=record.headers,
            partition=record.partition,
            offset=record.offset,
            timestamp_ms=record.timestamp_ms,
        )
    return DecodeOk(message=envelope_to_message(envelope, record))


def _message_id(record: KafkaRecord[bytes]) -> str:
    if record.partition is not None and record.offset is not None:
        return f"{record.topic}:{record.partition}:{record.offset}"
    if record.key is not None:
        return f"{record.topic}:{record.key!s}"
    if record.timestamp_ms is not None:
        return f"{record.topic}:{record.timestamp_ms}"
    return record.topic
