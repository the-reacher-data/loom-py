"""Kafka wire decoding helpers for streaming adapters."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Generic, TypeAlias, TypeVar

import msgspec

from loom.core.logger import get_logger
from loom.core.model import LoomFrozenStruct, LoomStruct
from loom.streaming.core._errors import ErrorEnvelope, ErrorKind
from loom.streaming.core._message import Message, MessageMeta
from loom.streaming.kafka._codec import KafkaCodec
from loom.streaming.kafka._message import MessageEnvelope
from loom.streaming.kafka._record import KafkaRecord

PayloadT = TypeVar("PayloadT", bound=LoomStruct | LoomFrozenStruct)

_ERROR_PREFIX = "loom.streaming.error."
logger = get_logger(__name__)


def _record_ctx(record: KafkaRecord[bytes]) -> dict[str, Any]:
    return {
        "topic": record.topic,
        "partition": record.partition,
        "offset": record.offset,
    }


class _DescriptorProbe(msgspec.Struct, frozen=True):
    message_type: str
    message_version: int


class _MetadataProbe(msgspec.Struct, frozen=True):
    descriptor: _DescriptorProbe


class _EnvelopeProbe(msgspec.Struct, frozen=True):
    meta: _MetadataProbe


class _ErrorPayloadProbe(msgspec.Struct, frozen=True):
    kind: str
    reason: str
    payload_type: str | None = None


class _EnvelopeErrorProbe(msgspec.Struct, frozen=True):
    meta: _MetadataProbe
    payload: _ErrorPayloadProbe


@dataclass(frozen=True)
class DispatchTable:
    """Pre-built decode dispatch table for heterogeneous Kafka topics.

    Args:
        plain: Maps outer ``message_type`` strings to their payload types.
        error: Maps inner ``ErrorEnvelope.payload_type`` strings to the
            corresponding ``ErrorEnvelope[T]`` generic alias.
    """

    plain: Mapping[str, type[LoomStruct | LoomFrozenStruct]]
    error: Mapping[str, Any]


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


def try_decode_multi_record(
    record: KafkaRecord[bytes],
    dispatch: DispatchTable,
    codec: KafkaCodec[Any],
) -> DecodeResult[Any]:
    """Decode one Kafka record from a heterogeneous topic using a dispatch table.

    Uses a two-stage probe strategy:

    1. Decode only ``MessageEnvelope.meta.descriptor.message_type`` (cheap).
    2. For error envelopes, additionally probe ``ErrorEnvelope.payload_type``.
    3. Look up the concrete type in ``dispatch`` and perform the full decode.

    Unknown ``message_type`` values and decode failures both produce a
    ``DecodeError`` with ``ErrorKind.WIRE``.

    Args:
        record: Raw Kafka record from a heterogeneous topic.
        dispatch: Pre-built dispatch table keyed by ``message_type`` and error
            ``payload_type`` strings.
        codec: Codec used for full envelope decoding.

    Returns:
        ``DecodeOk`` on success, ``DecodeError`` on probe or decode failure.
    """

    outer_message_type = _probe_message_type(record.value)
    if outer_message_type is None:
        return _wire_error("failed to probe message_type from envelope", record)
    logger.debug(
        "multi_source_probe",
        **_record_ctx(record),
        outer_message_type=outer_message_type,
    )

    if outer_message_type.startswith(_ERROR_PREFIX):
        return _decode_error_envelope(record, dispatch, codec, outer_message_type)

    payload_type = dispatch.plain.get(outer_message_type)
    if payload_type is None:
        return _wire_error(f"unknown message_type: {outer_message_type!r}", record)
    logger.debug(
        "multi_source_dispatch_plain",
        **_record_ctx(record),
        outer_message_type=outer_message_type,
        target_type=_describe_target_type(payload_type),
    )

    return try_decode_record(record, payload_type, codec)


def _decode_error_envelope(
    record: KafkaRecord[bytes],
    dispatch: DispatchTable,
    codec: KafkaCodec[Any],
    outer_message_type: str,
) -> DecodeResult[Any]:
    inner_payload_type = _probe_error_payload_type(record.value)
    if inner_payload_type is None:
        return _wire_error(
            f"error envelope missing payload_type for {outer_message_type!r}", record
        )

    target_type = dispatch.error.get(inner_payload_type)
    if target_type is None:
        return _wire_error(f"unknown error payload_type: {inner_payload_type!r}", record)
    logger.debug(
        "multi_source_dispatch_error",
        **_record_ctx(record),
        outer_message_type=outer_message_type,
        payload_type=inner_payload_type,
        target_type=_describe_target_type(target_type),
    )

    return try_decode_record(record, target_type, codec)


def _probe_message_type(raw: bytes) -> str | None:
    try:
        probe = msgspec.msgpack.decode(raw, type=_EnvelopeProbe)
        return probe.meta.descriptor.message_type
    except Exception:
        return None


def _probe_error_payload_type(raw: bytes) -> str | None:
    try:
        probe = msgspec.msgpack.decode(raw, type=_EnvelopeErrorProbe)
        return probe.payload.payload_type
    except Exception:
        return None


def _wire_error(reason: str, record: KafkaRecord[bytes]) -> DecodeError:
    logger.debug(
        "multi_source_wire_error",
        **_record_ctx(record),
        reason=reason,
    )
    return DecodeError(
        error=ErrorEnvelope(kind=ErrorKind.WIRE, reason=reason, original_message=None),
        raw=record.value,
        topic=record.topic,
        key=record.key,
        headers=record.headers,
        partition=record.partition,
        offset=record.offset,
        timestamp_ms=record.timestamp_ms,
    )


def _message_id(record: KafkaRecord[bytes]) -> str:
    if record.partition is not None and record.offset is not None:
        return f"{record.topic}:{record.partition}:{record.offset}"
    if record.key is not None:
        return f"{record.topic}:{record.key!s}"
    if record.timestamp_ms is not None:
        return f"{record.topic}:{record.timestamp_ms}"
    return record.topic


def _describe_target_type(t: Any) -> str:
    if not isinstance(t, type):
        return repr(t)
    return f"{t.__module__}.{t.__qualname__}"
