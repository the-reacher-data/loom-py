"""Dead-letter queue helpers for the streaming Bytewax runtime."""

from __future__ import annotations

import logging

from loom.streaming.core._message import Message
from loom.streaming.core._typing import StreamPayload
from loom.streaming.kafka._errors import KafkaDeliveryError
from loom.streaming.kafka._message import MessageDescriptor
from loom.streaming.kafka.message._producer import KafkaMessageProducer

logger = logging.getLogger(__name__)


def send_batch_to_dlq(
    producer: KafkaMessageProducer[StreamPayload],
    dlq_topic: str,
    messages: list[Message[StreamPayload]],
    exc: KafkaDeliveryError,
) -> None:
    """Best-effort: send all messages in *messages* to the DLQ topic."""
    error_bytes = str(exc).encode("utf-8")
    for message in messages:
        try:
            descriptor = MessageDescriptor(
                message_type=message.meta.message_type or dlq_topic,
                message_version=message.meta.message_version or 1,
            )
            headers = {**message.meta.headers, "x-dlq-error": error_bytes}
            producer.send(
                topic=dlq_topic,
                payload=message.payload,
                descriptor=descriptor,
                headers=headers,
                correlation_id=message.meta.correlation_id,
                causation_id=message.meta.causation_id,
                trace_id=message.meta.trace_id,
                produced_at_ms=message.meta.produced_at_ms,
            )
        except Exception:
            logger.warning("dlq_send_failed", extra={"dlq_topic": dlq_topic})


__all__ = ["send_batch_to_dlq"]
