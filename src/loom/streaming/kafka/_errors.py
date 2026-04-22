"""Kafka transport error hierarchy."""

from __future__ import annotations


class KafkaClientError(Exception):
    """Base error for Kafka transport operations."""


class KafkaConfigurationError(KafkaClientError):
    """Raised when client configuration is invalid or incomplete."""


class KafkaProducerError(KafkaClientError):
    """Base error for producer-side failures."""


class KafkaSerializationError(KafkaProducerError):
    """Raised when a record cannot be serialized for Kafka output."""


class KafkaDeliveryError(KafkaProducerError):
    """Raised when Kafka delivery fails."""


class KafkaConsumerError(KafkaClientError):
    """Base error for consumer-side failures."""


class KafkaPollError(KafkaConsumerError):
    """Raised when Kafka polling returns a backend error."""


class KafkaCommitError(KafkaConsumerError):
    """Raised when Kafka offset commit fails."""


class KafkaDeserializationError(KafkaConsumerError):
    """Raised when Kafka payload bytes cannot be decoded."""
