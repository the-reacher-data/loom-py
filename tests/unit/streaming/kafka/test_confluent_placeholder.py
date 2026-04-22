"""Verify public exports from the kafka package expose client classes."""

from __future__ import annotations

from loom.streaming.kafka import KafkaConsumerClient, KafkaProducerClient


def test_public_exports_expose_raw_kafka_clients() -> None:
    assert KafkaProducerClient.__name__ == "KafkaProducerClient"
    assert KafkaConsumerClient.__name__ == "KafkaConsumerClient"
