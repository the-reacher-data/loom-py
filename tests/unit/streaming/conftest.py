"""Shared fixtures for streaming unit tests."""

from __future__ import annotations

import pytest
from omegaconf import DictConfig, OmegaConf

from tests.unit.streaming.flows.flow_cases import (
    StreamFlowCase,
    build_async_flow_case,
    build_fork_flow_case,
    build_fork_when_flow_case,
    build_fork_with_flow_case,
    build_router_flow_case,
    build_simple_validation_flow_case,
    build_with_batch_flow_case,
    build_with_batch_scope_flow_case,
)


@pytest.fixture
def streaming_kafka_config() -> DictConfig:
    """Return the shared Kafka runtime config used by streaming flow cases."""
    return OmegaConf.create(_streaming_kafka_config_data())


@pytest.fixture
def streaming_kafka_config_dict() -> dict[str, object]:
    """Return the shared Kafka runtime config as a plain dictionary."""
    return _streaming_kafka_config_data()


@pytest.fixture
def simple_validation_flow_case(streaming_kafka_config: DictConfig) -> StreamFlowCase:
    """Return the simplest public DSL flow case."""
    return build_simple_validation_flow_case(streaming_kafka_config)


@pytest.fixture
def router_flow_case(streaming_kafka_config: DictConfig) -> StreamFlowCase:
    """Return a public DSL flow case with multiple router branches."""
    return build_router_flow_case(streaming_kafka_config)


@pytest.fixture
def with_batch_flow_case(streaming_kafka_config: DictConfig) -> StreamFlowCase:
    """Return a public DSL flow case with With and ForEach."""
    return build_with_batch_flow_case(streaming_kafka_config)


@pytest.fixture
def with_batch_scope_flow_case(streaming_kafka_config: DictConfig) -> StreamFlowCase:
    """Return a public DSL flow case with BATCH-scoped With resources."""
    return build_with_batch_scope_flow_case(streaming_kafka_config)


@pytest.fixture
def async_flow_case(streaming_kafka_config: DictConfig) -> StreamFlowCase:
    """Return a public DSL flow case with WithAsync and ForEach."""
    return build_async_flow_case(streaming_kafka_config)


@pytest.fixture
def fork_flow_case(streaming_kafka_config: DictConfig) -> StreamFlowCase:
    """Return a public DSL flow case with Fork terminal branches."""
    return build_fork_flow_case(streaming_kafka_config)


@pytest.fixture
def fork_with_flow_case(streaming_kafka_config: DictConfig) -> StreamFlowCase:
    """Return a public DSL flow case with Fork branches that open resources."""
    return build_fork_with_flow_case(streaming_kafka_config)


@pytest.fixture
def fork_when_flow_case(streaming_kafka_config: DictConfig) -> StreamFlowCase:
    """Return a public DSL flow case with ordered Fork.when branches."""
    return build_fork_when_flow_case(streaming_kafka_config)


def _streaming_kafka_config_data() -> dict[str, object]:
    return {
        "kafka": {
            "consumer": {
                "brokers": ["localhost:9092"],
                "group_id": "test",
                "topics": ["orders.raw"],
            },
            "producer": {"brokers": ["localhost:9092"], "topic": "orders.validated"},
            "producers": {
                "orders.validated": {
                    "brokers": ["localhost:9092"],
                    "topic": "orders.validated",
                },
                "orders.routed": {
                    "brokers": ["localhost:9092"],
                    "topic": "orders.routed",
                },
                "orders.a": {
                    "brokers": ["localhost:9092"],
                    "topic": "orders.a",
                },
                "orders.b": {
                    "brokers": ["localhost:9092"],
                    "topic": "orders.b",
                },
                "orders.default": {
                    "brokers": ["localhost:9092"],
                    "topic": "orders.default",
                },
                "orders.priced": {
                    "brokers": ["localhost:9092"],
                    "topic": "orders.priced",
                },
                "orders.priced.batch_scope": {
                    "brokers": ["localhost:9092"],
                    "topic": "orders.priced.batch_scope",
                },
                "orders.scored": {
                    "brokers": ["localhost:9092"],
                    "topic": "orders.scored",
                },
                "orders.fork.vip": {
                    "brokers": ["localhost:9092"],
                    "topic": "orders.fork.vip",
                },
                "orders.fork.standard": {
                    "brokers": ["localhost:9092"],
                    "topic": "orders.fork.standard",
                },
                "events.analytics": {
                    "brokers": ["localhost:9092"],
                    "topic": "events.analytics",
                },
                "orders.fulfillment": {
                    "brokers": ["localhost:9092"],
                    "topic": "orders.fulfillment",
                },
                "extra_output": {
                    "brokers": ["localhost:9092"],
                    "topic": "extra_output",
                },
            },
        }
    }
