"""Tests for runtime Kafka source wiring in the Bytewax adapter."""

from __future__ import annotations

from collections.abc import Callable

import pytest

from loom.streaming.bytewax import _runtime_io
from loom.streaming.compiler._plan import CompiledSource
from loom.streaming.kafka._config import ConsumerSettings


class TestKafkaPollingSource:
    def test_uses_configured_poll_timeout(
        self,
        monkeypatch: pytest.MonkeyPatch,
        bytewax_runtime_source_factory: Callable[[int], CompiledSource],
    ) -> None:
        """_KafkaPollingSource must forward poll_timeout_ms to the consumer."""
        polled_with: list[int] = []

        class _FakeConsumer:
            def __init__(self, settings: ConsumerSettings) -> None:
                del settings

            def poll(self, timeout_ms: int) -> None:
                polled_with.append(timeout_ms)
                return None

            def close(self) -> None:
                return None

        monkeypatch.setattr(_runtime_io, "KafkaConsumerClient", _FakeConsumer)

        source = _runtime_io._KafkaPollingSource(bytewax_runtime_source_factory(250))

        with pytest.raises(_runtime_io._KafkaPollingSource.Retry):
            source.next_item()

        assert polled_with == [250]


class TestConsumerSettings:
    def test_poll_timeout_ms_defaults_to_100(self) -> None:
        settings = ConsumerSettings(
            brokers=("localhost:9092",),
            group_id="test",
            topics=("orders.in",),
        )

        assert settings.poll_timeout_ms == 100
