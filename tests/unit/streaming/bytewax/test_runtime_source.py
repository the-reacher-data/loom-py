"""Tests for runtime Kafka source wiring in the Bytewax adapter."""

from __future__ import annotations

from collections.abc import Callable

import pytest

from loom.streaming.bytewax import _runtime_io
from loom.streaming.compiler._plan import CompiledSource
from loom.streaming.kafka._config import ConsumerSettings
from tests.unit.streaming.kafka.fakes import RuntimeConsumerStub

pytestmark = pytest.mark.bytewax


class TestKafkaPollingSource:
    def test_uses_configured_poll_timeout(
        self,
        monkeypatch: pytest.MonkeyPatch,
        bytewax_runtime_source_factory: Callable[[int], CompiledSource],
    ) -> None:
        """_KafkaPollingSource must forward poll_timeout_ms to the consumer."""
        polled_with: list[int] = []

        class _PollingConsumer(RuntimeConsumerStub):
            def __init__(self, settings: ConsumerSettings) -> None:
                super().__init__(settings)

            def poll(self, timeout_ms: int) -> None:
                polled_with.append(timeout_ms)
                return None

        monkeypatch.setattr(_runtime_io, "KafkaConsumerClient", _PollingConsumer)

        source = _runtime_io._KafkaPollingSource(bytewax_runtime_source_factory(250))

        with pytest.raises(_runtime_io._KafkaPollingSource.Retry):
            source.next_item()

        assert polled_with == [250]

    def test_close_delegates_to_raw_consumer(
        self,
        monkeypatch: pytest.MonkeyPatch,
        bytewax_runtime_source_factory: Callable[[int], CompiledSource],
    ) -> None:
        closed: list[str] = []

        class _ClosingConsumer(RuntimeConsumerStub):
            def __init__(self, settings: ConsumerSettings) -> None:
                super().__init__(settings)

            def close(self) -> None:
                closed.append("done")

        monkeypatch.setattr(_runtime_io, "KafkaConsumerClient", _ClosingConsumer)

        source = _runtime_io._KafkaPollingSource(bytewax_runtime_source_factory(250))
        source.close()

        assert closed == ["done"]


class TestConsumerSettings:
    def test_poll_timeout_ms_defaults_to_100(self) -> None:
        settings = ConsumerSettings(
            brokers=("localhost:9092",),
            group_id="test",
            topics=("orders.in",),
        )

        assert settings.poll_timeout_ms == 100
