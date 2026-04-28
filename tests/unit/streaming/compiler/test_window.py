"""Window strategy compiler validation."""

from __future__ import annotations

import pytest
from omegaconf import DictConfig

from loom.streaming import CollectBatch, Drain, FromTopic, Process, StreamFlow, WindowStrategy
from loom.streaming.compiler import CompilationError, compile_flow
from tests.unit.streaming.compiler.cases import Order, Result


class TestWindowStrategyValidation:
    """Compiler must reject unimplemented WindowStrategy values at compile time."""

    def test_collect_strategy_compiles_successfully(
        self,
        streaming_kafka_config: DictConfig,
    ) -> None:
        flow: StreamFlow[Order, Result] = StreamFlow(
            name="test",
            source=FromTopic("in", payload=Order),
            process=Process(
                CollectBatch(max_records=10, timeout_ms=500),
                Drain(),
            ),
        )

        plan = compile_flow(flow, runtime_config=streaming_kafka_config)

        batch_node = plan.nodes[0].node
        assert isinstance(batch_node, CollectBatch)
        assert batch_node.window is WindowStrategy.COLLECT

    def test_tumbling_strategy_raises_compilation_error(
        self,
        streaming_kafka_config: DictConfig,
    ) -> None:
        flow: StreamFlow[Order, Result] = StreamFlow(
            name="test",
            source=FromTopic("in", payload=Order),
            process=Process(
                CollectBatch(
                    max_records=10,
                    timeout_ms=500,
                    window=WindowStrategy.TUMBLING,
                ),
                Drain(),
            ),
        )

        with pytest.raises(CompilationError) as exc_info:
            compile_flow(flow, runtime_config=streaming_kafka_config)

        assert "tumbling" in str(exc_info.value)

    def test_session_strategy_raises_compilation_error(
        self,
        streaming_kafka_config: DictConfig,
    ) -> None:
        flow: StreamFlow[Order, Result] = StreamFlow(
            name="test",
            source=FromTopic("in", payload=Order),
            process=Process(
                CollectBatch(
                    max_records=10,
                    timeout_ms=500,
                    window=WindowStrategy.SESSION,
                ),
                Drain(),
            ),
        )

        with pytest.raises(CompilationError) as exc_info:
            compile_flow(flow, runtime_config=streaming_kafka_config)

        assert "session" in str(exc_info.value)
