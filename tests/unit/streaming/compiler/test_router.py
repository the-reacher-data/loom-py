"""Router branch compiler validation."""

from __future__ import annotations

import pytest
from omegaconf import DictConfig, OmegaConf

from loom.streaming import (
    CollectBatch,
    ExpandStep,
    FromTopic,
    IntoTopic,
    Message,
    Process,
    Route,
    Router,
    StreamFlow,
    StreamShape,
    msg,
)
from loom.streaming.compiler import compile_flow
from loom.streaming.compiler._compiler import CompilationError
from tests.unit.streaming.compiler.cases import FakeStep, Order, Result


class TestRouterCompiler:
    def test_validates_router_branch_shapes(
        self,
        streaming_kafka_config: DictConfig,
    ) -> None:
        flow: StreamFlow[Order, Result] = StreamFlow(
            name="test",
            source=FromTopic("in", payload=Order),
            process=Process(
                Router.when(
                    (
                        Route(
                            when=msg.payload.order_id == "batch",
                            process=Process(CollectBatch(max_records=10, timeout_ms=1000)),
                        ),
                    ),
                    default=Process(FakeStep()),
                ),
                IntoTopic("out", payload=Result),
            ),
        )

        with pytest.raises(CompilationError) as exc_info:
            compile_flow(flow, runtime_config=streaming_kafka_config)

        assert "router branches produce different shapes" in str(exc_info.value)

    def test_accepts_router_with_terminal_branch_output(
        self,
        streaming_kafka_config: DictConfig,
    ) -> None:
        flow: StreamFlow[Order, Result] = StreamFlow(
            name="test",
            source=FromTopic("in", payload=Order),
            process=Process(
                Router.by(
                    msg.payload.order_id,
                    routes={"vip": Process(FakeStep(), IntoTopic("out", payload=Result))},
                )
            ),
            output=None,
        )

        plan = compile_flow(flow, runtime_config=streaming_kafka_config)

        assert plan.nodes[0].output_shape is StreamShape.RECORD
        assert plan.output is None

    def test_mixed_keyed_and_predicate_router_sinks_have_unique_paths(
        self,
        streaming_kafka_config: DictConfig,
    ) -> None:
        # orders.a and orders.b are registered in the shared config fixture
        flow: StreamFlow[Order, Result] = StreamFlow(
            name="test",
            source=FromTopic("in", payload=Order),
            process=Process(
                Router(
                    selector=msg.payload.order_id,
                    routes={"vip": Process(FakeStep(), IntoTopic("orders.a", payload=Result))},
                    predicate_routes=[
                        Route(
                            when=msg.payload.order_id != "",
                            process=Process(FakeStep(), IntoTopic("orders.b", payload=Result)),
                        )
                    ],
                )
            ),
        )

        plan = compile_flow(flow, runtime_config=streaming_kafka_config)

        # Each branch must occupy a distinct path key — keyed branch at index 0,
        # predicate branch offset by len(keyed_routes) = 1, not restarting from 0.
        sink_paths = list(plan.terminal_sinks.keys())
        assert len(sink_paths) == 2, "one sink per branch expected"
        assert len(sink_paths) == len(set(sink_paths)), "terminal sink path keys must be unique"
        topics = {s.topic for s in plan.terminal_sinks.values()}
        assert topics == {"orders.a", "orders.b"}

    def test_rejects_expand_step_inside_router_branch(self) -> None:
        class _ExpandFakeStep(ExpandStep[Order, Result]):
            def execute(self, message: Message[Order], **kwargs: object) -> list[Message[Result]]:
                del kwargs
                return []

        flow: StreamFlow[Order, Result] = StreamFlow(
            name="test",
            source=FromTopic("in", payload=Order),
            process=Process(
                Router.when(
                    routes=[
                        Route(
                            when=msg.payload.order_id != "",
                            process=Process(
                                _ExpandFakeStep(),
                                IntoTopic("out", payload=Result),
                            ),
                        )
                    ],
                )
            ),
        )

        with pytest.raises(CompilationError, match="not supported in Router branches"):
            compile_flow(flow, runtime_config=OmegaConf.create({}))
