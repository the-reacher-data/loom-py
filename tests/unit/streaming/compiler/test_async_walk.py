"""Async bridge and process walker compiler validation."""

from __future__ import annotations

from omegaconf import DictConfig

from loom.streaming import (
    Fork,
    ForkRoute,
    FromTopic,
    IntoTopic,
    Process,
    Route,
    Router,
    StreamFlow,
    WithAsync,
    msg,
)
from loom.streaming.compiler import compile_flow
from tests.unit.streaming.compiler.cases import Order, Result


def _flow_with_async_inside_router() -> StreamFlow[Order, Result]:
    return StreamFlow(
        name="test_async_in_router",
        source=FromTopic("in", payload=Order),
        process=Process(
            Router.when(
                routes=[
                    Route(
                        when=msg.payload.order_id != "",
                        process=Process(
                            WithAsync(
                                process=Process(IntoTopic("out", payload=Result)),
                            ),
                        ),
                    )
                ],
            )
        ),
    )


class TestAsyncBridgeDetection:
    def test_needs_async_bridge_is_true_when_with_async_is_inside_router(
        self,
        streaming_kafka_config: DictConfig,
    ) -> None:
        flow = _flow_with_async_inside_router()
        plan = compile_flow(flow, runtime_config=streaming_kafka_config)

        assert plan.needs_async_bridge is True

    def test_needs_async_bridge_is_false_when_no_with_async_present(
        self,
        streaming_kafka_config: DictConfig,
    ) -> None:
        flow: StreamFlow[Order, Result] = StreamFlow(
            name="test",
            source=FromTopic("in", payload=Order),
            process=Process(IntoTopic("out", payload=Result)),
        )
        plan = compile_flow(flow, runtime_config=streaming_kafka_config)

        assert plan.needs_async_bridge is False

    def test_needs_async_bridge_is_true_when_with_async_is_inside_fork(
        self,
        streaming_kafka_config: DictConfig,
    ) -> None:
        flow: StreamFlow[Order, Result] = StreamFlow(
            name="test_async_in_fork",
            source=FromTopic("in", payload=Order),
            process=Process(
                Fork.when(
                    routes=[
                        ForkRoute(
                            when=msg.payload.order_id != "",
                            process=Process(
                                WithAsync(
                                    process=Process(IntoTopic("out", payload=Result)),
                                ),
                            ),
                        )
                    ],
                )
            ),
        )
        plan = compile_flow(flow, runtime_config=streaming_kafka_config)

        assert plan.needs_async_bridge is True
