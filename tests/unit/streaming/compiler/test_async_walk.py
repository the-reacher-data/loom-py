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
from loom.streaming.compiler._compiler import _walk_all_process_nodes
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


class TestProcessWalker:
    def test_walk_all_process_nodes_yields_nodes_inside_router_branch(self) -> None:
        """_walk_all_process_nodes must recurse into Router sub-processes."""
        async_node: WithAsync[Order, Result] = WithAsync(
            process=Process(IntoTopic("out", payload=Result))
        )
        router: Router[Order, Result] = Router.when(
            routes=[Route(when=msg.payload.order_id != "", process=Process(async_node))]
        )

        all_nodes = list(_walk_all_process_nodes([router]))

        assert router in all_nodes
        assert async_node in all_nodes

    def test_walk_all_process_nodes_yields_nodes_inside_fork_branch(self) -> None:
        """_walk_all_process_nodes must recurse into Fork sub-processes."""
        async_node: WithAsync[Order, Result] = WithAsync(
            process=Process(IntoTopic("out", payload=Result))
        )
        fork: Fork[Order] = Fork.when(
            routes=[
                ForkRoute(
                    when=msg.payload.order_id != "",
                    process=Process(async_node),
                )
            ]
        )

        all_nodes = list(_walk_all_process_nodes([fork]))

        assert fork in all_nodes
        assert async_node in all_nodes
