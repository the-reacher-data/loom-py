"""Tests for the streaming flow compiler."""

from __future__ import annotations

from typing import Any

import pytest
from omegaconf import OmegaConf

from loom.core.model import LoomStruct
from loom.streaming import (
    CollectBatch,
    Drain,
    ExpandStep,
    Fork,
    ForkRoute,
    FromTopic,
    IntoTopic,
    Process,
    RecordStep,
    Route,
    Router,
    StreamFlow,
    StreamShape,
    WindowStrategy,
    WithAsync,
    msg,
)
from loom.streaming.compiler import CompilationError, CompiledSource, compile_flow
from loom.streaming.compiler._compiler import _uses_kafka, _walk_all_process_nodes
from loom.streaming.core._message import Message


class _Order(LoomStruct):
    order_id: str


class _Result(LoomStruct):
    value: str


class _FakeStep(RecordStep[_Order, _Result]):
    def execute(self, message: Message[_Order], **kwargs: object) -> _Result:
        return _Result(value=message.payload.order_id)


def _kafka_config() -> dict[str, Any]:
    return {
        "kafka": {
            "consumer": {"brokers": ["localhost:9092"], "group_id": "test", "topics": ["in"]},
            "producer": {"brokers": ["localhost:9092"], "topic": "out"},
        }
    }


def test_compile_success_with_minimal_flow() -> None:
    flow: StreamFlow[_Order, _Result] = StreamFlow(
        name="test",
        source=FromTopic("in", payload=_Order),
        process=Process(IntoTopic("out", payload=_Result)),
    )
    cfg = OmegaConf.create(_kafka_config())

    plan = compile_flow(flow, runtime_config=cfg)

    assert plan.name == "test"
    assert isinstance(plan.source, CompiledSource)
    assert plan.source.payload_type is _Order
    assert plan.source.shape is StreamShape.RECORD
    assert plan.source.decode_strategy == "record"
    assert plan.output is None


def test_compile_fails_when_binding_path_missing() -> None:
    binding = _FakeStep.from_config("tasks.missing")
    flow: StreamFlow[_Order, _Result] = StreamFlow(
        name="test",
        source=FromTopic("in", payload=_Order),
        process=Process(binding, IntoTopic("out", payload=_Result)),
    )
    cfg = OmegaConf.create({})

    with pytest.raises(CompilationError) as exc_info:
        compile_flow(flow, runtime_config=cfg)

    assert "tasks.missing" in str(exc_info.value)


def test_compile_fails_when_kafka_missing_for_topic_flow() -> None:
    flow: StreamFlow[_Order, _Result] = StreamFlow(
        name="test",
        source=FromTopic("in", payload=_Order),
        process=Process(_FakeStep(), IntoTopic("out", payload=_Result)),
    )
    cfg = OmegaConf.create({})  # no kafka section

    with pytest.raises(CompilationError) as exc_info:
        compile_flow(flow, runtime_config=cfg)

    assert "kafka" in str(exc_info.value)


def test_compile_succeeds_when_kafka_present() -> None:
    flow: StreamFlow[_Order, _Result] = StreamFlow(
        name="test",
        source=FromTopic("in", payload=_Order),
        process=Process(_FakeStep(), IntoTopic("out", payload=_Result)),
    )
    cfg = OmegaConf.create(_kafka_config())

    plan = compile_flow(flow, runtime_config=cfg)

    assert plan.name == "test"


def test_compile_fails_on_shape_mismatch() -> None:
    """Step expects RECORD but source is BATCH without CollectBatch."""
    flow: StreamFlow[_Order, _Result] = StreamFlow(
        name="test",
        source=FromTopic("in", payload=_Order, shape=StreamShape.BATCH),
        process=Process(_FakeStep(), IntoTopic("out", payload=_Result)),
    )
    cfg = OmegaConf.create(_kafka_config())

    with pytest.raises(CompilationError) as exc_info:
        compile_flow(flow, runtime_config=cfg)

    assert "shape mismatch" in str(exc_info.value)


def test_compile_fails_without_output() -> None:
    flow: StreamFlow[_Order, _Result] = StreamFlow(
        name="test",
        source=FromTopic("in", payload=_Order),
        process=Process(_FakeStep()),
        output=None,
    )
    cfg = OmegaConf.create(
        {
            "kafka": {
                "consumer": {"brokers": ["localhost:9092"], "group_id": "test", "topics": ["in"]},
            }
        }
    )

    with pytest.raises(CompilationError) as exc_info:
        compile_flow(flow, runtime_config=cfg)

    assert "no terminal output" in str(exc_info.value)


def test_compile_drain_outputs_none_shape() -> None:
    flow: StreamFlow[_Order, _Result] = StreamFlow(
        name="test",
        source=FromTopic("in", payload=_Order),
        process=Process(Drain()),
    )
    cfg = OmegaConf.create(_kafka_config())

    plan = compile_flow(flow, runtime_config=cfg)

    assert plan.nodes[-1].output_shape is StreamShape.NONE
    assert plan.output is None


class TestWindowStrategyValidation:
    """Compiler must reject unimplemented WindowStrategy values at compile time."""

    def test_collect_strategy_compiles_successfully(self) -> None:
        flow: StreamFlow[_Order, _Result] = StreamFlow(
            name="test",
            source=FromTopic("in", payload=_Order),
            process=Process(
                CollectBatch(max_records=10, timeout_ms=500),
                Drain(),
            ),
        )
        cfg = OmegaConf.create(_kafka_config())

        plan = compile_flow(flow, runtime_config=cfg)

        batch_node = plan.nodes[0].node
        assert isinstance(batch_node, CollectBatch)
        assert batch_node.window is WindowStrategy.COLLECT

    def test_tumbling_strategy_raises_compilation_error(self) -> None:
        flow: StreamFlow[_Order, _Result] = StreamFlow(
            name="test",
            source=FromTopic("in", payload=_Order),
            process=Process(
                CollectBatch(
                    max_records=10,
                    timeout_ms=500,
                    window=WindowStrategy.TUMBLING,
                ),
                Drain(),
            ),
        )
        cfg = OmegaConf.create(_kafka_config())

        with pytest.raises(CompilationError) as exc_info:
            compile_flow(flow, runtime_config=cfg)

        assert "tumbling" in str(exc_info.value)

    def test_session_strategy_raises_compilation_error(self) -> None:
        flow: StreamFlow[_Order, _Result] = StreamFlow(
            name="test",
            source=FromTopic("in", payload=_Order),
            process=Process(
                CollectBatch(
                    max_records=10,
                    timeout_ms=500,
                    window=WindowStrategy.SESSION,
                ),
                Drain(),
            ),
        )
        cfg = OmegaConf.create(_kafka_config())

        with pytest.raises(CompilationError) as exc_info:
            compile_flow(flow, runtime_config=cfg)

        assert "session" in str(exc_info.value)


def test_compile_validates_router_branch_shapes() -> None:
    flow: StreamFlow[_Order, _Result] = StreamFlow(
        name="test",
        source=FromTopic("in", payload=_Order),
        process=Process(
            Router.when(
                (
                    Route(
                        when=msg.payload.order_id == "batch",
                        process=Process(CollectBatch(max_records=10, timeout_ms=1000)),
                    ),
                ),
                default=Process(_FakeStep()),
            ),
            IntoTopic("out", payload=_Result),
        ),
    )
    cfg = OmegaConf.create(_kafka_config())

    with pytest.raises(CompilationError) as exc_info:
        compile_flow(flow, runtime_config=cfg)

    assert "router branches produce different shapes" in str(exc_info.value)


def test_compile_accepts_router_with_terminal_branch_output() -> None:
    flow: StreamFlow[_Order, _Result] = StreamFlow(
        name="test",
        source=FromTopic("in", payload=_Order),
        process=Process(
            Router.by(
                msg.payload.order_id,
                routes={"vip": Process(_FakeStep(), IntoTopic("out", payload=_Result))},
            )
        ),
        output=None,
    )
    cfg = OmegaConf.create(_kafka_config())

    plan = compile_flow(flow, runtime_config=cfg)

    assert plan.nodes[0].output_shape is StreamShape.RECORD
    assert plan.output is None


def test_uses_kafka_detects_kafka_usage() -> None:
    flow_with_output: StreamFlow[_Order, _Result] = StreamFlow(
        name="test",
        source=FromTopic("in", payload=_Order),
        process=Process(IntoTopic("out", payload=_Result)),
    )
    flow_without_output: StreamFlow[_Order, _Result] = StreamFlow(
        name="test",
        source=FromTopic("in", payload=_Order),
        process=Process(_FakeStep()),
        output=None,
    )
    # Any flow with FromTopic source uses Kafka
    assert _uses_kafka(flow_with_output) is True
    assert _uses_kafka(flow_without_output) is True


def test_compile_fails_on_batch_scope_with_direct_context_manager() -> None:
    """BATCH scope with a direct CM instance must be rejected at compile time."""
    from loom.streaming import ResourceScope, With

    class _FakeSyncCM:
        def __enter__(self) -> _FakeSyncCM:
            return self

        def __exit__(self, *args: object) -> None:
            return None

    flow: StreamFlow[_Order, _Result] = StreamFlow(
        name="test",
        source=FromTopic("in", payload=_Order),
        process=Process(With(step=_FakeStep(), scope=ResourceScope.BATCH, db=_FakeSyncCM())),
        output=IntoTopic("out", payload=_Result),
    )

    with pytest.raises(CompilationError, match="ContextFactory"):
        compile_flow(flow, runtime_config=OmegaConf.create(_kafka_config()))


def test_compile_succeeds_on_batch_scope_with_context_factory() -> None:
    """BATCH scope with a ContextFactory is valid."""
    from loom.streaming import ContextFactory, ResourceScope, With

    class _FakeSyncCM:
        def __enter__(self) -> _FakeSyncCM:
            return self

        def __exit__(self, *args: object) -> None:
            return None

    flow: StreamFlow[_Order, _Result] = StreamFlow(
        name="test",
        source=FromTopic("in", payload=_Order),
        process=Process(
            With(
                step=_FakeStep(),
                scope=ResourceScope.BATCH,
                db=ContextFactory(lambda: _FakeSyncCM()),
            )
        ),
        output=IntoTopic("out", payload=_Result),
    )

    plan = compile_flow(flow, runtime_config=OmegaConf.create(_kafka_config()))
    assert plan.name == "test"


# ---------------------------------------------------------------------------
# WithAsync inside Router — async bridge detection must traverse branches
# ---------------------------------------------------------------------------


class _AsyncStep(RecordStep[_Order, _Result]):
    async def execute(self, message: Message[_Order], **kwargs: object) -> _Result:  # type: ignore[override]
        return _Result(value=message.payload.order_id)


def _flow_with_async_inside_router() -> StreamFlow[_Order, _Result]:
    return StreamFlow(
        name="test_async_in_router",
        source=FromTopic("in", payload=_Order),
        process=Process(
            Router.when(
                routes=[
                    Route(
                        when=msg.payload.order_id != "",
                        process=Process(
                            WithAsync(step=_AsyncStep()),
                            IntoTopic("out", payload=_Result),
                        ),
                    )
                ],
            )
        ),
    )


def test_needs_async_bridge_is_true_when_with_async_is_inside_router() -> None:
    """Regression: compiler must detect WithAsync nested inside Router branches."""
    flow = _flow_with_async_inside_router()
    plan = compile_flow(flow, runtime_config=OmegaConf.create(_kafka_config()))

    assert plan.needs_async_bridge is True


def test_needs_async_bridge_is_false_when_no_with_async_present() -> None:
    flow: StreamFlow[_Order, _Result] = StreamFlow(
        name="test",
        source=FromTopic("in", payload=_Order),
        process=Process(IntoTopic("out", payload=_Result)),
    )
    plan = compile_flow(flow, runtime_config=OmegaConf.create(_kafka_config()))

    assert plan.needs_async_bridge is False


def test_walk_all_process_nodes_yields_nodes_inside_router_branch() -> None:
    """_walk_all_process_nodes must recurse into Router sub-processes."""
    async_node = WithAsync(step=_AsyncStep())
    router = Router.when(
        routes=[Route(when=msg.payload.order_id != "", process=Process(async_node))]
    )

    all_nodes = list(_walk_all_process_nodes([router]))

    assert router in all_nodes
    assert async_node in all_nodes


def test_walk_all_process_nodes_yields_nodes_inside_fork_branch() -> None:
    """_walk_all_process_nodes must recurse into Fork sub-processes."""
    async_node = WithAsync(step=_AsyncStep())
    fork = Fork.when(
        routes=[
            ForkRoute(
                when=msg.payload.order_id != "",
                process=Process(async_node, IntoTopic("out", payload=_Result)),
            )
        ]
    )

    all_nodes = list(_walk_all_process_nodes([fork]))

    assert fork in all_nodes
    assert async_node in all_nodes


# ---------------------------------------------------------------------------
# Router branch — unsupported node types rejected at compile time
# ---------------------------------------------------------------------------


class _ExpandFakeStep(ExpandStep[_Order, _Result]):
    def execute(  # type: ignore[override]
        self, message: Message[_Order], **kwargs: object
    ) -> list[Message[_Result]]:
        return []


def test_compiler_rejects_expand_step_inside_router_branch() -> None:
    """ExpandStep in a Router branch must fail compilation — Router is 1-to-1."""
    flow: StreamFlow[_Order, _Result] = StreamFlow(
        name="test",
        source=FromTopic("in", payload=_Order),
        process=Process(
            Router.when(
                routes=[
                    Route(
                        when=msg.payload.order_id != "",
                        process=Process(
                            _ExpandFakeStep(),
                            IntoTopic("out", payload=_Result),
                        ),
                    )
                ],
            )
        ),
    )

    with pytest.raises(CompilationError, match="not supported in Router branches"):
        compile_flow(flow, runtime_config=OmegaConf.create(_kafka_config()))
