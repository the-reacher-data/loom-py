"""Bytewax runtime adapter.

Translates a :class:`CompiledPlan` into a Bytewax :class:`Dataflow`,
wiring decode, task execution, encode, and error routing operators.

Requires ``bytewax`` to be installed.
"""

from __future__ import annotations

import inspect
import logging
import time
from collections.abc import Awaitable, Callable, Iterator, Mapping
from contextlib import contextmanager
from datetime import timedelta
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, TypeAlias, cast

from bytewax.dataflow import Dataflow
from bytewax.inputs import Source
from bytewax.operators import collect, flat_map, key_on, key_rm
from bytewax.operators import input as bw_input
from bytewax.operators import map as bw_map
from bytewax.operators import output as bw_output
from bytewax.outputs import Sink

from loom.streaming.bytewax._operators import ResourceLifecycle, lifecycle_for
from loom.streaming.compiler import CompiledNode, CompiledPlan
from loom.streaming.core._message import Message
from loom.streaming.core._typing import StreamPayload
from loom.streaming.nodes._boundary import IntoTopic
from loom.streaming.nodes._router import Router, evaluate_predicate, select_value
from loom.streaming.nodes._shape import CollectBatch, Drain, ForEach
from loom.streaming.nodes._task import BatchTask, Task
from loom.streaming.nodes._with import OneEmit, With, WithAsync
from loom.streaming.observability.observers.protocol import StreamingFlowObserver

if TYPE_CHECKING:
    from loom.core.async_bridge import AsyncBridge

logger = logging.getLogger(__name__)
Stream: TypeAlias = Any
NodeHandler: TypeAlias = Callable[[Stream, object, int, "_BuildContext"], Stream]


def build_dataflow(
    plan: CompiledPlan,
    *,
    flow_observer: StreamingFlowObserver | None = None,
    source: Source[Any] | None = None,
    sink: Sink[Any] | None = None,
) -> Dataflow:
    """Build a Bytewax Dataflow from a compiled plan.

    Args:
        plan: Immutable compiled plan produced by the streaming compiler.
        flow_observer: Optional observer for flow execution lifecycle events.
        source: Optional Bytewax source. Tests can pass ``TestingSource`` here.
        sink: Optional Bytewax sink. Tests can pass ``TestingSink`` here.

    Returns:
        Configured Bytewax Dataflow ready for execution.
    """
    bridge = _maybe_create_bridge(plan)
    ctx = _BuildContext(
        plan=plan,
        bridge=bridge,
        flow_observer=flow_observer,
        source=source,
        sink=sink,
    )
    return _assemble_dataflow(plan, ctx)


def _assemble_dataflow(plan: CompiledPlan, ctx: _BuildContext) -> Dataflow:
    """Assemble a Bytewax Dataflow from a pre-built context."""
    flow = Dataflow(plan.name)
    stream = _wire_source(flow, ctx)

    for idx, node in enumerate(plan.nodes):
        stream = _wire_node(stream, node, idx, ctx)

    _wire_output(stream, ctx)
    return flow


# ---------------------------------------------------------------------------
# Build context - carries resolved state through the wiring phase
# ---------------------------------------------------------------------------


class _BuildContext:
    """Wiring-phase state shared across operator builders."""

    __slots__ = (
        "plan",
        "bridge",
        "flow_observer",
        "source",
        "sink",
        "terminal_output_wired",
        "_managers",
    )

    def __init__(
        self,
        plan: CompiledPlan,
        bridge: AsyncBridge | None,
        flow_observer: StreamingFlowObserver | None = None,
        source: Source[Any] | None = None,
        sink: Sink[Any] | None = None,
    ) -> None:
        self.plan = plan
        self.bridge = bridge
        self.flow_observer = flow_observer
        self.source = source
        self.sink = sink
        self.terminal_output_wired = False
        self._managers: dict[int, ResourceLifecycle] = {}

    def manager_for(
        self,
        idx: int,
        node: With[StreamPayload, StreamPayload] | WithAsync[StreamPayload, StreamPayload],
    ) -> ResourceLifecycle:
        """Get or create a resource manager for *node* at position *idx*."""
        if idx not in self._managers:
            self._managers[idx] = lifecycle_for(node, bridge=self.bridge)
        return self._managers[idx]

    def shutdown_all(self) -> None:
        """Shutdown all resource managers."""
        for manager in self._managers.values():
            manager.shutdown()


# ---------------------------------------------------------------------------
# Source wiring
# ---------------------------------------------------------------------------


def _wire_source(flow: Dataflow, ctx: _BuildContext) -> Stream:
    """Wire the Kafka source and decode operator."""
    source = ctx.source if ctx.source is not None else _empty_testing_source()

    stream: Stream = bw_input("source", flow, source)

    strategy = ctx.plan.source.decode_strategy
    step_id = f"decode_{strategy}"
    return bw_map(step_id, stream, _placeholder_decode)


# ---------------------------------------------------------------------------
# Node dispatch — frozen registry
# ---------------------------------------------------------------------------


def _wire_node(stream: Stream, node: CompiledNode, idx: int, ctx: _BuildContext) -> Stream:
    """Dispatch one compiled node to its handler."""
    raw = node.node
    for handler_type, handler in _NODE_HANDLERS.items():
        if isinstance(raw, handler_type):
            return handler(stream, raw, idx, ctx)
    raise TypeError(f"No adapter handler for {type(raw).__name__}")


# ---------------------------------------------------------------------------
# Node handlers — each wires one DSL node type into the Bytewax stream
# ---------------------------------------------------------------------------


def _resolve_node_name(raw: object) -> str:
    """Resolve a human-readable name for a DSL node."""
    task_name = getattr(raw, "task_name", None)
    if callable(task_name):
        return cast(str, task_name())
    return type(raw).__name__


@contextmanager
def _observe_node(
    observer: StreamingFlowObserver | None,
    flow_name: str,
    idx: int,
    node_type: str,
) -> Iterator[None]:
    """Emit observability events around one node execution.

    Calls ``on_node_start``, then ``on_node_end`` on success or
    ``on_node_error`` on exception.
    """
    if observer is not None:
        observer.on_node_start(flow_name, idx, node_type=node_type)
    t0 = time.monotonic()
    success = False
    try:
        yield
        success = True
    except Exception as exc:
        if observer is not None:
            observer.on_node_error(flow_name, idx, node_type=node_type, exc=exc)
        raise
    finally:
        if observer is not None and success:
            elapsed = int((time.monotonic() - t0) * 1000)
            observer.on_node_end(
                flow_name, idx, node_type=node_type, status="success", duration_ms=elapsed
            )


def _apply_task(stream: Stream, raw: object, idx: int, ctx: _BuildContext) -> Stream:
    task = cast(Task[StreamPayload, StreamPayload], raw)
    name = _resolve_node_name(task)
    observer = ctx.flow_observer
    flow_name = ctx.plan.name

    def step(msg: Any) -> Any:
        message = _require_message(msg)
        with _observe_node(observer, flow_name, idx, name):
            result = task.execute(message)
            return _replace_payload(message, result)

    return bw_map(f"task_{idx}_{name}", stream, step)


def _apply_batch_task(stream: Stream, raw: object, idx: int, ctx: _BuildContext) -> Stream:
    task = cast(BatchTask[StreamPayload, StreamPayload], raw)
    name = _resolve_node_name(task)
    observer = ctx.flow_observer
    flow_name = ctx.plan.name

    def step(batch: list[Any]) -> list[Any]:
        messages = [_require_message(item) for item in batch]
        with _observe_node(observer, flow_name, idx, name):
            result = task.execute(messages)
            return _replace_payloads(messages, result)

    mapped = bw_map(f"batch_{idx}_{name}", stream, step)
    return flat_map(f"flatten_{idx}", mapped, _identity)


def _apply_with(stream: Stream, raw: object, idx: int, ctx: _BuildContext) -> Stream:
    node = cast(With[StreamPayload, StreamPayload], raw)
    manager = ctx.manager_for(idx, node)
    worker_resources = manager.open_worker()
    observer = ctx.flow_observer
    flow_name = ctx.plan.name
    node_type = type(node).__name__

    def step(batch: list[Any]) -> list[Any]:
        messages = [_require_message(item) for item in batch]
        with _observe_node(observer, flow_name, idx, node_type):
            batch_resources = manager.open_batch()
            deps = {**worker_resources, **batch_resources}
            try:
                result = [node.task.execute(msg, **deps) for msg in messages]
                return _replace_payloads(messages, result)
            finally:
                manager.close_batch()

    return bw_map(f"with_{idx}", stream, step)


def _apply_with_async(stream: Stream, raw: object, idx: int, ctx: _BuildContext) -> Stream:
    import asyncio

    node = cast(WithAsync[StreamPayload, StreamPayload], raw)
    if ctx.bridge is None:
        raise RuntimeError("WithAsync requires an AsyncBridge but none was created.")

    manager = ctx.manager_for(idx, node)
    worker_resources = manager.open_worker()
    bridge = ctx.bridge
    observer = ctx.flow_observer
    flow_name = ctx.plan.name
    node_type = type(node).__name__

    async def _execute_batch(batch: list[Any]) -> list[Any]:
        messages = [_require_message(item) for item in batch]
        batch_resources = manager.open_batch()
        deps = {**worker_resources, **batch_resources}
        sem = asyncio.Semaphore(node.max_concurrency)

        async def _bounded(msg: Message[StreamPayload]) -> Any:
            async with sem:
                result = node.task.execute(msg, **deps)
                if not inspect.isawaitable(result):
                    raise TypeError("WithAsync task.execute must return an awaitable.")
                return await cast(Awaitable[object], result)

        try:
            result = await asyncio.gather(*[_bounded(msg) for msg in messages])
            return _replace_payloads(messages, result)
        finally:
            manager.close_batch()

    def step(batch: list[Any]) -> list[Any]:
        with _observe_node(observer, flow_name, idx, node_type):
            return bridge.run(_execute_batch(batch))

    return bw_map(f"with_async_{idx}", stream, step)


def _apply_collect_batch(stream: Stream, raw: object, idx: int, ctx: _BuildContext) -> Stream:
    node = cast(CollectBatch, raw)
    keyed = key_on(f"collect_key_{idx}", stream, _batch_key)
    collected = collect(
        f"collect_{idx}",
        keyed,
        timeout=timedelta(milliseconds=node.timeout_ms),
        max_size=node.max_records,
    )
    return key_rm(f"collect_unkey_{idx}", collected)


def _apply_for_each(stream: Stream, raw: object, idx: int, ctx: _BuildContext) -> Stream:
    return flat_map(f"foreach_{idx}", stream, _identity)


def _apply_router(stream: Stream, raw: object, idx: int, ctx: _BuildContext) -> Stream:
    router = cast(Router[StreamPayload, StreamPayload], raw)

    def step(msg: Any) -> Any:
        return _execute_router(router, _require_message(msg))

    return bw_map(f"router_{idx}", stream, step)


def _apply_drain(stream: Stream, raw: object, idx: int, ctx: _BuildContext) -> Stream:
    return flat_map(f"drain_{idx}", stream, _empty)


def _apply_into_topic(stream: Stream, raw: object, idx: int, ctx: _BuildContext) -> Stream:
    _wire_terminal_sink(f"into_topic_{idx}", stream, ctx)
    return stream


def _apply_one_emit(stream: Stream, raw: object, idx: int, ctx: _BuildContext) -> Stream:
    node = cast(OneEmit[StreamPayload, StreamPayload], raw)
    source = node.source

    if isinstance(source, WithAsync):
        inner_stream = _apply_with_async(stream, source, idx, ctx)
    elif isinstance(source, With):
        inner_stream = _apply_with(stream, source, idx, ctx)
    else:
        raise TypeError(f"OneEmit.source must be With or WithAsync, got {type(source).__name__}")

    return flat_map(f"one_emit_flatten_{idx}", inner_stream, _identity)


# ---------------------------------------------------------------------------
# Frozen handler registry — immutable after module load
# ---------------------------------------------------------------------------

_NODE_HANDLERS: Mapping[type[object], NodeHandler] = MappingProxyType(
    {
        Task: _apply_task,
        BatchTask: _apply_batch_task,
        With: _apply_with,
        WithAsync: _apply_with_async,
        CollectBatch: _apply_collect_batch,
        ForEach: _apply_for_each,
        Router: _apply_router,
        Drain: _apply_drain,
        IntoTopic: _apply_into_topic,
        OneEmit: _apply_one_emit,
    }
)


# ---------------------------------------------------------------------------
# Output wiring
# ---------------------------------------------------------------------------


def _wire_output(stream: Any, ctx: _BuildContext) -> None:
    """Wire the output sink and error routes."""
    if ctx.terminal_output_wired:
        return

    if ctx.sink is not None:
        _wire_terminal_sink("output", stream, ctx)
        return

    if ctx.plan.output is not None:
        bw_map("encode_output", stream, _placeholder_encode)
        # TODO: wire kop.output with resolved ProducerSettings + topic

    for kind, sink in ctx.plan.error_routes.items():
        logger.info(
            "error_route_registered",
            extra={"error_kind": kind.value, "topic": sink.topic},
        )
        # TODO: wire error branch → kop.output per ErrorKind


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _maybe_create_bridge(plan: CompiledPlan) -> AsyncBridge | None:
    """Create an AsyncBridge if the plan requires async task execution."""
    if not plan.needs_async_bridge:
        return None
    from loom.core.async_bridge import AsyncBridge

    return AsyncBridge(thread_name=f"loom-{plan.name}-async")


def _identity(items: Any) -> Any:
    """Pass-through for flat_map."""
    return items


def _empty(_item: Any) -> tuple[()]:
    """Drop one item from a stream."""
    return ()


def _batch_key(_item: Any) -> str:
    """Group test batches into one logical Bytewax key."""
    return "loom"


def _execute_router(
    router: Router[StreamPayload, StreamPayload],
    message: Message[StreamPayload],
) -> Message[StreamPayload]:
    """Execute a Router branch inline for record-shaped Bytewax streams."""
    branch_nodes = _select_router_branch(router, message)
    result = message
    for node in branch_nodes:
        result = _execute_router_node(node, result)
    return result


def _select_router_branch(
    router: Router[StreamPayload, StreamPayload],
    message: Message[StreamPayload],
) -> tuple[object, ...]:
    if router.selector is not None:
        key = select_value(router.selector, message)
        selected = router.routes.get(key)
        if selected is not None:
            return selected.nodes

    for route in router.predicate_routes:
        if evaluate_predicate(route.when, message):
            return route.process.nodes

    if router.default is not None:
        return router.default.nodes
    return ()


def _execute_router_node(node: object, message: Message[StreamPayload]) -> Message[StreamPayload]:
    if isinstance(node, Task):
        task = cast(Task[StreamPayload, StreamPayload], node)
        return _replace_payload(message, task.execute(message))
    if isinstance(node, (IntoTopic, Drain)):
        return message
    raise TypeError(
        f"Router branch node {type(node).__name__} is not supported by Bytewax adapter."
    )


def _empty_testing_source() -> Source[Any]:
    """Return an inert Bytewax testing source until Kafka input is wired."""
    from bytewax.testing import TestingSource

    return TestingSource([])


def _wire_terminal_sink(step_id: str, stream: Stream, ctx: _BuildContext) -> None:
    """Wire the configured test/runtime sink once."""
    if ctx.sink is not None:
        bw_output(step_id, stream, ctx.sink)
    else:
        bw_map(f"{step_id}_encode", stream, _placeholder_encode)
    ctx.terminal_output_wired = True


def _require_message(value: Any) -> Message[StreamPayload]:
    """Validate that the runtime stream carries Loom messages."""
    if not isinstance(value, Message):
        raise TypeError(f"Expected Message, got {type(value).__name__}.")
    return cast(Message[StreamPayload], value)


def _replace_payload(message: Message[StreamPayload], payload: Any) -> Message[StreamPayload]:
    """Preserve metadata while replacing the logical payload."""
    return Message(payload=cast(StreamPayload, payload), meta=message.meta)


def _replace_payloads(
    messages: list[Message[StreamPayload]],
    payloads: list[Any],
) -> list[Message[StreamPayload]]:
    """Preserve per-record metadata for batch task outputs."""
    if len(messages) != len(payloads):
        raise RuntimeError("Batch task output length must match input length.")
    return [
        _replace_payload(message, payload)
        for message, payload in zip(messages, payloads, strict=True)
    ]


def _placeholder_decode(payload: Any) -> Any:
    """Placeholder decoder - replaced when Kafka connector is wired."""
    return payload


def _placeholder_encode(record: Any) -> Any:
    """Placeholder encoder - replaced when Kafka connector is wired."""
    return record
