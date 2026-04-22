"""Bytewax runtime adapter.

Translates a :class:`CompiledPlan` into a Bytewax :class:`Dataflow`,
wiring decode, task execution, encode, and error routing operators.

Requires ``bytewax`` to be installed.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import timedelta
from typing import TYPE_CHECKING, Any, TypeAlias, cast

from bytewax.dataflow import Dataflow
from bytewax.operators import collect, flat_map
from bytewax.operators import input as bw_input
from bytewax.operators import map as bw_map

from loom.streaming._shape import CollectBatch, ForEach
from loom.streaming._task import BatchTask, Task
from loom.streaming._with import With, WithAsync
from loom.streaming.bytewax._operators import ResourceLifecycle, lifecycle_for
from loom.streaming.compiler import CompiledNode, CompiledPlan

if TYPE_CHECKING:
    from loom.core.async_bridge import AsyncBridge

logger = logging.getLogger(__name__)
Stream: TypeAlias = Any
NodeHandler: TypeAlias = Callable[[Stream, object, int, "_BuildContext"], Stream]


def build_dataflow(plan: CompiledPlan) -> Dataflow:
    """Build a Bytewax Dataflow from a compiled plan.

    Args:
        plan: Immutable compiled plan produced by the streaming compiler.

    Returns:
        Configured Bytewax Dataflow ready for execution.
    """
    bridge = _maybe_create_bridge(plan)
    ctx = _BuildContext(plan=plan, bridge=bridge)

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

    __slots__ = ("plan", "bridge", "_managers")

    def __init__(
        self,
        plan: CompiledPlan,
        bridge: AsyncBridge | None,
    ) -> None:
        self.plan = plan
        self.bridge = bridge
        self._managers: dict[int, ResourceLifecycle] = {}

    def manager_for(
        self,
        idx: int,
        node: With[Any, Any] | WithAsync[Any, Any],
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
    # TODO: replace with kop.input when Kafka connector is integrated
    from bytewax.testing import TestingSource

    stream: Stream = bw_input("source", flow, TestingSource([]))

    strategy = ctx.plan.source.decode_strategy
    step_id = f"decode_{strategy}"
    return bw_map(step_id, stream, _placeholder_decode)


# ---------------------------------------------------------------------------
# Node dispatch
# ---------------------------------------------------------------------------

# Each handler receives (stream, CompiledNode, index, BuildContext) and
# returns the transformed stream.  The index ensures unique step IDs.

_NODE_HANDLERS: dict[type[object], NodeHandler] = {}


def _handler(node_type: type[object]) -> Callable[[NodeHandler], NodeHandler]:
    """Decorator to register a node handler."""

    def decorator(fn: NodeHandler) -> NodeHandler:
        _NODE_HANDLERS[node_type] = fn
        return fn

    return decorator


def _wire_node(stream: Stream, node: CompiledNode, idx: int, ctx: _BuildContext) -> Stream:
    """Dispatch one compiled node to its handler."""
    raw = node.node
    handler = _NODE_HANDLERS.get(type(raw))
    if handler is not None:
        return handler(stream, raw, idx, ctx)
    raise TypeError(f"No adapter handler for {type(raw).__name__}")


@_handler(Task)
def _apply_task(stream: Stream, raw: object, idx: int, ctx: _BuildContext) -> Stream:
    task = cast(Task[Any, Any], raw)
    name = task.task_name() if hasattr(task, "task_name") else type(task).__name__

    def step(msg: Any) -> Any:
        return task.execute(msg)

    return bw_map(f"task_{idx}_{name}", stream, step)


@_handler(BatchTask)
def _apply_batch_task(stream: Stream, raw: object, idx: int, ctx: _BuildContext) -> Stream:
    task = cast(BatchTask[Any, Any], raw)
    name = task.task_name() if hasattr(task, "task_name") else type(task).__name__

    def step(batch: list[Any]) -> list[Any]:
        return task.execute(batch)

    mapped = bw_map(f"batch_{idx}_{name}", stream, step)
    return flat_map(f"flatten_{idx}", mapped, _identity)


@_handler(With)
def _apply_with(stream: Stream, raw: object, idx: int, ctx: _BuildContext) -> Stream:
    node = cast(With[Any, Any], raw)
    manager = ctx.manager_for(idx, node)
    worker_resources = manager.open_worker()

    def step(batch: list[Any]) -> list[Any]:
        batch_resources = manager.open_batch()
        deps = {**worker_resources, **batch_resources}
        try:
            return [node.task.execute(msg, **deps) for msg in batch]
        finally:
            manager.close_batch()

    return bw_map(f"with_{idx}", stream, step)


@_handler(WithAsync)
def _apply_with_async(stream: Stream, raw: object, idx: int, ctx: _BuildContext) -> Stream:
    import asyncio

    node = cast(WithAsync[Any, Any], raw)
    if ctx.bridge is None:
        raise RuntimeError("WithAsync requires an AsyncBridge but none was created.")

    manager = ctx.manager_for(idx, node)
    worker_resources = manager.open_worker()
    bridge = ctx.bridge

    async def _execute_batch(batch: list[Any]) -> list[Any]:
        batch_resources = manager.open_batch()
        deps = {**worker_resources, **batch_resources}
        sem = asyncio.Semaphore(node.max_concurrency)

        async def _bounded(msg: Any) -> Any:
            async with sem:
                return await node.task.execute(msg, **deps)

        try:
            return await asyncio.gather(*[_bounded(msg) for msg in batch])
        finally:
            manager.close_batch()

    def step(batch: list[Any]) -> list[Any]:
        return bridge.run(_execute_batch(batch))

    return bw_map(f"with_async_{idx}", stream, step)


@_handler(CollectBatch)
def _apply_collect_batch(stream: Stream, raw: object, idx: int, ctx: _BuildContext) -> Stream:
    node = cast(CollectBatch, raw)
    return collect(
        f"collect_{idx}",
        stream,
        timeout=timedelta(milliseconds=node.timeout_ms),
        max_size=node.max_records,
    )


@_handler(ForEach)
def _apply_for_each(stream: Stream, raw: object, idx: int, ctx: _BuildContext) -> Stream:
    return flat_map(f"foreach_{idx}", stream, _identity)


# ---------------------------------------------------------------------------
# Output wiring
# ---------------------------------------------------------------------------


def _wire_output(stream: Any, ctx: _BuildContext) -> None:
    """Wire the output sink and error routes."""
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


def _placeholder_decode(payload: Any) -> Any:
    """Placeholder decoder - replaced when Kafka connector is wired."""
    return payload


def _placeholder_encode(record: Any) -> Any:
    """Placeholder encoder - replaced when Kafka connector is wired."""
    return record
