"""Bytewax runtime adapter.

Translates a :class:`CompiledPlan` into a Bytewax :class:`Dataflow`,
wiring decode, task execution, encode, and error routing operators.

Requires ``bytewax`` to be installed.
"""

from __future__ import annotations

import inspect
import logging
import time
from collections.abc import Awaitable, Callable, Mapping
from datetime import timedelta
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, TypeAlias, cast

from bytewax.dataflow import Dataflow
from bytewax.operators import collect, flat_map
from bytewax.operators import input as bw_input
from bytewax.operators import map as bw_map

from loom.streaming.bytewax._operators import ResourceLifecycle, lifecycle_for
from loom.streaming.compiler import CompiledNode, CompiledPlan
from loom.streaming.core._typing import StreamPayload
from loom.streaming.nodes._shape import CollectBatch, ForEach
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
) -> Dataflow:
    """Build a Bytewax Dataflow from a compiled plan.

    Args:
        plan: Immutable compiled plan produced by the streaming compiler.
        flow_observer: Optional observer for flow execution lifecycle events.

    Returns:
        Configured Bytewax Dataflow ready for execution.
    """
    bridge = _maybe_create_bridge(plan)
    ctx = _BuildContext(plan=plan, bridge=bridge, flow_observer=flow_observer)

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

    __slots__ = ("plan", "bridge", "flow_observer", "_managers")

    def __init__(
        self,
        plan: CompiledPlan,
        bridge: AsyncBridge | None,
        flow_observer: StreamingFlowObserver | None = None,
    ) -> None:
        self.plan = plan
        self.bridge = bridge
        self.flow_observer = flow_observer
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
    # TODO: replace with kop.input when Kafka connector is integrated
    from bytewax.testing import TestingSource

    stream: Stream = bw_input("source", flow, TestingSource([]))

    strategy = ctx.plan.source.decode_strategy
    step_id = f"decode_{strategy}"
    return bw_map(step_id, stream, _placeholder_decode)


# ---------------------------------------------------------------------------
# Node dispatch — frozen registry
# ---------------------------------------------------------------------------


def _wire_node(stream: Stream, node: CompiledNode, idx: int, ctx: _BuildContext) -> Stream:
    """Dispatch one compiled node to its handler."""
    raw = node.node
    handler = _NODE_HANDLERS.get(type(raw))
    if handler is not None:
        return handler(stream, raw, idx, ctx)
    raise TypeError(f"No adapter handler for {type(raw).__name__}")


# ---------------------------------------------------------------------------
# Node handlers — each wires one DSL node type into the Bytewax stream
# ---------------------------------------------------------------------------


def _resolve_node_name(raw: object) -> str:
    """Resolve a human-readable name for a DSL node."""
    if hasattr(raw, "task_name"):
        return cast(str, raw.task_name())
    return type(raw).__name__


def _apply_task(stream: Stream, raw: object, idx: int, ctx: _BuildContext) -> Stream:
    task = cast(Task[StreamPayload, StreamPayload], raw)
    name = _resolve_node_name(task)
    observer = ctx.flow_observer
    flow_name = ctx.plan.name

    def step(msg: Any) -> Any:
        if observer is not None:
            observer.on_node_start(flow_name, idx, node_type=name)
        t0 = time.monotonic()
        try:
            result = task.execute(msg)
            if observer is not None:
                elapsed = int((time.monotonic() - t0) * 1000)
                observer.on_node_end(
                    flow_name, idx, node_type=name, status="success", duration_ms=elapsed
                )
            return result
        except Exception as exc:
            if observer is not None:
                observer.on_node_error(flow_name, idx, node_type=name, exc=exc)
            raise

    return bw_map(f"task_{idx}_{name}", stream, step)


def _apply_batch_task(stream: Stream, raw: object, idx: int, ctx: _BuildContext) -> Stream:
    task = cast(BatchTask[StreamPayload, StreamPayload], raw)
    name = _resolve_node_name(task)
    observer = ctx.flow_observer
    flow_name = ctx.plan.name

    def step(batch: list[Any]) -> list[Any]:
        if observer is not None:
            observer.on_node_start(flow_name, idx, node_type=name)
        t0 = time.monotonic()
        try:
            result = task.execute(batch)
            if observer is not None:
                elapsed = int((time.monotonic() - t0) * 1000)
                observer.on_node_end(
                    flow_name, idx, node_type=name, status="success", duration_ms=elapsed
                )
            return result
        except Exception as exc:
            if observer is not None:
                observer.on_node_error(flow_name, idx, node_type=name, exc=exc)
            raise

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
        if observer is not None:
            observer.on_node_start(flow_name, idx, node_type=node_type)
        t0 = time.monotonic()
        batch_resources = manager.open_batch()
        deps = {**worker_resources, **batch_resources}
        try:
            result = [node.task.execute(msg, **deps) for msg in batch]
            if observer is not None:
                elapsed = int((time.monotonic() - t0) * 1000)
                observer.on_node_end(
                    flow_name, idx, node_type=node_type, status="success", duration_ms=elapsed
                )
            return result
        except Exception as exc:
            if observer is not None:
                observer.on_node_error(flow_name, idx, node_type=node_type, exc=exc)
            raise
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
        batch_resources = manager.open_batch()
        deps = {**worker_resources, **batch_resources}
        sem = asyncio.Semaphore(node.max_concurrency)

        async def _bounded(msg: Any) -> Any:
            async with sem:
                result = node.task.execute(msg, **deps)
                if not inspect.isawaitable(result):
                    raise TypeError("WithAsync task.execute must return an awaitable.")
                return await cast(Awaitable[object], result)

        try:
            return await asyncio.gather(*[_bounded(msg) for msg in batch])
        finally:
            manager.close_batch()

    def step(batch: list[Any]) -> list[Any]:
        if observer is not None:
            observer.on_node_start(flow_name, idx, node_type=node_type)
        t0 = time.monotonic()
        try:
            result = bridge.run(_execute_batch(batch))
            if observer is not None:
                elapsed = int((time.monotonic() - t0) * 1000)
                observer.on_node_end(
                    flow_name, idx, node_type=node_type, status="success", duration_ms=elapsed
                )
            return result
        except Exception as exc:
            if observer is not None:
                observer.on_node_error(flow_name, idx, node_type=node_type, exc=exc)
            raise

    return bw_map(f"with_async_{idx}", stream, step)


def _apply_collect_batch(stream: Stream, raw: object, idx: int, ctx: _BuildContext) -> Stream:
    node = cast(CollectBatch, raw)
    return collect(
        f"collect_{idx}",
        stream,
        timeout=timedelta(milliseconds=node.timeout_ms),
        max_size=node.max_records,
    )


def _apply_for_each(stream: Stream, raw: object, idx: int, ctx: _BuildContext) -> Stream:
    return flat_map(f"foreach_{idx}", stream, _identity)


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
        OneEmit: _apply_one_emit,
    }
)


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
