"""Bytewax runtime adapter.

Builds Bytewax dataflow from a CompiledPlan, handling sync/async tasks,
context-manager lifecycles, and collect nodes.

Requires ``bytewax`` to be installed; import this module only at runtime
when the dependency is present.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any

from bytewax.dataflow import Dataflow
from bytewax.inputs import Source
from bytewax.operators import collect, input
from bytewax.operators import map as bw_map

from loom.core.async_bridge import AsyncWorker
from loom.streaming._task import Task
from loom.streaming._with import With, WithAsync
from loom.streaming.bytewax._operators import resource_manager_for
from loom.streaming.compiler import CompiledNode, CompiledPlan, CompiledSink

logger = logging.getLogger(__name__)


class _PlaceholderSource(Source[Any]):
    """Placeholder until Kafka source is wired."""

    pass


def build_dataflow(plan: CompiledPlan, worker: AsyncWorker | None = None) -> Dataflow:
    """Build a Bytewax Dataflow from a compiled plan."""
    flow = Dataflow(plan.name)
    stream = _add_source(flow, plan)
    stream = _add_nodes(stream, plan, worker)
    _add_output(stream, plan)
    return flow


def _add_source(flow: Dataflow, plan: CompiledPlan) -> Any:
    stream = input("inp", flow, _PlaceholderSource())
    return bw_map("decode", stream, _make_decoder(plan.source.decode_strategy))


def _add_nodes(stream: Any, plan: CompiledPlan, worker: AsyncWorker | None = None) -> Any:
    for node in plan.nodes:
        stream = _add_node(stream, node, worker)
    return stream


def _add_node(stream: Any, node: CompiledNode, worker: AsyncWorker | None = None) -> Any:
    raw_node = node.node

    if node.input_shape == "batch":
        stream = collect("collect", stream, timeout=timedelta(seconds=30), max_size=1000)

    if isinstance(raw_node, WithAsync):
        if worker is None:
            raise RuntimeError("WithAsync requires an AsyncWorker")
        stream = bw_map("async_map", stream, _build_async_step_fn(raw_node, worker))
    elif isinstance(raw_node, With):
        stream = bw_map("sync_map", stream, _build_sync_step_fn(raw_node))
    elif isinstance(raw_node, Task):
        stream = bw_map("task_map", stream, _build_task_fn(raw_node))
    else:
        raise TypeError(f"Unsupported node type: {type(raw_node).__name__}")
    return stream


def _add_output(stream: Any, plan: CompiledPlan) -> None:
    if plan.output is not None:
        stream = bw_map("encode", stream, _make_encoder())
        # Actual output connector would go here

    # TODO: error routing — Bytewax does not have a built-in inspect_error
    # operator; errors should be caught inside step functions and routed
    # to the configured sink.
    if plan.error_routes:
        logger.warning("error_routes configured but not yet implemented for Bytewax")


def _build_sync_step_fn(node: With) -> Any:
    """Build a pure function for sync context-manager batch processing."""
    manager = resource_manager_for(node)
    resources = manager.open_worker()

    def step(batch: list[Any]) -> list[Any]:
        batch_resources = manager.open_batch()
        deps = {**resources, **batch_resources}
        try:
            return [node.task.execute(msg, **deps) for msg in batch]
        finally:
            manager.close_batch()

    return step


def _build_async_step_fn(node: WithAsync, worker: AsyncWorker) -> Any:
    """Build a pure function for async context-manager batch processing."""
    import asyncio

    manager = resource_manager_for(node, worker=worker)
    resources = manager.open_worker()

    async def _execute_batch(batch: list[Any]) -> list[Any]:
        batch_resources = manager.open_batch()
        deps = {**resources, **batch_resources}
        sem = asyncio.Semaphore(node.max_concurrency)

        async def _one(msg: Any) -> Any:
            async with sem:
                return await node.task.execute(msg, **deps)

        return await asyncio.gather(*[_one(msg) for msg in batch])

    def step(batch: list[Any]) -> list[Any]:
        return worker.run(_execute_batch(batch))

    return step


def _build_task_fn(task: Task[Any, Any]) -> Any:
    """Build a pure function for a simple task (no context managers)."""

    def step(msg: Any) -> Any:
        return task.execute(msg)

    return step


def _make_decoder(strategy: str) -> Any:
    """Build a decoder function."""

    def decode(payload: bytes) -> Any:
        # Placeholder: actual implementation would use serde
        return payload

    return decode


def _make_encoder() -> Any:
    """Build an encoder function."""

    def encode(record: Any) -> bytes:
        # Placeholder: actual implementation would use serde
        return record if isinstance(record, bytes) else str(record).encode()

    return encode


def _make_error_handler(sink: CompiledSink) -> Any:
    """Build an error handler function."""

    def handle(err: Exception, msg: Any) -> None:
        logger.error("Error processing message: %s", err)

    return handle
