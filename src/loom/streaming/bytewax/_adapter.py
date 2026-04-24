"""Bytewax runtime adapter.

Translates a :class:`CompiledPlan` into a Bytewax :class:`Dataflow`,
wiring decode, task execution, encode, and error routing operators.

Requires ``bytewax`` to be installed.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import time
from collections.abc import Awaitable, Callable, Iterable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import timedelta
from types import MappingProxyType
from typing import Any, TypeAlias, cast

import bytewax.dataflow as _bytewax_dataflow
import bytewax.testing as _bytewax_testing
from bytewax.operators import (
    branch,
    collect,
    flat_map,
    key_on,
    key_rm,
)
from bytewax.operators import input as bw_input
from bytewax.operators import map as bw_map
from bytewax.operators import output as bw_output

from loom.core.async_bridge import AsyncBridge
from loom.streaming.bytewax._operators import ResourceLifecycle, lifecycle_for
from loom.streaming.compiler import CompiledNode, CompiledPlan
from loom.streaming.core._errors import ErrorKind
from loom.streaming.core._message import Message
from loom.streaming.core._typing import StreamPayload
from loom.streaming.kafka._codec import MsgspecCodec
from loom.streaming.kafka._record import KafkaRecord
from loom.streaming.kafka._wire import DecodeOk, DecodeResult, try_decode_record
from loom.streaming.nodes._boundary import IntoTopic
from loom.streaming.nodes._router import Router, evaluate_predicate, select_value
from loom.streaming.nodes._shape import CollectBatch, Drain, ForEach
from loom.streaming.nodes._step import BatchExpandStep, BatchStep, ExpandStep, RecordStep
from loom.streaming.nodes._with import OneEmit, With, WithAsync
from loom.streaming.observability.observers.protocol import StreamingFlowObserver

logger = logging.getLogger(__name__)
Stream: TypeAlias = Any
NodeHandler: TypeAlias = Callable[[Stream, object, int, "_BuildContext"], Stream]


@dataclass(frozen=True)
class _BuiltDataflow:
    """Bytewax dataflow plus adapter-owned shutdown callback.

    Args:
        dataflow: Configured Bytewax dataflow ready for execution.
        shutdown: Callback that releases adapter resources after execution.
    """

    dataflow: Any
    shutdown: Callable[[], None]


def build_dataflow(
    plan: CompiledPlan,
    *,
    flow_observer: StreamingFlowObserver | None = None,
    source: Any | None = None,
    sink: Any | None = None,
    error_sinks: Mapping[ErrorKind, Any] | None = None,
) -> Any:
    """Build a Bytewax Dataflow from a compiled plan.

    Args:
        plan: Immutable compiled plan produced by the streaming compiler.
        flow_observer: Optional observer for flow execution lifecycle events.
        source: Optional Bytewax source. Tests can pass ``TestingSource`` here.
        sink: Optional Bytewax sink. Tests can pass ``TestingSink`` here.
        error_sinks: Optional sinks for explicit error branches in tests.

    Returns:
        Configured Bytewax Dataflow ready for execution.
    """
    return build_dataflow_with_shutdown(
        plan,
        flow_observer=flow_observer,
        source=source,
        sink=sink,
        error_sinks=error_sinks,
    ).dataflow


def build_dataflow_with_shutdown(
    plan: CompiledPlan,
    *,
    flow_observer: StreamingFlowObserver | None = None,
    source: Any | None = None,
    sink: Any | None = None,
    error_sinks: Mapping[ErrorKind, Any] | None = None,
) -> _BuiltDataflow:
    """Build a Bytewax Dataflow and expose its shutdown callback.

    Args:
        plan: Immutable compiled plan produced by the streaming compiler.
        flow_observer: Optional observer for flow execution lifecycle events.
        source: Optional Bytewax source. Tests can pass ``TestingSource`` here.
        sink: Optional Bytewax sink. Tests can pass ``TestingSink`` here.
        error_sinks: Optional sinks for explicit error branches in tests.

    Returns:
        Built dataflow and a callback for releasing adapter-owned resources.
    """
    ctx = _BuildContext(
        plan=plan,
        bridge=_maybe_create_bridge(plan),
        flow_observer=flow_observer,
        source=source,
        sink=sink,
        error_sinks=error_sinks,
    )
    return _BuiltDataflow(dataflow=_assemble_dataflow(plan, ctx), shutdown=ctx.shutdown_all)


def _assemble_dataflow(plan: CompiledPlan, ctx: _BuildContext) -> Any:
    """Assemble a Bytewax Dataflow from a pre-built context."""
    flow = _bytewax_dataflow.Dataflow(plan.name)
    stream = _build_source_pipeline(flow, ctx)

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
        "outputs",
        "_managers",
    )

    def __init__(
        self,
        plan: CompiledPlan,
        bridge: AsyncBridge | None,
        flow_observer: StreamingFlowObserver | None = None,
        source: Any | None = None,
        sink: Any | None = None,
        error_sinks: Mapping[ErrorKind, Any] | None = None,
    ) -> None:
        self.plan = plan
        self.bridge = bridge
        self.flow_observer = flow_observer
        self.source = source
        self.outputs = _OutputWiring(
            sink=sink,
            error_sinks=error_sinks or {},
        )
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
        if self.bridge is not None:
            self.bridge.shutdown()
            self.bridge = None


class _OutputWiring:
    """Coordinate terminal output and explicit error routes for one build."""

    __slots__ = ("sink", "error_sinks", "_terminal_output_wired")

    def __init__(
        self,
        *,
        sink: Any | None,
        error_sinks: Mapping[ErrorKind, Any],
    ) -> None:
        self.sink = sink
        self.error_sinks = error_sinks
        self._terminal_output_wired = False

    def needs_flow_output(self) -> bool:
        """Return whether the flow-level output still needs wiring."""
        return not self._terminal_output_wired

    def wire_terminal(self, step_id: str, stream: Stream) -> None:
        """Wire the configured terminal sink exactly once."""
        if self.sink is not None:
            bw_output(step_id, stream, self.sink)
        else:
            bw_map(f"{step_id}_encode", stream, _placeholder_encode)
        self._terminal_output_wired = True

    def wire_flow_output(self, stream: Stream, plan: CompiledPlan) -> None:
        """Wire the flow-level output and declared error routes."""
        if not self.needs_flow_output():
            return

        if self.sink is not None:
            self.wire_terminal("output", stream)
        elif plan.output is not None:
            bw_map("encode_output", stream, _placeholder_encode)
            # Runtime Kafka output wiring stays deferred until the connector-backed
            # producer path is implemented for the Bytewax adapter.

        for kind, sink in plan.error_routes.items():
            logger.info(
                "error_route_registered",
                extra={"error_kind": kind.value, "topic": sink.topic},
            )
            # Runtime error-route publishing stays deferred until explicit
            # connector-backed sinks are available for error branches.

    def wire_decode_error(self, stream: Stream, plan: CompiledPlan) -> None:
        """Wire source decode errors to an explicit test sink or runtime route."""
        sink = self.error_sinks.get(ErrorKind.WIRE)
        if sink is not None:
            bw_output("decode_wire_errors", stream, sink)
            return

        if ErrorKind.WIRE in plan.error_routes:
            bw_map("decode_wire_errors_encode", stream, _placeholder_encode)


# ---------------------------------------------------------------------------
# Source wiring
# ---------------------------------------------------------------------------


def _build_source_pipeline(flow: Any, ctx: _BuildContext) -> Stream:
    """Build the source-side pipeline up to the first decoded Message stream."""
    source = ctx.source if ctx.source is not None else _empty_testing_source()
    codec: MsgspecCodec[Any] = MsgspecCodec()
    strategy = ctx.plan.source.decode_strategy
    step_id = f"decode_{strategy}"

    stream: Stream = bw_input("source", flow, source)
    decoded = _decode_source_stream(stream, ctx, codec, step_id)
    decoded_branch = _split_decode_results(decoded, step_id)
    ctx.outputs.wire_decode_error(decoded_branch.falses, ctx.plan)
    return bw_map(f"{step_id}_message", decoded_branch.trues, _decode_ok_message)


def _decode_source_stream(
    stream: Stream,
    ctx: _BuildContext,
    codec: MsgspecCodec[Any],
    step_id: str,
) -> Stream:
    """Map raw source items into decode results without raising wire errors."""
    return bw_map(step_id, stream, lambda item: _decode_source_record(item, ctx, codec))


def _split_decode_results(stream: Stream, step_id: str) -> Any:
    """Split decode results into successful messages and wire errors."""
    return branch(f"{step_id}_is_ok", stream, _is_decode_ok)


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
    step_name = getattr(raw, "step_name", None)
    if callable(step_name):
        return cast(str, step_name())
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


def _apply_record_step(stream: Stream, raw: object, idx: int, ctx: _BuildContext) -> Stream:
    record_step = cast(RecordStep[StreamPayload, StreamPayload], raw)
    name = _resolve_node_name(record_step)
    observer = ctx.flow_observer
    flow_name = ctx.plan.name

    def step_fn(msg: Any) -> Any:
        message = _require_message(msg)
        with _observe_node(observer, flow_name, idx, name):
            result = record_step.execute(message)
            return _replace_payload(message, result)

    return bw_map(f"record_{idx}_{name}", stream, step_fn)


def _apply_batch_step(stream: Stream, raw: object, idx: int, ctx: _BuildContext) -> Stream:
    batch_step = cast(BatchStep[StreamPayload, StreamPayload], raw)
    name = _resolve_node_name(batch_step)
    observer = ctx.flow_observer
    flow_name = ctx.plan.name

    def step_fn(batch: list[Any]) -> list[Any]:
        messages = [_require_message(item) for item in batch]
        with _observe_node(observer, flow_name, idx, name):
            result = batch_step.execute(messages)
            return _replace_payloads(messages, result)

    mapped = bw_map(f"batch_{idx}_{name}", stream, step_fn)
    return flat_map(f"flatten_{idx}", mapped, _identity)


def _apply_expand_step(stream: Stream, raw: object, idx: int, ctx: _BuildContext) -> Stream:
    expand_step = cast(ExpandStep[StreamPayload, StreamPayload], raw)
    name = _resolve_node_name(expand_step)
    observer = ctx.flow_observer
    flow_name = ctx.plan.name

    def step_fn(msg: Any) -> Iterable[Message[StreamPayload]]:
        message = _require_message(msg)
        with _observe_node(observer, flow_name, idx, name):
            return expand_step.execute(message)

    return flat_map(f"expand_{idx}_{name}", stream, step_fn)


def _apply_batch_expand_step(stream: Stream, raw: object, idx: int, ctx: _BuildContext) -> Stream:
    batch_expand_step = cast(BatchExpandStep[StreamPayload, StreamPayload], raw)
    name = _resolve_node_name(batch_expand_step)
    observer = ctx.flow_observer
    flow_name = ctx.plan.name

    def step_fn(batch: list[Any]) -> Iterable[Message[StreamPayload]]:
        messages = [_require_message(item) for item in batch]
        with _observe_node(observer, flow_name, idx, name):
            return batch_expand_step.execute(messages)

    mapped = bw_map(f"batch_expand_{idx}_{name}", stream, step_fn)
    return flat_map(f"flatten_expand_{idx}", mapped, _identity)


def _apply_with(stream: Stream, raw: object, idx: int, ctx: _BuildContext) -> Stream:
    node = cast(With[StreamPayload, StreamPayload], raw)
    manager = ctx.manager_for(idx, node)
    worker_resources = manager.open_worker()
    observer = ctx.flow_observer
    flow_name = ctx.plan.name
    node_type = type(node).__name__

    def step(batch: list[Any]) -> list[Any]:
        with _observe_node(observer, flow_name, idx, node_type):
            messages = _messages_from_batch(batch)
            with _batch_dependencies(manager, worker_resources) as deps:
                result = [node.step.execute(msg, **deps) for msg in messages]
                return _replace_payloads(messages, result)

    return bw_map(f"with_{idx}", stream, step)


def _apply_with_async(stream: Stream, raw: object, idx: int, ctx: _BuildContext) -> Stream:
    node = cast(WithAsync[StreamPayload, StreamPayload], raw)
    if ctx.bridge is None:
        raise RuntimeError("WithAsync requires an AsyncBridge but none was created.")

    manager = ctx.manager_for(idx, node)
    worker_resources = manager.open_worker()
    bridge = ctx.bridge
    observer = ctx.flow_observer
    flow_name = ctx.plan.name
    node_type = type(node).__name__
    sem = asyncio.Semaphore(node.max_concurrency)

    async def _execute_batch(batch: list[Any]) -> list[Any]:
        messages = _messages_from_batch(batch)

        with _batch_dependencies(manager, worker_resources) as deps:
            result = await asyncio.gather(
                *[_execute_async_message(node, sem, msg, deps) for msg in messages]
            )
            return _replace_payloads(messages, result)

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
    ctx.outputs.wire_terminal(f"into_topic_{idx}", stream)
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
        RecordStep: _apply_record_step,
        BatchStep: _apply_batch_step,
        ExpandStep: _apply_expand_step,
        BatchExpandStep: _apply_batch_expand_step,
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
    ctx.outputs.wire_flow_output(stream, ctx.plan)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _maybe_create_bridge(plan: CompiledPlan) -> AsyncBridge | None:
    """Create an AsyncBridge if the plan requires async task execution."""
    if not plan.needs_async_bridge:
        return None

    return AsyncBridge(thread_name=f"loom-{plan.name}-async")


def _identity(items: Any) -> Any:
    """Pass-through for flat_map."""
    return items


def _empty(_item: Any) -> tuple[()]:
    """Drop one item from a stream."""
    return ()


def _messages_from_batch(batch: list[Any]) -> list[Message[StreamPayload]]:
    """Coerce one batch of runtime values into DSL messages."""
    return [_require_message(item) for item in batch]


@contextmanager
def _batch_dependencies(
    manager: ResourceLifecycle,
    worker_resources: Mapping[str, object],
) -> Iterator[dict[str, object]]:
    """Open and close one batch-scoped dependency set."""
    batch_resources = manager.open_batch()
    try:
        yield {**worker_resources, **batch_resources}
    finally:
        manager.close_batch()


async def _execute_async_message(
    node: WithAsync[StreamPayload, StreamPayload],
    sem: asyncio.Semaphore,
    message: Message[StreamPayload],
    deps: Mapping[str, object],
) -> object:
    """Execute one async WithAsync message under bounded concurrency."""
    async with sem:
        result = node.step.execute(message, **deps)
        if not inspect.isawaitable(result):
            raise TypeError("WithAsync task.execute must return an awaitable.")
        return await cast(Awaitable[object], result)


def _batch_key(_item: Any) -> str:
    """Return a fixed grouping key for Bytewax ``collect``.

    This is sufficient for testing with ``TestingSource``.  When Kafka
    sources are wired directly, the key should derive from the record's
    partition or message key to preserve parallelism.
    """
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
    if isinstance(node, RecordStep):
        step = cast(RecordStep[StreamPayload, StreamPayload], node)
        return _replace_payload(message, step.execute(message))
    if isinstance(node, (IntoTopic, Drain)):
        return message
    raise TypeError(
        f"Router branch node {type(node).__name__} is not supported by Bytewax adapter."
    )


def _empty_testing_source() -> Any:
    """Return an inert Bytewax testing source until Kafka input is wired."""
    return _bytewax_testing.TestingSource([])


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


def _decode_source_record(
    payload: Any,
    ctx: _BuildContext,
    codec: MsgspecCodec[Any],
) -> DecodeResult[StreamPayload]:
    """Decode source records into DSL messages without raising decode errors."""
    if isinstance(payload, Message):
        return DecodeOk(message=cast(Message[StreamPayload], payload))
    if isinstance(payload, KafkaRecord):
        return try_decode_record(
            cast(KafkaRecord[bytes], payload),
            ctx.plan.source.payload_type,
            codec,
        )
    raise TypeError(f"Expected Message or KafkaRecord, got {type(payload).__name__}.")


def _is_decode_ok(result: DecodeResult[StreamPayload]) -> bool:
    """Return whether a source decode result can continue through the flow."""
    return isinstance(result, DecodeOk)


def _decode_ok_message(result: DecodeResult[StreamPayload]) -> Message[StreamPayload]:
    """Unwrap a successful source decode result."""
    if isinstance(result, DecodeOk):
        return result.message
    raise TypeError(f"Expected DecodeOk, got {type(result).__name__}.")


def _placeholder_encode(record: Any) -> Any:
    """Placeholder encoder - replaced when Kafka connector is wired."""
    return record
