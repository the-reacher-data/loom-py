"""Bytewax node handlers for streaming DSL steps."""

from __future__ import annotations

import inspect
import time
from collections.abc import Awaitable, Callable, Coroutine, Iterator, Mapping
from contextlib import AbstractContextManager, contextmanager
from datetime import timedelta
from types import MappingProxyType
from typing import Any, Protocol, TypeAlias, cast

import anyio
from anyio.to_thread import run_sync as _run_in_thread
from bytewax.operators import branch, collect, flat_map, key_on, key_rm
from bytewax.operators import map as bw_map

from loom.core.async_bridge import AsyncBridge
from loom.streaming.bytewax._error_boundary import (
    NodeResult,
    _classify_routing,
    _classify_task,
    _ErrorWireOutputs,
    _execute_batch_in_boundary,
    _execute_in_boundary,
    _split_batch_node_result,
    _split_node_result,
)
from loom.streaming.bytewax._operators import ResourceLifecycle
from loom.streaming.compiler._plan import CompiledPlan
from loom.streaming.core._errors import ErrorEnvelope, ErrorKind
from loom.streaming.core._message import Message
from loom.streaming.core._typing import StreamPayload
from loom.streaming.nodes._boundary import IntoTopic
from loom.streaming.nodes._broadcast import Broadcast
from loom.streaming.nodes._capabilities import RouterBranchSafe
from loom.streaming.nodes._fork import Fork, ForkKind
from loom.streaming.nodes._router import Router, evaluate_predicate, select_value
from loom.streaming.nodes._shape import CollectBatch, Drain, ForEach, WindowStrategy
from loom.streaming.nodes._step import BatchExpandStep, BatchStep, ExpandStep, RecordStep
from loom.streaming.nodes._with import With, WithAsync
from loom.streaming.observability.observers.protocol import StreamingFlowObserver

Stream: TypeAlias = Any


def _step_id(base: str, ctx: _BuildContextProtocol) -> str:
    """Build a Bytewax step ID qualified with the current wiring path."""
    path = ctx.current_path
    if not path:
        return base
    return "_".join(map(str, path)) + "_" + base


class _OutputWiringProtocol(_ErrorWireOutputs, Protocol):
    """Write node and terminal output branches during graph construction."""

    def wire_terminal(self, step_id: str, stream: Stream) -> None:
        """Wire one terminal output branch."""

    def wire_branch_terminal(self, step_id: str, stream: Stream, path: tuple[int, ...]) -> None:
        """Wire one branch terminal output branch."""

    def wire_node_error(self, kind: ErrorKind, step_id: str, stream: Stream) -> None:
        """Wire one node error branch."""

    def wire_flow_output(self, stream: Stream, plan: CompiledPlan) -> None:
        """Wire flow-level outputs after the process completes."""

    def wire_decode_error(self, stream: Stream, plan: CompiledPlan) -> None:
        """Wire source decode errors."""


class _BuildContextProtocol(Protocol):
    """Adapter build context required by node handlers."""

    plan: CompiledPlan
    bridge: AsyncBridge | None
    flow_observer: StreamingFlowObserver | None
    outputs: _OutputWiringProtocol

    @property
    def current_path(self) -> tuple[int, ...]:
        """Return the current wiring path inside the process tree."""
        ...

    def manager_for(
        self,
        idx: int,
        node: With[StreamPayload, StreamPayload] | WithAsync[StreamPayload, StreamPayload],
    ) -> ResourceLifecycle:
        """Return the resource manager for one scoped node."""
        ...

    def enter_path(self, path: tuple[int, ...]) -> AbstractContextManager[None]:
        """Temporarily set the current compilation path."""
        ...


NodeHandler: TypeAlias = Callable[[Stream, object, int, _BuildContextProtocol], Stream]


def _wire_process(
    stream: Stream,
    nodes: tuple[object, ...],
    ctx: _BuildContextProtocol,
    *,
    path_prefix: tuple[int, ...] = (),
) -> Stream:
    """Wire one process subtree under a path prefix."""
    for idx, node in enumerate(nodes):
        with ctx.enter_path(path_prefix + (idx,)):
            stream = _wire_node(stream, node, idx, ctx)
    return stream


def _wire_node(stream: Stream, node: object, idx: int, ctx: _BuildContextProtocol) -> Stream:
    """Dispatch one DSL node to its Bytewax handler."""
    for handler_type, handler in _NODE_HANDLERS.items():
        if isinstance(node, handler_type):
            return handler(stream, node, idx, ctx)
    raise TypeError(f"No adapter handler for {type(node).__name__}")


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


def _apply_record_step(stream: Stream, raw: object, idx: int, ctx: _BuildContextProtocol) -> Stream:
    record_step = cast(RecordStep[StreamPayload, StreamPayload], raw)
    name = _resolve_node_name(record_step)
    observer = ctx.flow_observer
    flow_name = ctx.plan.name

    def step_fn(msg: Any) -> NodeResult:
        message = _require_message(msg)
        return _execute_in_boundary(
            _classify_task,
            message,
            lambda: _execute_record_step(observer, flow_name, idx, name, record_step, message),
        )

    sid = _step_id(f"record_{idx}_{name}", ctx)
    mapped = bw_map(sid, stream, step_fn)
    return _split_node_result(mapped, sid, ctx, ErrorKind.TASK)


def _apply_batch_step(stream: Stream, raw: object, idx: int, ctx: _BuildContextProtocol) -> Stream:
    batch_step = cast(BatchStep[StreamPayload, StreamPayload], raw)
    name = _resolve_node_name(batch_step)
    observer = ctx.flow_observer
    flow_name = ctx.plan.name

    def step_fn(batch: list[Any]) -> list[NodeResult]:
        messages = [_require_message(item) for item in batch]
        return _execute_batch_in_boundary(
            _classify_task,
            messages,
            lambda: _execute_batch_step(
                observer,
                flow_name,
                idx,
                name,
                batch_step,
                messages,
            ),
        )

    sid = _step_id(f"batch_{idx}_{name}", ctx)
    mapped = bw_map(sid, stream, step_fn)
    return _split_batch_node_result(mapped, sid, ctx, ErrorKind.TASK)


def _apply_expand_step(
    stream: Stream,
    raw: object,
    idx: int,
    ctx: _BuildContextProtocol,
) -> Stream:
    expand_step = cast(ExpandStep[StreamPayload, StreamPayload], raw)
    name = _resolve_node_name(expand_step)
    observer = ctx.flow_observer
    flow_name = ctx.plan.name

    def step_fn(msg: Any) -> list[NodeResult]:
        message = _require_message(msg)
        return _execute_batch_in_boundary(
            _classify_task,
            [message],
            lambda: _execute_expand_step(
                observer,
                flow_name,
                idx,
                name,
                expand_step,
                message,
            ),
        )

    sid = _step_id(f"expand_{idx}_{name}", ctx)
    mapped = bw_map(sid, stream, step_fn)
    flattened = flat_map(_step_id(f"flatten_expand_{idx}", ctx), mapped, _identity)
    return _split_node_result(flattened, sid, ctx, ErrorKind.TASK)


def _apply_batch_expand_step(
    stream: Stream,
    raw: object,
    idx: int,
    ctx: _BuildContextProtocol,
) -> Stream:
    batch_expand_step = cast(BatchExpandStep[StreamPayload, StreamPayload], raw)
    name = _resolve_node_name(batch_expand_step)
    observer = ctx.flow_observer
    flow_name = ctx.plan.name

    def step_fn(batch: list[Any]) -> list[NodeResult]:
        messages = [_require_message(item) for item in batch]
        return _execute_batch_in_boundary(
            _classify_task,
            messages,
            lambda: _execute_batch_expand_step(
                observer,
                flow_name,
                idx,
                name,
                batch_expand_step,
                messages,
            ),
        )

    sid = _step_id(f"batch_expand_{idx}_{name}", ctx)
    mapped = bw_map(sid, stream, step_fn)
    flattened = flat_map(_step_id(f"flatten_batch_expand_{idx}", ctx), mapped, _identity)
    return _split_node_result(flattened, sid, ctx, ErrorKind.TASK)


def _apply_with(stream: Stream, raw: object, idx: int, ctx: _BuildContextProtocol) -> Stream:
    node = cast(With[StreamPayload, StreamPayload], raw)
    manager = ctx.manager_for(idx, node)
    worker_resources = manager.open_worker()
    observer = ctx.flow_observer
    flow_name = ctx.plan.name
    node_type = type(node).__name__

    def step(batch: list[Any]) -> list[NodeResult]:
        messages = _messages_from_batch(batch)
        return _execute_batch_in_boundary(
            _classify_task,
            messages,
            lambda: _execute_with_step(
                observer,
                flow_name,
                idx,
                node_type,
                manager,
                worker_resources,
                node.step,
                messages,
            ),
        )

    mapped = bw_map(_step_id(f"with_{idx}", ctx), stream, step)
    return _split_batch_node_result(
        mapped, _step_id(f"with_{idx}_{node_type}", ctx), ctx, ErrorKind.TASK
    )


def _apply_with_async(
    stream: Stream,
    raw: object,
    idx: int,
    ctx: _BuildContextProtocol,
) -> Stream:
    node = cast(WithAsync[StreamPayload, StreamPayload], raw)
    if ctx.bridge is None:
        raise RuntimeError("WithAsync requires an AsyncBridge but none was created.")

    manager = ctx.manager_for(idx, node)
    worker_resources = manager.open_worker()
    bridge = ctx.bridge
    observer = ctx.flow_observer
    flow_name = ctx.plan.name
    node_type = type(node).__name__
    sem = anyio.Semaphore(node.max_concurrency)

    if node.error_mode == "best_effort":
        step = _build_best_effort_step(
            node, manager, worker_resources, bridge, observer, flow_name, idx, node_type, sem
        )
    else:
        step = _build_fail_fast_step(
            node, manager, worker_resources, bridge, observer, flow_name, idx, node_type, sem
        )

    mapped = bw_map(_step_id(f"with_async_{idx}", ctx), stream, step)
    return _split_batch_node_result(
        mapped, _step_id(f"with_async_{idx}_{node_type}", ctx), ctx, ErrorKind.TASK
    )


def _apply_collect_batch(
    stream: Stream,
    raw: object,
    idx: int,
    ctx: _BuildContextProtocol,
) -> Stream:
    node = cast(CollectBatch, raw)
    if node.window is WindowStrategy.COLLECT:
        return _apply_collect_batch_default(stream, node, _step_id(str(idx), ctx))
    raise TypeError(
        f"WindowStrategy.{node.window} reached the adapter — "
        "this should have been rejected at compile time."
    )


def _apply_collect_batch_default(stream: Stream, node: CollectBatch, step_prefix: str) -> Stream:
    """Apply processing-time count-and-timeout collect (``WindowStrategy.COLLECT``)."""
    keyed = key_on(f"collect_key_{step_prefix}", stream, _batch_key)
    collected = collect(
        f"collect_{step_prefix}",
        keyed,
        timeout=timedelta(milliseconds=node.timeout_ms),
        max_size=node.max_records,
    )
    return key_rm(f"collect_unkey_{step_prefix}", collected)


def _apply_for_each(stream: Stream, raw: object, idx: int, ctx: _BuildContextProtocol) -> Stream:
    del raw
    return flat_map(_step_id(f"foreach_{idx}", ctx), stream, _identity)


def _apply_router(stream: Stream, raw: object, idx: int, ctx: _BuildContextProtocol) -> Stream:
    router = cast(Router[StreamPayload, StreamPayload], raw)
    observer = ctx.flow_observer
    flow_name = ctx.plan.name

    def step(msg: Any) -> NodeResult:
        message = _require_message(msg)
        return _execute_in_boundary(
            _classify_routing,
            message,
            lambda: _execute_router_step(observer, flow_name, idx, router, message),
        )

    sid = _step_id(f"router_{idx}", ctx)
    mapped = bw_map(sid, stream, step)
    return _split_node_result(mapped, sid, ctx, ErrorKind.ROUTING)


def _apply_broadcast(
    stream: Stream,
    raw: object,
    idx: int,
    ctx: _BuildContextProtocol,
) -> Stream:
    """Wire a Broadcast node: fan-out the same stream to every branch independently."""
    node = cast(Broadcast[StreamPayload], raw)
    broadcast_path = ctx.current_path

    for branch_idx, route in enumerate(node.routes):
        branch_stream = _wire_process(
            stream,
            route.process.nodes,
            ctx,
            path_prefix=broadcast_path + (branch_idx,),
        )
        ctx.outputs.wire_branch_terminal(
            f"broadcast_{idx}_out_{branch_idx}",
            branch_stream,
            broadcast_path + (branch_idx,),
        )

    return stream


def _apply_drain(stream: Stream, raw: object, idx: int, ctx: _BuildContextProtocol) -> Stream:
    del raw
    return flat_map(_step_id(f"drain_{idx}", ctx), stream, _empty)


def _apply_into_topic(stream: Stream, raw: object, idx: int, ctx: _BuildContextProtocol) -> Stream:
    del raw
    ctx.outputs.wire_branch_terminal(f"into_topic_{idx}", stream, ctx.current_path)
    return stream


def _messages_from_batch(batch: list[Any]) -> list[Message[StreamPayload]]:
    """Coerce one batch of runtime values into DSL messages."""
    return [_require_message(item) for item in batch]


def _execute_record_step(
    observer: StreamingFlowObserver | None,
    flow_name: str,
    idx: int,
    name: str,
    record_step: RecordStep[StreamPayload, StreamPayload],
    message: Message[StreamPayload],
) -> Message[StreamPayload]:
    with _observe_node(observer, flow_name, idx, name):
        result = record_step.execute(message)
        return _replace_payload(message, result)


def _execute_batch_step(
    observer: StreamingFlowObserver | None,
    flow_name: str,
    idx: int,
    name: str,
    batch_step: BatchStep[StreamPayload, StreamPayload],
    messages: list[Message[StreamPayload]],
) -> list[Message[StreamPayload]]:
    with _observe_node(observer, flow_name, idx, name):
        result = batch_step.execute(messages)
        return _replace_payloads(messages, result)


def _execute_expand_step(
    observer: StreamingFlowObserver | None,
    flow_name: str,
    idx: int,
    name: str,
    expand_step: ExpandStep[StreamPayload, StreamPayload],
    message: Message[StreamPayload],
) -> list[Message[StreamPayload]]:
    with _observe_node(observer, flow_name, idx, name):
        return list(expand_step.execute(message))


def _execute_batch_expand_step(
    observer: StreamingFlowObserver | None,
    flow_name: str,
    idx: int,
    name: str,
    batch_expand_step: BatchExpandStep[StreamPayload, StreamPayload],
    messages: list[Message[StreamPayload]],
) -> list[Message[StreamPayload]]:
    with _observe_node(observer, flow_name, idx, name):
        return list(batch_expand_step.execute(messages))


def _execute_with_step(
    observer: StreamingFlowObserver | None,
    flow_name: str,
    idx: int,
    node_type: str,
    manager: ResourceLifecycle,
    worker_resources: Mapping[str, object],
    step: RecordStep[StreamPayload, StreamPayload],
    messages: list[Message[StreamPayload]],
) -> list[Message[StreamPayload]]:
    with (
        _observe_node(observer, flow_name, idx, node_type),
        _batch_dependencies(manager, worker_resources) as deps,
    ):
        result = [step.execute(message, **deps) for message in messages]
        return _replace_payloads(messages, result)


def _execute_with_async_step(
    bridge: AsyncBridge,
    observer: StreamingFlowObserver | None,
    flow_name: str,
    idx: int,
    node_type: str,
    batch_result: Coroutine[Any, Any, list[Message[StreamPayload]]],
) -> list[Message[StreamPayload]]:
    with _observe_node(observer, flow_name, idx, node_type):
        return bridge.run(batch_result)


def _execute_router_step(
    observer: StreamingFlowObserver | None,
    flow_name: str,
    idx: int,
    router: Router[StreamPayload, StreamPayload],
    message: Message[StreamPayload],
) -> Message[StreamPayload]:
    with _observe_node(observer, flow_name, idx, "Router"):
        return _execute_router(router, message)


def _apply_fork(stream: Stream, raw: object, idx: int, ctx: _BuildContextProtocol) -> Stream:
    fork = cast(Fork[StreamPayload], raw)
    if fork.kind is ForkKind.KEYED:
        return _apply_fork_by(stream, fork, idx, ctx)
    return _apply_fork_when(stream, fork, idx, ctx)


def _apply_fork_by(
    stream: Stream,
    fork: Fork[StreamPayload],
    idx: int,
    ctx: _BuildContextProtocol,
) -> Stream:
    selector = fork.selector
    if selector is None:
        raise RuntimeError("Fork.by requires a selector.")
    remaining = stream
    fork_path = ctx.current_path

    for branch_idx, (key, process) in enumerate(fork.routes.items()):
        branch_name = _step_id(f"fork_{idx}_by_{branch_idx}", ctx)

        def predicate(message: Any, *, expected: object = key) -> bool:
            runtime_message = _require_message(message)
            return select_value(selector, runtime_message) == expected

        split = branch(branch_name, remaining, predicate)
        _wire_process(
            split.trues,
            process.nodes,
            ctx,
            path_prefix=fork_path + (branch_idx,),
        )
        remaining = split.falses

    if fork.default is not None:
        _wire_process(
            remaining,
            fork.default.nodes,
            ctx,
            path_prefix=fork_path + (len(fork.routes),),
        )
    return remaining


def _apply_fork_when(
    stream: Stream,
    fork: Fork[StreamPayload],
    idx: int,
    ctx: _BuildContextProtocol,
) -> Stream:
    remaining = stream
    fork_path = ctx.current_path

    for branch_idx, route in enumerate(fork.predicate_routes):
        branch_name = _step_id(f"fork_{idx}_when_{branch_idx}", ctx)
        route_when = route.when

        def predicate(message: Any, *, when: Any = route_when) -> bool:
            runtime_message = _require_message(message)
            return evaluate_predicate(when, runtime_message)

        split = branch(branch_name, remaining, predicate)
        _wire_process(
            split.trues,
            route.process.nodes,
            ctx,
            path_prefix=fork_path + (branch_idx,),
        )
        remaining = split.falses

    if fork.default is not None:
        _wire_process(
            remaining,
            fork.default.nodes,
            ctx,
            path_prefix=fork_path + (len(fork.predicate_routes),),
        )
    return remaining


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


def _build_best_effort_step(
    node: WithAsync[StreamPayload, StreamPayload],
    manager: ResourceLifecycle,
    worker_resources: Mapping[str, object],
    bridge: AsyncBridge,
    observer: StreamingFlowObserver | None,
    flow_name: str,
    idx: int,
    node_type: str,
    sem: anyio.Semaphore,
) -> Callable[[list[Any]], list[NodeResult]]:
    """Build a step closure that captures per-message failures individually.

    Each message runs inside the TaskGroup as an independent task.  If a task
    raises, its slot is filled with the exception; sibling tasks are not
    cancelled.  The TaskGroup itself only cancels on external signals (e.g.
    runtime shutdown).
    """
    is_coroutine = inspect.iscoroutinefunction(node.step.execute)

    async def _execute_batch(batch: list[Any]) -> list[NodeResult]:
        messages = _messages_from_batch(batch)
        slot: list[object | BaseException] = [None] * len(messages)
        with _batch_dependencies(manager, worker_resources) as deps:
            async with anyio.create_task_group() as tg:
                for i, msg in enumerate(messages):
                    tg.start_soon(
                        _run_message_best_effort, node, sem, msg, deps, slot, i, is_coroutine
                    )
        return _build_best_effort_results(messages, slot)

    def step(batch: list[Any]) -> list[NodeResult]:
        with _observe_node(observer, flow_name, idx, node_type):
            return bridge.run(_execute_batch(batch))

    return step


def _build_fail_fast_step(
    node: WithAsync[StreamPayload, StreamPayload],
    manager: ResourceLifecycle,
    worker_resources: Mapping[str, object],
    bridge: AsyncBridge,
    observer: StreamingFlowObserver | None,
    flow_name: str,
    idx: int,
    node_type: str,
    sem: anyio.Semaphore,
) -> Callable[[list[Any]], list[NodeResult]]:
    """Build a step closure that cancels all sibling tasks on first failure.

    If any message task raises, the TaskGroup propagates the exception
    immediately.  The outer boundary then wraps every message in the batch
    as a single error envelope.  Use this for transactional steps where
    partial success is worse than total failure.
    """
    is_coroutine = inspect.iscoroutinefunction(node.step.execute)

    async def _execute_batch(
        messages: list[Message[StreamPayload]],
    ) -> list[Message[StreamPayload]]:
        slot: list[object] = [None] * len(messages)
        with _batch_dependencies(manager, worker_resources) as deps:
            async with anyio.create_task_group() as tg:
                for i, msg in enumerate(messages):
                    tg.start_soon(_run_message_strict, node, sem, msg, deps, slot, i, is_coroutine)
        return _replace_payloads(messages, slot)

    def step(batch: list[Any]) -> list[NodeResult]:
        messages = _messages_from_batch(batch)
        return _execute_batch_in_boundary(
            _classify_task,
            messages,
            lambda: _execute_with_async_step(
                bridge, observer, flow_name, idx, node_type, _execute_batch(messages)
            ),
        )

    return step


async def _await_with_optional_timeout(
    awaitable: Awaitable[object],
    timeout_ms: int | None,
) -> object:
    """Await *awaitable*, optionally bounded by *timeout_ms* milliseconds."""
    if timeout_ms is None:
        return await awaitable
    with anyio.fail_after(timeout_ms / 1000):
        return await awaitable


async def _invoke_execute(
    step: RecordStep[StreamPayload, StreamPayload],
    message: Message[StreamPayload],
    deps: Mapping[str, object],
    is_coroutine: bool,
    timeout_ms: int | None,
) -> object:
    """Invoke *step.execute* and await the result, sync or async transparently.

    When *is_coroutine* is ``True`` the return value is awaited directly.
    When ``False`` the call is dispatched to a thread via
    ``anyio.to_thread.run_sync`` so the event loop is not blocked.
    *timeout_ms* applies to both execution paths.
    """
    if is_coroutine:
        awaitable: Awaitable[object] = cast(
            Coroutine[Any, Any, object], step.execute(message, **deps)
        )
    else:
        awaitable = _run_in_thread(lambda: step.execute(message, **deps))
    return await _await_with_optional_timeout(awaitable, timeout_ms)


async def _run_message_best_effort(
    node: WithAsync[StreamPayload, StreamPayload],
    sem: anyio.Semaphore,
    message: Message[StreamPayload],
    deps: Mapping[str, object],
    slot: list[object | BaseException],
    idx: int,
    is_coroutine: bool,
) -> None:
    """Execute one message and capture any failure in *slot* without propagating."""
    try:
        async with sem:
            slot[idx] = await _invoke_execute(
                node.step, message, deps, is_coroutine, node.task_timeout_ms
            )
    except Exception as exc:
        slot[idx] = exc


async def _run_message_strict(
    node: WithAsync[StreamPayload, StreamPayload],
    sem: anyio.Semaphore,
    message: Message[StreamPayload],
    deps: Mapping[str, object],
    slot: list[object],
    idx: int,
    is_coroutine: bool,
) -> None:
    """Execute one message and let failures propagate to the TaskGroup."""
    async with sem:
        slot[idx] = await _invoke_execute(
            node.step, message, deps, is_coroutine, node.task_timeout_ms
        )


def _build_best_effort_results(
    messages: list[Message[StreamPayload]],
    slot: list[object | BaseException],
) -> list[NodeResult]:
    """Convert per-message outcomes into NodeResult — errors become ErrorEnvelope."""
    out: list[NodeResult] = []
    for msg, outcome in zip(messages, slot, strict=True):
        if isinstance(outcome, BaseException):
            out.append(
                ErrorEnvelope(
                    kind=ErrorKind.TASK,
                    reason=str(outcome),
                    original_message=msg,
                )
            )
        else:
            out.append(_replace_payload(msg, outcome))
    return out


def _batch_key(item: Any) -> str:
    """Return a grouping key for Bytewax ``collect``.

    Derives the key from ``MessageMeta`` when available so that Kafka-sourced
    flows preserve partition-level parallelism — each Bytewax worker handles
    its own partition independently.

    Priority:
        1. ``topic:partition`` when both are present in ``MessageMeta``.
        2. ``meta.key`` decoded to ``str`` when partition is absent.
        3. ``"loom"`` fallback for sources without transport metadata.
    """
    if not isinstance(item, Message):
        return "loom"
    meta = item.meta
    if meta.partition is not None:
        return f"{meta.topic or 'default'}:{meta.partition}"
    if meta.key is not None:
        raw_key = meta.key
        return raw_key if isinstance(raw_key, str) else raw_key.decode("utf-8", errors="replace")
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
    if isinstance(node, RouterBranchSafe) and isinstance(node, RecordStep):
        step = cast(RecordStep[StreamPayload, StreamPayload], node)
        return _replace_payload(message, step.execute(message))
    if isinstance(node, RouterBranchSafe) and isinstance(node, (IntoTopic, Drain)):
        return message
    raise TypeError(
        f"Router branch node {type(node).__name__} is not supported by Bytewax adapter."
    )


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


def _empty(_item: Any) -> tuple[()]:
    """Drop one item from a stream."""
    return ()


def _identity(items: Any) -> Any:
    """Pass through one item unchanged for flat_map."""
    return items


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
        Fork: _apply_fork,
        Router: _apply_router,
        Broadcast: _apply_broadcast,
        Drain: _apply_drain,
        IntoTopic: _apply_into_topic,
    }
)
