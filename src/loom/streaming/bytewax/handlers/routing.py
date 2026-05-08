"""Bytewax handler family for routing nodes."""

from __future__ import annotations

from typing import Any, cast

from bytewax.operators import branch
from bytewax.operators import map as bw_map

from loom.core.observability.runtime import ObservabilityRuntime
from loom.streaming.bytewax._error_boundary import _classify_routing, _execute_in_boundary
from loom.streaming.bytewax.handlers._shared import (
    _BuildContextProtocol,
    _ExecutableBatchStep,
    _ExecutableRecordStep,
    _observe_node,
    _register_broadcast_fanout,
    _require_message,
    _step_id,
)
from loom.streaming.core._exceptions import UnsupportedNodeError
from loom.streaming.nodes._boundary import IntoTopic
from loom.streaming.nodes._broadcast import Broadcast
from loom.streaming.nodes._capabilities import RouterBranchSafe
from loom.streaming.nodes._fork import Fork, ForkKind
from loom.streaming.nodes._router import Router, evaluate_predicate, select_value
from loom.streaming.nodes._shape import Drain
from loom.streaming.nodes._step import BatchStep, RecordStep

Stream = Any


def _apply_router(stream: Stream, raw: object, idx: int, ctx: _BuildContextProtocol) -> Stream:
    if not isinstance(raw, Router):
        raise UnsupportedNodeError(f"Unsupported router node {type(raw).__name__}.")
    router = raw
    observer = ctx.flow_runtime
    flow_name = ctx.plan.name

    def step(msg: Any) -> Any:
        message = _require_message(msg)
        return _execute_in_boundary(
            _classify_routing,
            message,
            lambda: _execute_router_step(observer, flow_name, idx, router, message),
        )

    sid = _step_id(f"router_{idx}", ctx)
    mapped = bw_map(sid, stream, step)
    from loom.streaming.bytewax._error_boundary import _split_node_result
    from loom.streaming.core._errors import ErrorKind

    return _split_node_result(mapped, sid, ctx, ErrorKind.ROUTING)


def _execute_router_step(
    observer: ObservabilityRuntime,
    flow_name: str,
    idx: int,
    router: Router[Any, Any],
    message: Any,
) -> Any:
    with _observe_node(
        observer,
        flow_name,
        idx,
        "Router",
        trace_id=message.meta.trace_id,
        correlation_id=message.meta.correlation_id,
    ):
        return _execute_router(router, message)


def _apply_broadcast(
    stream: Stream,
    raw: object,
    idx: int,
    ctx: _BuildContextProtocol,
) -> Stream:
    if not isinstance(raw, Broadcast):
        raise UnsupportedNodeError(f"Unsupported broadcast node {type(raw).__name__}.")
    node = raw
    broadcast_path = ctx.current_path
    tracker = ctx.commit_tracker
    if tracker is not None and len(node.routes) > 1:
        stream = bw_map(
            _step_id(f"broadcast_{idx}_fanout", ctx),
            stream,
            lambda item: _register_broadcast_fanout(item, tracker, len(node.routes)),
        )

    for branch_idx, route in enumerate(node.routes):
        branch_stream = ctx.wire_process(
            stream,
            route.process.nodes,
            path_prefix=broadcast_path + (branch_idx,),
        )
        ctx.wire_branch_terminal(
            f"broadcast_{idx}_out_{branch_idx}",
            branch_stream,
            broadcast_path + (branch_idx,),
        )

    return stream


def _apply_fork(stream: Stream, raw: object, idx: int, ctx: _BuildContextProtocol) -> Stream:
    if not isinstance(raw, Fork):
        raise UnsupportedNodeError(f"Unsupported fork node {type(raw).__name__}.")
    fork = raw
    if fork.kind is ForkKind.KEYED:
        return _apply_fork_by(stream, fork, idx, ctx)
    return _apply_fork_when(stream, fork, idx, ctx)


def _apply_fork_by(
    stream: Stream,
    fork: Fork[Any],
    idx: int,
    ctx: _BuildContextProtocol,
) -> Stream:
    selector = fork.selector
    if selector is None:
        raise UnsupportedNodeError("Fork.by requires a selector.")
    remaining = stream
    fork_path = ctx.current_path

    for branch_idx, (key, process) in enumerate(fork.routes.items()):
        branch_name = _step_id(f"fork_{idx}_by_{branch_idx}", ctx)

        def predicate(message: Any, *, expected: object = key) -> bool:
            runtime_message = _require_message(message)
            return select_value(selector, runtime_message) == expected

        split = branch(branch_name, remaining, predicate)
        ctx.wire_process(
            split.trues,
            process.nodes,
            path_prefix=fork_path + (branch_idx,),
        )
        remaining = split.falses

    if fork.default is not None:
        ctx.wire_process(
            remaining,
            fork.default.nodes,
            path_prefix=fork_path + (len(fork.routes),),
        )
    return remaining


def _apply_fork_when(
    stream: Stream,
    fork: Fork[Any],
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
        ctx.wire_process(
            split.trues,
            route.process.nodes,
            path_prefix=fork_path + (branch_idx,),
        )
        remaining = split.falses

    if fork.default is not None:
        ctx.wire_process(
            remaining,
            fork.default.nodes,
            path_prefix=fork_path + (len(fork.predicate_routes),),
        )
    return remaining


def _execute_router(
    router: Router[Any, Any],
    message: Any,
) -> Any:
    branch_nodes = _select_router_branch(router, message)
    result = message
    for node in branch_nodes:
        result = _execute_router_node(node, result)
    return result


def _select_router_branch(router: Router[Any, Any], message: Any) -> tuple[object, ...]:
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


def _execute_router_node(node: object, message: Any) -> Any:
    if isinstance(node, RouterBranchSafe) and isinstance(node, BatchStep):
        from loom.streaming.bytewax.handlers._shared import _replace_payload, _resolve_batch_result

        batch_node = cast(_ExecutableBatchStep, node)
        results = _resolve_batch_result(batch_node.execute([message]), "Router")
        return _replace_payload(message, results[0])
    if isinstance(node, RouterBranchSafe) and isinstance(node, RecordStep):
        from loom.streaming.bytewax.handlers._shared import _replace_payload, _resolve_record_result

        record_node = cast(_ExecutableRecordStep, node)
        return _replace_payload(
            message,
            _resolve_record_result(record_node.execute(message), "Router"),
        )
    if isinstance(node, RouterBranchSafe) and isinstance(node, (IntoTopic, Drain)):
        return message
    raise TypeError(
        f"Router branch node {type(node).__name__} is not supported by Bytewax adapter."
    )
