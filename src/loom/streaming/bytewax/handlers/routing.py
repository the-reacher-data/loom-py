"""Bytewax handler family for routing nodes."""

from __future__ import annotations

from typing import Any, cast

from bytewax.operators import branch
from bytewax.operators import flat_map as bw_flat_map
from bytewax.operators import map as bw_map

from loom.core.observability.runtime import ObservabilityRuntime
from loom.streaming.bytewax._error_boundary import _classify_routing, _execute_in_boundary
from loom.streaming.bytewax.handlers._shared import (
    _BuildContextProtocol,
    _ExecutableBatchStep,
    _ExecutableRecordStep,
    _observe_node,
    _register_broadcast_fanout,
    _replace_payload,
    _require_message,
    _step_id,
)
from loom.streaming.core._exceptions import UnsupportedNodeError
from loom.streaming.core._message import Message
from loom.streaming.nodes._boundary import IntoTopic
from loom.streaming.nodes._broadcast import Broadcast
from loom.streaming.nodes._capabilities import RouterBranchSafe
from loom.streaming.nodes._expand_routes import ExpandRoutes
from loom.streaming.nodes._fork import Fork, ForkKind
from loom.streaming.nodes._router import Router, evaluate_predicate, select_value
from loom.streaming.nodes._shape import Drain
from loom.streaming.nodes._sink import IntoSink
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


def _apply_expand_routes(
    stream: Stream,
    raw: object,
    idx: int,
    ctx: _BuildContextProtocol,
) -> Stream:
    if not isinstance(raw, ExpandRoutes):
        raise UnsupportedNodeError(f"Unsupported expand_routes node {type(raw).__name__}.")
    node = raw
    expand_path = ctx.current_path
    tracker = ctx.commit_tracker
    all_processes: list[tuple[type | None, Any]] = list(node.routes.items())
    if node.default is not None:
        all_processes.append((None, node.default))
    route_count = len(all_processes)

    # Step 1: expand once — payload becomes dict[type, list[rows]].
    # Use Message() directly: _replace_payload rejects non-LoomStruct payloads.
    def do_expand(msg: Any) -> Any:
        message = _require_message(msg)
        expanded: Any = node.expander.expand(message.payload)
        return Message(payload=cast(Any, expanded), meta=message.meta)

    expanded_stream = bw_map(
        _step_id(f"expand_routes_{idx}_expand", ctx),
        stream,
        do_expand,
    )

    # Step 2: fanout tracking for Kafka offset commits (same as Broadcast)
    if tracker is not None and route_count > 1:
        expanded_stream = bw_map(
            _step_id(f"expand_routes_{idx}_fanout", ctx),
            expanded_stream,
            lambda item: _register_broadcast_fanout(item, tracker, route_count),
        )

    # Step 3: for each route, flat_map to extract rows of its type, then wire process
    for branch_idx, (output_type, process) in enumerate(all_processes):

        def extract_rows(msg: Any, t: type | None = output_type) -> list[Any]:
            message = _require_message(msg)
            expanded = cast(dict[type, list[Any]], message.payload)
            if t is None:
                # default route: collect rows for types not in declared routes
                declared = set(node.routes.keys())
                rows = [r for tp, rs in expanded.items() if tp not in declared for r in rs]
            else:
                rows = expanded.get(t) or []
            return [_replace_payload(message, row) for row in rows]

        route_stream = bw_flat_map(
            _step_id(f"expand_routes_{idx}_extract_{branch_idx}", ctx),
            expanded_stream,
            extract_rows,
        )
        ctx.wire_process(
            route_stream,
            process.nodes,
            path_prefix=expand_path + (branch_idx,),
        )
        ctx.wire_branch_terminal(
            f"expand_routes_{idx}_out_{branch_idx}",
            route_stream,
            expand_path + (branch_idx,),
        )

    return expanded_stream


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
    if isinstance(node, RouterBranchSafe) and isinstance(node, (IntoTopic, Drain, IntoSink)):
        return message
    raise TypeError(
        f"Router branch node {type(node).__name__} is not supported by Bytewax adapter."
    )
