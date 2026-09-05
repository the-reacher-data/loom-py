"""Bytewax handler family for routing nodes."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any, Final, cast

from bytewax.operators import branch as bw_branch
from bytewax.operators import flat_map as bw_flat_map
from bytewax.operators import map as bw_map

from loom.core.observability.runtime import ObservabilityRuntime
from loom.streaming.bytewax._error_boundary import (
    ErrorBoundary,
    _classify_routing,
    _execute_in_boundary,
)
from loom.streaming.bytewax.handlers._shared import (
    _BuildContextProtocol,
    _ExecutableBatchStep,
    _ExecutableRecordStep,
    _observe_node,
    _register_broadcast_fanout,
    _register_row_fanout,
    _replace_payload,
    _require_message,
    _resolve_batch_result,
    _resolve_record_result,
    _step_id,
)
from loom.streaming.core._exceptions import UnsupportedNodeError
from loom.streaming.core._message import Message
from loom.streaming.nodes._boundary import IntoTopic
from loom.streaming.nodes._branches import Branch, iter_branches
from loom.streaming.nodes._broadcast import Broadcast
from loom.streaming.nodes._capabilities import RouterBranchSafe
from loom.streaming.nodes._expand_routes import ExpandRoutes
from loom.streaming.nodes._fork import Fork, ForkKind
from loom.streaming.nodes._router import Router, evaluate_predicate, select_value
from loom.streaming.nodes._shape import Drain
from loom.streaming.nodes._sink import IntoSink
from loom.streaming.nodes._step import BatchStep, RecordStep

Stream = Any

_NO_KEY: Final = object()
"""Sentinel for a router without a selector, which no keyed branch can match."""


def _apply_router(stream: Stream, raw: object, idx: int, ctx: _BuildContextProtocol) -> Stream:
    if not isinstance(raw, Router):
        raise UnsupportedNodeError(f"Unsupported router node {type(raw).__name__}.")
    router = raw
    observer = ctx.flow_runtime
    flow_name = ctx.plan.name
    boundary = ErrorBoundary(observer=observer, flow=flow_name)
    branches = tuple(iter_branches(router))

    def step(msg: Any) -> Any:
        message = _require_message(msg)
        return _execute_in_boundary(
            _classify_routing,
            message,
            lambda: _execute_router_step(observer, flow_name, idx, router, branches, message),
            boundary,
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
    branches: Sequence[Branch],
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
        return _execute_router(router, branches, message)


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

    for branch in iter_branches(node):
        branch_path = broadcast_path + (branch.index,)
        branch_stream = ctx.wire_process(stream, branch.nodes, path_prefix=branch_path)
        ctx.wire_branch_terminal(
            f"broadcast_{idx}_out_{branch.index}",
            branch_stream,
            branch_path,
        )

    return stream


def _row_extractor(
    node: ExpandRoutes[Any],
    output_type: type | None,
) -> Callable[[Any], list[Any]]:
    """Build the extractor that turns one expanded payload into a route's rows.

    ``output_type`` of ``None`` is the default route: it collects the rows of
    every type no declared route claims.
    """
    declared = frozenset(node.routes.keys())

    def extract_rows(msg: Any) -> list[Any]:
        message = _require_message(msg)
        expanded = cast(dict[type, list[Any]], message.payload)
        if output_type is None:
            rows = [row for tp, rs in expanded.items() if tp not in declared for row in rs]
        else:
            rows = expanded.get(output_type) or []
        return [_replace_payload(message, row) for row in rows]

    return extract_rows


def _wire_row_fanout(
    stream: Stream,
    node: ExpandRoutes[Any],
    idx: int,
    ctx: _BuildContextProtocol,
) -> Stream:
    """Insert the commit-accounting step ahead of the per-route extraction.

    The fan-out is the number of ROWS the expander actually produced across
    every route, not the number of routes declared: each row becomes its own
    message and completes the source offset when it reaches a terminal, while a
    route matching no row contributes nothing. Accounting by the declared route
    count froze the partition whenever rows < routes and released the offset
    early whenever rows > routes.

    The step is wired even without a commit tracker, because it is also the
    only place that can see a message expand to zero rows and record that it
    died there.
    """
    tracker = ctx.commit_tracker
    observer = ctx.flow_runtime
    flow_name = ctx.plan.name
    declared_types = frozenset(node.routes.keys())
    has_default = node.default is not None
    return bw_map(
        _step_id(f"expand_routes_{idx}_fanout", ctx),
        stream,
        lambda item: _register_row_fanout(
            item, tracker, declared_types, has_default, observer, flow_name
        ),
    )


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

    expanded_stream = _wire_row_fanout(expanded_stream, node, idx, ctx)

    # Step 3: for each route, flat_map to extract rows of its type, then wire process
    for branch in iter_branches(node):
        branch_path = expand_path + (branch.index,)
        output_type = None if branch.is_default else cast(type, branch.key)
        route_stream = bw_flat_map(
            _step_id(f"expand_routes_{idx}_extract_{branch.index}", ctx),
            expanded_stream,
            _row_extractor(node, output_type),
        )
        ctx.wire_process(route_stream, branch.nodes, path_prefix=branch_path)
        ctx.wire_branch_terminal(
            f"expand_routes_{idx}_out_{branch.index}",
            route_stream,
            branch_path,
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

    for branch in iter_branches(fork):
        branch_path = fork_path + (branch.index,)
        if branch.is_default:
            ctx.wire_process(remaining, branch.nodes, path_prefix=branch_path)
            continue
        branch_name = _step_id(f"fork_{idx}_by_{branch.index}", ctx)

        def predicate(message: Any, *, expected: object = branch.key) -> bool:
            runtime_message = _require_message(message)
            return select_value(selector, runtime_message) == expected

        split = bw_branch(branch_name, remaining, predicate)
        ctx.wire_process(split.trues, branch.nodes, path_prefix=branch_path)
        remaining = split.falses

    return remaining


def _apply_fork_when(
    stream: Stream,
    fork: Fork[Any],
    idx: int,
    ctx: _BuildContextProtocol,
) -> Stream:
    remaining = stream
    fork_path = ctx.current_path

    for branch in iter_branches(fork):
        branch_path = fork_path + (branch.index,)
        if branch.is_default:
            ctx.wire_process(remaining, branch.nodes, path_prefix=branch_path)
            continue
        branch_name = _step_id(f"fork_{idx}_when_{branch.index}", ctx)

        def predicate(message: Any, *, when: Any = branch.when) -> bool:
            runtime_message = _require_message(message)
            return evaluate_predicate(when, runtime_message)

        split = bw_branch(branch_name, remaining, predicate)
        ctx.wire_process(split.trues, branch.nodes, path_prefix=branch_path)
        remaining = split.falses

    return remaining


def _execute_router(router: Router[Any, Any], branches: Sequence[Branch], message: Any) -> Any:
    branch = _select_router_branch(router, branches, message)
    if branch is None:
        return message
    result = message
    for node in branch.nodes:
        result = _execute_router_node(node, result)
    return result


def _select_router_branch(
    router: Router[Any, Any], branches: Sequence[Branch], message: Any
) -> Branch | None:
    """Return the branch of *branches* that claims *message*, the fallback, or ``None``."""
    key = select_value(router.selector, message) if router.selector is not None else _NO_KEY
    fallback: Branch | None = None
    for branch in branches:
        if branch.is_default:
            fallback = branch
        elif branch.when is not None:
            if evaluate_predicate(branch.when, message):
                return branch
        elif key is not _NO_KEY and branch.key == key:
            return branch
    return fallback


def _execute_router_node(node: object, message: Any) -> Any:
    if isinstance(node, RouterBranchSafe) and isinstance(node, BatchStep):
        batch_node = cast(_ExecutableBatchStep, node)
        results = _resolve_batch_result(batch_node.execute([message]), "Router")
        return _replace_payload(message, results[0])
    if isinstance(node, RouterBranchSafe) and isinstance(node, RecordStep):
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
