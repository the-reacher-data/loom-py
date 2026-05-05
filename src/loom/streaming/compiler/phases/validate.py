"""Validation phase for streaming flow compilation."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from omegaconf import DictConfig

from loom.core.config import ConfigError, section
from loom.streaming.core._exceptions import MissingSinkError, UnsupportedNodeError
from loom.streaming.core._typing import StreamPayload
from loom.streaming.graph._flow import StreamFlow
from loom.streaming.kafka._config import KafkaSettings
from loom.streaming.nodes._boundary import FromMultiTypeTopic, FromTopic, IntoTopic
from loom.streaming.nodes._broadcast import Broadcast
from loom.streaming.nodes._capabilities import RouterBranchSafe
from loom.streaming.nodes._fork import Fork, ForkKind
from loom.streaming.nodes._router import Router
from loom.streaming.nodes._shape import CollectBatch, Drain, ForEach, StreamShape, WindowStrategy
from loom.streaming.nodes._step import BatchExpandStep, BatchStep, ExpandStep, RecordStep
from loom.streaming.nodes._with import ResourceScope, With, WithAsync


def validate_kafka(flow: StreamFlow[Any, Any], cfg: DictConfig) -> list[str]:
    """Validate Kafka settings required by *flow*."""
    if not _uses_kafka(flow):
        return []
    try:
        section(cfg, "kafka", KafkaSettings)
        return []
    except ConfigError as exc:
        return [f"kafka: {exc}"]


def validate_resources(flow: StreamFlow[Any, Any]) -> list[str]:
    """Validate resource usage across the full process tree."""
    errors: list[str] = []
    for node in _walk_all_process_nodes(flow.process.nodes):
        if isinstance(node, (With, WithAsync)) and node.scope == ResourceScope.BATCH:
            direct_cms = list(node.sync_contexts.keys()) + list(node.async_contexts.keys())
            if direct_cms:
                errors.append(
                    f"{type(node).__name__} with scope=BATCH cannot use direct context "
                    f"manager instances: {', '.join(direct_cms)}. "
                    f"Use ContextFactory for batch-scoped resources."
                )
    return errors


def validate_shapes(flow: StreamFlow[Any, Any]) -> list[str]:
    """Validate shape transitions through the process tree."""
    errors, _ = _validate_shape_sequence(flow.process.nodes, flow.source.shape)
    errors.extend(_validate_window_strategies(flow.process.nodes))
    errors.extend(_validate_scoped_process_nodes(_walk_all_process_nodes(flow.process.nodes)))
    return errors


def validate_outputs(flow: StreamFlow[Any, Any]) -> list[str]:
    """Validate terminal outputs and branch terminality."""
    errors: list[str] = []
    has_terminal = flow.output is not None

    if _has_terminal_output(flow.process.nodes):
        has_terminal = True

    if flow.output is not None and _contains_fork(flow.process.nodes):
        errors.append(
            str(
                UnsupportedNodeError(
                    "flow.output cannot be combined with Fork: branches must be terminal"
                )
            )
        )

    if flow.output is not None and _contains_broadcast(flow.process.nodes):
        errors.append(
            str(
                UnsupportedNodeError(
                    "flow.output cannot be combined with Broadcast: branches must be terminal"
                )
            )
        )

    if not has_terminal:
        errors.append(
            str(MissingSinkError("no terminal output found: add IntoTopic or flow.output"))
        )

    return errors


def _uses_kafka(flow: StreamFlow[Any, Any]) -> bool:
    if isinstance(flow.source, (FromTopic, FromMultiTypeTopic)):
        return True
    if flow.output is not None:
        return True
    return _has_terminal_output(flow.process.nodes)


def _iter_child_node_groups(node: object) -> Iterable[Iterable[object]]:
    if isinstance(node, Router):
        for _, branch_nodes in _router_branch_nodes(node):
            yield branch_nodes
    elif isinstance(node, Fork):
        for _, branch_nodes in _fork_branch_nodes(node):
            yield branch_nodes
    elif isinstance(node, Broadcast):
        for route in node.routes:
            yield route.process.nodes
    elif isinstance(node, WithAsync) or (isinstance(node, With) and node.process is not None):
        yield node.process.nodes


def _walk_all_process_nodes(nodes: Iterable[object]) -> Iterable[object]:
    for node in nodes:
        yield node
        for child_nodes in _iter_child_node_groups(node):
            yield from _walk_all_process_nodes(child_nodes)


def _node_needs_async_bridge(node: object) -> bool:
    return isinstance(node, WithAsync)


def _check_input_shape(
    node: object,
    current_shape: StreamShape,
    errors: list[str],
) -> None:
    expected = _node_input_shape(node)
    if expected is not None and current_shape != expected:
        errors.append(
            f"shape mismatch: expected {expected.value} but got {current_shape.value} "
            f"before {type(node).__name__}"
        )


def _must_be_last_errors(idx: int, node_list: tuple[object, ...], message: str) -> list[str]:
    if idx != len(node_list) - 1:
        return [message]
    return []


def _is_scoped_process(node: object) -> bool:
    return isinstance(node, (WithAsync, With))


def _validate_shape_sequence(
    nodes: Iterable[object],
    initial_shape: StreamShape,
) -> tuple[list[str], StreamShape]:
    errors: list[str] = []
    current_shape = initial_shape
    node_list = tuple(nodes)

    for idx, node in enumerate(node_list):
        _check_input_shape(node, current_shape, errors)

        if isinstance(node, (IntoTopic, Drain)):
            errors.extend(
                _must_be_last_errors(
                    idx, node_list, f"{type(node).__name__} must be the last node in a process"
                )
            )
            break

        if isinstance(node, Fork):
            fork_errors, current_shape = _validate_fork_shapes(node, current_shape)
            errors.extend(fork_errors)
            errors.extend(
                _must_be_last_errors(idx, node_list, "fork must be the last node in a process")
            )
            break

        if isinstance(node, Broadcast):
            broadcast_errors, current_shape = _validate_broadcast_shapes(node, current_shape)
            errors.extend(broadcast_errors)
            errors.extend(
                _must_be_last_errors(idx, node_list, "broadcast must be the last node in a process")
            )
            break

        if _is_scoped_process(node):
            errors.extend(
                _must_be_last_errors(
                    idx,
                    node_list,
                    f"{type(node).__name__}(process=...) must be the last node in a process",
                )
            )
            current_shape = StreamShape.NONE
            break

        if isinstance(node, Router):
            router_errors, current_shape = _validate_router_shapes(node, current_shape)
            errors.extend(router_errors)
            continue

        current_shape = _node_output_shape(node, current_shape)

    return errors, current_shape


def _validate_fork_shapes(
    fork: Fork[StreamPayload],
    initial_shape: StreamShape,
) -> tuple[list[str], StreamShape]:
    errors: list[str] = []

    for label, nodes in _fork_branch_nodes(fork):
        branch_errors, _ = _validate_shape_sequence(nodes, initial_shape)
        errors.extend(f"fork branch {label}: {error}" for error in branch_errors)
        if not _has_terminal_output(nodes):
            errors.append(f"fork branch {label}: no terminal output found")

    return errors, StreamShape.NONE


def _validate_broadcast_shapes(
    broadcast: Broadcast[Any],
    initial_shape: StreamShape,
) -> tuple[list[str], StreamShape]:
    errors: list[str] = []
    for branch_idx, route in enumerate(broadcast.routes):
        branch_errors, _ = _validate_shape_sequence(route.process.nodes, initial_shape)
        errors.extend(f"broadcast branch {branch_idx}: {e}" for e in branch_errors)
    return errors, StreamShape.NONE


def _validate_router_branch_shape_sequence(
    nodes: Iterable[object],
    initial_shape: StreamShape,
) -> tuple[list[str], StreamShape]:
    errors: list[str] = []
    current_shape = initial_shape
    node_list = tuple(nodes)

    for idx, node in enumerate(node_list):
        if isinstance(node, BatchStep):
            current_shape = StreamShape.RECORD
            continue
        expected = _node_input_shape(node)
        if expected is not None and current_shape != expected:
            errors.append(
                f"shape mismatch: expected {expected.value} but got {current_shape.value} "
                f"before {type(node).__name__}"
            )
        if isinstance(node, (IntoTopic, Drain)) and idx != len(node_list) - 1:
            errors.append(f"{type(node).__name__} must be the last node in a process")
            break
        current_shape = _node_output_shape(node, current_shape)

    return errors, current_shape


def _validate_router_shapes(
    router: Router[StreamPayload, StreamPayload],
    initial_shape: StreamShape,
) -> tuple[list[str], StreamShape]:
    errors: list[str] = []
    outputs: list[StreamShape] = []

    for label, nodes in _router_branch_nodes(router):
        branch_errors, branch_output = _validate_router_branch_shape_sequence(nodes, initial_shape)
        errors.extend(f"router branch {label}: {error}" for error in branch_errors)
        for node in nodes:
            if not isinstance(node, RouterBranchSafe):
                errors.append(
                    f"router branch {label}: node {type(node).__name__} is not router-branch safe"
                )
            elif isinstance(node, (ExpandStep, BatchExpandStep)):
                errors.append(
                    f"router branch {label}: {type(node).__name__} is not supported in Router "
                    f"branches — Router is 1-to-1; use Fork for fan-out."
                )
        outputs.append(branch_output)

    unique_outputs = set(outputs)
    if len(unique_outputs) > 1:
        ordered = ", ".join(sorted(shape.value for shape in unique_outputs))
        errors.append(f"router branches produce different shapes: {ordered}")

    return errors, outputs[0] if outputs else initial_shape


def _router_branch_nodes(
    router: Router[StreamPayload, StreamPayload],
) -> Iterable[tuple[str, tuple[object, ...]]]:
    for key, process in router.routes.items():
        yield repr(key), process.nodes
    for index, route in enumerate(router.predicate_routes):
        yield f"predicate[{index}]", route.process.nodes
    if router.default is not None:
        yield "default", router.default.nodes


def _fork_branch_nodes(
    fork: Fork[StreamPayload],
) -> Iterable[tuple[str, tuple[object, ...]]]:
    if fork.kind is ForkKind.KEYED:
        for key, process in fork.routes.items():
            yield repr(key), process.nodes
        return
    for index, route in enumerate(fork.predicate_routes):
        yield f"predicate[{index}]", route.process.nodes


def _fork_branch_count(fork: Fork[StreamPayload]) -> int:
    if fork.kind is ForkKind.KEYED:
        return len(fork.routes)
    return len(fork.predicate_routes)


def _node_has_terminal_output(node: object) -> bool:
    if isinstance(node, (IntoTopic, Drain)):
        return True
    if isinstance(node, Router):
        return _router_has_terminal_output(node)
    if isinstance(node, Fork):
        return _fork_has_terminal_output(node)
    if isinstance(node, Broadcast):
        return True
    if isinstance(node, WithAsync):
        return _has_terminal_output(node.process.nodes)
    if isinstance(node, With):
        return _has_terminal_output(node.process.nodes)
    return False


def _has_terminal_output(nodes: Iterable[object]) -> bool:
    return any(_node_has_terminal_output(node) for node in nodes)


def _router_has_terminal_output(router: Router[StreamPayload, StreamPayload]) -> bool:
    return any(_has_terminal_output(nodes) for _, nodes in _router_branch_nodes(router))


def _fork_has_terminal_output(fork: Fork[StreamPayload]) -> bool:
    return any(_has_terminal_output(nodes) for _, nodes in _fork_branch_nodes(fork))


def _contains_fork(nodes: Iterable[object]) -> bool:
    return any(isinstance(node, Fork) for node in nodes)


def _contains_broadcast(nodes: Iterable[object]) -> bool:
    return any(isinstance(node, Broadcast) for node in nodes)


def _node_input_shape(node: object) -> StreamShape | None:
    if isinstance(node, RecordStep):
        return StreamShape.RECORD
    if isinstance(node, BatchStep):
        return StreamShape.BATCH
    if isinstance(node, ExpandStep):
        return StreamShape.RECORD
    if isinstance(node, BatchExpandStep):
        return StreamShape.BATCH
    if isinstance(node, (With, WithAsync)):
        return None
    if isinstance(node, ForEach):
        return StreamShape.MANY
    if isinstance(node, Drain):
        return None
    if isinstance(node, Fork):
        return None
    return None


def _validate_window_strategies(nodes: Iterable[object]) -> list[str]:
    errors: list[str] = []
    for node in nodes:
        if isinstance(node, CollectBatch) and node.window is not WindowStrategy.COLLECT:
            errors.append(
                f"CollectBatch.window={node.window} is not yet supported by the Bytewax adapter. "
                f"Only WindowStrategy.COLLECT is available in this adapter version."
            )
    return errors


def _validate_scoped_process_nodes(nodes: Iterable[object]) -> list[str]:
    errors: list[str] = []
    for node in nodes:
        if not isinstance(node, (With, WithAsync)):
            continue
        inner_nodes = tuple(node.process.nodes)
        for idx, inner_node in enumerate(inner_nodes):
            if isinstance(inner_node, RecordStep):
                continue
            if isinstance(inner_node, IntoTopic):
                if idx != len(inner_nodes) - 1:
                    errors.append(
                        f"{type(node).__name__}(process=...) requires IntoTopic to be last; "
                        f"found {type(inner_nodes[idx + 1]).__name__} after it."
                    )
                continue
            errors.append(
                f"{type(node).__name__}(process=...) only supports RecordStep nodes and an "
                f"optional terminal IntoTopic; found {type(inner_node).__name__}."
            )
    return errors


def _node_output_shape(node: object, current: StreamShape) -> StreamShape:
    if isinstance(node, CollectBatch):
        return StreamShape.BATCH
    if isinstance(node, ForEach):
        return StreamShape.RECORD
    if isinstance(node, RecordStep):
        return StreamShape.RECORD
    if isinstance(node, BatchStep):
        return StreamShape.BATCH
    if isinstance(node, ExpandStep):
        return StreamShape.RECORD
    if isinstance(node, BatchExpandStep):
        return StreamShape.RECORD
    if isinstance(node, WithAsync):
        return StreamShape.NONE
    if isinstance(node, With):
        if node.process is not None:
            return StreamShape.NONE
        return StreamShape.MANY
    if isinstance(node, IntoTopic):
        return node.shape
    if isinstance(node, Drain):
        return StreamShape.NONE
    if isinstance(node, Fork):
        return StreamShape.NONE
    if isinstance(node, Broadcast):
        return StreamShape.NONE
    return current
