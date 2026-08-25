"""Validation phase for streaming flow compilation.

Each validator returns a list of :class:`CompilationIssue` — structured
failures with machine-readable codes — accumulated by the compiler into a
single :class:`~loom.streaming.compiler.CompilationError`.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any, cast

from loom.core.config import ConfigContext, ConfigError
from loom.core.config.keys import ConfigKey
from loom.streaming.compiler._errors import (
    CompilationIssue,
    batch_scope_direct_context,
    broadcast_not_last,
    delivery_conflict,
    explode_without_router,
    fork_branch_no_terminal,
    fork_not_last,
    kafka_config_invalid,
    missing_terminal_output,
    mongo_config_invalid,
    output_with_broadcast,
    output_with_fork,
    router_branch_fanout_unsupported,
    router_branch_shape_divergence,
    router_branch_unsafe_node,
    scoped_into_topic_not_last,
    scoped_process_not_last,
    scoped_process_unsupported_node,
    shape_mismatch,
    sink_config_invalid,
    sink_missing_name,
    terminal_not_last,
    window_strategy_unsupported,
)
from loom.streaming.core._typing import StreamPayload
from loom.streaming.graph._flow import StreamFlow
from loom.streaming.kafka._config import ConsumerSettings, KafkaSettings
from loom.streaming.mongo import MongoConfig
from loom.streaming.nodes._boundary import FromMultiTypeTopic, FromTopic, IntoTopic
from loom.streaming.nodes._broadcast import Broadcast
from loom.streaming.nodes._capabilities import RouterBranchSafe
from loom.streaming.nodes._decompose import Explode
from loom.streaming.nodes._expand_routes import ExpandRoutes
from loom.streaming.nodes._fork import Fork, ForkKind
from loom.streaming.nodes._mongo import FromMongoCDC
from loom.streaming.nodes._router import Router
from loom.streaming.nodes._shape import CollectBatch, Drain, ForEach, StreamShape, WindowStrategy
from loom.streaming.nodes._sink import IntoSink
from loom.streaming.nodes._step import BatchExpandStep, BatchStep, ExpandStep, RecordStep
from loom.streaming.nodes._table import Backend, IntoTable
from loom.streaming.nodes._table.config import (
    resolve_delta_table_config,
    resolve_sqlalchemy_table_config,
)
from loom.streaming.nodes._with import ResourceScope, With, WithAsync


def validate_storage_sinks(
    flow: StreamFlow[Any, Any],
    ctx: ConfigContext,
) -> list[CompilationIssue]:
    """Validate that every named IntoSink node has a config section at streaming.sinks.<name>."""
    errors: list[CompilationIssue] = []
    for node in _iter_unique_storage_sinks(flow.process.nodes, errors):
        errors.extend(_validate_storage_sink_node(node, ctx))
    return errors


def validate_kafka(flow: StreamFlow[Any, Any], ctx: ConfigContext) -> list[CompilationIssue]:
    """Validate Kafka settings required by *flow*."""
    if not _uses_kafka(flow):
        return []
    try:
        ctx.section(ConfigKey.KAFKA, KafkaSettings)
        return []
    except ConfigError as exc:
        return [kafka_config_invalid(exc)]


def validate_delivery(flow: StreamFlow[Any, Any], ctx: ConfigContext) -> list[CompilationIssue]:
    """Validate that explicit delivery semantics do not contradict legacy flags.

    Emits :data:`StreamingErrorCode.DELIVERY_CONFLICT` when a resolved consumer
    sets ``delivery`` and an explicit ``enable_auto_commit`` that contradicts
    it.  Missing Kafka config is reported by :func:`validate_kafka`, not here.
    """
    if not _uses_kafka(flow):
        return []
    if not isinstance(flow.source, (FromTopic, FromMultiTypeTopic)):
        return []
    consumer = _resolve_consumer_settings(flow.source, ctx)
    if consumer is None:
        return []
    return _delivery_conflict_issues(flow.source.name, consumer)


def validate_mongo(flow: StreamFlow[Any, Any], ctx: ConfigContext) -> list[CompilationIssue]:
    """Validate Mongo settings required by *flow*."""
    if not isinstance(flow.source, FromMongoCDC):
        return []
    try:
        mongo = ctx.section(ConfigKey.MONGO, MongoConfig)
        mongo.source_for(flow.source.logical_ref)
        return []
    except (ConfigError, KeyError) as exc:
        return [mongo_config_invalid(flow.source.name, exc)]


def validate_resources(flow: StreamFlow[Any, Any]) -> list[CompilationIssue]:
    """Validate resource usage across the full process tree."""
    errors: list[CompilationIssue] = []
    for node in _walk_all_process_nodes(flow.process.nodes):
        if isinstance(node, (With, WithAsync)) and node.scope == ResourceScope.BATCH:
            direct_cms = list(node.sync_contexts.keys()) + list(node.async_contexts.keys())
            if direct_cms:
                errors.append(batch_scope_direct_context(node, direct_cms))
    return errors


def validate_shapes(flow: StreamFlow[Any, Any]) -> list[CompilationIssue]:
    """Validate shape transitions through the process tree."""
    errors, _ = _validate_shape_sequence(flow.process.nodes, flow.source.shape)
    errors.extend(_validate_window_strategies(flow.process.nodes))
    errors.extend(_validate_scoped_process_nodes(_walk_all_process_nodes(flow.process.nodes)))
    return errors


def validate_outputs(flow: StreamFlow[Any, Any]) -> list[CompilationIssue]:
    """Validate terminal outputs and branch terminality."""
    errors: list[CompilationIssue] = []
    has_terminal = flow.output is not None or _has_terminal_output(flow.process.nodes)

    if flow.output is not None and _contains_fork(flow.process.nodes):
        errors.append(output_with_fork())

    if flow.output is not None and _contains_broadcast(flow.process.nodes):
        errors.append(output_with_broadcast())

    if not has_terminal:
        errors.append(missing_terminal_output())

    return errors


def _uses_kafka(flow: StreamFlow[Any, Any]) -> bool:
    if isinstance(flow.source, (FromTopic, FromMultiTypeTopic)):
        return True
    if flow.output is not None:
        return True
    return _has_kafka_topic_output(flow.process.nodes)


def _resolve_consumer_settings(
    source: FromTopic[Any] | FromMultiTypeTopic[Any],
    ctx: ConfigContext,
) -> ConsumerSettings | None:
    """Resolve the consumer settings for one Kafka source, or None if unresolvable."""
    try:
        kafka = ctx.section(ConfigKey.KAFKA, KafkaSettings)
        return kafka.consumer_for(source.logical_ref)
    except (ConfigError, KeyError):
        return None


def _delivery_conflict_issues(
    consumer_ref: str,
    settings: ConsumerSettings,
) -> list[CompilationIssue]:
    """Return a conflict issue when delivery and enable_auto_commit contradict."""
    if settings.delivery is None or settings.enable_auto_commit is None:
        return []
    expected_auto_commit = settings.delivery == "at_most_once"
    if settings.enable_auto_commit == expected_auto_commit:
        return []
    return [delivery_conflict(consumer_ref, settings.delivery, settings.enable_auto_commit)]


def _iter_expand_routes_groups(node: ExpandRoutes[Any]) -> Iterable[Iterable[object]]:
    for process in node.routes.values():
        yield process.nodes
    if node.default is not None:
        yield node.default.nodes


def _is_scoped_node_with_process(node: object) -> bool:
    return isinstance(node, WithAsync) or (isinstance(node, With) and node.process is not None)


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
    elif isinstance(node, ExpandRoutes):
        yield from _iter_expand_routes_groups(node)
    elif _is_scoped_node_with_process(node):
        yield cast(Any, node).process.nodes


def _walk_all_process_nodes(nodes: Iterable[object]) -> Iterable[object]:
    for node in nodes:
        yield node
        for child_nodes in _iter_child_node_groups(node):
            yield from _walk_all_process_nodes(child_nodes)


def _node_needs_async_bridge(node: object) -> bool:
    return isinstance(node, WithAsync) or (
        isinstance(node, IntoTable) and node.backend is Backend.SQLALCHEMY
    )


def _check_input_shape(
    node: object,
    current_shape: StreamShape,
    errors: list[CompilationIssue],
) -> None:
    expected = _node_input_shape(node)
    if expected is not None and current_shape != expected:
        errors.append(shape_mismatch(expected.value, current_shape.value, node))


def _must_be_last_errors(
    idx: int,
    node_list: tuple[object, ...],
    issue: CompilationIssue,
) -> list[CompilationIssue]:
    if idx != len(node_list) - 1:
        return [issue]
    return []


def _is_scoped_process(node: object) -> bool:
    return isinstance(node, (WithAsync, With))


def _validate_shape_sequence(
    nodes: Iterable[object],
    initial_shape: StreamShape,
) -> tuple[list[CompilationIssue], StreamShape]:
    errors: list[CompilationIssue] = []
    current_shape = initial_shape
    node_list = tuple(nodes)

    for idx, node in enumerate(node_list):
        _check_input_shape(node, current_shape, errors)
        node_errors, next_shape, should_stop = _validate_shape_node(
            node,
            idx,
            node_list,
            current_shape,
        )
        errors.extend(node_errors)
        current_shape = next_shape
        if should_stop:
            break

    return errors, current_shape


def _iter_unique_storage_sinks(
    nodes: Iterable[object],
    errors: list[CompilationIssue],
) -> Iterable[IntoSink[Any]]:
    """Yield named storage sinks once while recording missing-name errors."""
    seen: set[str] = set()
    for node in _walk_all_process_nodes(nodes):
        if not isinstance(node, IntoSink):
            continue
        if not node.name:
            errors.append(sink_missing_name(node))
            continue
        if node.name in seen:
            continue
        seen.add(node.name)
        yield node


def _validate_storage_sink_node(node: IntoSink[Any], ctx: ConfigContext) -> list[CompilationIssue]:
    """Validate backend-specific config for one storage sink."""
    if not isinstance(node, IntoTable):
        return []
    if node.backend is Backend.SQLALCHEMY:
        return _validate_storage_sink_resolution(node, ctx, resolve_sqlalchemy_table_config)
    if node.backend is Backend.DELTA:
        return _validate_storage_sink_resolution(node, ctx, resolve_delta_table_config)
    return []


def _validate_storage_sink_resolution(
    node: IntoTable[Any],
    ctx: ConfigContext,
    resolver: Any,
) -> list[CompilationIssue]:
    """Resolve one IntoTable config and return validation errors."""
    try:
        resolver(node, ctx)
    except ValueError as exc:
        return [sink_config_invalid(node.name or type(node).__name__, exc)]
    return []


def _validate_shape_node(
    node: object,
    idx: int,
    node_list: tuple[object, ...],
    current_shape: StreamShape,
) -> tuple[list[CompilationIssue], StreamShape, bool]:
    """Validate one node inside a process shape sequence."""
    if _is_leaf_terminal(node):
        return _validate_leaf_terminal_node(node, idx, node_list, current_shape)
    if isinstance(node, Fork):
        return _validate_fork_node(node, idx, node_list, current_shape)
    if isinstance(node, Broadcast):
        return _validate_broadcast_node(node, idx, node_list, current_shape)
    if _is_scoped_process(node):
        return _validate_scoped_process_node(node, idx, node_list)
    if isinstance(node, Explode):
        return _validate_explode_node(node, idx, node_list, current_shape)
    if isinstance(node, Router):
        router_errors, next_shape = _validate_router_shapes(node, current_shape)
        return router_errors, next_shape, False
    return [], _node_output_shape(node, current_shape), False


def _validate_leaf_terminal_node(
    node: object,
    idx: int,
    node_list: tuple[object, ...],
    current_shape: StreamShape,
) -> tuple[list[CompilationIssue], StreamShape, bool]:
    """Validate a terminal leaf node."""
    errors = _must_be_last_errors(idx, node_list, terminal_not_last(node))
    return errors, current_shape, True


def _validate_fork_node(
    node: Fork[StreamPayload],
    idx: int,
    node_list: tuple[object, ...],
    current_shape: StreamShape,
) -> tuple[list[CompilationIssue], StreamShape, bool]:
    """Validate a fork node and stop the enclosing process."""
    errors, next_shape = _validate_fork_shapes(node, current_shape)
    errors.extend(_must_be_last_errors(idx, node_list, fork_not_last()))
    return errors, next_shape, True


def _validate_broadcast_node(
    node: Broadcast[Any],
    idx: int,
    node_list: tuple[object, ...],
    current_shape: StreamShape,
) -> tuple[list[CompilationIssue], StreamShape, bool]:
    """Validate a broadcast node and stop the enclosing process."""
    errors, next_shape = _validate_broadcast_shapes(node, current_shape)
    errors.extend(_must_be_last_errors(idx, node_list, broadcast_not_last()))
    return errors, next_shape, True


def _validate_scoped_process_node(
    node: object,
    idx: int,
    node_list: tuple[object, ...],
) -> tuple[list[CompilationIssue], StreamShape, bool]:
    """Validate a scoped process node and stop the enclosing process."""
    errors = _must_be_last_errors(idx, node_list, scoped_process_not_last(node))
    return errors, StreamShape.NONE, True


def _validate_explode_node(
    node: Explode[Any],
    idx: int,
    node_list: tuple[object, ...],
    current_shape: StreamShape,
) -> tuple[list[CompilationIssue], StreamShape, bool]:
    """Validate that Explode is followed immediately by Router."""
    next_node = node_list[idx + 1] if idx + 1 < len(node_list) else None
    errors: list[CompilationIssue] = []
    if not isinstance(next_node, Router):
        errors.append(explode_without_router(next_node))
    return errors, _node_output_shape(node, current_shape), False


def _validate_fork_shapes(
    fork: Fork[StreamPayload],
    initial_shape: StreamShape,
) -> tuple[list[CompilationIssue], StreamShape]:
    errors: list[CompilationIssue] = []

    for label, nodes in _fork_branch_nodes(fork):
        branch_errors, _ = _validate_shape_sequence(nodes, initial_shape)
        errors.extend(issue.prefixed(f"fork branch {label}") for issue in branch_errors)
        if not _has_terminal_output(nodes):
            errors.append(fork_branch_no_terminal(label))

    return errors, StreamShape.NONE


def _validate_broadcast_shapes(
    broadcast: Broadcast[Any],
    initial_shape: StreamShape,
) -> tuple[list[CompilationIssue], StreamShape]:
    errors: list[CompilationIssue] = []
    for branch_idx, route in enumerate(broadcast.routes):
        branch_errors, _ = _validate_shape_sequence(route.process.nodes, initial_shape)
        errors.extend(issue.prefixed(f"broadcast branch {branch_idx}") for issue in branch_errors)
    return errors, StreamShape.NONE


def _validate_router_branch_shape_sequence(
    nodes: Iterable[object],
    initial_shape: StreamShape,
) -> tuple[list[CompilationIssue], StreamShape]:
    errors: list[CompilationIssue] = []
    current_shape = initial_shape
    node_list = tuple(nodes)

    for idx, node in enumerate(node_list):
        if isinstance(node, BatchStep):
            current_shape = StreamShape.RECORD
            continue
        expected = _node_input_shape(node)
        if expected is not None and current_shape != expected:
            errors.append(shape_mismatch(expected.value, current_shape.value, node))
        if _is_leaf_terminal(node) and idx != len(node_list) - 1:
            errors.append(terminal_not_last(node))
            break
        current_shape = _node_output_shape(node, current_shape)

    return errors, current_shape


def _validate_router_shapes(
    router: Router[StreamPayload, StreamPayload],
    initial_shape: StreamShape,
) -> tuple[list[CompilationIssue], StreamShape]:
    errors: list[CompilationIssue] = []
    outputs: list[StreamShape] = []

    for label, nodes in _router_branch_nodes(router):
        branch_errors, branch_output = _validate_router_branch_shape_sequence(nodes, initial_shape)
        errors.extend(issue.prefixed(f"router branch {label}") for issue in branch_errors)
        for node in nodes:
            if not isinstance(node, RouterBranchSafe):
                errors.append(router_branch_unsafe_node(label, node))
            elif isinstance(node, (ExpandStep, BatchExpandStep)):
                errors.append(router_branch_fanout_unsupported(label, node))
        outputs.append(branch_output)

    unique_outputs = set(outputs)
    if len(unique_outputs) > 1:
        ordered = ", ".join(sorted(shape.value for shape in unique_outputs))
        errors.append(router_branch_shape_divergence(ordered))

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


def _is_leaf_terminal(node: object) -> bool:
    """Return True for nodes that are themselves terminal with no children to recurse into."""
    return isinstance(node, (IntoTopic, Drain, IntoSink, Broadcast))


def _node_has_terminal_output(node: object) -> bool:
    if _is_leaf_terminal(node):
        return True
    if isinstance(node, Router):
        return _router_has_terminal_output(node)
    if isinstance(node, Fork):
        return _fork_has_terminal_output(node)
    if isinstance(node, ExpandRoutes):
        return _expand_routes_has_terminal_output(node)
    if isinstance(node, (WithAsync, With)):
        return _has_terminal_output(node.process.nodes)
    return False


def _has_terminal_output(nodes: Iterable[object]) -> bool:
    return any(_node_has_terminal_output(node) for node in nodes)


def _router_has_terminal_output(router: Router[StreamPayload, StreamPayload]) -> bool:
    return any(_has_terminal_output(nodes) for _, nodes in _router_branch_nodes(router))


def _fork_has_terminal_output(fork: Fork[StreamPayload]) -> bool:
    return any(_has_terminal_output(nodes) for _, nodes in _fork_branch_nodes(fork))


def _expand_routes_has_terminal_output(node: ExpandRoutes[Any]) -> bool:
    all_processes = list(node.routes.values())
    if node.default is not None:
        all_processes.append(node.default)
    return any(_has_terminal_output(p.nodes) for p in all_processes)


def _node_has_kafka_topic_output(node: object) -> bool:
    if isinstance(node, IntoTopic):
        return True
    if isinstance(node, Router):
        return any(_has_kafka_topic_output(nodes) for _, nodes in _router_branch_nodes(node))
    if isinstance(node, Fork):
        return any(_has_kafka_topic_output(nodes) for _, nodes in _fork_branch_nodes(node))
    if isinstance(node, Broadcast):
        return any(_has_kafka_topic_output(route.process.nodes) for route in node.routes)
    if isinstance(node, (WithAsync, With)):
        return _has_kafka_topic_output(node.process.nodes)
    return False


def _has_kafka_topic_output(nodes: Iterable[object]) -> bool:
    if isinstance(nodes, tuple):
        return any(_node_has_kafka_topic_output(node) for node in nodes)
    return any(_node_has_kafka_topic_output(node) for node in nodes)


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
    if isinstance(node, Explode):
        return StreamShape.RECORD
    if isinstance(node, (With, WithAsync)):
        return None
    if isinstance(node, ForEach):
        return StreamShape.MANY
    if isinstance(node, Drain):
        return None
    if isinstance(node, Fork):
        return None
    return None


def _validate_window_strategies(nodes: Iterable[object]) -> list[CompilationIssue]:
    errors: list[CompilationIssue] = []
    for node in nodes:
        if isinstance(node, CollectBatch) and node.window is not WindowStrategy.COLLECT:
            errors.append(window_strategy_unsupported(node.window))
    return errors


def _validate_scoped_process_nodes(nodes: Iterable[object]) -> list[CompilationIssue]:
    errors: list[CompilationIssue] = []
    for node in nodes:
        if not isinstance(node, (With, WithAsync)):
            continue
        inner_nodes = tuple(node.process.nodes)
        for idx, inner_node in enumerate(inner_nodes):
            if isinstance(inner_node, RecordStep):
                continue
            if isinstance(inner_node, IntoTopic):
                if idx != len(inner_nodes) - 1:
                    errors.append(scoped_into_topic_not_last(node, inner_nodes[idx + 1]))
                continue
            errors.append(scoped_process_unsupported_node(node, inner_node))
    return errors


_FIXED_OUTPUT_SHAPES: dict[type, StreamShape] = {
    CollectBatch: StreamShape.BATCH,
    Explode: StreamShape.RECORD,
    ForEach: StreamShape.RECORD,
    RecordStep: StreamShape.RECORD,
    BatchStep: StreamShape.BATCH,
    ExpandStep: StreamShape.RECORD,
    BatchExpandStep: StreamShape.RECORD,
    WithAsync: StreamShape.NONE,
    Drain: StreamShape.NONE,
    Fork: StreamShape.NONE,
    Broadcast: StreamShape.NONE,
}


def _node_output_shape(node: object, current: StreamShape) -> StreamShape:
    fixed = _FIXED_OUTPUT_SHAPES.get(type(node))
    if fixed is not None:
        return fixed
    if isinstance(node, IntoSink):
        return StreamShape.NONE
    if isinstance(node, IntoTopic):
        return node.shape
    if isinstance(node, With):
        return StreamShape.NONE if node.process is not None else StreamShape.MANY
    return current
