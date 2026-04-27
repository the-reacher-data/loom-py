"""Streaming flow compiler: validates and compiles StreamFlow into CompiledPlan."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any, Literal

from omegaconf import DictConfig

from loom.core.config import ConfigError, section
from loom.core.config.configurable import ConfigBinding
from loom.streaming.compiler._plan import (
    CompiledNode,
    CompiledPlan,
    CompiledSink,
    CompiledSource,
)
from loom.streaming.core._typing import StreamPayload
from loom.streaming.graph._flow import StreamFlow
from loom.streaming.kafka._config import KafkaSettings
from loom.streaming.nodes._boundary import FromTopic, IntoTopic
from loom.streaming.nodes._broadcast import Broadcast
from loom.streaming.nodes._capabilities import RouterBranchSafe
from loom.streaming.nodes._fork import Fork, ForkKind
from loom.streaming.nodes._router import Router
from loom.streaming.nodes._shape import CollectBatch, Drain, ForEach, StreamShape, WindowStrategy
from loom.streaming.nodes._step import BatchExpandStep, BatchStep, ExpandStep, RecordStep
from loom.streaming.nodes._with import ResourceScope, With, WithAsync


class CompilationError(Exception):
    """Raised when a StreamFlow fails validation."""

    def __init__(self, errors: list[str]) -> None:
        self.errors = errors
        super().__init__(f"Compilation failed with {len(errors)} error(s): {'; '.join(errors)}")


def compile_flow(flow: StreamFlow[Any, Any], *, runtime_config: DictConfig) -> CompiledPlan:
    """Compile a flow into an immutable plan.

    Raises:
        CompilationError: If any validation fails.
    """
    compiler = _Compiler()
    return compiler.compile(flow, runtime_config=runtime_config)


def _collect_deps(node: With[Any, Any] | WithAsync[Any, Any]) -> dict[str, object]:
    """Return all dependency kinds of a With/WithAsync node merged into one mapping."""
    return {
        **node.sync_contexts,
        **node.async_contexts,
        **node.context_factories,
        **node.plain_deps,
    }


class _Compiler:
    """Validates a StreamFlow and produces a CompiledPlan.

    Each validator is a pure function that returns a list of error strings.
    """

    def compile(self, flow: StreamFlow[Any, Any], *, runtime_config: DictConfig) -> CompiledPlan:
        errors: list[str] = []

        errors.extend(self._validate_bindings(flow, runtime_config))
        errors.extend(self._validate_kafka(flow, runtime_config))
        errors.extend(self._validate_resources(flow))
        errors.extend(self._validate_shapes(flow))
        errors.extend(self._validate_outputs(flow))

        if errors:
            raise CompilationError(errors)

        return self._build_plan(flow, runtime_config)

    @staticmethod
    def _validate_bindings(flow: StreamFlow[Any, Any], cfg: DictConfig) -> list[str]:
        errors: list[str] = []
        for binding in _iter_config_bindings(flow):
            try:
                section(cfg, binding.config_path, dict)
            except ConfigError as exc:
                errors.append(f"binding {binding.config_path}: {exc}")
        return errors

    @staticmethod
    def _validate_kafka(flow: StreamFlow[Any, Any], cfg: DictConfig) -> list[str]:
        if not _uses_kafka(flow):
            return []
        try:
            section(cfg, "kafka", KafkaSettings)
            return []
        except ConfigError as exc:
            return [f"kafka: {exc}"]

    @staticmethod
    def _validate_resources(flow: StreamFlow[Any, Any]) -> list[str]:
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

    @staticmethod
    def _validate_shapes(flow: StreamFlow[Any, Any]) -> list[str]:
        errors, _ = _validate_shape_sequence(flow.process.nodes, flow.source.shape)
        errors.extend(_validate_window_strategies(flow.process.nodes))
        return errors

    @staticmethod
    def _validate_outputs(flow: StreamFlow[Any, Any]) -> list[str]:
        errors: list[str] = []
        has_terminal = flow.output is not None

        if _has_terminal_output(flow.process.nodes):
            has_terminal = True

        if flow.output is not None and _contains_fork(flow.process.nodes):
            errors.append("flow.output cannot be combined with Fork: branches must be terminal")

        if flow.output is not None and _contains_broadcast(flow.process.nodes):
            errors.append(
                "flow.output cannot be combined with Broadcast: branches must be terminal"
            )

        if not has_terminal:
            errors.append("no terminal output found: add IntoTopic or flow.output")

        return errors

    def _build_plan(self, flow: StreamFlow[Any, Any], runtime_config: DictConfig) -> CompiledPlan:
        # Resolve source
        source = self._build_source(flow, runtime_config)

        # Build annotated nodes
        nodes = self._build_nodes(flow)

        # Resolve output
        output = self._build_sink(flow.output, runtime_config) if flow.output else None

        terminal_sinks = self._build_terminal_sinks(flow.process.nodes, runtime_config)

        # Resolve error routes
        error_routes = {
            kind: self._build_sink(topic, runtime_config) for kind, topic in flow.errors.items()
        }

        # Detect async need — must walk the full tree; WithAsync may live inside
        # Router or Fork branches which are opaque at the top-level node list.
        needs_async = any(
            _node_needs_async_bridge(node) for node in _walk_all_process_nodes(flow.process.nodes)
        )

        return CompiledPlan(
            name=flow.name,
            source=source,
            nodes=tuple(nodes),
            output=output,
            terminal_sinks=terminal_sinks,
            error_routes=error_routes,
            needs_async_bridge=needs_async,
        )

    def _build_source(
        self, flow: StreamFlow[Any, Any], runtime_config: DictConfig
    ) -> CompiledSource:
        kafka = section(runtime_config, "kafka", KafkaSettings)
        consumer = kafka.consumer_for(flow.source.logical_ref)

        # Infer decode strategy from nodes
        decode_strategy: Literal["record", "batch"] = "record"
        if any(isinstance(n, CollectBatch) for n in flow.process.nodes):
            decode_strategy = "batch"

        return CompiledSource(
            settings=consumer,
            topics=consumer.topics,
            payload_type=flow.source.payload,
            shape=flow.source.shape,
            decode_strategy=decode_strategy,
        )

    def _build_nodes(self, flow: StreamFlow[Any, Any]) -> list[CompiledNode]:
        nodes: list[CompiledNode] = []
        current_shape = flow.source.shape

        for node in flow.process.nodes:
            input_shape = current_shape
            output_shape = _node_output_shape(node, current_shape)
            nodes.append(
                CompiledNode(
                    node=node,
                    input_shape=input_shape,
                    output_shape=output_shape,
                    path=(len(nodes),),
                )
            )
            current_shape = output_shape

        return nodes

    def _build_terminal_sinks(
        self,
        nodes: Iterable[object],
        runtime_config: DictConfig,
        *,
        path_prefix: tuple[int, ...] = (),
    ) -> dict[tuple[int, ...], CompiledSink]:
        sinks: dict[tuple[int, ...], CompiledSink] = {}
        node_list = tuple(nodes)
        for idx, node in enumerate(node_list):
            path = path_prefix + (idx,)
            if isinstance(node, IntoTopic):
                sinks[path] = self._build_sink(node, runtime_config)
            elif isinstance(node, Fork):
                sinks.update(
                    self._build_fork_terminal_sinks(node, runtime_config, path_prefix=path)
                )
            elif isinstance(node, Router):
                sinks.update(
                    self._build_router_terminal_sinks(node, runtime_config, path_prefix=path)
                )
            elif isinstance(node, Broadcast):
                sinks.update(
                    self._build_broadcast_terminal_sinks(node, runtime_config, path_prefix=path)
                )
        return sinks

    def _build_fork_terminal_sinks(
        self,
        fork: Fork[Any],
        runtime_config: DictConfig,
        *,
        path_prefix: tuple[int, ...],
    ) -> dict[tuple[int, ...], CompiledSink]:
        sinks: dict[tuple[int, ...], CompiledSink] = {}
        branch_count = _fork_branch_count(fork)
        for branch_idx, (_, nodes) in enumerate(_fork_branch_nodes(fork)):
            sinks.update(
                self._build_terminal_sinks(
                    nodes,
                    runtime_config,
                    path_prefix=path_prefix + (branch_idx,),
                )
            )
        if fork.default is not None:
            sinks.update(
                self._build_terminal_sinks(
                    fork.default.nodes,
                    runtime_config,
                    path_prefix=path_prefix + (branch_count,),
                )
            )
        return sinks

    def _build_router_terminal_sinks(
        self,
        router: Router[Any, Any],
        runtime_config: DictConfig,
        *,
        path_prefix: tuple[int, ...],
    ) -> dict[tuple[int, ...], CompiledSink]:
        sinks: dict[tuple[int, ...], CompiledSink] = {}
        for branch_idx, process in enumerate(router.routes.values()):
            sinks.update(
                self._build_terminal_sinks(
                    process.nodes,
                    runtime_config,
                    path_prefix=path_prefix + (branch_idx,),
                )
            )
        for branch_idx, route in enumerate(router.predicate_routes):
            sinks.update(
                self._build_terminal_sinks(
                    route.process.nodes,
                    runtime_config,
                    path_prefix=path_prefix + (branch_idx,),
                )
            )
        if router.default is not None:
            sinks.update(
                self._build_terminal_sinks(
                    router.default.nodes,
                    runtime_config,
                    path_prefix=path_prefix + (len(router.routes),),
                )
            )
        return sinks

    def _build_broadcast_terminal_sinks(
        self,
        broadcast: Broadcast[Any],
        runtime_config: DictConfig,
        *,
        path_prefix: tuple[int, ...],
    ) -> dict[tuple[int, ...], CompiledSink]:
        sinks: dict[tuple[int, ...], CompiledSink] = {}
        for branch_idx, route in enumerate(broadcast.routes):
            branch_path = path_prefix + (branch_idx,)
            sinks[branch_path] = self._build_sink(route.output, runtime_config)
            # Traverse the branch process for any nested terminal nodes
            sinks.update(
                self._build_terminal_sinks(
                    route.process.nodes,
                    runtime_config,
                    path_prefix=branch_path,
                )
            )
        return sinks

    def _build_sink(self, topic: IntoTopic[Any], runtime_config: DictConfig) -> CompiledSink:
        kafka = section(runtime_config, "kafka", KafkaSettings)
        producer = kafka.producer_for(topic.logical_ref)

        return CompiledSink(
            settings=producer,
            topic=producer.topic or str(topic.logical_ref),
            partition_policy=topic.partitioning,
            dlq_topic=topic.dlq,
        )


def _uses_kafka(flow: StreamFlow[Any, Any]) -> bool:
    """Return True if the flow uses Kafka topics."""
    if isinstance(flow.source, FromTopic):
        return True
    if flow.output is not None:
        return True
    return _has_terminal_output(flow.process.nodes)


def _iter_config_bindings(flow: StreamFlow[Any, Any]) -> Iterable[ConfigBinding]:
    """Yield all config bindings used directly or through With/WithAsync nodes."""
    for node in _walk_all_process_nodes(flow.process.nodes):
        yield from _node_config_bindings(node)


def _node_config_bindings(node: object) -> Iterable[ConfigBinding]:
    """Yield config bindings referenced by one process node."""
    if isinstance(node, ConfigBinding):
        yield node
        return

    scoped = _scoped_node(node)
    if scoped is None:
        return

    for dep in _collect_deps(scoped).values():
        if isinstance(dep, ConfigBinding):
            yield dep


def _scoped_node(node: object) -> With[Any, Any] | WithAsync[Any, Any] | None:
    """Return the scoped node carried by a With-like declaration, if any."""
    if isinstance(node, (With, WithAsync)):
        return node
    return None


def _walk_all_process_nodes(nodes: Iterable[object]) -> Iterable[object]:
    """Yield every process node recursively, including inside Router, Fork, and Broadcast branches.

    Top-level iteration misses nodes nested inside branch sub-processes.  Use
    this walker whenever a property (async bridge, resource scope, config
    binding) must be checked across the full node tree.
    """
    for node in nodes:
        yield node
        if isinstance(node, Router):
            for _, branch_nodes in _router_branch_nodes(node):
                yield from _walk_all_process_nodes(branch_nodes)
        elif isinstance(node, Fork):
            for _, branch_nodes in _fork_branch_nodes(node):
                yield from _walk_all_process_nodes(branch_nodes)
        elif isinstance(node, Broadcast):
            for route in node.routes:
                yield from _walk_all_process_nodes(route.process.nodes)


def _node_needs_async_bridge(node: object) -> bool:
    """Return whether a single node requires an AsyncBridge."""
    return isinstance(node, WithAsync)


def _validate_shape_sequence(
    nodes: Iterable[object],
    initial_shape: StreamShape,
) -> tuple[list[str], StreamShape]:
    errors: list[str] = []
    current_shape = initial_shape
    node_list = tuple(nodes)

    for idx, node in enumerate(node_list):
        expected = _node_input_shape(node)
        if expected is not None and current_shape != expected:
            errors.append(
                f"shape mismatch: expected {expected.value} but got {current_shape.value} "
                f"before {type(node).__name__}"
            )

        if isinstance(node, (IntoTopic, Drain)) and idx != len(node_list) - 1:
            errors.append(f"{type(node).__name__} must be the last node in a process")
            break

        if isinstance(node, Fork):
            fork_errors, current_shape = _validate_fork_shapes(node, current_shape)
            errors.extend(fork_errors)
            if idx != len(node_list) - 1:
                errors.append("fork must be the last node in a process")
            break

        if isinstance(node, Broadcast):
            broadcast_errors, current_shape = _validate_broadcast_shapes(node, current_shape)
            errors.extend(broadcast_errors)
            if idx != len(node_list) - 1:
                errors.append("broadcast must be the last node in a process")
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
    """Validate shape sequence inside a Router branch.

    Identical to :func:`_validate_shape_sequence` but treats ``BatchStep`` as
    accepting any input shape and producing RECORD output.  The Bytewax adapter
    wraps the incoming record into a singleton batch and unwraps the result, so
    the effective cardinality remains 1-to-1.
    """
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


def _has_terminal_output(nodes: Iterable[object]) -> bool:
    for node in nodes:
        if isinstance(node, (IntoTopic, Drain)):
            return True
        if isinstance(node, Router) and _router_has_terminal_output(node):
            return True
        if isinstance(node, Fork) and _fork_has_terminal_output(node):
            return True
        if isinstance(node, Broadcast):
            return True  # Broadcast always declares explicit outputs per route
    return False


def _router_has_terminal_output(router: Router[StreamPayload, StreamPayload]) -> bool:
    return any(_has_terminal_output(nodes) for _, nodes in _router_branch_nodes(router))


def _fork_has_terminal_output(fork: Fork[StreamPayload]) -> bool:
    return any(_has_terminal_output(nodes) for _, nodes in _fork_branch_nodes(fork))


def _contains_fork(nodes: Iterable[object]) -> bool:
    return any(isinstance(node, Fork) for node in nodes)


def _contains_broadcast(nodes: Iterable[object]) -> bool:
    return any(isinstance(node, Broadcast) for node in nodes)


def _node_input_shape(node: object) -> StreamShape | None:
    """Expected input shape for a node, or None if any shape is accepted."""
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
    """Return errors for any CollectBatch node using an unimplemented strategy."""
    errors: list[str] = []
    for node in nodes:
        if isinstance(node, CollectBatch) and node.window is not WindowStrategy.COLLECT:
            errors.append(
                f"CollectBatch.window={node.window} is not yet supported by the Bytewax adapter. "
                f"Only WindowStrategy.COLLECT is available in this adapter version."
            )
    return errors


def _node_output_shape(node: object, current: StreamShape) -> StreamShape:
    """Output shape produced by a node."""
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
    if isinstance(node, (With, WithAsync)):
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
