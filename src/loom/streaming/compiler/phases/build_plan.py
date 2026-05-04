"""Plan-building phase for streaming flow compilation."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from types import MappingProxyType
from typing import Any, Literal

from omegaconf import DictConfig

from loom.core.config import section
from loom.streaming.compiler._plan import (
    CompiledNode,
    CompiledPlan,
    CompiledSink,
    CompiledSource,
)
from loom.streaming.compiler.phases.validate import (
    _fork_branch_count,
    _fork_branch_nodes,
    _node_needs_async_bridge,
    _node_output_shape,
    _walk_all_process_nodes,
)
from loom.streaming.graph._flow import StreamFlow
from loom.streaming.kafka._config import KafkaSettings
from loom.streaming.nodes._boundary import IntoTopic
from loom.streaming.nodes._broadcast import Broadcast
from loom.streaming.nodes._fork import Fork
from loom.streaming.nodes._router import Router
from loom.streaming.nodes._shape import CollectBatch
from loom.streaming.nodes._with import With, WithAsync


def build_plan(flow: StreamFlow[Any, Any], runtime_config: DictConfig) -> CompiledPlan:
    """Build a compiled plan from a validated flow and runtime config.

    Args:
        flow: Validated stream flow.
        runtime_config: Resolved OmegaConf runtime configuration.

    Returns:
        Immutable compiled plan ready for adapter wiring.
    """
    source = _build_source(flow, runtime_config)
    nodes = _build_nodes(flow)
    output = _build_sink(flow.output, runtime_config) if flow.output else None
    terminal_sinks = _build_terminal_sinks(flow.process.nodes, runtime_config)
    error_routes = {kind: _build_sink(topic, runtime_config) for kind, topic in flow.errors.items()}
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


def _build_source(flow: StreamFlow[Any, Any], runtime_config: DictConfig) -> CompiledSource:
    kafka = section(runtime_config, "kafka", KafkaSettings)
    consumer = kafka.consumer_for(flow.source.logical_ref)
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


def _build_nodes(flow: StreamFlow[Any, Any]) -> list[CompiledNode]:
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
    nodes: Iterable[object],
    runtime_config: DictConfig,
    *,
    path_prefix: tuple[int, ...] = (),
) -> dict[tuple[int, ...], CompiledSink]:
    sinks: dict[tuple[int, ...], CompiledSink] = {}
    for idx, node in enumerate(tuple(nodes)):
        path = path_prefix + (idx,)
        if isinstance(node, IntoTopic):
            sinks[path] = _build_sink(node, runtime_config)
            continue
        builder = _BRANCH_BUILDERS.get(type(node))
        if builder is not None:
            sinks.update(builder(node, runtime_config, path_prefix=path))
            continue
        if isinstance(node, WithAsync) or (isinstance(node, With) and node.process is not None):
            sinks.update(
                _build_terminal_sinks(node.process.nodes, runtime_config, path_prefix=path)
            )
    return sinks


def _build_fork_terminal_sinks(
    fork: Fork[Any],
    runtime_config: DictConfig,
    *,
    path_prefix: tuple[int, ...],
) -> dict[tuple[int, ...], CompiledSink]:
    sinks: dict[tuple[int, ...], CompiledSink] = {}
    branch_count = _fork_branch_count(fork)
    for branch_idx, (_, nodes) in enumerate(_fork_branch_nodes(fork)):
        sinks.update(
            _build_terminal_sinks(nodes, runtime_config, path_prefix=path_prefix + (branch_idx,))
        )
    if fork.default is not None:
        sinks.update(
            _build_terminal_sinks(
                fork.default.nodes, runtime_config, path_prefix=path_prefix + (branch_count,)
            )
        )
    return sinks


def _build_router_terminal_sinks(
    router: Router[Any, Any],
    runtime_config: DictConfig,
    *,
    path_prefix: tuple[int, ...],
) -> dict[tuple[int, ...], CompiledSink]:
    sinks: dict[tuple[int, ...], CompiledSink] = {}
    keyed_count = len(router.routes)
    for branch_idx, process in enumerate(router.routes.values()):
        sinks.update(
            _build_terminal_sinks(
                process.nodes, runtime_config, path_prefix=path_prefix + (branch_idx,)
            )
        )
    for i, route in enumerate(router.predicate_routes):
        sinks.update(
            _build_terminal_sinks(
                route.process.nodes,
                runtime_config,
                path_prefix=path_prefix + (keyed_count + i,),
            )
        )
    if router.default is not None:
        sinks.update(
            _build_terminal_sinks(
                router.default.nodes,
                runtime_config,
                path_prefix=path_prefix + (len(router.routes),),
            )
        )
    return sinks


def _build_broadcast_terminal_sinks(
    broadcast: Broadcast[Any],
    runtime_config: DictConfig,
    *,
    path_prefix: tuple[int, ...],
) -> dict[tuple[int, ...], CompiledSink]:
    sinks: dict[tuple[int, ...], CompiledSink] = {}
    for branch_idx, route in enumerate(broadcast.routes):
        branch_path = path_prefix + (branch_idx,)
        if route.output is not None:
            sinks[branch_path] = _build_sink(route.output, runtime_config)
        sinks.update(
            _build_terminal_sinks(route.process.nodes, runtime_config, path_prefix=branch_path)
        )
    return sinks


def _build_sink(topic: IntoTopic[Any], runtime_config: DictConfig) -> CompiledSink:
    kafka = section(runtime_config, "kafka", KafkaSettings)
    producer = kafka.producer_for(topic.logical_ref)
    return CompiledSink(
        settings=producer,
        topic=producer.topic or str(topic.logical_ref),
        partition_policy=topic.partitioning,
        dlq_topic=topic.dlq,
    )


_BRANCH_BUILDERS: MappingProxyType[type, Callable[..., dict[tuple[int, ...], CompiledSink]]] = (
    MappingProxyType(
        {
            Fork: _build_fork_terminal_sinks,
            Router: _build_router_terminal_sinks,
            Broadcast: _build_broadcast_terminal_sinks,
        }
    )
)
