"""Streaming flow compiler: validates and compiles StreamFlow into CompiledPlan."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

from omegaconf import DictConfig

from loom.core.config import ConfigError, section
from loom.core.config.configurable import ConfigBinding
from loom.streaming._boundary import IntoTopic
from loom.streaming._process import StreamFlow
from loom.streaming._shape import CollectBatch, ForEach, StreamShape
from loom.streaming._task import BatchTask, Task
from loom.streaming._with import OneEmit, With, WithAsync
from loom.streaming.compiler._plan import (
    CompiledNode,
    CompiledPlan,
    CompiledSink,
    CompiledSource,
)

if TYPE_CHECKING:
    from loom.streaming._boundary import IntoTopic


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


class _Compiler:
    """Validates a StreamFlow and produces a CompiledPlan.

    Each validator is a pure function that returns a list of error strings.
    """

    def compile(self, flow: StreamFlow[Any, Any], *, runtime_config: DictConfig) -> CompiledPlan:
        errors: list[str] = []

        errors.extend(self._validate_bindings(flow, runtime_config))
        errors.extend(self._validate_kafka(flow, runtime_config))
        errors.extend(self._validate_shapes(flow))
        errors.extend(self._validate_outputs(flow))

        if errors:
            raise CompilationError(errors)

        return self._build_plan(flow, runtime_config)

    @staticmethod
    def _validate_bindings(flow: StreamFlow[Any, Any], cfg: DictConfig) -> list[str]:
        errors: list[str] = []
        for node in flow.process.nodes:
            if isinstance(node, ConfigBinding):
                try:
                    section(cfg, node.config_path, dict)
                except ConfigError as exc:
                    errors.append(f"binding {node.config_path}: {exc}")
            elif isinstance(node, OneEmit) and isinstance(node.source, (With, WithAsync)):
                for dep in {**node.source.contexts, **node.source.plain_deps}.values():
                    if isinstance(dep, ConfigBinding):
                        try:
                            section(cfg, dep.config_path, dict)
                        except ConfigError as exc:
                            errors.append(f"binding {dep.config_path}: {exc}")
        return errors

    @staticmethod
    def _validate_kafka(flow: StreamFlow[Any, Any], cfg: DictConfig) -> list[str]:
        if not _uses_kafka(flow):
            return []
        try:
            from loom.streaming.kafka._config import KafkaSettings

            section(cfg, "kafka", KafkaSettings)
            return []
        except ConfigError as exc:
            return [f"kafka: {exc}"]
        except ImportError:
            return ["kafka: KafkaSettings not available"]

    @staticmethod
    def _validate_shapes(flow: StreamFlow[Any, Any]) -> list[str]:
        errors: list[str] = []
        nodes = list(flow.process.nodes)

        if not nodes:
            return errors

        current_shape = flow.source.shape
        for node in nodes:
            expected = _node_input_shape(node)
            if expected is not None and current_shape != expected:
                errors.append(
                    f"shape mismatch: expected {expected.value} but got {current_shape.value} "
                    f"before {type(node).__name__}"
                )
            current_shape = _node_output_shape(node, current_shape)

        return errors

    @staticmethod
    def _validate_outputs(flow: StreamFlow[Any, Any]) -> list[str]:
        errors: list[str] = []
        has_terminal = flow.output is not None

        for node in flow.process.nodes:
            if isinstance(node, (IntoTopic, OneEmit)):
                has_terminal = True

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

        # Resolve error routes
        error_routes = {
            kind: self._build_sink(topic, runtime_config) for kind, topic in flow.errors.items()
        }

        # Detect async need
        needs_async = any(isinstance(n.node, (WithAsync,)) for n in nodes)

        return CompiledPlan(
            name=flow.name,
            source=source,
            nodes=tuple(nodes),
            output=output,
            error_routes=error_routes,
            needs_async_bridge=needs_async,
        )

    def _build_source(
        self, flow: StreamFlow[Any, Any], runtime_config: DictConfig
    ) -> CompiledSource:
        from loom.streaming.kafka._config import KafkaSettings

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
                CompiledNode(node=node, input_shape=input_shape, output_shape=output_shape)
            )
            current_shape = output_shape

        return nodes

    def _build_sink(self, topic: IntoTopic[Any], runtime_config: DictConfig) -> CompiledSink:
        from loom.streaming.kafka._config import KafkaSettings

        kafka = section(runtime_config, "kafka", KafkaSettings)
        producer = kafka.producer_for(topic.logical_ref)

        return CompiledSink(
            settings=producer,
            topic=producer.topic or str(topic.logical_ref),
            partition_policy=topic.partitioning,
        )


def _uses_kafka(flow: StreamFlow[Any, Any]) -> bool:
    """Return True if the flow uses Kafka topics."""
    from loom.streaming._boundary import FromTopic

    if isinstance(flow.source, FromTopic):
        return True
    if flow.output is not None:
        return True
    return any(isinstance(node, (IntoTopic, OneEmit)) for node in flow.process.nodes)


def _node_input_shape(node: object) -> StreamShape | None:
    """Expected input shape for a node, or None if any shape is accepted."""
    if isinstance(node, (Task, BatchTask)):
        return StreamShape.RECORD
    if isinstance(node, (With, WithAsync)):
        return StreamShape.BATCH
    if isinstance(node, ForEach):
        return StreamShape.MANY
    return None


def _node_output_shape(node: object, current: StreamShape) -> StreamShape:
    """Output shape produced by a node."""
    if isinstance(node, CollectBatch):
        return StreamShape.BATCH
    if isinstance(node, ForEach):
        return StreamShape.RECORD
    if isinstance(node, (With, WithAsync)):
        return StreamShape.MANY
    if isinstance(node, OneEmit):
        return StreamShape.RECORD
    if isinstance(node, IntoTopic):
        return node.shape
    return current
