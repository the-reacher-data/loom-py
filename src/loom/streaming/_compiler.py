"""Streaming flow compiler: validates and compiles StreamFlow into CompiledPlan."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from omegaconf import DictConfig

from loom.core.config import ConfigError, section
from loom.core.config.configurable import ConfigBinding
from loom.streaming._boundary import FromTopic, IntoTopic
from loom.streaming._errors import ErrorKind
from loom.streaming._process import StreamFlow
from loom.streaming._shape import CollectBatch, Drain, ForEach, StreamShape
from loom.streaming._task import BatchTask, Task
from loom.streaming._with import OneEmit, With, WithAsync


class CompilationError(Exception):
    """Raised when a StreamFlow fails validation."""

    def __init__(self, errors: list[str]) -> None:
        self.errors = errors
        super().__init__(f"Compilation failed with {len(errors)} error(s): {'; '.join(errors)}")


@dataclass(frozen=True)
class CompiledPlan:
    """Immutable result of compiling a StreamFlow."""

    name: str
    source: FromTopic[Any]
    nodes: tuple[object, ...]
    output: IntoTopic[Any] | None
    errors: dict[ErrorKind, IntoTopic[Any]]


class Compiler:
    """Validates a StreamFlow and produces a CompiledPlan.

    Each validator is a pure function that returns a list of error strings.
    No mutable state is kept between validations.
    """

    def compile(self, flow: StreamFlow[Any, Any], *, runtime_config: DictConfig) -> CompiledPlan:
        """Compile a flow into an immutable plan.

        Raises:
            CompilationError: If any validation fails.
        """
        errors: list[str] = []

        errors.extend(self._validate_bindings(flow, runtime_config))
        errors.extend(self._validate_kafka(flow, runtime_config))
        errors.extend(self._validate_shapes(flow))
        errors.extend(self._validate_outputs(flow))

        if errors:
            raise CompilationError(errors)

        return CompiledPlan(
            name=flow.name,
            source=flow.source,
            nodes=flow.process.nodes,
            output=flow.output,
            errors=dict(flow.errors),
        )

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


def _uses_kafka(flow: StreamFlow[Any, Any]) -> bool:
    """Return True if the flow uses Kafka topics."""
    if isinstance(flow.source, FromTopic):
        return True
    if flow.output is not None:
        return True
    for node in flow.process.nodes:
        if isinstance(node, IntoTopic):
            return True
        if isinstance(node, OneEmit) and isinstance(node.into, IntoTopic):
            return True
    return False


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
    if isinstance(node, Drain):
        return StreamShape.NONE
    if isinstance(node, (With, WithAsync)):
        return StreamShape.MANY
    if isinstance(node, OneEmit):
        return StreamShape.RECORD
    if isinstance(node, IntoTopic):
        return node.shape
    return current
