"""Compiled plan structures and compiler errors - output of the compiler, input to the adapter."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from loom.core.model import LoomFrozenStruct, LoomStruct
from loom.streaming.core._errors import ErrorKind
from loom.streaming.kafka._config import ConsumerSettings, ProducerSettings
from loom.streaming.kafka._wire import DispatchTable
from loom.streaming.nodes._boundary import PartitionPolicy
from loom.streaming.nodes._shape import StreamShape


@dataclass(frozen=True)
class CompiledSource:
    """Resolved Kafka source with decode strategy."""

    settings: ConsumerSettings
    topics: tuple[str, ...]
    payload_type: type[LoomStruct | LoomFrozenStruct]
    shape: StreamShape
    decode_strategy: Literal["record", "batch"]


@dataclass(frozen=True)
class CompiledMultiSource:
    """Resolved heterogeneous Kafka source with a pre-built dispatch table.

    Args:
        settings: Resolved Kafka consumer settings.
        topics: Kafka topic names to subscribe to.
        dispatch: Pre-built table mapping ``message_type`` and error
            ``payload_type`` strings to their concrete Python types.
        shape: Declared source shape.
        decode_strategy: Whether to decode records individually or in batches.
    """

    settings: ConsumerSettings
    topics: tuple[str, ...]
    dispatch: DispatchTable
    shape: StreamShape
    decode_strategy: Literal["record", "batch"]


@dataclass(frozen=True)
class CompiledSink:
    """Resolved Kafka sink with partitioning."""

    settings: ProducerSettings
    topic: str
    partition_policy: PartitionPolicy[LoomStruct | LoomFrozenStruct] | None
    dlq_topic: str | None = None


@dataclass(frozen=True)
class CompiledNode:
    """DSL node annotated with input/output shapes."""

    node: object
    input_shape: StreamShape
    output_shape: StreamShape
    path: tuple[int, ...] = ()


class CompilationError(Exception):
    """Raised when a StreamFlow fails validation or cannot be compiled.

    Args:
        errors: List of human-readable error messages.
    """

    def __init__(self, errors: list[str]) -> None:
        self.errors = errors
        super().__init__(f"Compilation failed with {len(errors)} error(s): {'; '.join(errors)}")


CompiledSourceLike = CompiledSource | CompiledMultiSource
"""Union of all compiled source types accepted by a CompiledPlan."""


@dataclass(frozen=True)
class CompiledPlan:
    """Immutable result of compiling a StreamFlow."""

    name: str
    source: CompiledSourceLike
    nodes: tuple[CompiledNode, ...]
    output: CompiledSink | None
    error_routes: dict[ErrorKind, CompiledSink]
    needs_async_bridge: bool
    terminal_sinks: dict[tuple[int, ...], CompiledSink] = field(default_factory=dict)
