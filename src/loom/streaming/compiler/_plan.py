"""Compiled plan structures - output of the compiler, input to the adapter."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from loom.core.model import LoomFrozenStruct, LoomStruct
from loom.streaming.core._errors import ErrorKind
from loom.streaming.kafka._config import ConsumerSettings, ProducerSettings
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
class CompiledSink:
    """Resolved Kafka sink with partitioning."""

    settings: ProducerSettings
    topic: str
    partition_policy: PartitionPolicy[LoomStruct | LoomFrozenStruct] | None


@dataclass(frozen=True)
class CompiledNode:
    """DSL node annotated with input/output shapes."""

    node: object
    input_shape: StreamShape
    output_shape: StreamShape
    path: tuple[int, ...] = ()


@dataclass(frozen=True)
class CompiledPlan:
    """Immutable result of compiling a StreamFlow."""

    name: str
    source: CompiledSource
    nodes: tuple[CompiledNode, ...]
    output: CompiledSink | None
    error_routes: dict[ErrorKind, CompiledSink]
    needs_async_bridge: bool
    terminal_sinks: dict[tuple[int, ...], CompiledSink] = field(default_factory=dict)
