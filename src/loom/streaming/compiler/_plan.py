"""Compiled plan structures — output of the compiler, input to the adapter."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from loom.streaming._errors import ErrorKind
from loom.streaming._shape import StreamShape


@dataclass(frozen=True)
class CompiledSource:
    """Resolved Kafka source with decode strategy."""

    settings: object  # ConsumerSettings — typed as object to avoid kafka import
    topics: tuple[str, ...]
    payload_type: type
    shape: StreamShape
    decode_strategy: Literal["record", "batch"]


@dataclass(frozen=True)
class CompiledSink:
    """Resolved Kafka sink with partitioning."""

    settings: object  # ProducerSettings
    topic: str
    partition_policy: object | None  # PartitionPolicy | None


@dataclass(frozen=True)
class CompiledNode:
    """DSL node annotated with input/output shapes."""

    node: object
    input_shape: StreamShape
    output_shape: StreamShape


@dataclass(frozen=True)
class CompiledPlan:
    """Immutable result of compiling a StreamFlow."""

    name: str
    source: CompiledSource
    nodes: tuple[CompiledNode, ...]
    output: CompiledSink | None
    error_routes: dict[ErrorKind, CompiledSink]
    needs_async_bridge: bool
