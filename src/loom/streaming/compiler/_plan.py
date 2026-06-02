"""Compiled plan structures and compiler errors - output of the compiler, input to the adapter."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, ClassVar, Literal

from loom.core.model import LoomFrozenStruct, LoomStruct
from loom.streaming.core._errors import ErrorKind
from loom.streaming.kafka._config import ConsumerSettings, ProducerSettings
from loom.streaming.kafka._wire import DispatchTable
from loom.streaming.mongo import MongoSourceConfig
from loom.streaming.nodes._boundary import PartitionPolicy
from loom.streaming.nodes._shape import StreamShape
from loom.streaming.nodes._table.common import (
    ClickHouseSinkConfig,
    DeltaSinkConfig,
    SqlAlchemyDatabaseConfig,
    SqlAlchemySinkConfig,
)


@dataclass(frozen=True)
class CompiledSingleSource:
    """Resolved Kafka source with a single payload type and decode strategy."""

    needs_decode: ClassVar[bool] = True

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
            ``payload_type`` strings to their concrete Python types, plus
            explicit wire-error payload types.
        shape: Declared source shape.
        decode_strategy: Whether to decode records individually or in batches.
    """

    needs_decode: ClassVar[bool] = True

    settings: ConsumerSettings
    topics: tuple[str, ...]
    dispatch: DispatchTable
    shape: StreamShape
    decode_strategy: Literal["record", "batch"]


@dataclass(frozen=True)
class CompiledMongoCDCSource:
    """Resolved MongoDB CDC source configuration."""

    needs_decode: ClassVar[bool] = False

    settings: MongoSourceConfig
    collections: tuple[str, ...]
    watch_options: Mapping[str, object]
    shape: StreamShape


@dataclass(frozen=True)
class CompiledSink:
    """Resolved Kafka sink with partitioning."""

    settings: ProducerSettings
    topic: str
    partition_policy: PartitionPolicy[LoomStruct | LoomFrozenStruct] | None
    dlq_topic: str | None = None


@dataclass(frozen=True)
class CompiledStorageSink:
    """Resolved storage sink with its DSL node and pre-fetched config section.

    The adapter calls ``node.build_partition(config, worker_index, worker_count, bridge)``
    at startup once per Bytewax worker to obtain the :class:`SinkPartition` that
    handles epoch writes and shutdown.  Storage backends that require async
    execution receive the adapter-managed :class:`~loom.core.async_bridge.AsyncBridge`.

    Args:
        node:   The :class:`~loom.streaming.nodes.IntoSink` node as declared in the DSL.
        config: Resolved storage sink config for the selected backend.
        database_config: Resolved shared SQLAlchemy database config, if needed.
    """

    node: Any
    config: SqlAlchemySinkConfig | DeltaSinkConfig | ClickHouseSinkConfig
    database_config: SqlAlchemyDatabaseConfig | None = None


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


CompiledSource = CompiledSingleSource | CompiledMultiSource | CompiledMongoCDCSource
"""Union of all compiled source variants accepted by a :class:`CompiledPlan`."""


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
    terminal_storage_sinks: dict[tuple[int, ...], CompiledStorageSink] = field(default_factory=dict)
