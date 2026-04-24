"""Loom Streaming — topic-oriented declarations and transport adapters.

Public authoring API
--------------------

Use :mod:`loom.streaming` for user-facing flow declarations::

    from loom.streaming import (
        CollectBatch,
        ErrorKind,
        FromTopic,
        IntoTopic,
        Message,
        StreamShape,
    )

Kafka-specific codecs, clients, and transport settings live under
:mod:`loom.streaming.kafka`.
"""

from loom.streaming.compiler import CompilationError, compile_flow
from loom.streaming.core._errors import ErrorEnvelope, ErrorKind
from loom.streaming.core._message import Message, MessageMeta
from loom.streaming.graph._flow import Process, ProcessNode, StreamFlow
from loom.streaming.nodes._boundary import (
    FromTopic,
    IntoTopic,
    PartitionGuarantee,
    PartitionPolicy,
    PartitionStrategy,
)
from loom.streaming.nodes._helpers import msg
from loom.streaming.nodes._protocols import Predicate, Selector
from loom.streaming.nodes._router import Route, Router
from loom.streaming.nodes._shape import CollectBatch, Drain, ForEach, StreamShape
from loom.streaming.nodes._task import BatchTask, ResourceFactory, Task, TaskContext
from loom.streaming.nodes._with import (
    AsyncContextDependency,
    ContextFactory,
    OneEmit,
    ResourceScope,
    SyncContextDependency,
    With,
    WithAsync,
)

__all__ = [
    "AsyncContextDependency",
    "BatchTask",
    "CollectBatch",
    "CompilationError",
    "ContextFactory",
    "Drain",
    "ErrorEnvelope",
    "ErrorKind",
    "ForEach",
    "FromTopic",
    "IntoTopic",
    "Message",
    "MessageMeta",
    "OneEmit",
    "PartitionGuarantee",
    "PartitionPolicy",
    "PartitionStrategy",
    "Process",
    "ProcessNode",
    "Predicate",
    "ResourceFactory",
    "ResourceScope",
    "Route",
    "Router",
    "Selector",
    "StreamFlow",
    "StreamShape",
    "SyncContextDependency",
    "Task",
    "TaskContext",
    "With",
    "WithAsync",
    "compile_flow",
    "msg",
]
