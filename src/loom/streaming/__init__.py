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
from loom.streaming.nodes._shape import CollectBatch, Drain, ForEach, StreamShape, WindowStrategy
from loom.streaming.nodes._step import (
    BatchExpandStep,
    BatchStep,
    ExpandStep,
    RecordStep,
    ResourceFactory,
    Step,
    StepContext,
)
from loom.streaming.nodes._with import (
    AsyncContextDependency,
    ContextFactory,
    ResourceScope,
    SyncContextDependency,
    With,
    WithAsync,
)

__all__ = [
    "AsyncContextDependency",
    "BatchExpandStep",
    "BatchStep",
    "CollectBatch",
    "CompilationError",
    "ContextFactory",
    "Drain",
    "ErrorEnvelope",
    "ErrorKind",
    "ForEach",
    "FromTopic",
    "ExpandStep",
    "IntoTopic",
    "Message",
    "MessageMeta",
    "PartitionGuarantee",
    "PartitionPolicy",
    "PartitionStrategy",
    "Process",
    "ProcessNode",
    "Predicate",
    "RecordStep",
    "ResourceFactory",
    "ResourceScope",
    "Route",
    "Router",
    "Selector",
    "StreamFlow",
    "StreamShape",
    "WindowStrategy",
    "Step",
    "WindowStrategy",
    "StepContext",
    "SyncContextDependency",
    "With",
    "WithAsync",
    "compile_flow",
    "msg",
]
