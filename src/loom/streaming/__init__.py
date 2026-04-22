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

from loom.streaming._boundary import FromTopic, IntoTopic
from loom.streaming._errors import ErrorEnvelope, ErrorKind
from loom.streaming._message import Message, MessageMeta
from loom.streaming._partitioning import PartitionGuarantee, PartitionPolicy, PartitionStrategy
from loom.streaming._process import Process, StreamFlow
from loom.streaming._resources import ResourceFactory, TaskContext
from loom.streaming._shape import CollectBatch, Drain, ForEach, StreamShape
from loom.streaming._task import BatchTask, Task
from loom.streaming._with import OneEmit, ResourceScope, With, WithAsync
from loom.streaming.compiler import CompilationError, compile_flow
from loom.streaming.routing import Predicate, Route, Router, Selector, msg

__all__ = [
    "BatchTask",
    "CollectBatch",
    "Drain",
    "ErrorEnvelope",
    "ErrorKind",
    "ForEach",
    "FromTopic",
    "IntoTopic",
    "Message",
    "MessageMeta",
    "PartitionGuarantee",
    "PartitionPolicy",
    "PartitionStrategy",
    "Process",
    "Predicate",
    "ResourceFactory",
    "Route",
    "Router",
    "Selector",
    "StreamShape",
    "StreamFlow",
    "Task",
    "TaskContext",
    "With",
    "WithAsync",
    "OneEmit",
    "ResourceScope",
    "CompilationError",
    "compile_flow",
    "msg",
]
