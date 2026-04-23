"""Streaming DSL node primitives.

These are the building blocks used to declare a :class:`Process`.
"""

from loom.streaming.nodes._boundary import (
    FromTopic,
    IntoTopic,
    PartitionGuarantee,
    PartitionPolicy,
    PartitionStrategy,
)
from loom.streaming.nodes._helpers import msg
from loom.streaming.nodes._protocols import Predicate, Selector
from loom.streaming.nodes._router import Route, Router, evaluate_predicate, select_value
from loom.streaming.nodes._shape import CollectBatch, Drain, ForEach, StreamShape
from loom.streaming.nodes._task import BatchTask, ResourceFactory, Task, TaskContext
from loom.streaming.nodes._with import (
    AsyncContextDependency,
    ContextDependency,
    ContextFactory,
    OneEmit,
    ResourceScope,
    SyncContextDependency,
    With,
    WithAsync,
)

__all__ = [
    "BatchTask",
    "CollectBatch",
    "AsyncContextDependency",
    "ContextDependency",
    "ContextFactory",
    "Drain",
    "ForEach",
    "FromTopic",
    "IntoTopic",
    "OneEmit",
    "PartitionGuarantee",
    "PartitionPolicy",
    "PartitionStrategy",
    "Predicate",
    "ResourceFactory",
    "ResourceScope",
    "Route",
    "Router",
    "Selector",
    "StreamShape",
    "SyncContextDependency",
    "Task",
    "TaskContext",
    "With",
    "WithAsync",
    "evaluate_predicate",
    "msg",
    "select_value",
]
