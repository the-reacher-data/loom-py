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
from loom.streaming.nodes._fork import Fork, ForkRoute
from loom.streaming.nodes._protocols import Predicate, Selector
from loom.streaming.nodes._router import Route, Router, evaluate_predicate, select_value
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
    ContextDependency,
    ContextFactory,
    ResourceScope,
    SyncContextDependency,
    With,
    WithAsync,
)
from loom.streaming.nodes.refs import msg as msg
from loom.streaming.nodes.refs import payload as payload

__all__ = [
    "BatchExpandStep",
    "BatchStep",
    "CollectBatch",
    "AsyncContextDependency",
    "ContextDependency",
    "ContextFactory",
    "Drain",
    "ExpandStep",
    "ForEach",
    "FromTopic",
    "Fork",
    "ForkRoute",
    "IntoTopic",
    "PartitionGuarantee",
    "PartitionPolicy",
    "PartitionStrategy",
    "Predicate",
    "RecordStep",
    "ResourceFactory",
    "ResourceScope",
    "Route",
    "Router",
    "Selector",
    "StreamShape",
    "SyncContextDependency",
    "WindowStrategy",
    "Step",
    "StepContext",
    "With",
    "WithAsync",
    "evaluate_predicate",
    "msg",
    "payload",
    "select_value",
]
