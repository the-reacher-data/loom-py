"""Contract tests for streaming adapter capabilities."""

from __future__ import annotations

import inspect

from loom.streaming.bytewax._adapter import _NODE_HANDLERS
from loom.streaming.nodes import (
    BatchExpandStep,
    BatchStep,
    CollectBatch,
    Drain,
    ExpandStep,
    ForEach,
    Fork,
    IntoTopic,
    RecordStep,
    Router,
    With,
    WithAsync,
)


def test_router_branch_safe_nodes_are_handled_by_adapter() -> None:
    """Every router-branch-safe node must have a Bytewax adapter handler."""
    handled_types = set(_NODE_HANDLERS.keys())
    router_branch_safe_types = {
        cls
        for cls in (
            RecordStep,
            BatchStep,
            ExpandStep,
            BatchExpandStep,
            CollectBatch,
            ForEach,
            Router,
            With,
            WithAsync,
            IntoTopic,
            Drain,
        )
        if getattr(cls, "router_branch_safe", False)
    }

    assert router_branch_safe_types <= handled_types


def test_router_branch_safe_nodes_are_marked_on_public_api() -> None:
    """The public nodes module should expose router-branch-safe declarations."""
    import loom.streaming.nodes as nodes

    marked = {
        cls.__name__
        for _, cls in inspect.getmembers(nodes, inspect.isclass)
        if getattr(cls, "router_branch_safe", False)
        and cls.__module__.startswith("loom.streaming.nodes.")
        and cls.__name__ != "Step"
    }

    assert {
        "RecordStep",
        "BatchStep",
        "ExpandStep",
        "BatchExpandStep",
        "CollectBatch",
        "ForEach",
        "Router",
        "With",
        "WithAsync",
        "IntoTopic",
        "Drain",
    } <= marked


def test_fork_is_handled_by_adapter() -> None:
    """Fork must be executable by the Bytewax adapter."""
    assert Fork in _NODE_HANDLERS


def test_fork_when_with_is_handled_by_adapter() -> None:
    """Fork branches with scoped resources must be executable by the adapter."""
    assert With in _NODE_HANDLERS
