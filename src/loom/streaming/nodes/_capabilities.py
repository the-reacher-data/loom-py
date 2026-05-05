"""Capability markers for streaming node declarations."""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class RouterBranchSafe(Protocol):
    """Marker for nodes that may appear inside a Router branch."""

    router_branch_safe: bool
