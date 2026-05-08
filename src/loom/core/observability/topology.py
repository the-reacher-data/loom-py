"""Topology rules for observability scopes and parent relationships."""

from __future__ import annotations

from loom.core.observability.event import Scope

ROOT_SCOPES: frozenset[Scope] = frozenset(
    {Scope.USE_CASE, Scope.JOB, Scope.POLL_CYCLE, Scope.PIPELINE, Scope.MAINTENANCE}
)

PARENT_SCOPES: dict[Scope, Scope] = {
    Scope.NODE: Scope.POLL_CYCLE,
    Scope.BATCH_COLLECT: Scope.POLL_CYCLE,
    Scope.WRITE: Scope.POLL_CYCLE,
    Scope.TRANSPORT: Scope.POLL_CYCLE,
}


def parent_scope(scope: Scope) -> Scope | None:
    """Return the parent scope for a given scope, if any."""
    return PARENT_SCOPES.get(scope)


def span_parent_key(scope: Scope, trace_id: str | None) -> str:
    """Return the registry key used to link a span to its parent scope."""
    parent = parent_scope(scope)
    if parent is None:
        return ""
    return f"{parent}::{trace_id or ''}"


__all__ = ["ROOT_SCOPES", "PARENT_SCOPES", "parent_scope", "span_parent_key"]
