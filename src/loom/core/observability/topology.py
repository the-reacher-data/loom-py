"""Topology rules for observability scopes.

Root scopes are the ones that end a unit of work a short-lived process is
built around, so the runtime drains its own span exporter when one closes.
"""

from __future__ import annotations

from loom.core.observability.event import Scope

ROOT_SCOPES: frozenset[Scope] = frozenset(
    {Scope.USE_CASE, Scope.JOB, Scope.POLL_CYCLE, Scope.PIPELINE, Scope.MAINTENANCE}
)


__all__ = ["ROOT_SCOPES"]
