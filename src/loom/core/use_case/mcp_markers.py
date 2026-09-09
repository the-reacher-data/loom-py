"""Cheap, AI-pillar-free scan for declared ``Mcp()`` marker bindings.

A composition root that admits the marker — the FastAPI and Celery
bootstraps — must know whether *any* compiled use case declares one before
deciding whether to import the AI pillar at all. Importing it only to find
nothing to check would be exactly the containment leak an application
without an ``ai:`` section is meant to avoid (FR-050): this module reads
only the already-compiled :class:`~loom.core.engine.plan.ExecutionPlan` of
each use case, so it never imports :mod:`loom.ai`.

This module is the deliberate twin of
:mod:`loom.core.use_case.agent_markers`, kept as a near-literal copy rather
than generalized behind a shared abstraction: the two scan different
binding tuples (``mcp_bindings`` vs. ``agent_bindings``) for different
composition roots, and factoring out that similarity now would be
abstraction bought before a second real variation asks for it.
"""

from __future__ import annotations

from collections.abc import Sequence

from loom.core.engine.compilable import Compilable
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.plan import McpBinding

DeclaringMcpBindings = list[tuple[type[Compilable], tuple[McpBinding, ...]]]


def declaring_mcp_bindings(
    use_cases: Sequence[type[Compilable]],
    compiler: UseCaseCompiler,
) -> DeclaringMcpBindings:
    """Return every compiled use case that declares at least one ``Mcp()`` marker.

    Args:
        use_cases: Every use case compiled for this deployment.
        compiler: Compiler holding the cached plan of each of them.

    Returns:
        Pairs of (use case type, its MCP bindings), one per use case that
        declares at least one binding, in the order ``use_cases`` lists
        them. Empty when none does.
    """
    return [
        (uc_type, plan.mcp_bindings)
        for uc_type in use_cases
        for plan in (compiler.get_plan(uc_type),)
        if plan is not None and plan.mcp_bindings
    ]


__all__ = ["DeclaringMcpBindings", "declaring_mcp_bindings"]
