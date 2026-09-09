"""Cheap, AI-pillar-free scan for declared ``Agent()`` marker bindings.

A composition root that admits the marker — the FastAPI and Celery
bootstraps — must know whether *any* compiled use case declares one before
deciding whether to import the AI pillar at all. Importing it only to find
nothing to check would be exactly the containment leak an application
without an ``ai:`` section is meant to avoid (FR-050): this module reads
only the already-compiled :class:`~loom.core.engine.plan.ExecutionPlan` of
each use case, so it never imports :mod:`loom.ai`.
"""

from __future__ import annotations

from collections.abc import Sequence

from loom.core.engine.compilable import Compilable
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.plan import AgentBinding

DeclaringAgentBindings = list[tuple[type[Compilable], tuple[AgentBinding, ...]]]


def declaring_agent_bindings(
    use_cases: Sequence[type[Compilable]],
    compiler: UseCaseCompiler,
) -> DeclaringAgentBindings:
    """Return every compiled use case that declares at least one ``Agent()`` marker.

    Args:
        use_cases: Every use case compiled for this deployment.
        compiler: Compiler holding the cached plan of each of them.

    Returns:
        Pairs of (use case type, its agent bindings), one per use case that
        declares at least one binding, in the order ``use_cases`` lists
        them. Empty when none does.
    """
    return [
        (uc_type, plan.agent_bindings)
        for uc_type in use_cases
        for plan in (compiler.get_plan(uc_type),)
        if plan is not None and plan.agent_bindings
    ]


__all__ = ["DeclaringAgentBindings", "declaring_agent_bindings"]
