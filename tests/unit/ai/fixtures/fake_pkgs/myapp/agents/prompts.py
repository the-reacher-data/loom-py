"""Instructions factories referenced by the corpus and by the phase tests.

Every symbol here is what an application would legitimately write, or exactly
one deliberate fault, so a test names the fault by importing its symbol rather
than by building a callable the artifact could never reference.
"""

from __future__ import annotations

from loom.ai.abc import InstructionsProvider, InstructionsRequest, ToolsetContext

NOT_CALLABLE: int = 42
"""Resolves fine, but is not callable, so it cannot satisfy ``InstructionsFactory``."""


def build_checklist(context: ToolsetContext, *, locale: str = "en") -> InstructionsProvider:
    """Build the per-request checklist provider once, at start-up.

    Args:
        context: Build-time context; the agent name is all this factory reads.
        locale: Language tag the rendered checklist is labelled with.

    Returns:
        The provider called once per model request, which for a run that calls
        tools is more than once.
    """
    agent = context.agent

    def provide(request: InstructionsRequest) -> str | None:
        return f"[{locale}] {agent} checklist for: {request.prompt}"

    return provide


def build_checklist_without_context(*, locale: str = "en") -> InstructionsProvider:
    """A factory with no slot for the build-time context: the wrong shape."""
    del locale

    def provide(request: InstructionsRequest) -> str | None:
        del request
        return None

    return provide
