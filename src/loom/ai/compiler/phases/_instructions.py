"""Instructions phase: the string form compiles; the block form does not yet.

``AgentSpecV1.instructions`` accepts a literal string or a non-empty sequence
of :class:`~loom.ai.declarative.InstructionBlock` (spec 016, T101), but
:class:`~loom.ai.compiler._plan.AgentPlan.instructions` is still ``str``:
compiling a block sequence into a plan is T202. Until then, this phase fails
closed on the block form instead of letting the compiler forward the tuple
where the plan expects a string.
"""

from __future__ import annotations

from loom.ai.declarative import InstructionBlock
from loom.ai.errors import AgentCompilationIssue, instruction_block_invalid

_CompileResult = tuple[str | None, list[AgentCompilationIssue]]


def compile_instructions(
    instructions: str | tuple[InstructionBlock, ...], component: str
) -> _CompileResult:
    """Compile the declared ``instructions`` into the plan's ``str`` field.

    Args:
        instructions: Value decoded from the artifact's ``instructions``.
        component: Artifact path or agent name the issue points at.

    Returns:
        The literal string unchanged and no issues; or ``None`` and one
        ``INSTRUCTION_BLOCK_INVALID`` issue when the artifact declares the
        block form, which no phase compiles yet.
    """
    if isinstance(instructions, str):
        return instructions, []
    reason = "the block form is not compiled yet; author a literal string instead"
    return None, [instruction_block_invalid(component, reason)]
