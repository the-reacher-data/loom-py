"""Projection of an artifact's declared ``instructions`` onto its plan.

:attr:`~loom.ai.declarative.AgentSpecV1.instructions` accepts a literal string
or a non-empty sequence of :class:`~loom.ai.declarative.InstructionBlock`,
while :attr:`~loom.ai.compiler._plan.AgentPlan.instructions` is ``str``. The
block form is therefore refused with a coded issue rather than forwarded, so
an artifact declaring it fails at boot naming the field instead of running
against instructions nothing reads.
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
        block form.
    """
    if isinstance(instructions, str):
        return instructions, []
    reason = "a block sequence is not compiled; author instructions as a literal string"
    return None, [instruction_block_invalid(component, reason)]
