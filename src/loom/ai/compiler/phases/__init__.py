"""Validation and compilation phases of the agent compiler.

Each phase is a pure function returning the value it compiled (when any) plus
the list of :class:`~loom.ai.errors.AgentCompilationIssue` it found; the
compiler accumulates issues across every phase and raises once.
"""

from loom.ai.compiler.phases._capabilities import compile_capabilities
from loom.ai.compiler.phases._limits import validate_policies
from loom.ai.compiler.phases._model_role import resolve_model_role
from loom.ai.compiler.phases._output import compile_output

__all__ = [
    "compile_capabilities",
    "compile_output",
    "resolve_model_role",
    "validate_policies",
]
