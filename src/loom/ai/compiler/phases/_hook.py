"""Output-hook phase: resolve ``on_output.usecase`` and prove its Input is feedable.

The runtime offers one nested ``output`` key, the run's serialised new
``messages``, the run's ``tool_calls`` summary and a fixed set of run-context
names (:data:`~loom.ai.compiler._plan.HOOK_CONTEXT_FIELDS`).  The feedability proof
itself lives in :mod:`loom.ai.compiler.phases._feedable`, shared with the
conversation phase so the two rules cannot drift.

Nothing about the output schema is inspected: ``output`` is always offered
as one nested value, whatever the artifact's ``output`` block says.
"""

from __future__ import annotations

from loom.ai.compiler._plan import (
    HOOK_CONTEXT_FIELDS,
    HOOK_MESSAGES_FIELD,
    HOOK_OUTPUT_FIELD,
    HOOK_TOOL_CALLS_FIELD,
    CompiledOutputHook,
)
from loom.ai.compiler.phases._feedable import feedable_input, is_granted
from loom.ai.declarative import AgentSpecV1
from loom.ai.errors import (
    AgentCompilationIssue,
    on_output_input_unsatisfied,
    on_output_usecase_also_granted,
    on_output_usecase_unknown,
)
from loom.core.use_case.registry import UseCaseRegistry

_HookResult = tuple[CompiledOutputHook | None, list[AgentCompilationIssue]]

_OFFERED: frozenset[str] = frozenset(
    {HOOK_OUTPUT_FIELD, HOOK_MESSAGES_FIELD, HOOK_TOOL_CALLS_FIELD, *HOOK_CONTEXT_FIELDS}
)


def compile_output_hook(
    spec: AgentSpecV1,
    *,
    component: str,
    registry: UseCaseRegistry,
) -> _HookResult:
    """Resolve the artifact's output hook and prove the run can feed it.

    Args:
        spec: Decoded artifact; ``spec.on_output`` may be ``None``.
        component: Artifact provenance every issue points at.
        registry: Use-case registry the key resolves against.

    Returns:
        The compiled hook and no issues; ``None`` and no issues when the
        artifact declares no hook; ``None`` and the issues found otherwise.
    """
    if spec.on_output is None:
        return None, []
    key = spec.on_output.usecase
    issues: list[AgentCompilationIssue] = []
    if is_granted(spec.capabilities, key):
        issues.append(on_output_usecase_also_granted(component, key))
    try:
        use_case = registry.resolve(key)
    except KeyError:
        issues.append(on_output_usecase_unknown(component, key))
        return None, issues
    accepted, reason = feedable_input(use_case, _OFFERED)
    if reason is not None:
        issues.append(on_output_input_unsatisfied(component, key, reason))
    if issues:
        return None, issues
    return CompiledOutputHook(usecase=key, use_case=use_case, accepted=accepted), []
