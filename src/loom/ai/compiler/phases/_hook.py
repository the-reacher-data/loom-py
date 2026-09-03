"""Output-hook phase: resolve ``on_output.usecase`` and prove its Input is feedable.

The runtime offers one nested ``output`` key plus a fixed set of run-context
names (:data:`~loom.ai.compiler._plan.HOOK_CONTEXT_FIELDS`).  The proof is a
small, exact rule over the use case's compiled execution plan: no primitive
parameters, an ``Input()`` whose type builds from a payload, and every
required, user-supplied Input name inside the offered set.  ``internal`` and
``calculated`` command fields are infrastructure-owned and never demanded;
``Caller()`` is injected by the executor and never demanded either.

Nothing about the output schema is inspected: ``output`` is always offered
as one nested value, whatever the artifact's ``output`` block says.

Use cases are compiled by the bootstrap before the agent compiler runs, so
this phase reads ``__execution_plan__`` and never calls the use-case compiler
itself — an uncompiled use case is a refusal, not a job.
"""

from __future__ import annotations

from collections.abc import Sequence

import msgspec

from loom.ai.compiler._plan import HOOK_CONTEXT_FIELDS, HOOK_OUTPUT_FIELD, CompiledOutputHook
from loom.ai.declarative import AgentSpecV1, UsecaseCapability
from loom.ai.errors import (
    AgentCompilationIssue,
    on_output_input_unsatisfied,
    on_output_usecase_also_granted,
    on_output_usecase_unknown,
)
from loom.core.command.introspection import get_command_fields, get_input_fields
from loom.core.engine.compilable import Compilable
from loom.core.engine.plan import ExecutionPlan
from loom.core.use_case.registry import UseCaseRegistry

_HookResult = tuple[CompiledOutputHook | None, list[AgentCompilationIssue]]

_OFFERED: frozenset[str] = frozenset({HOOK_OUTPUT_FIELD, *HOOK_CONTEXT_FIELDS})


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
    if _is_granted(spec.capabilities, key):
        issues.append(on_output_usecase_also_granted(component, key))
    try:
        use_case = registry.resolve(key)
    except KeyError:
        issues.append(on_output_usecase_unknown(component, key))
        return None, issues
    accepted, reason = _accepted_names(use_case)
    if reason is not None:
        issues.append(on_output_input_unsatisfied(component, key, reason))
    if issues:
        return None, issues
    return CompiledOutputHook(usecase=key, use_case=use_case, accepted=accepted), []


def _is_granted(capabilities: Sequence[object], key: str) -> bool:
    """Whether a ``kind: usecase`` grant of the spec lists ``key``.

    Checked on the spec rather than on the compiled capability so the conflict
    is reported even when that grant fails on another key.
    """
    return any(
        key in capability.keys
        for capability in capabilities
        if isinstance(capability, UsecaseCapability)
    )


def _accepted_names(use_case: type[Compilable]) -> tuple[frozenset[str], str | None]:
    """Return the Input's declared names, or the reason the Input is unfeedable."""
    execution = use_case.__execution_plan__
    if execution is None:
        return frozenset(), "the use case is not compiled (no __execution_plan__)"
    reason = _binding_reason(execution)
    if reason is not None or execution.input_binding is None:
        return frozenset(), reason
    command_type = execution.input_binding.command_type
    declared = {info.name: info for info in msgspec.structs.fields(command_type)}
    excluded = get_command_fields(command_type).keys() - get_input_fields(command_type).keys()
    extra = [
        name
        for name, info in declared.items()
        if info.required and name not in excluded and name not in _OFFERED
    ]
    if extra:
        return frozenset(), (
            f"Input requires {', '.join(sorted(extra))} but the run offers only "
            f"{', '.join(sorted(_OFFERED))}"
        )
    return frozenset(declared), None


def _binding_reason(execution: ExecutionPlan) -> str | None:
    """Reason the execution plan's bindings cannot be fed from a run, if any."""
    if execution.param_bindings:
        names = ", ".join(binding.name for binding in execution.param_bindings)
        return f"execute declares primitive parameters the run cannot bind: {names}"
    if execution.input_binding is None:
        return "execute declares no Input()"
    # Defensive: ``UseCaseCompiler`` already refuses an Input type without
    # ``from_payload``, but a plan built through another path must not pass.
    if not callable(getattr(execution.input_binding.command_type, "from_payload", None)):
        return "the Input type does not implement from_payload(payload)"
    return None
