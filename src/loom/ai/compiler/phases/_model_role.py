"""Model-role phase: the declared role must be bound in ``ai.models``."""

from __future__ import annotations

from collections.abc import Mapping

from loom.ai.errors import AgentCompilationIssue, model_role_unbound
from loom.ai.inference import InferenceTarget

_ResolveResult = tuple[InferenceTarget | None, list[AgentCompilationIssue]]


def resolve_model_role(
    role: str,
    models: Mapping[str, InferenceTarget],
    component: str,
) -> _ResolveResult:
    """Resolve the agent's model role to its configured binding.

    Args:
        role: Logical model role the artifact declares.
        models: ``ai.models`` bindings from deployment configuration.
        component: Artifact path or agent name the issue points at.

    Returns:
        The resolved :class:`InferenceTarget` (or ``None``) and a
        ``MODEL_ROLE_UNBOUND`` issue when the role is absent (research R-002).
    """
    target = models.get(role)
    if target is None:
        return None, [model_role_unbound(component, role)]
    return target, []
