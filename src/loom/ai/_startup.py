"""Start-up verification of ``Agent()`` marker bindings, shared by every worker.

Both composition roots that admit the marker — the FastAPI auto-bootstrap
(:mod:`loom.rest.fastapi.auto`) and the Celery worker bootstrap
(:mod:`loom.celery.bootstrap`) — call :func:`verify_agent_markers` once every
compiled use case declaring at least one ``Agent()`` binding is already known
via :func:`~loom.core.use_case.agent_markers.declaring_agent_bindings`, so
neither ever imports this module for a deployment that declares no marker at
all (FR-050).

The Celery worker never builds an AI runtime (T402: task workers do not serve
the AI pillar), so it always calls this with an empty ``plans_by_name`` —
which reports every declared agent as unknown, the same message a mistyped
name gets under a real runtime, naming the parameter and the use case that
could not run.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from typing import get_args

from loom.ai.compiler import AgentPlan
from loom.ai.errors import (
    AgentCompilationError,
    AgentCompilationIssue,
    agent_marker_output_mismatch,
    agent_marker_unknown,
)
from loom.core.engine.compilable import Compilable
from loom.core.engine.plan import AgentBinding
from loom.core.use_case.registry import UseCaseRegistry


def verify_agent_markers(
    declaring: Sequence[tuple[type[Compilable], tuple[AgentBinding, ...]]],
    registry: UseCaseRegistry,
    plans_by_name: Mapping[str, AgentPlan],
) -> None:
    """Abort start-up when an ``Agent()`` marker cannot be satisfied.

    Args:
        declaring: Use cases declaring at least one ``Agent()`` binding,
            paired with those bindings — see
            :func:`~loom.core.use_case.agent_markers.declaring_agent_bindings`.
        registry: Resolves a use case's registered key for the error message.
        plans_by_name: Every compiled agent plan reachable in this
            deployment, by its own name. Empty when no AI runtime was built
            at all, which reports every declared agent as unknown.

    Raises:
        AgentCompilationError: Aggregating one issue per unknown agent name
            and per mismatched output type, so a single run reports every
            problem at once.
    """
    issues: list[AgentCompilationIssue] = [
        issue
        for uc_type, agent_bindings in declaring
        for issue in _agent_binding_issues(
            registry.key_for(uc_type) or uc_type.__qualname__, agent_bindings, plans_by_name
        )
    ]
    if issues:
        raise AgentCompilationError(issues)


def _agent_binding_issues(
    uc_name: str,
    agent_bindings: Sequence[AgentBinding],
    plans_by_name: Mapping[str, AgentPlan],
) -> Iterator[AgentCompilationIssue]:
    """Yield one issue per ``Agent()`` binding that start-up cannot satisfy.

    Args:
        uc_name: Registered key (or qualname) of the declaring use case.
        agent_bindings: Every ``Agent()`` parameter that use case declares.
        plans_by_name: Every compiled agent plan, by its own name.
    """
    available = tuple(plans_by_name)
    for binding in agent_bindings:
        agent_plan = plans_by_name.get(binding.agent)
        if agent_plan is None:
            yield agent_marker_unknown(uc_name, binding.name, binding.agent, available)
            continue
        expected_args = get_args(binding.annotation)
        if not expected_args:
            # No type argument to check against — e.g. a bare 'AgentHandle'
            # annotation with no subscript. Nothing this pass can compare,
            # so it is not reported as a mismatch.
            continue
        expected = expected_args[0]
        declared = agent_plan.output.decoder.type
        if expected is declared:
            continue
        yield agent_marker_output_mismatch(
            uc_name,
            binding.name,
            binding.agent,
            expected=getattr(expected, "__name__", str(expected)),
            declared=getattr(declared, "__name__", str(declared)),
        )


__all__ = ["verify_agent_markers"]
