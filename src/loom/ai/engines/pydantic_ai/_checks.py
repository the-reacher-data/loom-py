"""The artifact's ``output_check``, registered as the engine's output validator.

The plan carries a :data:`~loom.ai.abc.OutputCheck`: a rule over the answer's
*values* that returns ``None`` to accept and the text the model must read to
correct itself to reject. The engine's own vocabulary for "answer again" is
``ModelRetry``, so the whole adaptation is one translation of that return value
into that exception, and it lives here rather than in the provider so the
engine factory keeps one responsibility.

**Why a validator and not a transformer.** Loom decodes the answer from the
run's raw messages after the run ends (``_output``), not from the value the
engine hands the validator, so a validator that returned a *different* value
would see its change silently dropped. The wrapper therefore returns the
received object by identity: it never copies it, and it never builds another.
A check that mutates the mapping in place changes nothing downstream for the
same reason.

**Cost.** One function call per output attempt, and no serialisation: the
mapping the check receives is the one the engine already parsed.

**The native output mode cannot serve a check**, so an artifact declaring one
under a binding that pins it is refused at build by
:func:`reject_unservable_output_check`, not silently degraded. In that mode the
provider delivers the structured answer as text parts, loom projects text parts
as deltas, and the engine retries inside a single run — so the caller would read
the rejected answer and then the accepted one. Loom holds a written rule that a
run is never replayed once a delta has reached the caller, and this is the one
combination that would break it.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any, Final

from pydantic_ai import Agent, ModelRetry

from loom.ai.abc import OutputCheck
from loom.ai.compiler import AgentPlan
from loom.ai.errors import AgentCompilationError, output_check_native_mode_unsupported

_NATIVE_OUTPUT_MODE: Final[str] = "native"
"""Mode of ``ai.models.<role>`` whose consumer-visible retry a check would duplicate."""


def reject_unservable_output_check(plan: AgentPlan) -> None:
    """Refuse the one artifact-and-deployment pair loom cannot serve.

    Both halves are known here: the plan says whether the artifact declares a
    check, and its resolved binding says whether the operator pinned the native
    output mode. Neither half may be overridden — the mode is deployment
    configuration and the check is the artifact's contract — so the mismatch is
    reported instead of resolved.

    Args:
        plan: Compiled plan about to be built into an engine.

    Raises:
        AgentCompilationError: When the plan declares an ``output_check`` and
            its binding pins the native output mode.
    """
    if plan.output_check is None or plan.inference.output_mode != _NATIVE_OUTPUT_MODE:
        return
    raise AgentCompilationError(
        [output_check_native_mode_unsupported(plan.name, plan.inference.model)]
    )


def register_output_check(agent: Agent[Any, Any], check: OutputCheck | None) -> None:
    """Register the artifact's answer rule on an already-built agent.

    Args:
        agent: Agent built from the plan's spec.
        check: The plan's compiled check, or ``None`` when the artifact
            declares none — in which case nothing is registered and the agent
            keeps no wrapper at all.
    """
    if check is None:
        return
    agent.output_validator(_as_validator(check))


def _as_validator(check: OutputCheck) -> Callable[[Mapping[str, Any]], Mapping[str, Any]]:
    """Wrap a check in the engine's validator contract.

    The wrapper takes one parameter, which is how the engine tells a validator
    that wants the run context from one that does not: an output check is pure
    by contract and has no use for it.
    """

    def validate(answer: Mapping[str, Any]) -> Mapping[str, Any]:
        message = check(answer)
        if message is not None:
            raise ModelRetry(message)
        return answer

    return validate
