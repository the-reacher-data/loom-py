"""The artifact's ``dynamic_instructions``, registered on the built agent.

Two moments, and keeping them apart is the whole design.

**At build, once.** The plan carries the imported factory; it is called here
with the same build-time context a ``kind: python`` capability factory gets,
and with the same failure handling: anything it raises is reported by
*exception class* only, never by its message, because that message could echo
a ``params`` value into the issues an operator reads. What it returns is
checked before the agent ever runs: a provider that is not callable, or that
is a coroutine function, is refused here. The engine would happily await a
coroutine, and the await would enter the prompt path silently — which is
exactly what the synchronous contract of
:data:`~loom.ai.abc.InstructionsProvider` exists to prevent.

**At run, once per model request.** The engine rebuilds the instructions before
every request it makes to the model, so a run that calls a tool calls the
provider more than once; the wrapper below is therefore kept free of anything
that would be wrong to repeat. The registered function is where the run's
identity exists, so it is where :class:`~loom.ai.abc.InstructionsRequest` is
built, from a fail-closed read of the dependency bundle: a bundle without a
verified caller fails there with ``UNAUTHORIZED``, before any tool runs. A
provider that raises ends the run with a fixed message and a server-side log
line, as a failing ``on_output`` hook already does — the application's own
exception text never reaches the caller.

The engine composes the literal ``instructions`` first and appends what is
registered here, which is the order the artifact promises.
"""

from __future__ import annotations

import inspect
import logging
from collections.abc import Callable
from typing import Any, Final, cast

from pydantic_ai import Agent
from pydantic_ai.tools import RunContext

from loom.ai.abc import InstructionsProvider, InstructionsRequest
from loom.ai.compiler import AgentPlan, CompiledDynamicInstructions
from loom.ai.engines.pydantic_ai._capabilities import build_toolset_context
from loom.ai.engines.pydantic_ai._guards import BuildContext, capability_deps
from loom.ai.engines.pydantic_ai._mcp import SharedMcpToolsets
from loom.ai.errors import (
    AgentCompilationError,
    AgentRunError,
    AgentRunErrorCode,
    dynamic_instructions_factory_failed,
    dynamic_instructions_not_callable,
    dynamic_instructions_provider_coroutine,
)
from loom.core.di import LoomContainer

_logger = logging.getLogger(__name__)

INSTRUCTIONS_FAILED_MESSAGE: Final[str] = (
    "the agent could not build its instructions; the detail is recorded server-side"
)
"""Fixed text of an ``INSTRUCTIONS_FAILED`` error: the exception never reaches the caller."""


def register_dynamic_instructions(
    agent: Agent[Any, Any],
    plan: AgentPlan,
    container: LoomContainer,
    *,
    mcp: SharedMcpToolsets,
) -> None:
    """Build the plan's instructions provider and register it on the agent.

    Args:
        agent: Agent built from the plan's spec.
        plan: Compiled plan, carrying the imported factory when the artifact
            declares one — in which case the factory is called here, once.
        container: Application container the factory receives on its context.
        mcp: The worker's shared MCP toolsets, so the context this factory
            gets is the one a ``kind: python`` factory of the same plan gets.

    Raises:
        AgentCompilationError: When the factory raises, returns something that
            cannot be called, or returns a coroutine function.
    """
    compiled = plan.dynamic_instructions
    if compiled is None:
        return
    context = BuildContext.of(plan, container, mcp)
    provider = _build_provider(compiled, context)
    agent.instructions(_as_instructions(plan.name, compiled.factory_ref, provider))


def _build_provider(
    compiled: CompiledDynamicInstructions, context: BuildContext
) -> InstructionsProvider:
    """Call the factory once and refuse a provider the run path cannot use."""
    produced = _call_factory(compiled, context)
    if not callable(produced):
        raise AgentCompilationError(
            [dynamic_instructions_not_callable(context.agent, compiled.factory_ref)]
        )
    if inspect.iscoroutinefunction(produced):
        raise AgentCompilationError(
            [dynamic_instructions_provider_coroutine(context.agent, compiled.factory_ref)]
        )
    # The factory is typed as returning a provider and the two checks above are
    # all a static narrowing can do; the return type is the author's contract,
    # which Python enforces on the first call.
    return cast("InstructionsProvider", produced)


def _call_factory(compiled: CompiledDynamicInstructions, context: BuildContext) -> object:
    """Run the factory, turning anything it raises into a coded issue.

    A refusal the context itself raised (``PYTHON_REMOTE_NOT_GRANTED``) passes
    through untouched. Any other exception is reported by class name only: its
    text could echo a ``params`` value, and the artifact's issues are shown to
    whoever deploys it.
    """
    factory_context = build_toolset_context(compiled.factory_ref, context)
    try:
        return compiled.factory(factory_context, **compiled.params)
    except AgentCompilationError:
        raise
    except Exception as exc:
        raise AgentCompilationError(
            [
                dynamic_instructions_factory_failed(
                    context.agent, compiled.factory_ref, type(exc).__name__
                )
            ]
        ) from exc


def _as_instructions(
    agent: str, factory_ref: str, provider: InstructionsProvider
) -> Callable[[RunContext[Any]], str | None]:
    """Wrap a provider in the engine's instructions contract.

    The wrapper takes the run context, which is the only place the run's
    identity and prompt exist, and hands the provider loom's own request
    struct instead — so prompt-building code never reaches the container or
    the caller-bound invoker the bundle also carries.
    """

    def instructions(run: RunContext[Any]) -> str | None:
        deps = capability_deps(run)
        request = InstructionsRequest(
            agent=agent,
            prompt=_prompt_text(run),
            subject=deps.identity.subject,
            mechanism=deps.identity.mechanism,
        )
        try:
            return provider(request)
        # Recovery: the run fails closed with a coded, detail-free error, and
        # the application's exception is logged rather than returned.
        except Exception as exc:
            _logger.exception("instructions factory %r of agent %r failed", factory_ref, agent)
            raise AgentRunError(
                AgentRunErrorCode.INSTRUCTIONS_FAILED, INSTRUCTIONS_FAILED_MESSAGE
            ) from exc

    return instructions


def _prompt_text(run: RunContext[Any]) -> str:
    """Return the run's prompt as text.

    The engine types the prompt as text *or* a sequence of content parts, and
    loom's engine only ever calls it with text. A non-text prompt is therefore
    unreachable through loom, and it is answered with an empty prompt rather
    than with a rendering of engine-native content objects, which is not what
    the contract says the field is.
    """
    return run.prompt if isinstance(run.prompt, str) else ""
