"""The escape hatch: this engine's own objects, for what loom does not serve.

It lives in the engine package rather than in :mod:`loom.ai.abc` so that the
neutral surface keeps naming no engine: importing this module is the explicit
act of leaving loom's guarantees behind. What a run driven from here keeps and
loses is stated once, on :meth:`~loom.ai.abc.AgentHandle.native` — this module
carries the contract, not the reasoning.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from typing import Any

from pydantic_ai import Agent
from pydantic_ai.usage import UsageLimits

from loom.ai.abc import AgentHandle
from loom.ai.engines.pydantic_ai._errors import as_run_error


@dataclass(frozen=True, slots=True)
class NativeAgent:
    """What the pydantic-ai engine would itself have used to serve one run.

    Attributes:
        agent: The agent built from this plan's spec, carrying the artefact's
            ``output_check`` validator when it declares one. Shared by every
            run of this plan for the life of the worker: handed over as it
            is, not copied, so mutating it changes every run of that agent
            in this worker, including the ones loom itself drives. Refuses a
            run-level ``output_type`` when it holds a validator (pydantic-ai's
            own ``UserError``); use :attr:`shaped_agent` for a run that
            overrides the output shape.
        shaped_agent: The same agent spec, built with no ``output_check``
            validator registered, for a run overriding the plan's declared
            output shape. Identical to :attr:`agent` — the very same object —
            whenever the plan declares no ``output_check``.
        deps: The dependency bundle of one caller, carrying their verified
            identity and this run's state. Every capability call made with it
            runs as that caller, which is what keeps a run driven from here
            inside the artefact's own grants.
        usage_limits: The artefact's spend caps, projected onto the engine's
            own type and copied for this call: mutating the returned value
            never changes what the engine enforces on its own, loom-supervised
            runs. Passing it on to a driven run is the caller's own choice.
        guard: Zero-argument factory of the async context manager that
            rejoins this runtime's agent-chain and admission bounds for a
            driven run's body; enter it through :func:`native_run`.
    """

    agent: Agent[Any, Any]
    shaped_agent: Agent[Any, Any]
    deps: object
    usage_limits: UsageLimits
    guard: Callable[[], AbstractAsyncContextManager[None]]


def native_agent(handle: AgentHandle[Any], *, state: object | None = None) -> NativeAgent:
    """Return the pydantic-ai objects behind one agent handle.

    Args:
        handle: Handle an ``Agent()`` marker filled, already bound to the
            caller of the use case holding it.
        state: This run's state, resolved against the artefact's declared
            shape exactly as :meth:`~loom.ai.abc.AgentHandle.run` resolves it.

    Returns:
        This engine's own objects; see :class:`NativeAgent`.

    Raises:
        TypeError: When the agent behind *handle* is served by another engine,
            whose native form this function cannot name.
        NotImplementedError: When that engine publishes no native form at all.
        AgentRunError: With ``UNAUTHORIZED`` when *handle*'s identity is
            anonymous; with ``STATE_UNDECLARED`` or ``STATE_REQUIRED`` when
            *state* does not match the artefact's declared shape.

    Example::

        access = native_agent(handle)
        async with (
            native_run(access),
            access.agent.iter("triage this", deps=access.deps) as run,
        ):
            async for node in run:
                ...
    """
    native = handle.native(state=state)
    if not isinstance(native, NativeAgent):
        raise TypeError(
            "this agent is not served by the pydantic-ai engine: its native form is "
            f"{type(native).__name__}, not NativeAgent"
        )
    return native


@asynccontextmanager
async def native_run(access: NativeAgent) -> AsyncIterator[None]:
    """Rejoin loom's chain and admission bounds around a hand-driven run.

    Args:
        access: The carrier returned by :func:`native_agent`.

    Yields:
        Nothing; the caller drives the agent inside this block.

    Raises:
        AgentRunError: ``AGENT_CALL_CYCLE`` or ``AGENT_CALL_TOO_DEEP`` when
            entering the guard refuses this run; ``TOO_MANY_RUNS`` when no
            admission permit is free; otherwise, whatever coded failure
            :func:`~loom.ai.engines.pydantic_ai._errors.as_run_error`
            classifies a raw exception raised inside the block into.
    """
    async with access.guard():
        try:
            yield
        except Exception as exc:
            raise as_run_error(exc) from exc


__all__ = ["NativeAgent", "as_run_error", "native_agent", "native_run"]
