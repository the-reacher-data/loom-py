"""The escape hatch: this engine's own objects, for what loom does not serve.

Loom is a wrapper over pydantic-ai, and a wrapper that cannot be stepped
around is a ceiling. :func:`native_agent` is the step around it: the very
``Agent`` this deployment's plan runs on, the dependency bundle bound to one
caller, and the artefact's spend caps already projected — enough to drive a
run with pydantic-ai's own API (``iter``, ``run(event_stream_handler=...)``,
``capture_run_messages``, a capability of one's own) when loom's neutral
surface does not reach far enough.

It lives in the engine package rather than in :mod:`loom.ai.abc` so that the
neutral surface keeps naming no engine: importing this module *is* the
explicit act of leaving loom's guarantees behind, and it is why the neutral
:meth:`~loom.ai.abc.AgentHandle.native` returns ``object`` for this module to
narrow.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pydantic_ai import Agent
from pydantic_ai.usage import UsageLimits

from loom.ai.abc import AgentHandle


@dataclass(frozen=True, slots=True)
class NativeAgent:
    """What the pydantic-ai engine would itself have used to serve one run.

    Attributes:
        agent: The agent built from this plan's spec, shared by every run of
            this plan for the life of the worker. It is handed over as it is,
            not copied: mutating it — registering a tool, a validator or a
            capability on it — changes every run of that agent in this
            worker, including the ones loom itself drives.
        deps: The dependency bundle of one caller, carrying their verified
            identity and this run's state. Every capability call made with it
            runs as that caller, which is what keeps a run driven from here
            inside the artefact's own grants.
        usage_limits: The artefact's spend caps, already projected onto the
            engine's own type. Passing them on is the caller's choice: a run
            driven from here spends nothing against them unless it does.
    """

    agent: Agent[Any, Any]
    deps: object
    usage_limits: UsageLimits


def native_agent(handle: AgentHandle[Any], *, state: object | None = None) -> NativeAgent:
    """Return the pydantic-ai objects behind one agent handle.

    What a run driven through them keeps and what it loses is stated once, on
    :meth:`~loom.ai.abc.AgentHandle.native`. The short of it: the caller's
    identity and the artefact's grants survive; every supervision loom applies
    around its own runs does not.

    Args:
        handle: Handle an ``Agent()`` marker filled, already bound to the
            caller of the use case holding it.
        state: This run's state, resolved against the artefact's declared
            shape exactly as :meth:`~loom.ai.abc.AgentHandle.run` resolves it.

    Returns:
        The agent, the caller's dependency bundle and the artefact's spend
        caps.

    Raises:
        TypeError: When the agent behind *handle* is served by another engine,
            whose native form this function cannot name.
        NotImplementedError: When that engine publishes no native form at all.
        AgentRunError: With ``STATE_UNDECLARED`` when *state* is given and the
            artefact declares no state shape.

    Example::

        access = native_agent(handle)
        async with access.agent.iter("triage this", deps=access.deps) as run:
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
