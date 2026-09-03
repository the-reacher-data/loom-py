"""The ``on_output`` hook: one use case per completed run, engine-neutral.

Sits outside the limit supervisor and outside every engine: when the
supervised stream produces its terminal event, the wrapper here runs the
plan's hook use case as the caller — through the deps bundle's bound invoker,
so the executor binds ``Caller()`` to the run's identity — and re-creates the
terminal event with the run's ``interaction_id`` and the hook's result.  An
engine never sees the hook, so its own retry loop cannot replay it.

Nothing here imports an engine or an optional extra.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncGenerator, AsyncIterator
from dataclasses import dataclass
from typing import Any, Final

import msgspec

from loom.ai._usecase import invoke_as, require_invoker
from loom.ai.abc import AgentEvent, DepsFactory, ErrorEvent, FinalEvent
from loom.ai.compiler._plan import (
    HOOK_OUTPUT_FIELD,
    AgentPlan,
    CompiledOutputHook,
)
from loom.ai.errors import AgentRunError, AgentRunErrorCode
from loom.core.di import LoomContainer
from loom.core.errors import Forbidden, Unauthenticated
from loom.core.identity import Identity
from loom.core.sql.abc import RoleNotAllowedError, RolesNotBoundError

_logger = logging.getLogger(__name__)

HOOK_FAILED_MESSAGE: Final[str] = "the output hook failed; the detail is recorded server-side"
"""Fixed text of a ``HOOK_FAILED`` error: the exception never reaches the caller."""

_DENIED_MESSAGE: Final[str] = "the caller is not allowed to perform this operation"
"""Fixed text of an authorization denial, the same the tool path answers."""

_DENIALS: Final[tuple[type[Exception], ...]] = (
    Forbidden,
    Unauthenticated,
    RoleNotAllowedError,
    RolesNotBoundError,
)
"""Application denials mapped to ``UNAUTHORIZED``, as the tool path maps them."""

_CANCEL_GRACE_S: Final[float] = 1.0
"""Time a hook cut at its bound gets to observe its cancellation."""


def no_terminal_message(agent: str) -> str:
    """Return the text of the error closing a stream that ended without a terminal event.

    Args:
        agent: Name of the agent whose engine exhausted its stream silently.

    Returns:
        The ``PROVIDER_UNAVAILABLE`` message the stream and ``run()`` share.
    """
    return f"agent {agent!r} produced no terminal event"


@dataclass(frozen=True, slots=True)
class HookRun:
    """The run-owned context of one admitted run.

    Args:
        plan: Compiled plan being run.
        identity: Caller the run and its hook execute as.
        interaction_id: Identifier minted at admission.
        conversation_id: Opaque value the caller supplied, or ``None``.
    """

    plan: AgentPlan
    identity: Identity
    interaction_id: str
    conversation_id: str | None


def hook_command(output: object, run: HookRun, accepted: frozenset[str]) -> dict[str, Any]:
    """Build the command the hook use case receives, filtered to its Input's names.

    The validated output is nested under ``output`` and the run context is
    offered beside it; nothing from the output can shadow a context name.
    Filtering to ``accepted`` — the Input's declared names, computed once at
    compile — lets a ``forbid_unknown_fields`` Command decode the result.

    Args:
        output: Validated answer of the run.
        run: Context of the admitted run.
        accepted: Internal names the Input declares.

    Returns:
        The payload ``from_payload`` will decode.
    """
    offered: dict[str, Any] = {
        HOOK_OUTPUT_FIELD: msgspec.to_builtins(output),
        "interaction_id": run.interaction_id,
        "conversation_id": run.conversation_id,
        "subject": run.identity.subject,
        "mechanism": run.identity.mechanism,
        "agent": run.plan.name,
        "provider": run.plan.inference.provider,
        "model": run.plan.inference.model,
    }
    return {name: value for name, value in offered.items() if name in accepted}


async def _invoke_hook(
    hook: CompiledOutputHook,
    output: object,
    run: HookRun,
    deps: DepsFactory,
    container: LoomContainer,
) -> object:
    """Run the hook use case as the caller through the bundle's bound invoker."""
    bundle = deps.build(run.identity, container)
    invoker = require_invoker(bundle, f"on_output hook '{hook.usecase}'")
    command = hook_command(output, run, hook.accepted)
    return await invoke_as(invoker, hook.use_case, run.identity, params=None, payload=command)


async def execute_hook(
    hook: CompiledOutputHook,
    output: object,
    run: HookRun,
    deps: DepsFactory,
    container: LoomContainer,
) -> object:
    """Execute the hook, shielded from the consumer and bounded by ``tool_timeout_ms``.

    A started record finishes or fails cleanly even when the consumer leaves:
    the executor only rolls back on ``Exception``, so an unshielded
    cancellation would leave a begun unit of work without rollback.  The
    shielded task is awaited on every exit path, so no outcome goes
    unretrieved.

    Args:
        hook: Compiled hook of the plan.
        output: Validated answer of the run.
        run: Context of the admitted run.
        deps: Per-invocation dependency factory.
        container: Application container.

    Returns:
        The use case's return value.

    Raises:
        TimeoutError: When the hook does not complete within the bound.
        asyncio.CancelledError: When the consumer was cancelled; re-raised
            once the hook settled within the remaining bound.  A second
            cancellation during that wait cancels the hook as well.
        RuntimeError: When the hook task ended cancelled on its own, without
            the consumer being cancelled: a hook failure, not a consumer exit.
        Exception: Whatever the use case raised.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + run.plan.policies.tool_timeout_ms / 1000
    task = asyncio.ensure_future(_invoke_hook(hook, output, run, deps, container))
    try:
        async with asyncio.timeout_at(deadline):
            return await asyncio.shield(task)
    except asyncio.CancelledError:
        if task.cancelled() and not _consumer_cancelled():
            raise RuntimeError("the output hook was cancelled internally") from None
        try:
            await _settle(task, max(deadline - loop.time(), 0.0), run)
        except asyncio.CancelledError:
            task.cancel()
            raise
        raise
    except TimeoutError:
        task.cancel()
        await _settle(task, _CANCEL_GRACE_S, run)
        raise


def _consumer_cancelled() -> bool:
    """Report whether the current task itself has a cancellation pending.

    ``asyncio.shield`` raises ``CancelledError`` both when the awaiting task is
    cancelled and when the shielded task ended cancelled on its own; only the
    task's own cancel count tells the two apart.
    """
    current = asyncio.current_task()
    return current is not None and current.cancelling() > 0


async def _settle(task: asyncio.Future[object], timeout: float, run: HookRun) -> None:
    """Wait for the hook task without cancelling it, then record how it ended."""
    await asyncio.wait({task}, timeout=timeout)
    if not task.done():
        task.cancel()
        # The task runs detached from here on: retrieve a late outcome so an
        # exception raised after this point is never "never retrieved".
        task.add_done_callback(_retrieve_outcome)
        _logger.warning(
            "on_output hook of agent %r still running at its bound after the consumer "
            "left; cancelled (interaction %s)",
            run.plan.name,
            run.interaction_id,
        )
        return
    if task.cancelled():
        return
    if task.exception() is not None:
        _logger.error(
            "on_output hook of agent %r failed after the consumer left (interaction %s)",
            run.plan.name,
            run.interaction_id,
            exc_info=task.exception(),
        )
        return
    _logger.info(
        "on_output hook of agent %r completed after the consumer left (interaction %s)",
        run.plan.name,
        run.interaction_id,
    )


def _retrieve_outcome(task: asyncio.Future[object]) -> None:
    """Mark a detached task's outcome as retrieved once it finally ends."""
    if not task.cancelled():
        task.exception()


async def hooked_events(
    events: AsyncIterator[AgentEvent],
    run: HookRun,
    deps: DepsFactory,
    container: LoomContainer,
) -> AsyncGenerator[AgentEvent, None]:
    """Forward a supervised stream, running the hook at its terminal event.

    Every terminal event leaves carrying ``interaction_id``; a ``final`` event
    additionally carries the hook's result, or becomes an ``error`` when the
    hook fails.  A consumer that closes the stream early never reaches the
    terminal event, so no hook runs for an abandoned run.  A supervised stream
    exhausted without a terminal event (engine misbehaviour) is closed with a
    ``PROVIDER_UNAVAILABLE`` error, so every admitted run ends named.

    Args:
        events: The supervised event stream of one run.
        run: Context of the admitted run.
        deps: Per-invocation dependency factory.
        container: Application container.

    Yields:
        The run's events, terminal event re-created.
    """
    async for event in events:
        if type(event) is ErrorEvent:
            yield msgspec.structs.replace(event, interaction_id=run.interaction_id)
            return
        if type(event) is FinalEvent:
            yield await _terminal(event, run, deps, container)
            return
        yield event
    yield ErrorEvent(
        code=AgentRunErrorCode.PROVIDER_UNAVAILABLE,
        message=no_terminal_message(run.plan.name),
        interaction_id=run.interaction_id,
    )


async def _terminal(
    final: FinalEvent, run: HookRun, deps: DepsFactory, container: LoomContainer
) -> FinalEvent | ErrorEvent:
    """Run the hook, if any, and produce the stream's terminal event."""
    hook = run.plan.on_output
    if hook is None:
        return msgspec.structs.replace(final, interaction_id=run.interaction_id)
    try:
        result = await execute_hook(hook, final.output, run, deps, container)
    except AgentRunError as exc:
        return ErrorEvent(code=exc.code, message=str(exc), interaction_id=run.interaction_id)
    except _DENIALS:
        return ErrorEvent(
            code=AgentRunErrorCode.UNAUTHORIZED,
            message=_DENIED_MESSAGE,
            interaction_id=run.interaction_id,
        )
    except Exception:  # recovery: the run fails closed with a coded, detail-free error
        _logger.exception(
            "on_output hook of agent %r failed (interaction %s)", run.plan.name, run.interaction_id
        )
        return ErrorEvent(
            code=AgentRunErrorCode.HOOK_FAILED,
            message=HOOK_FAILED_MESSAGE,
            interaction_id=run.interaction_id,
        )
    return msgspec.structs.replace(final, interaction_id=run.interaction_id, hook_result=result)
