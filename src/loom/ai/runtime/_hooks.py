"""The ``on_output`` hook: one use case per completed run, engine-neutral.

Sits outside the limit supervisor and outside every engine: when the
supervised stream produces its terminal event, the wrapper here runs the
plan's hook use case as the caller — through the deps bundle's bound invoker,
so the executor binds ``Caller()`` to the run's identity — and re-creates the
terminal event with the run's ``interaction_id`` and the hook's result.  An
engine never sees the hook, so its own retry loop cannot replay it.

The bounded, shielded execution of a use case (``bounded``, ``failure_error``)
and the admitted-run context (``HookRun``) live here and are shared with the
conversation loader in ``_conversation.py``.

Nothing here imports an engine or an optional extra.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncGenerator, AsyncIterator, Coroutine
from dataclasses import dataclass
from typing import Any, Final

import msgspec

from loom.ai._usecase import invoke_as, require_invoker
from loom.ai.abc import AgentEvent, DepsFactory, ErrorEvent, FinalEvent
from loom.ai.compiler._plan import (
    HOOK_MESSAGES_FIELD,
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

_HOOK: Final[str] = "on_output hook"
"""Name of the hook step in log lines and errors."""

_CANCEL_GRACE_S: Final[float] = 1.0
"""Time a bounded use case cut at its bound gets to observe its cancellation."""


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


def hook_command(
    output: object,
    run: HookRun,
    accepted: frozenset[str],
    *,
    messages: bytes | None = None,
) -> dict[str, Any]:
    """Build the command the hook use case receives, filtered to its Input's names.

    The validated output is nested under ``output``, the run's new messages
    under ``messages``, and the run context is offered beside them; nothing
    from the output can shadow a context name.  Filtering to ``accepted`` —
    the Input's declared names, computed once at compile — lets a
    ``forbid_unknown_fields`` Command decode the result.

    Args:
        output: Validated answer of the run.
        run: Context of the admitted run.
        accepted: Internal names the Input declares.
        messages: The run's new messages in the engine's serialised form;
            ``None`` on a run without a conversation.

    Returns:
        The payload ``from_payload`` will decode.
    """
    offered: dict[str, Any] = {
        HOOK_OUTPUT_FIELD: msgspec.to_builtins(output),
        HOOK_MESSAGES_FIELD: messages,
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
    final: FinalEvent,
    run: HookRun,
    deps: DepsFactory,
    container: LoomContainer,
) -> object:
    """Run the hook use case as the caller through the bundle's bound invoker."""
    bundle = deps.build(run.identity, container)
    invoker = require_invoker(bundle, f"{_HOOK} '{hook.usecase}'")
    command = hook_command(final.output, run, hook.accepted, messages=final.messages)
    return await invoke_as(invoker, hook.use_case, run.identity, params=None, payload=command)


async def bounded(coro: Coroutine[Any, Any, object], run: HookRun, *, what: str) -> object:
    """Await ``coro`` shielded from the consumer and bounded by ``tool_timeout_ms``.

    The coroutine runs as its own task and is only waited on, never cancelled
    by the consumer's cancellation, so a started use case finishes or fails
    cleanly even when the consumer leaves; its outcome is retrieved on every
    exit path, so nothing ends "never retrieved".  Transaction safety is not
    the shield's job: the executor exits the unit of work with the exception
    on any ``BaseException`` and the adapter rolls back.

    Args:
        coro: The use-case invocation to run.
        run: Context of the admitted run; its policies carry the bound.
        what: Name of the bounded step for log lines and errors
            (``"on_output hook"``, ``"conversation loader"``).

    Returns:
        The coroutine's return value.

    Raises:
        TimeoutError: When the task does not complete within the bound.
        asyncio.CancelledError: When the consumer was cancelled; re-raised
            once the task settled within the remaining bound.  A second
            cancellation during that wait cancels the task as well.
        RuntimeError: When the task ended cancelled on its own, without the
            consumer being cancelled: a use-case failure, not a consumer exit.
        Exception: Whatever the coroutine raised.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + run.plan.policies.tool_timeout_ms / 1000
    task = asyncio.ensure_future(coro)
    try:
        async with asyncio.timeout_at(deadline):
            # ``wait`` never cancels the task and returns once it is done,
            # cancelled included: a ``CancelledError`` here is the consumer's.
            await asyncio.wait({task})
    except asyncio.CancelledError:
        try:
            await _settle(task, max(deadline - loop.time(), 0.0), run, what)
        except asyncio.CancelledError:
            task.cancel()
            raise
        raise
    except TimeoutError:
        task.cancel()
        await _settle(task, _CANCEL_GRACE_S, run, what)
        raise
    if task.cancelled():
        raise RuntimeError(f"the {what} was cancelled internally")
    return task.result()


async def _settle(task: asyncio.Future[object], bound: float, run: HookRun, what: str) -> None:
    """Wait up to ``bound`` seconds for the task, never cancelling it; record how it ended."""
    if not task.done():
        try:
            async with asyncio.timeout(bound):
                await asyncio.wait({task})
        except TimeoutError:
            pass
    if not task.done():
        task.cancel()
        # The task runs detached from here on: retrieve a late outcome so an
        # exception raised after this point is never "never retrieved".
        task.add_done_callback(_retrieve_outcome)
        _logger.warning(
            "%s of agent %r still running at its bound after the consumer "
            "left; cancelled (interaction %s)",
            what,
            run.plan.name,
            run.interaction_id,
        )
        return
    if task.cancelled():
        return
    if task.exception() is not None:
        _logger.error(
            "%s of agent %r failed after the consumer left (interaction %s)",
            what,
            run.plan.name,
            run.interaction_id,
            exc_info=task.exception(),
        )
        return
    _logger.info(
        "%s of agent %r completed after the consumer left (interaction %s)",
        what,
        run.plan.name,
        run.interaction_id,
    )


def failure_error(
    exc: BaseException,
    run: HookRun,
    *,
    code: AgentRunErrorCode,
    message: str,
    what: str,
) -> AgentRunError:
    """Map a bounded use case's failure to the coded error the caller receives.

    An ``AgentRunError`` keeps its own code and text and only gains the run's
    ``interaction_id``; an application denial becomes ``UNAUTHORIZED`` with
    the fixed denial text; anything else is logged server-side and answered
    with ``code`` and ``message``, never with the exception's detail.  The
    error's ``usage`` is left ``None``: the caller stamps what the run spent.

    Args:
        exc: What the bounded step raised.
        run: Context of the admitted run.
        code: Code of the generic failure branch.
        message: Fixed client text of the generic failure branch.
        what: Name of the failed step for the server-side log line.

    Returns:
        The error to raise or to turn into an ``ErrorEvent``.
    """
    if isinstance(exc, AgentRunError):
        return AgentRunError(exc.code, str(exc), interaction_id=run.interaction_id)
    if isinstance(exc, _DENIALS):
        return AgentRunError(
            AgentRunErrorCode.UNAUTHORIZED, _DENIED_MESSAGE, interaction_id=run.interaction_id
        )
    _logger.exception(
        "%s of agent %r failed (interaction %s)", what, run.plan.name, run.interaction_id
    )
    return AgentRunError(code, message, interaction_id=run.interaction_id)


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
        result = await bounded(_invoke_hook(hook, final, run, deps, container), run, what=_HOOK)
    except Exception as exc:  # recovery: the run fails closed with a coded, detail-free error
        error = failure_error(
            exc, run, code=AgentRunErrorCode.HOOK_FAILED, message=HOOK_FAILED_MESSAGE, what=_HOOK
        )
        # The model run itself succeeded, so its usage is known and travels
        # with the failure: a hook that fails does not make the run free.
        return ErrorEvent(
            code=error.code,
            message=str(error),
            interaction_id=run.interaction_id,
            usage=final.usage,
        )
    return msgspec.structs.replace(final, interaction_id=run.interaction_id, hook_result=result)
