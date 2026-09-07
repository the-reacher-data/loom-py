"""The ``conversation`` loader: one use case before a run that continues a thread.

Runs in the runtime's single path, before any engine stream opens, as the
caller — through the deps bundle's bound invoker, exactly as the output hook
— and hands the engine a neutral :class:`~loom.ai.abc.Conversation`.  The
history is opaque bytes the loader returned: nothing here decodes, inspects
or retains it, and nothing here imports an engine or an optional extra.
"""

from __future__ import annotations

from typing import Any, Final

from loom.ai._usecase import invoke_as, require_invoker
from loom.ai.abc import Conversation, DepsFactory
from loom.ai.compiler._plan import CompiledConversation
from loom.ai.errors import CONVERSATION_LOAD_FAILED_MESSAGE, AgentRunErrorCode
from loom.ai.runtime._hooks import HookRun, bounded, failure_error
from loom.core.di import LoomContainer

_LOADER: Final[str] = "conversation loader"
"""Name of the loader step in log lines and errors."""


def loader_command(run: HookRun, accepted: frozenset[str]) -> dict[str, Any]:
    """Build the command the loader use case receives, filtered to its Input's names.

    Filtering to ``accepted`` — the Input's declared names, computed once at
    compile — lets a ``forbid_unknown_fields`` Command decode the result.

    Args:
        run: Context of the admitted run; ``conversation_id`` is set.
        accepted: Internal names the Input declares.

    Returns:
        The payload ``from_payload`` will decode.
    """
    offered: dict[str, Any] = {
        "conversation_id": run.conversation_id,
        "interaction_id": run.interaction_id,
        "subject": run.identity.subject,
        "mechanism": run.identity.mechanism,
        "agent": run.plan.name,
    }
    return {name: value for name, value in offered.items() if name in accepted}


async def _invoke_loader(
    loader: CompiledConversation, run: HookRun, deps: DepsFactory, container: LoomContainer
) -> object:
    """Run the loader use case as the caller through the bundle's bound invoker."""
    bundle = deps.build(run.identity, container)
    invoker = require_invoker(bundle, f"{_LOADER} '{loader.usecase}'")
    command = loader_command(run, loader.accepted)
    return await invoke_as(invoker, loader.use_case, run.identity, params=None, payload=command)


def _as_history(value: object) -> bytes | None:
    """Accept only ``None`` or ``bytes`` across the boundary; refuse anything else."""
    if value is None or isinstance(value, bytes):
        return value
    raise TypeError(f"{_LOADER} returned {type(value).__name__}, expected bytes or None")


async def load_conversation(
    run: HookRun, deps: DepsFactory, container: LoomContainer
) -> Conversation | None:
    """Load the conversation a run continues, or ``None`` when it is single-shot.

    The loader runs only when the plan declares one and the run carries a
    ``conversation_id``; it is bounded by ``tool_timeout_ms`` and shielded
    like the hook.  A failure is mapped to a coded, detail-free error before
    the engine starts, so no model tokens are spent.

    Args:
        run: Context of the admitted run.
        deps: Per-invocation dependency factory.
        container: Application container.

    Returns:
        The conversation the engine receives, or ``None``.

    Raises:
        AgentRunError: ``CONVERSATION_LOAD_FAILED`` when the loader raises,
            exceeds its bound or returns anything but ``bytes | None``;
            ``UNAUTHORIZED`` on an application denial.  Its ``usage`` is
            ``None``: nothing was spent.
    """
    loader = run.plan.conversation
    if loader is None or run.conversation_id is None:
        return None
    try:
        raw = await bounded(_invoke_loader(loader, run, deps, container), run, what=_LOADER)
        history = _as_history(raw)
    except Exception as exc:  # recovery: the run fails closed with a coded, detail-free error
        raise failure_error(
            exc,
            run,
            code=AgentRunErrorCode.CONVERSATION_LOAD_FAILED,
            message=CONVERSATION_LOAD_FAILED_MESSAGE,
            what=_LOADER,
        ) from exc
    return Conversation(conversation_id=run.conversation_id, history=history)
