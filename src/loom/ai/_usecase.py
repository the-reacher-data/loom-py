"""Engine-free helpers to run a granted use case as a given caller.

Both the pydantic-ai capability tools and the runtime output hook invoke a use
case through the dependency bundle's **bound** invoker, with the caller
installed as the ambient identity for the duration of the call. This module
holds that shared piece without importing any engine, so the runtime can use
it with no optional extra installed.
"""

from __future__ import annotations

from typing import Any

from loom.ai.errors import AgentRunError, AgentRunErrorCode
from loom.core.engine.compilable import Compilable
from loom.core.identity import Identity, reset_identity, set_identity
from loom.core.use_case.invoker import ApplicationInvoker


def require_invoker(bundle: object, label: str) -> ApplicationInvoker:
    """Return the bundle's invoker, refusing a bundle that carries none.

    Read from the bundle rather than resolved from the container: the bundle's
    invoker is the one the composition root already bound to this invocation's
    caller, and the container holds only the unbound singleton.

    Args:
        bundle: Dependency bundle built for the caller by the deps factory.
        label: Caller of the refused call, worded as it should appear in the
            error (``"tool 'lookup'"``, ``"on_output hook 'incidents.record'"``).

    Returns:
        The invoker bound to the caller.

    Raises:
        AgentRunError: ``UNAUTHORIZED`` when the bundle carries no invoker.
    """
    invoker = getattr(bundle, "invoker", None)
    if isinstance(invoker, ApplicationInvoker):
        return invoker
    raise AgentRunError(
        AgentRunErrorCode.UNAUTHORIZED,
        f"{label} requires a dependency bundle exposing an 'invoker' bound to its caller",
    )


async def invoke_as(
    invoker: ApplicationInvoker,
    use_case: type[Compilable],
    identity: Identity,
    *,
    params: dict[str, Any] | None,
    payload: dict[str, Any] | None,
) -> object:
    """Invoke ``use_case`` through ``invoker`` with ``identity`` as the ambient caller.

    The identity is installed only for the duration of the call and restored in
    a ``finally`` block, so a reused task never inherits it.

    Args:
        invoker: Invoker already bound to ``identity`` by the composition root.
        use_case: Use case type to run.
        identity: Caller to install as the ambient identity during the call.
        params: Query-style parameters forwarded untouched.
        payload: Body-style payload forwarded untouched.

    Returns:
        Whatever the use case returns.
    """
    token = set_identity(identity)
    try:
        return await invoker.invoke(use_case, params=params, payload=payload)
    finally:
        reset_identity(token)
