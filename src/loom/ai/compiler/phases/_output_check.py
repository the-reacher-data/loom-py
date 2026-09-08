"""Output-check phase: resolve the artifact's answer rule to a callable.

The artifact names a rule its schema cannot express — "a report that claims
resolution must name a root cause" — as a bare ``module:symbol`` reference.
It is resolved here, offline, exactly as a ``kind: python`` toolset factory is,
so a broken reference is a compile-time issue and never a failed run.

Three faults are rejected, each with its own code. Two are the same faults the
capability phase rejects for a factory: the reference does not import, or it
resolves to something that cannot be called. The third is specific to this
contract: a coroutine function is refused rather than awaited. The engine calls
the check once per output attempt inside its own retry loop, where an await
would enter a path no deadline of loom's covers, and the returned coroutine
would be truthy, so every answer would be rejected with an unreadable message.

The symbol's *signature* is not inspected. ``params`` do not exist here, so
there is nothing to bind against, and the arity is the alias' contract
(:data:`~loom.ai.abc.OutputCheck`), which Python enforces on the first call.
"""

from __future__ import annotations

import inspect
from typing import cast

from loom.ai.abc import OutputCheck
from loom.ai.compiler._symbols import import_symbol
from loom.ai.errors import (
    AgentCompilationIssue,
    output_check_coroutine_unsupported,
    output_check_not_callable,
    output_check_unresolvable,
)

_CompileResult = tuple[OutputCheck | None, list[AgentCompilationIssue]]


def compile_output_check(ref: str | None, component: str) -> _CompileResult:
    """Resolve the declared ``output_check`` reference to the callable to register.

    Args:
        ref: ``module:symbol`` reference the artifact declared, or ``None``
            when it declares no check.
        component: Artifact path or agent name the issues point at.

    Returns:
        The imported check (or ``None`` when none was declared, and when the
        declared one is faulty) and the issues found.
    """
    if ref is None:
        return None, []
    try:
        symbol = import_symbol(ref)
    except (ImportError, AttributeError, ValueError) as exc:
        return None, [output_check_unresolvable(component, ref, str(exc))]
    if not callable(symbol):
        return None, [output_check_not_callable(component, ref)]
    if inspect.iscoroutinefunction(symbol):
        return None, [output_check_coroutine_unsupported(component, ref)]
    # An imported symbol is ``object``: the checks above are all a static
    # narrowing can do, and the rest of the alias — arity and return type — is
    # the author's contract, enforced by Python on the first call.
    return cast("OutputCheck", symbol), []
