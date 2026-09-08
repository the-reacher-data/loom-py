"""Dynamic-instructions phase: resolve the artifact's prompt factory offline.

The artifact names application code that contributes instructions per request.
It is resolved here, exactly as a ``kind: python`` toolset factory is, so a
broken reference is a compile-time issue and never a failed run — and, because
this is prompt material, never a run whose model silently loses half its
instructions.

Three faults are rejected here, each with its own code: the reference does not
import, it resolves to something that cannot be called as
``factory(context, **params)``, or the declared ``params`` do not bind to the
signature. The shape check is the one every ``factory(context, **params)``
declaration shares (:mod:`loom.ai.compiler.phases._factories`); only the
mapping from its named fault to *this* field's codes lives here, so a
mis-signed instructions factory can never be reported as a ``python``
capability fault.

What is *not* checked here is what the factory returns, because nothing is
called at compile time: the provider is inspected where the factory actually
runs, at engine build (``engines.pydantic_ai._instructions``).
"""

from __future__ import annotations

from typing import cast

from loom.ai.abc import InstructionsFactory
from loom.ai.compiler._plan import CompiledDynamicInstructions
from loom.ai.compiler._symbols import import_symbol
from loom.ai.compiler.phases._factories import FactoryFault, reject_factory_signature
from loom.ai.declarative import DynamicInstructionsSpec
from loom.ai.errors import (
    AgentCompilationIssue,
    dynamic_instructions_not_callable,
    dynamic_instructions_params_rejected,
    dynamic_instructions_unresolvable,
)

_CompileResult = tuple[CompiledDynamicInstructions | None, list[AgentCompilationIssue]]


def compile_dynamic_instructions(
    spec: DynamicInstructionsSpec | None, component: str
) -> _CompileResult:
    """Resolve the declared ``dynamic_instructions`` block to its factory handle.

    Args:
        spec: The declared block, or ``None`` when the artifact declares none.
        component: Artifact path or agent name the issues point at.

    Returns:
        The compiled block (or ``None`` when none was declared, and when the
        declared one is faulty) and the issues found.
    """
    if spec is None:
        return None, []
    try:
        symbol = import_symbol(spec.factory)
    except (ImportError, AttributeError, ValueError) as exc:
        return None, [dynamic_instructions_unresolvable(component, spec.factory, str(exc))]
    if not callable(symbol):
        return None, [dynamic_instructions_not_callable(component, spec.factory)]
    issue = _signature_issue(symbol, spec, component)
    if issue is not None:
        return None, [issue]
    # An imported symbol is ``object``: the checks above are all a static
    # narrowing can do here. What the factory *returns* is checked at build,
    # where it is called.
    compiled = CompiledDynamicInstructions(
        factory_ref=spec.factory,
        factory=cast("InstructionsFactory", symbol),
        params=dict(spec.params),
    )
    return compiled, []


def _signature_issue(
    factory: object, spec: DynamicInstructionsSpec, component: str
) -> AgentCompilationIssue | None:
    """Map a refused ``factory(context, **params)`` signature to this field's codes."""
    rejection = reject_factory_signature(cast("InstructionsFactory", factory), spec.params)
    if rejection is None:
        return None
    if rejection.fault is FactoryFault.NOT_CALLABLE:
        return dynamic_instructions_not_callable(component, spec.factory)
    return dynamic_instructions_params_rejected(component, spec.factory, rejection.detail)
