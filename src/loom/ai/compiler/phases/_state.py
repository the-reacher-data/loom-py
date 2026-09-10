"""State phase: resolve ``deps_type``/``deps_schema`` into one :class:`StateShape`.

``deps_schema``, ``deps_type: <symbol>`` and ``deps_type: dict`` are three
authored spellings of one thing: an optional JSON Schema (FR-003). The symbol
form is sugar — the compiler resolves the reference and calls
``msgspec.json.schema()`` on it, and from that point the artifact is
indistinguishable from one that wrote the schema by hand. The ``dict`` form
is the absence of a schema and therefore the absence of validation (FR-006).
:func:`compile_state` resolves all three to one small frozen value with two
fields, built by three branches of one factory function.

The symbol-resolution step and the hand-written-schema-to-decoder step are
not reimplemented here: both come from
:mod:`loom.ai.compiler.phases._output` (:func:`~loom.ai.compiler.phases._output._resolve_symbol`,
:func:`~loom.ai.compiler.phases._output._schema_to_decoder`), parametrised by
this module's own issue factories. What genuinely differs stays local: the
state symbol path derives its schema straight from ``msgspec.json.schema()``
rather than validating a ``msgspec.Struct`` first, the way the output side
does.

State reaches the artifact through a caller's request body, so it forbids
unknown fields exactly as the output side does (FR-017): the ``deps_schema``
path already does through ``_annotation_for``'s generated struct, and
:func:`_schema_admits_unknown_fields` extends the same requirement to the
``deps_type`` symbol path — by reading msgspec's own
``additionalProperties`` verdict on the derived schema, never by inspecting
the symbol.

**The first draft of this module was a Strategy** — ``SymbolState`` /
``SchemaState`` / ``OpenState`` behind a Protocol — justified by the claim
that the engine offers three distinct template entry points. The spike this
train measured (see ``spec.md``, "What the spike measured") found that it
offers one: the typed template path raises ``PydanticSchemaGenerationError``
on a ``msgspec.Struct``, and ``msgspec.json.schema()`` is what bridges a
declared symbol onto the one schema-based path that remains. Three classes to
carry a ``Mapping | None`` and a decoder is the premature abstraction
``.claude/rules/engineering.md`` forbids. The branching happens once, in this
module's factory function; what leaves it is a value with two fields, and
nothing downstream asks which of the three spellings produced it — it asks
whether ``schema`` is set and whether ``decoder`` is set.

The check on a ``deps_type`` symbol is the call to ``msgspec.json.schema()``
itself, not an inspection of the symbol: the question is not *is this a
type* but *can a schema be derived from it*, and only msgspec answers that
(FR-004).

This module does not import ``pydantic_ai`` or any other engine package: the
compiler is engine-agnostic.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType
from typing import Any

import msgspec

from loom.ai.abc import StateShape
from loom.ai.compiler.phases._output import _resolve_symbol, _schema_to_decoder
from loom.ai.errors import (
    AgentCompilationIssue,
    state_declaration_conflict,
    state_schema_invalid,
    state_type_ref_unresolvable,
    state_type_ref_unsupported,
)

_CompileResult = tuple[StateShape | None, list[AgentCompilationIssue]]


def compile_state(
    deps_type: str | None,
    deps_schema: Mapping[str, Any] | None,
    component: str,
) -> _CompileResult:
    """Compile a ``deps_type``/``deps_schema`` declaration into a :class:`StateShape`.

    Args:
        deps_type: Declared ``deps_type`` value: ``"dict"``, a
            ``module:Symbol`` reference, or ``None``.
        deps_schema: Declared ``deps_schema`` value, or ``None``.
        component: Artifact path or agent name the issues point at.

    Returns:
        The compiled state shape (``None`` when neither field is declared, or
        on failure) and the issues found.
    """
    if deps_type is not None and deps_schema is not None:
        return None, [state_declaration_conflict(component)]
    if deps_type == "dict":
        return StateShape(schema=None, decoder=None), []
    if deps_type is not None:
        return _compile_type_ref(deps_type, component)
    if deps_schema is not None:
        return _compile_schema(deps_schema, component)
    return None, []


def _compile_type_ref(ref: str, component: str) -> _CompileResult:
    symbol, issues = _resolve_symbol(ref, component, state_type_ref_unresolvable)
    if symbol is None:
        return None, issues
    try:
        schema = msgspec.json.schema(symbol)
    except TypeError as exc:
        return None, [state_type_ref_unsupported(component, ref, str(exc))]
    if _schema_admits_unknown_fields(schema):
        return None, [
            state_type_ref_unsupported(
                component,
                ref,
                "the struct must declare forbid_unknown_fields=True: state decoded from a "
                "caller's request body must reject a field the artifact did not declare, "
                "exactly as the output side already requires (FR-017)",
            )
        ]
    # ``symbol``, not the schema just derived from it: ``msgspec.json.schema``
    # emits a root ``$ref``-plus-``$defs`` document for a struct (measured),
    # which ``_annotation_for`` cannot resolve — it has no notion of ``$ref``.
    # The type itself decodes directly, exactly as ``_compile_type_ref`` in
    # ``phases/_output.py`` already does for the output side of this split.
    decoder: msgspec.json.Decoder[Any] = msgspec.json.Decoder(symbol)
    return StateShape(schema=MappingProxyType(schema), decoder=decoder), []


def _schema_admits_unknown_fields(schema: Mapping[str, Any]) -> bool:
    """Return whether *schema* lacks ``additionalProperties: false`` on its root ``$defs`` entry.

    Reads msgspec's own verdict rather than inspecting the resolved symbol
    (FR-004): a struct schema built with ``forbid_unknown_fields=True``
    carries the key (measured); a schema msgspec did not wrap in ``$defs`` —
    a scalar, a list, a bare ``dict`` — carries no such notion and answers
    ``False``.
    """
    defs = schema.get("$defs")
    ref = schema.get("$ref")
    if not isinstance(defs, Mapping) or not isinstance(ref, str):
        return False
    node = defs.get(ref.removeprefix("#/$defs/"))
    if not isinstance(node, Mapping):
        return False
    return node.get("additionalProperties") is not False


def _compile_schema(schema: Mapping[str, Any], component: str) -> _CompileResult:
    compiled, issues = _schema_to_decoder(
        schema, "CompiledStateModel", component, state_schema_invalid
    )
    if compiled is None:
        return None, issues
    result_schema, decoder = compiled
    return StateShape(schema=result_schema, decoder=decoder), []
