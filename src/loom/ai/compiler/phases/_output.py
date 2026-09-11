"""Output phase: schema validation, ``type_ref`` resolution, decoder build.

A ``json_schema`` output compiles to a concrete type via
``msgspec.defstruct()`` and the plan stores a **built** decoder, so nothing
reflects per invocation (research R-004, invariant 5).  A ``type_ref`` output
accepts ``msgspec.Struct`` subclasses only in v1, so pydantic never enters the
compiler path (T053).

:func:`_resolve_symbol` and :func:`_schema_to_decoder` are shared with the
state phase (:mod:`loom.ai.compiler.phases._state`), which compiles the same
two authored shapes — a symbol reference and a hand-written JSON Schema —
into a schema-and-decoder pair. Each caller supplies its own issue factory
and, for the schema path, its own generated-struct name; nothing about the
issue codes, the messages, or the exceptions caught changes between the two
callers. What differs between the two ``type_ref``/``deps_type`` paths stays
local to each phase: the output side additionally requires the resolved
symbol to be a ``msgspec.Struct`` declaring ``forbid_unknown_fields=True``
(invariant 5); the state side derives its schema straight from
``msgspec.json.schema()`` and admits whatever symbol that call accepts.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from types import MappingProxyType
from typing import Any, cast

import msgspec

from loom.ai.abc import OutputCheck
from loom.ai.compiler._plan import CompiledOutput
from loom.ai.declarative import JsonSchemaOutput, OutputSpec, TypeRefOutput
from loom.ai.errors import (
    AgentCompilationIssue,
    output_check_unresolvable,
    output_schema_invalid,
    output_type_ref_unresolvable,
    output_type_ref_unsupported,
)
from loom.core.symbols import import_symbol

_CompileResult = tuple[CompiledOutput | None, list[AgentCompilationIssue]]

_IssueFactory = Callable[[str, str], AgentCompilationIssue]
"""Builds one compilation issue from ``(component, detail)`` — the shape
every ``*_unresolvable`` and ``*_schema_invalid`` factory in
:mod:`loom.ai.errors` already has."""

# ``Any`` in the values: the mapped Python annotations are runtime objects
# (``str``, ``list[...]``, generated structs) that a static alias cannot name.
_SCALAR_TYPES: Mapping[str, Any] = {
    "string": str,
    "integer": int,
    "number": float,
    "boolean": bool,
    "null": type(None),
}


def compile_output(output: OutputSpec, component: str) -> _CompileResult:
    """Compile the output declaration into a :class:`CompiledOutput`.

    Args:
        output: Declared output variant.
        component: Artifact path or agent name the issues point at.

    Returns:
        The compiled output (or ``None`` on failure) and the issues found.
    """
    if isinstance(output, JsonSchemaOutput):
        return _compile_json_schema(output, component)
    return _compile_type_ref(output, component)


def compile_output_check(
    ref: str | None, component: str
) -> tuple[OutputCheck | None, list[AgentCompilationIssue]]:
    """Resolve the artifact's ``output_check`` reference, when it declares one.

    Args:
        ref: ``module:symbol`` reference from :attr:`~loom.ai.declarative.AgentSpecV1.output_check`,
            or ``None`` when the artifact declares no check.
        component: Artifact path or agent name the issue points at.

    Returns:
        The resolved :data:`~loom.ai.abc.OutputCheck`, or ``None`` when *ref*
        is ``None`` or does not resolve; paired with the issues found.
    """
    if ref is None:
        return None, []
    symbol, issues = _resolve_symbol(ref, component, output_check_unresolvable)
    if symbol is None:
        return None, issues
    return cast("OutputCheck", symbol), []


def _compile_json_schema(output: JsonSchemaOutput, component: str) -> _CompileResult:
    compiled, issues = _schema_to_decoder(
        output.schema, "CompiledOutputModel", component, output_schema_invalid
    )
    if compiled is None:
        return None, issues
    schema, decoder = compiled
    return CompiledOutput(schema=schema, decoder=decoder), []


def _compile_type_ref(output: TypeRefOutput, component: str) -> _CompileResult:
    symbol, issues = _resolve_symbol(output.ref, component, output_type_ref_unresolvable)
    if symbol is None:
        return None, issues
    if not (isinstance(symbol, type) and issubclass(symbol, msgspec.Struct)):
        return None, [
            output_type_ref_unsupported(
                component, output.ref, "only msgspec.Struct subclasses are supported in v1"
            )
        ]
    if not symbol.__struct_config__.forbid_unknown_fields:
        return None, [
            output_type_ref_unsupported(
                component,
                output.ref,
                "the struct must declare forbid_unknown_fields=True: pass-through of the "
                "validated bytes is only safe under a strict decode (invariant 5)",
            )
        ]
    decoder: msgspec.json.Decoder[Any] = msgspec.json.Decoder(symbol)
    return CompiledOutput(schema=MappingProxyType(msgspec.json.schema(symbol)), decoder=decoder), []


def _resolve_symbol(
    ref: str, component: str, unresolvable_issue: _IssueFactory
) -> tuple[Any | None, list[AgentCompilationIssue]]:
    """Resolve *ref*, or report it as unresolvable via *unresolvable_issue*.

    Shared with the state ``deps_type`` symbol path (:mod:`loom.ai.compiler.phases._state`).
    """
    try:
        return import_symbol(ref), []
    except (ImportError, AttributeError, ValueError):
        return None, [unresolvable_issue(component, ref)]


def _schema_to_decoder(
    schema: Mapping[str, Any], model_name: str, component: str, invalid_issue: _IssueFactory
) -> tuple[tuple[Mapping[str, Any], msgspec.json.Decoder[Any]] | None, list[AgentCompilationIssue]]:
    """Compile a hand-written JSON Schema object into a ``(schema, decoder)`` pair.

    *model_name* names the generated struct type; *invalid_issue* reports a
    structural fault or a build failure. Shared with the state ``deps_schema``
    path (:mod:`loom.ai.compiler.phases._state`).
    """
    fault = _schema_fault(schema)
    if fault is not None:
        return None, [invalid_issue(component, fault)]
    try:
        annotation = _annotation_for(schema, model_name)
        decoder: msgspec.json.Decoder[Any] = msgspec.json.Decoder(annotation)
    except (TypeError, ValueError) as exc:
        return None, [invalid_issue(component, str(exc))]
    return (MappingProxyType(dict(schema)), decoder), []


def _schema_fault(schema: Mapping[str, Any]) -> str | None:
    """Return the first structural fault of a JSON Schema object, if any."""
    type_value = schema.get("type")
    if type_value is not None and not isinstance(type_value, str):
        return "'type' must be a string"
    required_fault = _required_fault(schema.get("required"))
    if required_fault is not None:
        return required_fault
    properties_fault = _properties_fault(schema.get("properties"))
    if properties_fault is not None:
        return properties_fault
    items = schema.get("items")
    if items is None:
        return None
    if not isinstance(items, Mapping):
        return "'items' must be a schema object"
    return _schema_fault(items)


def _required_fault(required: object) -> str | None:
    if required is None:
        return None
    if isinstance(required, str) or not isinstance(required, Sequence):
        return "'required' must be an array of strings"
    if not all(isinstance(name, str) for name in required):
        return "'required' must be an array of strings"
    return None


def _properties_fault(properties: object) -> str | None:
    if properties is None:
        return None
    if not isinstance(properties, Mapping):
        return "'properties' must be an object mapping names to schemas"
    for name, sub_schema in properties.items():
        if not isinstance(sub_schema, Mapping):
            return f"property '{name}' must be a schema object"
        fault = _schema_fault(sub_schema)
        if fault is not None:
            return fault
    return None


def _annotation_for(schema: Mapping[str, Any], name: str) -> Any:
    """Map a validated JSON Schema node to a runtime type annotation."""
    type_value = schema.get("type")
    if type_value == "object":
        return _object_annotation(schema, name)
    if type_value == "array":
        items = schema.get("items")
        item = _annotation_for(items, f"{name}Item") if items is not None else Any
        # Runtime subscription with a computed annotation. The explicit
        # __class_getitem__ call this used to make is correct but trips
        # Sonar S930, which miscounts cls on a builtin classmethod; the
        # subscript is the same object and reads better.
        # Subscript a value typed Any: mypy rejects ``list[item]`` in type
        # position with a computed argument, and the explicit
        # __class_getitem__ call trips Sonar S930, which miscounts cls on a
        # builtin classmethod. Going through a value satisfies both and is
        # the same object at runtime.
        list_type: Any = list
        return list_type[item]
    if isinstance(type_value, str):
        return _SCALAR_TYPES.get(type_value, Any)
    return Any


def _object_annotation(schema: Mapping[str, Any], name: str) -> Any:
    """Build a strict struct type for an object node with declared properties."""
    properties: Mapping[str, Mapping[str, Any]] = schema.get("properties") or {}
    if not properties:
        return dict[str, Any]
    required = frozenset(schema.get("required") or ())
    mandatory: list[tuple[str, Any] | tuple[str, Any, Any]] = []
    optional: list[tuple[str, Any] | tuple[str, Any, Any]] = []
    for prop, sub_schema in properties.items():
        annotation = _annotation_for(sub_schema, f"{name}_{prop}")
        if prop in required:
            mandatory.append((prop, annotation))
        else:
            optional.append((prop, annotation | None, None))
    return msgspec.defstruct(name, [*mandatory, *optional], frozen=True, forbid_unknown_fields=True)
