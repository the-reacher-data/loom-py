"""OpenAPI schema helpers for FastAPI route binding.

Builds request/response schema fragments from Loom UseCase contracts while
keeping runtime execution transport-agnostic.
"""

from __future__ import annotations

from copy import deepcopy
from types import NoneType, UnionType
from typing import Any, Union, get_args, get_origin, get_type_hints

import msgspec

from loom.core.command.introspection import (
    get_calculated_fields,
    get_internal_fields,
    get_patch_fields,
)
from loom.core.engine.plan import ExecutionPlan
from loom.core.repository.abc.query import PaginationMode, QuerySpec
from loom.rest.compiler import CompiledRoute

JsonSchema = dict[str, Any]

_PARAM_IN_QUERY = "query"
_PARAM_NAME = "name"
_PARAM_IN = "in"
_PARAM_REQUIRED = "required"
_PARAM_SCHEMA = "schema"
_PARAM_DESCRIPTION = "description"

QUERY_PARAM_PAGE = "page"
QUERY_PARAM_LIMIT = "limit"
QUERY_PARAM_PAGINATION = "pagination"
QUERY_PARAM_AFTER = "after"
QUERY_PARAM_CURSOR = "cursor"
QUERY_PARAM_SORT = "sort"
QUERY_PARAM_DIRECTION = "direction"
QUERY_PARAM_PROFILE = "profile"

QUERY_SPEC_PARAMETER_NAMES: tuple[str, ...] = (
    QUERY_PARAM_PAGE,
    QUERY_PARAM_LIMIT,
    QUERY_PARAM_PAGINATION,
    QUERY_PARAM_AFTER,
    QUERY_PARAM_CURSOR,
    QUERY_PARAM_SORT,
    QUERY_PARAM_DIRECTION,
)

_SCHEMA_DEFS_KEY = "$defs"
_SCHEMA_REF_KEY = "$ref"
_LOCAL_DEFS_REF_PREFIX = "#/$defs/"
_COMPONENT_REF_PREFIX = "#/components/schemas/"


def build_request_body_schema(
    compiled_route: CompiledRoute,
    component_registry: dict[str, JsonSchema] | None = None,
) -> JsonSchema | None:
    """Return OpenAPI ``requestBody`` schema for the route, if it has Input().

    Args:
        compiled_route: Fully resolved route from ``RestInterfaceCompiler``.
        component_registry: Optional mutable dict that accumulates shared
            component schemas.  When provided, nested ``$defs`` are extracted
            as ``#/components/schemas/`` entries instead of being inlined.

    Returns:
        OpenAPI ``requestBody`` fragment, or ``None`` when the route has no
        ``Input()`` binding.
    """
    plan = _get_execution_plan(compiled_route)
    if plan is None or plan.input_binding is None:
        return None

    request_schema = _command_request_schema(plan.input_binding.command_type, component_registry)
    if request_schema is None:
        return None

    return {
        "required": True,
        "content": {
            "application/json": {
                "schema": request_schema,
            }
        },
    }


def build_success_response_schema(
    compiled_route: CompiledRoute,
    component_registry: dict[str, JsonSchema] | None = None,
) -> JsonSchema | None:
    """Return OpenAPI response entry for the route success status, if resolvable.

    Args:
        compiled_route: Fully resolved route from ``RestInterfaceCompiler``.
        component_registry: Optional mutable dict that accumulates shared
            component schemas.  When provided, nested ``$defs`` are extracted
            as ``#/components/schemas/`` entries instead of being inlined.

    Returns:
        OpenAPI response schema fragment, or ``None`` when the return type
        cannot be resolved to a JSON Schema.
    """
    response_schema = _use_case_response_schema(compiled_route.route.use_case, component_registry)
    if response_schema is None:
        return None

    return {
        "content": {
            "application/json": {
                "schema": response_schema,
            }
        }
    }


def build_query_parameters_schema(compiled_route: CompiledRoute) -> list[JsonSchema]:
    """Return OpenAPI query parameters inferred from the use-case contract."""
    use_case_type = compiled_route.route.use_case
    hints = get_type_hints(use_case_type.execute)
    params: list[JsonSchema] = []

    if _has_query_spec_parameter(hints):
        params.extend(_query_spec_openapi_parameters(compiled_route.effective_pagination_mode))

    if _has_profile_parameter(hints) and compiled_route.effective_expose_profile:
        profile_param: JsonSchema = {
            _PARAM_NAME: QUERY_PARAM_PROFILE,
            _PARAM_IN: _PARAM_IN_QUERY,
            _PARAM_REQUIRED: False,
            _PARAM_SCHEMA: {"type": "string"},
            _PARAM_DESCRIPTION: "Projection profile used by repository mappings.",
        }
        allowed = compiled_route.effective_allowed_profiles
        if allowed:
            profile_param[_PARAM_SCHEMA]["enum"] = list(allowed)
        params.append(profile_param)

    return params


def _get_execution_plan(compiled_route: CompiledRoute) -> ExecutionPlan | None:
    plan = getattr(compiled_route.route.use_case, "__execution_plan__", None)
    if isinstance(plan, ExecutionPlan):
        return plan
    return None


def _has_profile_parameter(type_hints: dict[str, Any]) -> bool:
    return "profile" in type_hints


def _has_query_spec_parameter(type_hints: dict[str, Any]) -> bool:
    for annotation in type_hints.values():
        if annotation is QuerySpec:
            return True
        origin = get_origin(annotation)
        args = get_args(annotation)
        if origin in (UnionType, Union) and QuerySpec in args:
            return True
    return False


def _query_spec_openapi_parameters(default_mode: PaginationMode) -> list[JsonSchema]:
    mode_values = [item.value for item in PaginationMode]
    return [
        {
            _PARAM_NAME: QUERY_PARAM_PAGE,
            _PARAM_IN: _PARAM_IN_QUERY,
            _PARAM_REQUIRED: False,
            _PARAM_SCHEMA: {"type": "integer", "minimum": 1, "default": 1},
            _PARAM_DESCRIPTION: "Offset page number.",
        },
        {
            _PARAM_NAME: QUERY_PARAM_LIMIT,
            _PARAM_IN: _PARAM_IN_QUERY,
            _PARAM_REQUIRED: False,
            _PARAM_SCHEMA: {"type": "integer", "minimum": 1, "maximum": 1000, "default": 50},
            _PARAM_DESCRIPTION: "Maximum rows per page.",
        },
        {
            _PARAM_NAME: QUERY_PARAM_PAGINATION,
            _PARAM_IN: _PARAM_IN_QUERY,
            _PARAM_REQUIRED: False,
            _PARAM_SCHEMA: {"type": "string", "enum": mode_values, "default": default_mode.value},
            _PARAM_DESCRIPTION: "Pagination strategy.",
        },
        {
            _PARAM_NAME: QUERY_PARAM_AFTER,
            _PARAM_IN: _PARAM_IN_QUERY,
            _PARAM_REQUIRED: False,
            _PARAM_SCHEMA: {"type": "string"},
            _PARAM_DESCRIPTION: "Cursor token for cursor pagination.",
        },
        {
            _PARAM_NAME: QUERY_PARAM_CURSOR,
            _PARAM_IN: _PARAM_IN_QUERY,
            _PARAM_REQUIRED: False,
            _PARAM_SCHEMA: {"type": "string"},
            _PARAM_DESCRIPTION: "Alias for 'after'.",
        },
        {
            _PARAM_NAME: QUERY_PARAM_SORT,
            _PARAM_IN: _PARAM_IN_QUERY,
            _PARAM_REQUIRED: False,
            _PARAM_SCHEMA: {"type": "string"},
            _PARAM_DESCRIPTION: "Field used for ordering.",
        },
        {
            _PARAM_NAME: QUERY_PARAM_DIRECTION,
            _PARAM_IN: _PARAM_IN_QUERY,
            _PARAM_REQUIRED: False,
            _PARAM_SCHEMA: {"type": "string", "enum": ["ASC", "DESC"], "default": "ASC"},
            _PARAM_DESCRIPTION: "Sort direction.",
        },
    ]


def _use_case_response_schema(
    use_case_type: type[Any],
    component_registry: dict[str, JsonSchema] | None = None,
) -> JsonSchema | None:
    hints = get_type_hints(use_case_type.execute)
    return_type = hints.get("return")
    if return_type is None or return_type is Any:
        return None

    return _safe_schema(return_type, component_registry)


def _command_request_schema(
    command_type: type[Any],
    component_registry: dict[str, JsonSchema] | None = None,
) -> JsonSchema | None:
    if not isinstance(command_type, type):
        return None
    if issubclass(command_type, msgspec.Struct):
        public_struct = _build_public_command_struct(command_type)
        return _safe_schema(public_struct, component_registry)

    # Fallback for user-defined plain types (dataclass/pydantic/typing).
    return _safe_schema(command_type, component_registry)


def _build_public_command_struct(command_type: type[Any]) -> type[Any]:
    struct_fields = {field.name: field for field in msgspec.structs.fields(command_type)}
    excluded = set(get_internal_fields(command_type)) | set(get_calculated_fields(command_type))
    patch_fields = set(get_patch_fields(command_type))

    definitions: list[tuple[Any, ...]] = []
    for name, sf in struct_fields.items():
        if name in excluded:
            continue

        annotation = _without_unset_type(sf.type)
        if name in patch_fields:
            definitions.append((name, _with_optional_none(annotation), None))
            continue

        if sf.default is msgspec.NODEFAULT:
            definitions.append((name, annotation))
            continue

        definitions.append((name, annotation, sf.default))

    use_camel = any(field.encode_name != field.name for field in struct_fields.values())
    return msgspec.defstruct(
        f"{command_type.__name__}Request",
        definitions,
        kw_only=True,
        rename="camel" if use_camel else None,
    )


def _safe_msgspec_schema(annotation: Any) -> JsonSchema | None:
    try:
        return msgspec.json.schema(annotation)
    except (TypeError, ValueError):
        return None


def _safe_pydantic_schema(annotation: Any) -> JsonSchema | None:
    try:
        from pydantic import TypeAdapter  # lazy — only when pydantic is installed

        adapter = TypeAdapter(annotation)
        return adapter.json_schema()
    except Exception:
        return None


def _rewrite_to_component_refs(node: Any) -> Any:
    """Rewrite ``#/$defs/X`` references to ``#/components/schemas/X`` recursively.

    Strips ``$defs`` keys and rewrites every ``$ref`` value that points to a
    local definition so the resulting fragment is valid at the OpenAPI document
    root where ``#/components/schemas/`` is resolvable.
    """
    if isinstance(node, list):
        return [_rewrite_to_component_refs(item) for item in node]
    if not isinstance(node, dict):
        return node
    result: dict[str, Any] = {}
    for k, v in node.items():
        if k == _SCHEMA_DEFS_KEY:
            continue
        if k == _SCHEMA_REF_KEY and isinstance(v, str) and v.startswith(_LOCAL_DEFS_REF_PREFIX):
            result[k] = _COMPONENT_REF_PREFIX + v.removeprefix(_LOCAL_DEFS_REF_PREFIX)
        else:
            result[k] = _rewrite_to_component_refs(v)
    return result


def _safe_schema(
    annotation: Any,
    component_registry: dict[str, JsonSchema] | None = None,
) -> JsonSchema | None:
    origin = get_origin(annotation)
    if origin in (UnionType, Union):
        union_members = [
            _safe_schema(member, component_registry) for member in get_args(annotation)
        ]
        if any(member is None for member in union_members):
            return None
        members = [member for member in union_members if member is not None]
        if len(members) == 1:
            return members[0]
        return {"anyOf": members}

    raw = _safe_msgspec_schema(annotation) or _safe_pydantic_schema(annotation)
    if raw is None:
        return None

    if component_registry is not None:
        defs = raw.get(_SCHEMA_DEFS_KEY)
        if isinstance(defs, dict):
            for name, def_schema in defs.items():
                component_registry[name] = _rewrite_to_component_refs(def_schema)
        rewritten: JsonSchema = _rewrite_to_component_refs(raw)
        return rewritten

    return _inline_local_defs(raw)


def _without_unset_type(annotation: Any) -> Any:
    if annotation is msgspec.UnsetType:
        return Any

    origin = get_origin(annotation)
    if origin is None:
        return annotation

    if origin in (UnionType, Union):
        args = tuple(arg for arg in get_args(annotation) if arg is not msgspec.UnsetType)
        if not args:
            return Any
        result = args[0]
        for arg in args[1:]:
            result = result | arg
        return result

    return annotation


def _with_optional_none(annotation: Any) -> Any:
    origin = get_origin(annotation)
    if origin in (UnionType, Union) and NoneType in get_args(annotation):
        return annotation
    if annotation is NoneType:
        return annotation
    return annotation | None


def _inline_local_defs(schema: JsonSchema) -> JsonSchema:
    """Inline ``#/$defs/...`` references so OpenAPI can resolve schemas.

    OpenAPI resolves ``$ref`` from the document root. JSON Schema fragments
    returned by msgspec/pydantic may use local ``$defs`` references, which
    become invalid once embedded under ``requestBody``/``responses``.
    """
    raw_defs = schema.get(_SCHEMA_DEFS_KEY)
    if not isinstance(raw_defs, dict):
        return schema

    defs: dict[str, Any] = {name: deepcopy(value) for name, value in raw_defs.items()}
    return _resolve_schema_root(schema, defs)


def _resolve_schema_root(schema: JsonSchema, defs: dict[str, Any]) -> JsonSchema:
    resolved = _resolve_local_def_node(_drop_defs_key(schema), defs, ())
    if isinstance(resolved, dict):
        return resolved
    return {}


def _resolve_local_def_node(node: Any, defs: dict[str, Any], stack: tuple[str, ...]) -> Any:
    if isinstance(node, list):
        return [_resolve_local_def_node(item, defs, stack) for item in node]
    if not isinstance(node, dict):
        return node
    ref_name = _extract_local_ref_name(node)
    if ref_name is None:
        return _resolve_plain_schema_object(node, defs, stack)
    return _resolve_ref_schema_object(node, defs, stack, ref_name)


def _resolve_plain_schema_object(
    node: dict[str, Any],
    defs: dict[str, Any],
    stack: tuple[str, ...],
) -> dict[str, Any]:
    cleaned = _drop_defs_key(node)
    return {key: _resolve_local_def_node(value, defs, stack) for key, value in cleaned.items()}


def _resolve_ref_schema_object(
    node: dict[str, Any],
    defs: dict[str, Any],
    stack: tuple[str, ...],
    ref_name: str,
) -> Any:
    target = defs.get(ref_name)
    if target is None or ref_name in stack:
        return _resolve_plain_schema_object(node, defs, stack)
    resolved_target = _resolve_local_def_node(deepcopy(target), defs, (*stack, ref_name))
    siblings = {k: v for k, v in node.items() if k != _SCHEMA_REF_KEY}
    if not siblings:
        return resolved_target
    return _merge_resolved_with_siblings(resolved_target, siblings, defs, stack)


def _merge_resolved_with_siblings(
    resolved_target: Any,
    siblings: dict[str, Any],
    defs: dict[str, Any],
    stack: tuple[str, ...],
) -> Any:
    merged = deepcopy(resolved_target)
    if isinstance(merged, dict):
        for key, value in siblings.items():
            merged[key] = _resolve_local_def_node(value, defs, stack)
        return merged
    return _resolve_local_def_node(siblings, defs, stack)


def _drop_defs_key(node: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in node.items() if key != _SCHEMA_DEFS_KEY}


def _extract_local_ref_name(node: dict[str, Any]) -> str | None:
    ref = node.get(_SCHEMA_REF_KEY)
    if not isinstance(ref, str):
        return None
    if not ref.startswith(_LOCAL_DEFS_REF_PREFIX):
        return None
    return ref.removeprefix(_LOCAL_DEFS_REF_PREFIX)
