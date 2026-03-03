"""OpenAPI schema helpers for FastAPI route binding.

Builds request/response schema fragments from Loom UseCase contracts while
keeping runtime execution transport-agnostic.
"""

from __future__ import annotations

from copy import deepcopy
from types import NoneType, UnionType
from typing import Any, Union, cast, get_args, get_origin, get_type_hints

import msgspec
from pydantic import TypeAdapter

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


def build_request_body_schema(compiled_route: CompiledRoute) -> JsonSchema | None:
    """Return OpenAPI ``requestBody`` schema for the route, if it has Input()."""
    plan = _get_execution_plan(compiled_route)
    if plan is None or plan.input_binding is None:
        return None

    request_schema = _command_request_schema(plan.input_binding.command_type)
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


def build_success_response_schema(compiled_route: CompiledRoute) -> JsonSchema | None:
    """Return OpenAPI response entry for the route success status, if resolvable."""
    response_schema = _use_case_response_schema(compiled_route.route.use_case)
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


def _use_case_response_schema(use_case_type: type[Any]) -> JsonSchema | None:
    hints = get_type_hints(use_case_type.execute)
    return_type = hints.get("return")
    if return_type is None or return_type is Any:
        return None

    return _safe_schema(return_type)


def _command_request_schema(command_type: type[Any]) -> JsonSchema | None:
    if not isinstance(command_type, type):
        return None
    if issubclass(command_type, msgspec.Struct):
        public_struct = _build_public_command_struct(command_type)
        return _safe_schema(public_struct)

    # Fallback for user-defined plain types (dataclass/pydantic/typing).
    return _safe_pydantic_schema(command_type)


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
        adapter = TypeAdapter(annotation)
        return adapter.json_schema()
    except Exception:
        return None


def _safe_schema(annotation: Any) -> JsonSchema | None:
    origin = get_origin(annotation)
    if origin in (UnionType, Union):
        union_members = [_safe_schema(member) for member in get_args(annotation)]
        if any(member is None for member in union_members):
            return None
        members = [member for member in union_members if member is not None]
        if len(members) == 1:
            return members[0]
        return {"anyOf": members}

    msgspec_schema = _safe_msgspec_schema(annotation)
    if msgspec_schema is not None:
        return _inline_local_defs(msgspec_schema)
    pydantic_schema = _safe_pydantic_schema(annotation)
    if pydantic_schema is not None:
        return _inline_local_defs(pydantic_schema)
    return None


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
    raw_defs = schema.get("$defs")
    if not isinstance(raw_defs, dict):
        return schema

    defs: dict[str, Any] = {name: deepcopy(value) for name, value in raw_defs.items()}

    def _resolve(node: Any, stack: tuple[str, ...] = ()) -> Any:
        if isinstance(node, list):
            return [_resolve(item, stack) for item in node]
        if not isinstance(node, dict):
            return node

        ref = node.get("$ref")
        if isinstance(ref, str) and ref.startswith("#/$defs/"):
            name = ref.removeprefix("#/$defs/")
            target = defs.get(name)
            if target is None or name in stack:
                return {k: _resolve(v, stack) for k, v in node.items() if k != "$defs"}

            resolved_target = _resolve(deepcopy(target), (*stack, name))
            siblings = {k: v for k, v in node.items() if k != "$ref"}
            if not siblings:
                return resolved_target

            merged = deepcopy(resolved_target)
            if isinstance(merged, dict):
                for key, value in siblings.items():
                    merged[key] = _resolve(value, stack)
                return merged
            return _resolve(siblings, stack)

        return {key: _resolve(value, stack) for key, value in node.items() if key != "$defs"}

    return cast(JsonSchema, _resolve({k: v for k, v in schema.items() if k != "$defs"}))
