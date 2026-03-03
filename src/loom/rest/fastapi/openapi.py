"""OpenAPI schema helpers for FastAPI route binding.

Builds request/response schema fragments from Loom UseCase contracts while
keeping runtime execution transport-agnostic.
"""

from __future__ import annotations

from types import NoneType, UnionType
from typing import Any, Union, get_args, get_origin, get_type_hints

import msgspec
from pydantic import TypeAdapter

from loom.core.command.introspection import (
    get_calculated_fields,
    get_internal_fields,
    get_patch_fields,
)
from loom.core.engine.plan import ExecutionPlan
from loom.rest.compiler import CompiledRoute

JsonSchema = dict[str, Any]


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


def _get_execution_plan(compiled_route: CompiledRoute) -> ExecutionPlan | None:
    plan = getattr(compiled_route.route.use_case, "__execution_plan__", None)
    if isinstance(plan, ExecutionPlan):
        return plan
    return None


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
    msgspec_schema = _safe_msgspec_schema(annotation)
    if msgspec_schema is not None:
        return msgspec_schema
    return _safe_pydantic_schema(annotation)


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
