"""Router runtime — binds CompiledRoute records to a FastAPI application.

Generates async handler functions at startup, one per
:class:`~loom.rest.compiler.CompiledRoute`.  Each handler:

1. Extracts path parameters from ``request.path_params`` (populated by
   Starlette's routing layer from the URL).
2. Reads the raw request body and decodes it with ``msgspec.json.decode``
   when bytes are present.
3. Builds the :class:`~loom.core.use_case.use_case.UseCase` instance via the
   :class:`~loom.core.use_case.factory.UseCaseFactory`.
4. Drives execution through :class:`~loom.core.engine.executor.RuntimeExecutor`.
5. Returns a :class:`~loom.rest.fastapi.response.MsgspecJSONResponse`.

Handler ``__signature__`` is manipulated so FastAPI validates and documents
path parameters correctly in OpenAPI while keeping the implementation generic.

No reflection occurs at request time — all structural decisions (path params,
status codes, tags) are taken from the ``CompiledRoute`` produced at startup.
"""

from __future__ import annotations

import inspect
import re
import types
import typing
from collections.abc import Sequence
from typing import Any

import msgspec
from fastapi import FastAPI, HTTPException
from starlette.datastructures import QueryParams
from starlette.requests import Request
from starlette.responses import Response

from loom.core.engine.executor import RuntimeExecutor
from loom.core.errors import LoomError
from loom.core.observability.event import Scope
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.repository.abc.query import (
    FilterGroup,
    FilterOp,
    FilterSpec,
    PaginationMode,
    QuerySpec,
    SortSpec,
)
from loom.core.tracing import get_trace_id
from loom.core.use_case.factory import UseCaseFactory
from loom.rest.compiler import CompiledRoute
from loom.rest.constants import QueryParam
from loom.rest.errors import ErrorField, HttpErrorMapper
from loom.rest.fastapi.openapi import (
    QUERY_SPEC_PARAMETER_NAMES,
    build_query_parameters_schema,
    build_request_body_schema,
    build_success_response_schema,
)
from loom.rest.fastapi.response import MsgspecJSONResponse

_error_mapper = HttpErrorMapper()

_DEFAULT_PAGE = 1
_DEFAULT_LIMIT = 50


def _internal_error_response(trace_id: str) -> MsgspecJSONResponse:
    """Build the generic 500 response returned by REST handlers."""
    return MsgspecJSONResponse(
        status_code=500,
        content={
            ErrorField.CODE: "internal_error",
            ErrorField.MESSAGE: "An unexpected error occurred",
            ErrorField.TRACE_ID: trace_id,
        },
    )


def _extract_path_params(path: str) -> list[str]:
    """Return ordered path-parameter names from a FastAPI path template.

    Args:
        path: Path string, e.g. ``"/{user_id}/orders/{order_id}"``.

    Returns:
        List of parameter names in declaration order, e.g.
        ``["user_id", "order_id"]``.
    """
    return re.findall(r"\{(\w+)\}", path)


def _normalize_path_param_annotation(annotation: Any) -> Any:
    if annotation is inspect._empty or annotation is Any:
        return str

    origin = typing.get_origin(annotation)
    if origin is typing.Annotated:
        args = typing.get_args(annotation)
        if not args:
            return str
        return _normalize_path_param_annotation(args[0])

    if origin in {typing.Union, types.UnionType}:
        args = tuple(arg for arg in typing.get_args(annotation) if arg is not type(None))
        if len(args) != 1:
            return str
        return _normalize_path_param_annotation(args[0])

    if isinstance(annotation, type):
        return annotation
    return str


_RESERVED_QUERY_KEYS = frozenset((*QUERY_SPEC_PARAMETER_NAMES, QueryParam.PROFILE))
_FILTER_OP_VALUES = frozenset(item.value for item in FilterOp)


def _camel_to_snake(value: str) -> str:
    s = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", value)
    s = re.sub(r"(?<=[A-Z])(?=[A-Z][a-z])", "_", s)
    return s.lower()


def _normalize_field_name(value: str) -> str:
    if any(char.isupper() for char in value):
        return _camel_to_snake(value)
    return value


def _coerce_scalar(value: str) -> Any:
    lowered = value.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    try:
        if "." in value:
            return float(value)
        return int(value)
    except ValueError:
        return value


def _parse_filter_op(op: str) -> FilterOp:
    try:
        return FilterOp(op.lower())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Unsupported filter operator: {op!r}") from exc


def _parse_pagination_mode(
    raw: str | None,
    cursor: str | None,
    *,
    default_mode: PaginationMode,
    allow_override: bool,
) -> PaginationMode:
    if not allow_override:
        if raw is not None and raw.lower() != default_mode.value:
            raise HTTPException(
                status_code=400,
                detail=(
                    "Query parameter 'pagination' cannot override this route's "
                    f"default mode ({default_mode.value!r})."
                ),
            )
        if cursor is not None and default_mode is PaginationMode.OFFSET:
            raise HTTPException(
                status_code=400,
                detail=(
                    "Cursor parameters are not allowed when pagination mode is fixed to 'offset'."
                ),
            )
        return default_mode

    if raw is None:
        if cursor is not None:
            return PaginationMode.CURSOR
        return default_mode
    try:
        return PaginationMode(raw.lower())
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid pagination mode: {raw!r}.",
        ) from exc


def _parse_sort(sort_field: str | None, direction_raw: str) -> tuple[SortSpec, ...]:
    direction = direction_raw.upper()
    if direction not in {"ASC", "DESC"}:
        raise HTTPException(status_code=400, detail="direction must be 'ASC' or 'DESC'.")
    if sort_field is None or sort_field == "":
        return ()
    return (
        SortSpec(field=_normalize_field_name(sort_field), direction=typing.cast(Any, direction)),
    )


def _parse_filter_specs(query_params: QueryParams) -> list[FilterSpec]:
    filters: list[FilterSpec] = []
    for key, raw_value in query_params.items():
        if key in _RESERVED_QUERY_KEYS:
            continue

        parts = [part for part in key.split("__") if part]
        if not parts:
            continue

        maybe_op = parts[-1].lower()
        if maybe_op in _FILTER_OP_VALUES:
            op = _parse_filter_op(maybe_op)
            field_parts = parts[:-1]
        else:
            op = FilterOp.EQ
            field_parts = parts

        if not field_parts:
            raise HTTPException(status_code=400, detail=f"Invalid filter field: {key!r}.")
        field = ".".join(_normalize_field_name(part) for part in field_parts)

        if op == FilterOp.IN:
            value = [_coerce_scalar(item) for item in raw_value.split(",") if item != ""]
        else:
            value = _coerce_scalar(raw_value)

        filters.append(FilterSpec(field=field, op=op, value=value))
    return filters


def _build_query_spec(
    request: Request,
    *,
    default_pagination_mode: PaginationMode,
    allow_pagination_override: bool,
) -> QuerySpec:
    query_params = request.query_params
    page = int(query_params.get(QueryParam.PAGE, _DEFAULT_PAGE))
    limit = int(query_params.get(QueryParam.LIMIT, _DEFAULT_LIMIT))
    cursor = query_params.get(QueryParam.AFTER) or query_params.get(QueryParam.CURSOR)
    pagination = _parse_pagination_mode(
        query_params.get(QueryParam.PAGINATION),
        cursor,
        default_mode=default_pagination_mode,
        allow_override=allow_pagination_override,
    )
    sort = _parse_sort(
        query_params.get(QueryParam.SORT),
        query_params.get(QueryParam.DIRECTION, "ASC"),
    )
    filters = _parse_filter_specs(query_params)
    filter_group = FilterGroup(filters=tuple(filters)) if filters else None
    return QuerySpec(
        filters=filter_group,
        sort=sort,
        pagination=pagination,
        limit=limit,
        page=page,
        cursor=cursor,
    )


def _resolve_query_param_name(uc_type: type[Any]) -> str | None:
    execute_sig = inspect.signature(uc_type.execute)
    hints = typing.get_type_hints(uc_type.execute)
    for name, param in execute_sig.parameters.items():
        if name == "self":
            continue
        annotation = hints.get(name, param.annotation)
        origin = typing.get_origin(annotation)
        args = typing.get_args(annotation)
        if annotation is QuerySpec:
            return name
        if origin in {typing.Union, types.UnionType} and QuerySpec in args:
            return name
    return None


def _route_docs(compiled_route: CompiledRoute) -> tuple[str | None, str | None]:
    """Resolve OpenAPI summary/description from route metadata or UseCase docs."""
    summary = compiled_route.route.summary.strip()
    description = compiled_route.route.description.strip()
    if summary or description:
        return summary or None, description or None

    uc_doc = inspect.getdoc(compiled_route.route.use_case) or ""
    cleaned_lines = tuple(line.strip() for line in uc_doc.splitlines() if line.strip())
    if not cleaned_lines:
        return None, None

    auto_summary, *rest = cleaned_lines
    auto_description = "\n".join(rest) if rest else None
    return auto_summary, auto_description


def _make_handler(
    compiled_route: CompiledRoute,
    factory: UseCaseFactory,
    executor: RuntimeExecutor,
    observability_runtime: ObservabilityRuntime,
) -> Any:
    """Build an async handler for the given compiled route.

    The returned callable has its ``__signature__`` overridden to expose
    path parameters so FastAPI injects and validates them correctly.

    Args:
        compiled_route: Fully resolved route from ``RestInterfaceCompiler``.
        factory: Factory used to construct the use-case instance per request.
        executor: Executor that drives the use-case pipeline.
        observability_runtime: Shared runtime used to emit a span event per request.

    Returns:
        Async callable suitable for ``FastAPI.add_api_route``.
    """
    path_params = _extract_path_params(compiled_route.route.path)
    execute_param_types = dict(compiled_route.execute_param_types)
    uc_type = compiled_route.route.use_case
    status_code = compiled_route.route.status_code
    route_read_only = compiled_route.read_only
    execute_sig = inspect.signature(uc_type.execute)
    accepts_profile_param = "profile" in execute_sig.parameters
    query_param_name = _resolve_query_param_name(uc_type)

    # Resolved at startup — avoids per-request attribute lookup.
    plan = getattr(uc_type, "__execution_plan__", None)
    has_input_binding = plan is not None and plan.input_binding is not None

    def _resolve_profile(request: Request) -> str:
        requested = request.query_params.get(QueryParam.PROFILE)
        if requested is None:
            return compiled_route.effective_profile_default

        if not compiled_route.effective_expose_profile:
            raise HTTPException(
                status_code=400,
                detail="Query parameter 'profile' is not allowed for this route.",
            )

        allowed = compiled_route.effective_allowed_profiles
        if allowed and requested not in allowed:
            raise HTTPException(
                status_code=400,
                detail=(f"Invalid profile {requested!r}. Allowed: {', '.join(allowed)}"),
            )
        return requested

    async def _handler(request: Request, **kwargs: Any) -> Response:
        try:
            with observability_runtime.span(
                Scope.USE_CASE,
                uc_type.__name__,
                trace_id=get_trace_id(),
                route=compiled_route.full_path,
                method=compiled_route.route.method.upper(),
                status_code=status_code,
                read_only=route_read_only,
            ):
                params: dict[str, Any] = {p: kwargs[p] for p in path_params}
                selected_profile = _resolve_profile(request)
                if accepts_profile_param:
                    params["profile"] = selected_profile
                if query_param_name is not None:
                    params[query_param_name] = _build_query_spec(
                        request,
                        default_pagination_mode=compiled_route.effective_pagination_mode,
                        allow_pagination_override=compiled_route.effective_allow_pagination_override,
                    )
                payload: dict[str, Any] | None = None
                if has_input_binding:
                    body = await request.body()
                    if body:
                        payload = msgspec.json.decode(body)

                uc = factory.build(uc_type)
                result = await executor.execute(
                    uc, params=params, payload=payload, read_only=route_read_only
                )
            return MsgspecJSONResponse(content=result, status_code=status_code)
        except HTTPException:
            raise
        except LoomError as exc:
            raise _error_mapper.to_http(exc) from exc
        except Exception:
            return _internal_error_response(get_trace_id() or "")

    sig_params: list[inspect.Parameter] = [
        inspect.Parameter(
            "request",
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            annotation=Request,
        )
    ]
    for p_name in path_params:
        annotation = _normalize_path_param_annotation(execute_param_types.get(p_name, str))
        sig_params.append(
            inspect.Parameter(
                p_name,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                annotation=annotation,
            )
        )
    _handler.__signature__ = inspect.Signature(  # type: ignore[attr-defined]
        sig_params, return_annotation=Response
    )
    _handler.__name__ = f"handle_{uc_type.__name__}"

    return _handler


def bind_interfaces(
    app: FastAPI,
    compiled_routes: Sequence[CompiledRoute],
    factory: UseCaseFactory,
    executor: RuntimeExecutor,
    observability_runtime: ObservabilityRuntime,
) -> dict[str, Any]:
    """Register compiled routes on a FastAPI application.

    For each :class:`~loom.rest.compiler.CompiledRoute`, creates a dynamic
    async handler and registers it via ``app.add_api_route``.  Path parameters
    are inferred from the route path template and exposed in the handler
    signature so FastAPI validates and documents them correctly.

    Nested JSON Schema ``$defs`` produced by msgspec/pydantic are collected
    into a shared component registry and returned.  The caller is responsible
    for injecting these into ``components.schemas`` of the OpenAPI document.

    Args:
        app: FastAPI application instance to register routes on.
        compiled_routes: Ordered list of fully resolved routes produced by
            :class:`~loom.rest.compiler.RestInterfaceCompiler`.
        factory: Use-case factory for constructing instances per request.
        executor: Runtime executor that drives the use-case pipeline.
        observability_runtime: Shared runtime used to emit request lifecycle
            events around each handler execution.

    Returns:
        Mapping of schema name → JSON Schema fragment for all collected
        ``$defs`` that should appear under ``components.schemas``.

    Example::

        compiler = RestInterfaceCompiler(use_case_compiler)
        routes = compiler.compile(UserRestInterface)
        component_schemas = bind_interfaces(
            app,
            routes,
            factory,
            executor,
            observability_runtime=ObservabilityRuntime.noop(),
        )
    """
    component_registry: dict[str, Any] = {}

    for cr in compiled_routes:
        handler = _make_handler(cr, factory, executor, observability_runtime)
        summary, description = _route_docs(cr)
        request_body = build_request_body_schema(cr, component_registry)
        success_response = build_success_response_schema(cr, component_registry)
        responses: dict[int | str, dict[str, Any]] | None = None
        if success_response is not None:
            responses = {cr.route.status_code: success_response}

        openapi_extra: dict[str, Any] | None = None
        query_parameters = build_query_parameters_schema(cr)
        if request_body is not None or query_parameters:
            openapi_extra = {}
            if request_body is not None:
                openapi_extra["requestBody"] = request_body
            if query_parameters:
                openapi_extra["parameters"] = query_parameters

        app.add_api_route(
            path=cr.full_path,
            endpoint=handler,
            methods=[cr.route.method.upper()],
            summary=summary,
            description=description,
            status_code=cr.route.status_code,
            tags=list(cr.interface_tags) if cr.interface_tags else [],
            responses=responses,
            openapi_extra=openapi_extra,
        )

    return component_registry
