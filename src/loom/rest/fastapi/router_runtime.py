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
from collections.abc import Sequence
from typing import Any

import msgspec
from fastapi import FastAPI
from starlette.requests import Request
from starlette.responses import Response

from loom.core.engine.executor import RuntimeExecutor
from loom.core.use_case.factory import UseCaseFactory
from loom.rest.compiler import CompiledRoute
from loom.rest.fastapi.response import MsgspecJSONResponse


def _extract_path_params(path: str) -> list[str]:
    """Return ordered path-parameter names from a FastAPI path template.

    Args:
        path: Path string, e.g. ``"/{user_id}/orders/{order_id}"``.

    Returns:
        List of parameter names in declaration order, e.g.
        ``["user_id", "order_id"]``.
    """
    return re.findall(r"\{(\w+)\}", path)


def _make_handler(
    compiled_route: CompiledRoute,
    factory: UseCaseFactory,
    executor: RuntimeExecutor,
) -> Any:
    """Build an async handler for the given compiled route.

    The returned callable has its ``__signature__`` overridden to expose
    path parameters so FastAPI injects and validates them correctly.

    Args:
        compiled_route: Fully resolved route from ``RestInterfaceCompiler``.
        factory: Factory used to construct the use-case instance per request.
        executor: Executor that drives the use-case pipeline.

    Returns:
        Async callable suitable for ``FastAPI.add_api_route``.
    """
    path_params = _extract_path_params(compiled_route.route.path)
    uc_type = compiled_route.route.use_case
    status_code = compiled_route.route.status_code

    async def _handler(request: Request, **kwargs: Any) -> Response:
        params: dict[str, Any] = {p: kwargs[p] for p in path_params}

        payload: dict[str, Any] | None = None
        body = await request.body()
        if body:
            raw: Any = msgspec.json.decode(body)
            payload = raw

        uc = factory.build(uc_type)
        result = await executor.execute(uc, params=params, payload=payload)
        return MsgspecJSONResponse(content=result, status_code=status_code)

    sig_params: list[inspect.Parameter] = [
        inspect.Parameter(
            "request",
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            annotation=Request,
        )
    ]
    for p_name in path_params:
        sig_params.append(
            inspect.Parameter(
                p_name,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                annotation=str,
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
) -> None:
    """Register compiled routes on a FastAPI application.

    For each :class:`~loom.rest.compiler.CompiledRoute`, creates a dynamic
    async handler and registers it via ``app.add_api_route``.  Path parameters
    are inferred from the route path template and exposed in the handler
    signature so FastAPI validates and documents them correctly.

    Args:
        app: FastAPI application instance to register routes on.
        compiled_routes: Ordered list of fully resolved routes produced by
            :class:`~loom.rest.compiler.RestInterfaceCompiler`.
        factory: Use-case factory for constructing instances per request.
        executor: Runtime executor that drives the use-case pipeline.

    Example::

        compiler = RestInterfaceCompiler(use_case_compiler)
        routes = compiler.compile(UserRestInterface)
        bind_interfaces(app, routes, factory, executor)
    """
    for cr in compiled_routes:
        handler = _make_handler(cr, factory, executor)
        app.add_api_route(
            path=cr.full_path,
            endpoint=handler,
            methods=[cr.route.method.upper()],
            summary=cr.route.summary or None,
            description=cr.route.description or None,
            status_code=cr.route.status_code,
            tags=list(cr.interface_tags) if cr.interface_tags else [],
        )
