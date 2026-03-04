"""FastAPI application factory.

:func:`create_fastapi_app` is the composition root that wires together the
domain bootstrap result and REST interface declarations into a runnable
``FastAPI`` instance.

It is intentionally kept thin — all validation happens during
:class:`~loom.rest.compiler.RestInterfaceCompiler` compilation (fail-fast at
startup) and all request handling is delegated to
:func:`~loom.rest.fastapi.router_runtime.bind_interfaces`.

Usage::

    result = bootstrap_app(
        config=cfg,
        use_cases=[CreateOrderUseCase, GetOrderUseCase],
        modules=[register_repositories],
    )
    app = create_fastapi_app(
        result,
        interfaces=[OrderRestInterface],
        title="Orders API",
        version="1.0.0",
    )
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from fastapi import FastAPI
from starlette.requests import Request

from loom.core.bootstrap.bootstrap import BootstrapResult
from loom.core.engine.executor import RuntimeExecutor
from loom.core.logger import get_logger
from loom.core.tracing import get_trace_id
from loom.rest.compiler import RestInterfaceCompiler
from loom.rest.errors import ErrorField
from loom.rest.fastapi.response import MsgspecJSONResponse
from loom.rest.fastapi.router_runtime import bind_interfaces
from loom.rest.model import RestApiDefaults, RestInterface

_log = get_logger(__name__)

# Type alias for ASGI middleware classes accepted by FastAPI.add_middleware.
_MiddlewareClass = Any


def create_fastapi_app(
    result: BootstrapResult,
    interfaces: Sequence[type[RestInterface[Any]]],
    *,
    middleware: Sequence[_MiddlewareClass] = (),
    defaults: RestApiDefaults | None = None,
    **fastapi_kwargs: Any,
) -> FastAPI:
    """Create a FastAPI application from a bootstrap result and REST interfaces.

    Compiles all ``RestInterface`` declarations via
    :class:`~loom.rest.compiler.RestInterfaceCompiler`, binds each compiled
    route to the ``FastAPI`` instance, and returns the ready application.

    Compilation is fail-fast: any structural error (missing use-case plan,
    duplicate route, missing prefix) raises
    :class:`~loom.rest.compiler.InterfaceCompilationError` before the app
    starts accepting requests.

    Args:
        result: Fully initialised :class:`~loom.core.bootstrap.bootstrap.BootstrapResult`
            from :func:`~loom.core.bootstrap.bootstrap.bootstrap_app`.
        interfaces: ``RestInterface`` subclasses declaring which endpoints to
            expose.  Compiled in declaration order.
        middleware: ASGI middleware classes to register on the application.
            Added in declaration order (first = outermost wrapper).
            Accepts any class compatible with ``FastAPI.add_middleware``.
            Example::

                from loom.rest.middleware import TraceIdMiddleware
                from loom.prometheus import PrometheusMiddleware

                app = create_fastapi_app(
                    result,
                    interfaces=[...],
                    middleware=[TraceIdMiddleware, PrometheusMiddleware],
                )
        defaults: Global REST API defaults (pagination mode, profile policy).
            Falls back to :class:`~loom.rest.model.RestApiDefaults` when not
            provided.
        **fastapi_kwargs: Additional keyword arguments forwarded to the
            ``FastAPI`` constructor (e.g. ``title``, ``version``,
            ``docs_url``).

    Returns:
        Configured :class:`fastapi.FastAPI` instance ready to serve requests.

    Raises:
        InterfaceCompilationError: If any interface fails structural validation.

    Example::

        app = create_fastapi_app(
            result,
            interfaces=[UserRestInterface, OrderRestInterface],
            defaults=RestApiDefaults(pagination_mode=PaginationMode.CURSOR),
            title="My API",
            version="2.0.0",
        )
    """
    app = FastAPI(**fastapi_kwargs)

    @app.exception_handler(Exception)
    async def _unhandled_exception(request: Request, exc: Exception) -> MsgspecJSONResponse:
        trace_id = get_trace_id()
        _log.error("UnhandledException", error=repr(exc), trace_id=trace_id)
        return MsgspecJSONResponse(
            status_code=500,
            content={
                ErrorField.CODE: "internal_error",
                ErrorField.MESSAGE: "An unexpected error occurred",
                ErrorField.TRACE_ID: trace_id,
            },
        )

    for mw_class in middleware:
        app.add_middleware(mw_class)

    interface_compiler = RestInterfaceCompiler(
        result.compiler,
        defaults=defaults,
    )
    executor = RuntimeExecutor(
        result.compiler,
        metrics=result.metrics,
        repo_resolver=result.container.resolve_repo,
    )

    all_routes = []
    for iface in interfaces:
        all_routes.extend(interface_compiler.compile(iface))

    component_registry = bind_interfaces(app, all_routes, result.factory, executor)
    if component_registry:
        _register_openapi_components(app, component_registry)

    return app


def _register_openapi_components(app: FastAPI, schemas: dict[str, Any]) -> None:
    """Patch ``app.openapi`` to inject ``schemas`` into ``components.schemas``.

    Args:
        app: FastAPI application whose OpenAPI generator to patch.
        schemas: Mapping of component name → JSON Schema fragment collected
            during route binding (nested ``$defs`` from msgspec/pydantic).
    """
    original_openapi = app.openapi

    def _openapi() -> dict[str, Any]:
        doc = original_openapi()
        doc.setdefault("components", {}).setdefault("schemas", {}).update(schemas)
        return doc

    app.openapi = _openapi  # type: ignore[method-assign]
