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
        RouteSources(python=[OrderRestInterface]),
        observability_runtime=ObservabilityRuntime.noop(),
        title="Orders API",
        version="1.0.0",
    )
"""

from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import Any, cast

from fastapi import FastAPI

from loom.core.bootstrap.bootstrap import BootstrapResult
from loom.core.engine.executor import RuntimeExecutor
from loom.core.observability.runtime import ObservabilityRuntime
from loom.rest.compiler import RestInterfaceCompiler, RouteSources
from loom.rest.fastapi._errors import register_error_handlers
from loom.rest.fastapi.router_runtime import bind_interfaces
from loom.rest.model import RestApiDefaults, RestInterface

# Type alias for ASGI middleware classes accepted by FastAPI.add_middleware.
_MiddlewareClass = Any


def _resolve_executor(result: BootstrapResult) -> RuntimeExecutor:
    """Return the application-scoped RuntimeExecutor registered at bootstrap."""
    if not result.container.is_registered(RuntimeExecutor):
        raise RuntimeError(
            "RuntimeExecutor is not registered in the container. "
            "Use bootstrap_app(...) or create_kernel(...) to build BootstrapResult."
        )
    return cast(RuntimeExecutor, result.container.resolve(RuntimeExecutor))


def _resolve_route_sources(
    routes: RouteSources | None,
    interfaces: Sequence[type[RestInterface[Any]]] | None,
) -> RouteSources:
    """Resolve the deprecated *interfaces* alias into *routes*.

    Exactly one of the two must be given: neither leaves nothing to compile,
    and both leave two conflicting answers with no rule to prefer one over
    the other. Warns when *interfaces* is used, since ``RouteSources`` is the
    replacement.

    A caller who passed the old second positional argument reaches this with a
    sequence where *routes* is expected, so that case is named here too: left
    alone it would surface much later as an attribute error on the sequence,
    which says nothing about how to fix the call.

    Raises:
        TypeError: If neither or both are given, or if *routes* is not a
            ``RouteSources`` instance (e.g. a bare sequence of interfaces).
    """
    if routes is not None and not isinstance(routes, RouteSources):
        raise TypeError(
            "create_fastapi_app()'s 'routes' parameter no longer accepts a bare sequence "
            "of interfaces. Pass RouteSources(python=[...]) instead."
        )
    if routes is not None and interfaces is not None:
        raise TypeError(
            "create_fastapi_app() accepts either 'routes' or the deprecated 'interfaces', not both."
        )
    if routes is not None:
        return routes
    if interfaces is not None:
        warnings.warn(
            "create_fastapi_app(interfaces=...) is deprecated and will be removed in 2.0. "
            "Use create_fastapi_app(routes=RouteSources(python=interfaces)) instead.",
            DeprecationWarning,
            stacklevel=3,
        )
        return RouteSources(python=interfaces)
    raise TypeError("create_fastapi_app() requires 'routes' (RouteSources).")


def create_fastapi_app(
    result: BootstrapResult,
    routes: RouteSources | None = None,
    *,
    interfaces: Sequence[type[RestInterface[Any]]] | None = None,
    observability_runtime: ObservabilityRuntime | None = None,
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
        routes: Which interfaces to mount and from which origin — see
            :class:`~loom.rest.compiler.RouteSources`. ``routes.config``
            compiles after ``routes.python``, deterministically; a
            ``(method, path)`` collision between the two aborts naming both
            — neither side takes precedence. ``routes.disabled`` is applied
            to ``routes.python`` before the merge, so a disabled Python
            route can be redeclared in ``routes.config`` without colliding
            with itself; an entry matching no Python-declared route aborts
            startup instead of doing nothing. Required unless *interfaces*
            is given instead.
        interfaces: Deprecated keyword-only alias for
            ``routes=RouteSources(python=interfaces)``. Kept only so code
            written before ``RouteSources`` existed keeps working when it
            called ``create_fastapi_app(result, interfaces=[...])`` —
            every published example did; emits a ``DeprecationWarning``
            naming *routes* as the replacement. Mutually exclusive with
            *routes*.
        observability_runtime: Shared runtime used to emit lifecycle events
            around each request.
        middleware: ASGI middleware classes to register on the application.
            Added in declaration order (first = outermost wrapper).
            Accepts any class compatible with ``FastAPI.add_middleware``.
            Example::

                from loom.rest.middleware import TraceIdMiddleware
                from loom.prometheus import PrometheusMiddleware

                app = create_fastapi_app(
                    result,
                    RouteSources(python=[...]),
                    observability_runtime=ObservabilityRuntime.noop(),
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
        TypeError: If neither *routes* nor *interfaces* is given, or both are.

    Example::

        app = create_fastapi_app(
            result,
            RouteSources(python=[UserRestInterface, OrderRestInterface]),
            defaults=RestApiDefaults(pagination_mode=PaginationMode.CURSOR),
            observability_runtime=ObservabilityRuntime.noop(),
            title="My API",
            version="2.0.0",
        )
    """
    resolved_routes = _resolve_route_sources(routes, interfaces)
    runtime = observability_runtime or ObservabilityRuntime.noop()
    app = FastAPI(**fastapi_kwargs)
    register_error_handlers(app)

    for mw_class in middleware:
        app.add_middleware(mw_class)

    interface_compiler = RestInterfaceCompiler(
        result.compiler,
        defaults=defaults,
    )
    executor = _resolve_executor(result)

    all_routes = interface_compiler.compile_sources(resolved_routes)

    component_registry = bind_interfaces(
        app,
        all_routes,
        result.factory,
        executor,
        observability_runtime=runtime,
    )
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
