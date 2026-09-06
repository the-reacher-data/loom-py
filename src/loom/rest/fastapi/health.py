"""``GET /health``: aggregated readiness of the persistence backend.

The route is registered ahead of every interface router so a catch-all route
can never capture it, and it belongs to the default authentication exclusions
so an orchestrator needs no credentials to ask.

Internal module: consumed by :func:`loom.rest.fastapi.auto.create_app`.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from http import HTTPStatus

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute

from loom.rest.compiler import InterfaceCompilationError

HEALTH_PATH = "/health"
"""Path of the readiness route every auto-bootstrapped application serves."""

ReadinessProbe = Callable[[], Awaitable[bool]]
"""Asynchronous probe answering whether a backend can serve."""


def mount_health(app: FastAPI, backend: str, readiness: ReadinessProbe | None) -> None:
    """Register ``GET /health`` ahead of every route already on *app*.

    The response is ``{"status": "ok" | "degraded", "backends": {name: bool}}``
    with ``503`` when any backend is not ready. A backend without a probe
    reports no backends and is always ``ok``.

    Args:
        app: Application to mutate.
        backend: Name of the persistence backend, the key of its readiness
            in the response.
        readiness: The backend's probe; ``None`` when it has nothing to probe.
    """

    async def _health() -> JSONResponse:
        backends = {} if readiness is None else {backend: await readiness()}
        return _health_response(backends)

    route = APIRoute(HEALTH_PATH, _health, methods=["GET"], include_in_schema=False)
    app.router.routes.insert(0, route)


def reject_health_collision(app: FastAPI) -> None:
    """Refuse to start when another route declares the literal ``/health``.

    The path is served without authentication, so a business route compiled
    to it would answer anonymously — or never answer, hidden behind the
    readiness route.

    Args:
        app: Application whose routes are already registered.

    Raises:
        InterfaceCompilationError: When more than one route declares
            :data:`HEALTH_PATH`.
    """
    declared = [route for route in app.router.routes if getattr(route, "path", None) == HEALTH_PATH]
    if len(declared) > 1:
        raise InterfaceCompilationError(
            f"{HEALTH_PATH!r} is reserved for the readiness route, but another route "
            "compiles to it. Declare that interface under a different prefix."
        )


def _health_response(backends: dict[str, bool]) -> JSONResponse:
    ready = all(backends.values())
    return JSONResponse(
        {"status": "ok" if ready else "degraded", "backends": backends},
        status_code=HTTPStatus.OK if ready else HTTPStatus.SERVICE_UNAVAILABLE,
    )


__all__ = ["HEALTH_PATH", "ReadinessProbe", "mount_health", "reject_health_collision"]
