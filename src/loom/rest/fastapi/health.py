"""``GET /health``: aggregated readiness of the persistence backend.

The route is registered ahead of every interface router so a catch-all route
can never capture it, and it belongs to the default authentication exclusions
so an orchestrator needs no credentials to ask.

Internal module: consumed by :func:`loom.rest.fastapi.auto.create_app`.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from http import HTTPStatus
from time import monotonic

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute

from loom.rest.compiler import InterfaceCompilationError

HEALTH_PATH = "/health"
"""Path of the readiness route every auto-bootstrapped application serves."""

ReadinessProbe = Callable[[], Awaitable[bool]]
"""Asynchronous probe answering whether a backend can serve."""

PROBE_TTL_SECONDS = 2.0
"""Seconds a probe result is served from memory before the backend is asked again."""

PROBE_TIMEOUT_SECONDS = 5.0
"""Seconds a probe may run before it is abandoned and reported as not ready."""


class _CachedProbe:
    """Readiness probe memoised for a short TTL, single-flight and time-bounded.

    Concurrent callers share one in-flight probe; a result is reused until
    :data:`PROBE_TTL_SECONDS` elapse; a probe exceeding
    :data:`PROBE_TIMEOUT_SECONDS` counts as not ready.
    """

    def __init__(self, probe: ReadinessProbe) -> None:
        self._probe = probe
        self._lock = asyncio.Lock()
        self._result: bool | None = None
        self._expires_at = 0.0

    async def __call__(self) -> bool:
        async with self._lock:
            if self._result is not None and monotonic() < self._expires_at:
                return self._result
            self._result = await self._bounded_probe()
            self._expires_at = monotonic() + PROBE_TTL_SECONDS
            return self._result

    async def _bounded_probe(self) -> bool:
        try:
            return await asyncio.wait_for(self._probe(), PROBE_TIMEOUT_SECONDS)
        except TimeoutError:
            return False


def mount_health(app: FastAPI, backend: str, readiness: ReadinessProbe | None) -> None:
    """Register ``GET /health`` ahead of every route already on *app*.

    The response is ``{"status": "ok" | "degraded", "backends": {name: bool}}``
    with ``503`` when any backend is not ready. A backend without a probe
    reports no backends and is always ``ok``. The probe is wrapped in
    :class:`_CachedProbe`: its result is cached for :data:`PROBE_TTL_SECONDS`,
    concurrent requests share one probe, and one running longer than
    :data:`PROBE_TIMEOUT_SECONDS` reports the backend as not ready.

    Args:
        app: Application to mutate.
        backend: Name of the persistence backend, the key of its readiness
            in the response.
        readiness: The backend's probe; ``None`` when it has nothing to probe.
    """
    probe = None if readiness is None else _CachedProbe(readiness)

    async def _health() -> JSONResponse:
        backends = {} if probe is None else {backend: await probe()}
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


__all__ = [
    "HEALTH_PATH",
    "PROBE_TIMEOUT_SECONDS",
    "PROBE_TTL_SECONDS",
    "ReadinessProbe",
    "mount_health",
    "reject_health_collision",
]
