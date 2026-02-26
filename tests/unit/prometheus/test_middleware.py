"""Unit tests for PrometheusMiddleware."""

from __future__ import annotations

from typing import Any

import pytest
from prometheus_client import CollectorRegistry

from loom.prometheus import PrometheusMiddleware


def _make_scope(path: str = "/", method: str = "GET", route_path: str | None = None) -> dict[str, Any]:
    scope: dict[str, Any] = {
        "type": "http",
        "method": method,
        "path": path,
        "headers": [],
    }
    if route_path is not None:
        class _FakeRoute:
            def __init__(self, p: str) -> None:
                self.path = p
        scope["route"] = _FakeRoute(route_path)
    return scope


async def _null_receive() -> dict[str, Any]:
    return {}


def _metric_value(registry: CollectorRegistry, sample_name: str, labels: dict[str, str]) -> float:
    for metric in registry.collect():
        for sample in metric.samples:
            if sample.name == sample_name and all(
                sample.labels.get(k) == v for k, v in labels.items()
            ):
                return sample.value
    return 0.0


def _make_async_sink(target: list[Any]) -> Any:
    async def _sink(message: Any) -> None:
        target.append(message)
    return _sink


class TestPrometheusMiddlewareHTTP:
    @pytest.mark.asyncio
    async def test_increments_request_counter(self) -> None:
        registry = CollectorRegistry()

        async def app(scope: Any, receive: Any, send: Any) -> None:
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b""})

        mw = PrometheusMiddleware(app, registry=registry)
        sent: list[Any] = []
        await mw(_make_scope("/products", route_path="/products"), _null_receive,
                 _make_async_sink(sent))  # type: ignore[arg-type]

        # Counter named "http_requests" → sample name "http_requests_total"
        val = _metric_value(
            registry, "http_requests_total",
            {"method": "GET", "path_template": "/products", "status_code": "200"},
        )
        assert val == 1.0

    @pytest.mark.asyncio
    async def test_records_duration(self) -> None:
        registry = CollectorRegistry()

        async def app(scope: Any, receive: Any, send: Any) -> None:
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b""})

        mw = PrometheusMiddleware(app, registry=registry)
        sent: list[Any] = []
        await mw(_make_scope(), _null_receive,
                 _make_async_sink(sent))  # type: ignore[arg-type]

        count = _metric_value(
            registry, "http_request_duration_seconds_count",
            {"method": "GET", "path_template": "/"},
        )
        assert count == 1.0

    @pytest.mark.asyncio
    async def test_uses_route_template_not_real_path(self) -> None:
        registry = CollectorRegistry()

        async def app(scope: Any, receive: Any, send: Any) -> None:
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b""})

        mw = PrometheusMiddleware(app, registry=registry)
        sent: list[Any] = []
        scope = _make_scope("/products/42", route_path="/products/{product_id}")
        await mw(scope, _null_receive,
                 _make_async_sink(sent))  # type: ignore[arg-type]

        template_val = _metric_value(
            registry, "http_requests_total",
            {"path_template": "/products/{product_id}", "status_code": "200"},
        )
        real_val = _metric_value(
            registry, "http_requests_total",
            {"path_template": "/products/42", "status_code": "200"},
        )
        assert template_val == 1.0
        assert real_val == 0.0

    @pytest.mark.asyncio
    async def test_records_status_code(self) -> None:
        registry = CollectorRegistry()

        async def app(scope: Any, receive: Any, send: Any) -> None:
            await send({"type": "http.response.start", "status": 404, "headers": []})
            await send({"type": "http.response.body", "body": b""})

        mw = PrometheusMiddleware(app, registry=registry)
        sent: list[Any] = []
        await mw(_make_scope(), _null_receive,
                 _make_async_sink(sent))  # type: ignore[arg-type]

        val = _metric_value(
            registry, "http_requests_total",
            {"method": "GET", "path_template": "/", "status_code": "404"},
        )
        assert val == 1.0


class TestPrometheusMiddlewareNonHTTP:
    @pytest.mark.asyncio
    async def test_non_http_scope_passed_through(self) -> None:
        called: list[bool] = []

        async def app(scope: Any, receive: Any, send: Any) -> None:
            called.append(True)

        registry = CollectorRegistry()
        mw = PrometheusMiddleware(app, registry=registry)
        await mw({"type": "lifespan"}, _null_receive, _null_receive)  # type: ignore[arg-type]
        assert called == [True]
