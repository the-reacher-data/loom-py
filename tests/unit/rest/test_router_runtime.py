"""Unit tests for bind_interfaces and create_fastapi_app."""

from __future__ import annotations

import json
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from loom.core.bootstrap.bootstrap import BootstrapResult, bootstrap_app
from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.use_case.factory import UseCaseFactory
from loom.core.use_case.use_case import UseCase
from loom.rest.compiler import CompiledRoute, InterfaceCompilationError, RestInterfaceCompiler
from loom.rest.model import RestApiDefaults, RestInterface, RestRoute
from loom.rest.fastapi.router_runtime import bind_interfaces
from loom.rest.fastapi.app import create_fastapi_app


# ---------------------------------------------------------------------------
# Dummy use cases
# ---------------------------------------------------------------------------


class PingUseCase(UseCase[str]):
    async def execute(self, **kwargs: Any) -> str:  # type: ignore[override]
        return "pong"


class EchoUseCase(UseCase[dict[str, Any]]):
    async def execute(self, **kwargs: Any) -> dict[str, Any]:  # type: ignore[override]
        return {"status": "received"}


class ItemUseCase(UseCase[dict[str, Any]]):
    async def execute(self, item_id: str, **kwargs: Any) -> dict[str, Any]:  # type: ignore[override]
        return {"item_id": item_id}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _uc_compiler(*types: type[UseCase[Any]]) -> UseCaseCompiler:
    c = UseCaseCompiler()
    for t in types:
        c.compile(t)
    return c


def _factory(uc_compiler: UseCaseCompiler, *types: type[UseCase[Any]]) -> UseCaseFactory:
    container = LoomContainer()
    factory = UseCaseFactory(container)
    for t in types:
        factory.register(t)
    return factory


def _make_app(*compiled_routes: CompiledRoute, **factory_types: Any) -> FastAPI:
    """Build a minimal FastAPI app from pre-compiled routes."""
    # Extract use case types from compiled routes
    types = [cr.route.use_case for cr in compiled_routes]
    uc_comp = _uc_compiler(*types)
    factory = _factory(uc_comp, *types)
    executor = RuntimeExecutor(uc_comp)
    app = FastAPI()
    bind_interfaces(app, list(compiled_routes), factory, executor)
    return app


def _compile_routes(
    interface: type[RestInterface[Any]],
    *extra_use_cases: type[UseCase[Any]],
) -> list[CompiledRoute]:
    """Compile a RestInterface, pre-compiling all declared use cases."""
    use_case_types = [r.use_case for r in interface.routes] + list(extra_use_cases)
    uc_comp = _uc_compiler(*use_case_types)
    return RestInterfaceCompiler(uc_comp).compile(interface)


# ---------------------------------------------------------------------------
# bind_interfaces — basic routing
# ---------------------------------------------------------------------------


def test_bind_interfaces_registers_route() -> None:
    class IFace(RestInterface[str]):
        prefix = "/ping"
        routes = (RestRoute(use_case=PingUseCase, method="GET", path="/"),)

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    response = client.get("/ping/")
    assert response.status_code == 200
    assert response.json() == "pong"


def test_bind_interfaces_content_type_is_json() -> None:
    class IFace(RestInterface[str]):
        prefix = "/ping"
        routes = (RestRoute(use_case=PingUseCase, method="GET", path="/"),)

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    response = client.get("/ping/")
    assert "application/json" in response.headers["content-type"]


def test_bind_interfaces_path_param_extracted() -> None:
    class IFace(RestInterface[dict[str, Any]]):
        prefix = "/items"
        routes = (RestRoute(use_case=ItemUseCase, method="GET", path="/{item_id}"),)

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    response = client.get("/items/42")
    assert response.status_code == 200
    assert response.json()["item_id"] == "42"


def test_bind_interfaces_status_code_respected() -> None:
    class IFace(RestInterface[str]):
        prefix = "/things"
        routes = (
            RestRoute(use_case=PingUseCase, method="POST", path="/", status_code=201),
        )

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    response = client.post("/things/")
    assert response.status_code == 201


def test_bind_interfaces_multiple_routes() -> None:
    class IFace(RestInterface[Any]):
        prefix = "/v1"
        routes = (
            RestRoute(use_case=PingUseCase, method="GET", path="/ping"),
            RestRoute(use_case=ItemUseCase, method="GET", path="/items/{item_id}"),
        )

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    assert client.get("/v1/ping").json() == "pong"
    assert client.get("/v1/items/7").json()["item_id"] == "7"


# ---------------------------------------------------------------------------
# bind_interfaces — POST with body
# ---------------------------------------------------------------------------


def test_bind_interfaces_request_body_decoded() -> None:
    class IFace(RestInterface[dict[str, Any]]):
        prefix = "/echo"
        routes = (RestRoute(use_case=EchoUseCase, method="POST", path="/"),)

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    response = client.post("/echo/", json={"name": "loom"})
    assert response.status_code == 200
    assert response.json() == {"status": "received"}


def test_bind_interfaces_empty_body_no_payload() -> None:
    class IFace(RestInterface[str]):
        prefix = "/ping"
        routes = (RestRoute(use_case=PingUseCase, method="GET", path="/"),)

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    response = client.get("/ping/")
    assert response.status_code == 200


# ---------------------------------------------------------------------------
# bind_interfaces — tags propagated to OpenAPI
# ---------------------------------------------------------------------------


def test_bind_interfaces_tags_on_routes() -> None:
    class IFace(RestInterface[str]):
        prefix = "/ping"
        tags = ("Pings",)
        routes = (RestRoute(use_case=PingUseCase, method="GET", path="/"),)

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    # Verify OpenAPI schema has the tag on the route
    client = TestClient(app)
    schema = client.get("/openapi.json").json()
    operation = schema["paths"]["/ping/"]["get"]
    assert "Pings" in operation.get("tags", [])


# ---------------------------------------------------------------------------
# create_fastapi_app — integration
# ---------------------------------------------------------------------------


class _FakeConfig:
    debug: bool = False


def _bootstrap(*use_cases: type[UseCase[Any]]) -> BootstrapResult:
    return bootstrap_app(config=_FakeConfig(), use_cases=list(use_cases))


def test_create_fastapi_app_returns_fastapi_instance() -> None:
    class IFace(RestInterface[str]):
        prefix = "/ping"
        routes = (RestRoute(use_case=PingUseCase, method="GET", path="/"),)

    result = _bootstrap(PingUseCase)
    app = create_fastapi_app(result, interfaces=[IFace])
    assert isinstance(app, FastAPI)


def test_create_fastapi_app_routes_work() -> None:
    class IFace(RestInterface[str]):
        prefix = "/ping"
        routes = (RestRoute(use_case=PingUseCase, method="GET", path="/"),)

    result = _bootstrap(PingUseCase)
    app = create_fastapi_app(result, interfaces=[IFace])
    client = TestClient(app)
    assert client.get("/ping/").json() == "pong"


def test_create_fastapi_app_accepts_fastapi_kwargs() -> None:
    class IFace(RestInterface[str]):
        prefix = "/ping"
        routes = (RestRoute(use_case=PingUseCase, method="GET", path="/"),)

    result = _bootstrap(PingUseCase)
    app = create_fastapi_app(result, interfaces=[IFace], title="My API", version="2.0.0")
    assert app.title == "My API"
    assert app.version == "2.0.0"


def test_create_fastapi_app_multiple_interfaces() -> None:
    class PingIFace(RestInterface[str]):
        prefix = "/ping"
        routes = (RestRoute(use_case=PingUseCase, method="GET", path="/"),)

    class ItemIFace(RestInterface[dict[str, Any]]):
        prefix = "/items"
        routes = (RestRoute(use_case=ItemUseCase, method="GET", path="/{item_id}"),)

    result = _bootstrap(PingUseCase, ItemUseCase)
    app = create_fastapi_app(result, interfaces=[PingIFace, ItemIFace])
    client = TestClient(app)
    assert client.get("/ping/").json() == "pong"
    assert client.get("/items/99").json()["item_id"] == "99"


def test_create_fastapi_app_uncompiled_use_case_raises() -> None:
    class IFace(RestInterface[str]):
        prefix = "/ping"
        routes = (RestRoute(use_case=PingUseCase, method="GET", path="/"),)

    # bootstrap with NO use cases — PingUseCase not compiled
    result = _bootstrap()
    with pytest.raises(InterfaceCompilationError, match="not been compiled"):
        create_fastapi_app(result, interfaces=[IFace])


def test_create_fastapi_app_accepts_defaults() -> None:
    from loom.rest.model import PaginationMode

    class IFace(RestInterface[str]):
        prefix = "/ping"
        routes = (RestRoute(use_case=PingUseCase, method="GET", path="/"),)

    result = _bootstrap(PingUseCase)
    defaults = RestApiDefaults(pagination_mode=PaginationMode.CURSOR)
    app = create_fastapi_app(result, interfaces=[IFace], defaults=defaults)
    assert isinstance(app, FastAPI)
