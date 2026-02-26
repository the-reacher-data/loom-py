"""Unit tests for bind_interfaces and create_fastapi_app."""

from __future__ import annotations

from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from loom.core.bootstrap.bootstrap import BootstrapResult, bootstrap_app
from loom.core.di.container import LoomContainer
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.repository.abc.query import FilterOp, PaginationMode, QuerySpec
from loom.core.use_case.factory import UseCaseFactory
from loom.core.use_case.use_case import UseCase
from loom.rest.compiler import CompiledRoute, InterfaceCompilationError, RestInterfaceCompiler
from loom.rest.fastapi.app import create_fastapi_app
from loom.rest.fastapi.router_runtime import bind_interfaces
from loom.rest.model import RestApiDefaults, RestInterface, RestRoute

# ---------------------------------------------------------------------------
# Dummy use cases
# ---------------------------------------------------------------------------


class PingUseCase(UseCase[Any, str]):
    async def execute(self, **kwargs: Any) -> str:
        return "pong"


class EchoUseCase(UseCase[Any, dict[str, Any]]):
    async def execute(self, **kwargs: Any) -> dict[str, Any]:
        return {"status": "received"}


class ItemUseCase(UseCase[Any, dict[str, Any]]):
    async def execute(self, item_id: str, **kwargs: Any) -> dict[str, Any]:
        return {"item_id": item_id}


class ProfileAwareUseCase(UseCase[Any, dict[str, Any]]):
    async def execute(self, profile: str, **kwargs: Any) -> dict[str, Any]:
        return {"profile": profile}


class QueryAwareUseCase(UseCase[Any, dict[str, Any]]):
    async def execute(self, query: QuerySpec, **kwargs: Any) -> dict[str, Any]:
        first_filter = query.filters.filters[0] if query.filters else None
        first_sort = query.sort[0] if query.sort else None
        return {
            "pagination": query.pagination.value,
            "page": query.page,
            "limit": query.limit,
            "cursor": query.cursor,
            "filter_field": first_filter.field if first_filter else None,
            "filter_op": first_filter.op.value if first_filter else None,
            "filter_value": first_filter.value if first_filter else None,
            "sort_field": first_sort.field if first_sort else None,
            "sort_direction": first_sort.direction if first_sort else None,
        }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _uc_compiler(*types: type[UseCase[Any, Any]]) -> UseCaseCompiler:
    c = UseCaseCompiler()
    for t in types:
        c.compile(t)
    return c


def _factory(uc_compiler: UseCaseCompiler, *types: type[UseCase[Any, Any]]) -> UseCaseFactory:
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
    *extra_use_cases: type[UseCase[Any, Any]],
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
        routes = (RestRoute(use_case=PingUseCase, method="POST", path="/", status_code=201),)

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


def test_bind_interfaces_rejects_profile_query_when_not_exposed() -> None:
    class IFace(RestInterface[str]):
        prefix = "/ping"
        routes = (RestRoute(use_case=PingUseCase, method="GET", path="/"),)

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    response = client.get("/ping/?profile=detail")
    assert response.status_code == 400
    assert "not allowed" in response.json()["detail"]


def test_bind_interfaces_passes_default_profile_when_use_case_accepts_it() -> None:
    class IFace(RestInterface[dict[str, Any]]):
        prefix = "/items"
        profile_default = "summary"
        routes = (RestRoute(use_case=ProfileAwareUseCase, method="GET", path="/"),)

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    response = client.get("/items/")
    assert response.status_code == 200
    assert response.json() == {"profile": "summary"}


def test_bind_interfaces_accepts_profile_query_when_exposed() -> None:
    class IFace(RestInterface[dict[str, Any]]):
        prefix = "/items"
        expose_profile = True
        allowed_profiles = ("summary", "detail")
        profile_default = "summary"
        routes = (RestRoute(use_case=ProfileAwareUseCase, method="GET", path="/"),)

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    response = client.get("/items/?profile=detail")
    assert response.status_code == 200
    assert response.json() == {"profile": "detail"}


def test_bind_interfaces_rejects_disallowed_profile_query() -> None:
    class IFace(RestInterface[dict[str, Any]]):
        prefix = "/items"
        expose_profile = True
        allowed_profiles = ("summary",)
        routes = (RestRoute(use_case=ProfileAwareUseCase, method="GET", path="/"),)

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    response = client.get("/items/?profile=detail")
    assert response.status_code == 400
    assert "Invalid profile" in response.json()["detail"]


def test_bind_interfaces_builds_query_spec_from_offset_url_params() -> None:
    class IFace(RestInterface[dict[str, Any]]):
        prefix = "/items"
        routes = (RestRoute(use_case=QueryAwareUseCase, method="GET", path="/"),)

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    response = client.get("/items/?page=2&limit=10&price__gte=20&sort=id&direction=DESC")
    assert response.status_code == 200
    assert response.json() == {
        "pagination": PaginationMode.OFFSET.value,
        "page": 2,
        "limit": 10,
        "cursor": None,
        "filter_field": "price",
        "filter_op": FilterOp.GTE.value,
        "filter_value": 20,
        "sort_field": "id",
        "sort_direction": "DESC",
    }


def test_bind_interfaces_builds_query_spec_from_cursor_url_params() -> None:
    class IFace(RestInterface[dict[str, Any]]):
        prefix = "/items"
        routes = (RestRoute(use_case=QueryAwareUseCase, method="GET", path="/"),)

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    response = client.get("/items/?after=abc123&limit=1&reviews__exists=true")
    assert response.status_code == 200
    assert response.json() == {
        "pagination": PaginationMode.CURSOR.value,
        "page": 1,
        "limit": 1,
        "cursor": "abc123",
        "filter_field": "reviews",
        "filter_op": FilterOp.EXISTS.value,
        "filter_value": True,
        "sort_field": None,
        "sort_direction": None,
    }


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


def _bootstrap(*use_cases: type[UseCase[Any, Any]]) -> BootstrapResult:
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
