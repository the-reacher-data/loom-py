"""Unit tests for bind_interfaces and create_fastapi_app."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from loom.core.bootstrap.bootstrap import BootstrapResult, bootstrap_app
from loom.core.command import Command, Internal
from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.model import LoomStruct
from loom.core.repository.abc.query import FilterOp, PaginationMode, QuerySpec
from loom.core.use_case.factory import UseCaseFactory
from loom.core.use_case.markers import Exists, Input
from loom.core.use_case.use_case import UseCase
from loom.rest.compiler import CompiledRoute, InterfaceCompilationError, RestInterfaceCompiler
from loom.rest.fastapi.app import _register_openapi_components, create_fastapi_app
from loom.rest.fastapi.router_runtime import bind_interfaces
from loom.rest.model import RestApiDefaults, RestInterface, RestRoute

# ---------------------------------------------------------------------------
# Dummy use cases
# ---------------------------------------------------------------------------


class PingUseCase(UseCase[Any, str]):
    async def execute(self, **kwargs: Any) -> str:
        await asyncio.sleep(0)
        return "pong"


class EchoUseCase(UseCase[Any, dict[str, Any]]):
    async def execute(self, **kwargs: Any) -> dict[str, Any]:
        await asyncio.sleep(0)
        return {"status": "received"}


class CreateItemCmd(Command, frozen=True):
    full_name: str
    tenant_id: Internal[int] = 0


class CreateItemUseCase(UseCase[Any, dict[str, Any]]):
    async def execute(self, cmd: CreateItemCmd = Input()) -> dict[str, Any]:
        await asyncio.sleep(0)
        return {"full_name": cmd.full_name}


@dataclass(frozen=True)
class PlainCreateCmd:
    full_name: str

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> tuple[PlainCreateCmd, frozenset[str]]:
        return cls(full_name=str(payload["full_name"])), frozenset({"full_name"})


class PlainCreateUseCase(UseCase[Any, dict[str, Any]]):
    async def execute(self, cmd: PlainCreateCmd = Input()) -> dict[str, Any]:
        await asyncio.sleep(0)
        return {"full_name": cmd.full_name}


class ItemUseCase(UseCase[Any, dict[str, Any]]):
    async def execute(self, item_id: str, **kwargs: Any) -> dict[str, Any]:
        await asyncio.sleep(0)
        return {"item_id": item_id}


class IntItemUseCase(UseCase[Any, dict[str, Any]]):
    async def execute(self, item_id: int, **kwargs: Any) -> dict[str, Any]:
        await asyncio.sleep(0)
        return {"item_id": item_id}


class ProfileAwareUseCase(UseCase[Any, dict[str, Any]]):
    async def execute(self, profile: str, **kwargs: Any) -> dict[str, Any]:
        await asyncio.sleep(0)
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


class DocUseCase(UseCase[Any, str]):
    """Doc summary from use case.

    Extended documentation for OpenAPI description.
    """

    async def execute(self, **kwargs: Any) -> str:
        await asyncio.sleep(0)
        return "ok"


class _ExistsUserRecord(LoomStruct):
    email: str


class _ExistsCheckEmailCmd(Command, frozen=True):
    email: str


class _ExistsCheckEmailUseCase(UseCase[_ExistsUserRecord, bool]):
    async def execute(
        self,
        cmd: _ExistsCheckEmailCmd = Input(),
        email_exists: bool = Exists(_ExistsUserRecord, from_command="email", against="email"),
    ) -> bool:
        await asyncio.sleep(0)
        return email_exists


class _AutoItem(LoomStruct):
    id: int
    name: str


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


def _resolve_schema(schema_ref: dict[str, Any], openapi_doc: dict[str, Any]) -> dict[str, Any]:
    """Follow a ``$ref`` to its component definition when present."""
    ref = schema_ref.get("$ref", "")
    if ref.startswith("#/components/schemas/"):
        name = ref.removeprefix("#/components/schemas/")
        return openapi_doc["components"]["schemas"][name]  # type: ignore[no-any-return]
    return schema_ref


def _make_app(*compiled_routes: CompiledRoute, **factory_types: Any) -> FastAPI:
    """Build a minimal FastAPI app from pre-compiled routes."""
    # Extract use case types from compiled routes
    types = [cr.route.use_case for cr in compiled_routes]
    uc_comp = _uc_compiler(*types)
    factory = _factory(uc_comp, *types)
    executor = RuntimeExecutor(uc_comp)
    app = FastAPI()
    component_registry = bind_interfaces(app, list(compiled_routes), factory, executor)
    if component_registry:
        _register_openapi_components(app, component_registry)
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


def test_bind_interfaces_path_param_typed_from_execute_signature() -> None:
    class IFace(RestInterface[dict[str, Any]]):
        prefix = "/typed-items"
        routes = (RestRoute(use_case=IntItemUseCase, method="GET", path="/{item_id}"),)

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    response = client.get("/typed-items/42")
    assert response.status_code == 200
    assert response.json()["item_id"] == 42


def test_bind_interfaces_path_param_validation_uses_typed_signature() -> None:
    class IFace(RestInterface[dict[str, Any]]):
        prefix = "/typed-items"
        routes = (RestRoute(use_case=IntItemUseCase, method="GET", path="/{item_id}"),)

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    response = client.get("/typed-items/not-an-int")
    assert response.status_code == 422


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


def test_bind_interfaces_uses_compiled_default_pagination_mode() -> None:
    class IFace(RestInterface[dict[str, Any]]):
        prefix = "/items"
        pagination_mode = PaginationMode.CURSOR
        routes = (RestRoute(use_case=QueryAwareUseCase, method="GET", path="/"),)

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    response = client.get("/items/?limit=1")
    assert response.status_code == 200
    assert response.json()["pagination"] == PaginationMode.CURSOR.value


def test_bind_interfaces_rejects_pagination_override_when_disabled() -> None:
    class IFace(RestInterface[dict[str, Any]]):
        prefix = "/items"
        pagination_mode = PaginationMode.OFFSET
        allow_pagination_override = False
        routes = (RestRoute(use_case=QueryAwareUseCase, method="GET", path="/"),)

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    response = client.get("/items/?pagination=cursor")
    assert response.status_code == 400
    assert "cannot override" in response.json()["detail"]


def test_bind_interfaces_normalizes_camelcase_query_fields() -> None:
    class IFace(RestInterface[dict[str, Any]]):
        prefix = "/items"
        routes = (RestRoute(use_case=QueryAwareUseCase, method="GET", path="/"),)

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    response = client.get("/items/?sort=createdAt&priceTotal__gte=20")
    assert response.status_code == 200
    assert response.json()["sort_field"] == "created_at"
    assert response.json()["filter_field"] == "price_total"


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


def test_bind_interfaces_uses_use_case_docstring_for_route_docs() -> None:
    class IFace(RestInterface[str]):
        prefix = "/docs"
        routes = (RestRoute(use_case=DocUseCase, method="GET", path="/"),)

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    schema = client.get("/openapi.json").json()
    operation = schema["paths"]["/docs/"]["get"]
    assert operation["summary"] == "Doc summary from use case."
    assert "Extended documentation" in operation["description"]


def test_openapi_response_schema_inferred_from_use_case_return_type() -> None:
    class IFace(RestInterface[str]):
        prefix = "/ping"
        routes = (RestRoute(use_case=PingUseCase, method="GET", path="/"),)

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    schema = client.get("/openapi.json").json()
    response_schema = schema["paths"]["/ping/"]["get"]["responses"]["200"]["content"][
        "application/json"
    ]["schema"]
    assert response_schema["type"] == "string"


def test_openapi_request_schema_from_command_excludes_internal_fields() -> None:
    class IFace(RestInterface[dict[str, Any]]):
        prefix = "/items"
        routes = (RestRoute(use_case=CreateItemUseCase, method="POST", path="/"),)

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    schema = client.get("/openapi.json").json()
    raw_schema = schema["paths"]["/items/"]["post"]["requestBody"]["content"]["application/json"][
        "schema"
    ]
    request_schema = _resolve_schema(raw_schema, schema)
    properties = request_schema["properties"]
    assert "fullName" in properties
    assert "tenant_id" not in properties


def test_openapi_request_schema_for_plain_command_type() -> None:
    class IFace(RestInterface[dict[str, Any]]):
        prefix = "/plain"
        routes = (RestRoute(use_case=PlainCreateUseCase, method="POST", path="/"),)

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    schema = client.get("/openapi.json").json()
    raw_schema = schema["paths"]["/plain/"]["post"]["requestBody"]["content"]["application/json"][
        "schema"
    ]
    request_schema = _resolve_schema(raw_schema, schema)
    assert request_schema["type"] == "object"
    assert "full_name" in request_schema["properties"]


def test_openapi_autocrud_list_has_response_schema_and_query_parameters() -> None:
    class IFace(RestInterface[_AutoItem]):
        prefix = "/auto-items"
        auto = True
        include = ("list",)

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    schema = client.get("/openapi.json").json()
    operation = schema["paths"]["/auto-items/"]["get"]

    response_schema = operation["responses"]["200"]["content"]["application/json"]["schema"]
    assert response_schema

    parameter_names = {parameter["name"] for parameter in operation["parameters"]}
    expected_params = {"page", "limit", "pagination", "after", "cursor", "sort", "direction"}
    assert expected_params <= parameter_names


def test_openapi_autocrud_summary_uses_generated_docstring() -> None:
    class IFace(RestInterface[_AutoItem]):
        prefix = "/auto-docs"
        auto = True
        include = ("list",)

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    schema = client.get("/openapi.json").json()
    operation = schema["paths"]["/auto-docs/"]["get"]
    assert operation["summary"] == "List _AutoItem with filtering, sorting and pagination."


def test_openapi_autocrud_get_path_param_uses_model_id_type() -> None:
    class IFace(RestInterface[_AutoItem]):
        prefix = "/auto-item"
        auto = True
        include = ("get",)

    routes = _compile_routes(IFace)
    app = _make_app(*routes)
    client = TestClient(app)
    schema = client.get("/openapi.json").json()
    operation = schema["paths"]["/auto-item/{id}"]["get"]
    id_param = next(param for param in operation["parameters"] if param["name"] == "id")
    assert id_param["schema"]["type"] == "integer"


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


def test_create_fastapi_app_exists_marker_uses_registered_repository() -> None:
    class _UsersIFace(RestInterface[bool]):
        prefix = "/users"
        routes = (RestRoute(use_case=_ExistsCheckEmailUseCase, method="POST", path="/exists"),)

    class _FakeRepo:
        model = _ExistsUserRecord

        async def exists_by(self, field: str, value: Any) -> bool:
            await asyncio.sleep(0)
            return field == "email" and value == "taken@example.com"

    _repo = _FakeRepo()
    _token = ("repo", _ExistsUserRecord)

    def _register_repo(container: LoomContainer) -> None:
        container.register(_token, lambda: _repo, scope=Scope.APPLICATION)
        container.register_repo(_ExistsUserRecord, _token)

    result = bootstrap_app(
        config=_FakeConfig(),
        use_cases=[_ExistsCheckEmailUseCase],
        modules=[_register_repo],
    )
    app = create_fastapi_app(result, interfaces=[_UsersIFace])
    client = TestClient(app)

    response = client.post("/users/exists", json={"email": "taken@example.com"})
    assert response.status_code == 200
    assert response.json() is True
