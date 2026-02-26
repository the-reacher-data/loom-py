"""Unit tests for RestInterfaceCompiler."""

from __future__ import annotations

from typing import Any

import pytest

from loom.core.engine.compiler import UseCaseCompiler
from loom.core.use_case.use_case import UseCase
from loom.rest.compiler import CompiledRoute, InterfaceCompilationError, RestInterfaceCompiler
from loom.rest.model import PaginationMode, RestApiDefaults, RestInterface, RestRoute


# ---------------------------------------------------------------------------
# Dummy use cases (compiled fixtures)
# ---------------------------------------------------------------------------


class CreateUserUseCase(UseCase[str]):
    async def execute(self, **kwargs: Any) -> str:  # type: ignore[override]
        return "ok"


class GetUserUseCase(UseCase[str]):
    async def execute(self, **kwargs: Any) -> str:  # type: ignore[override]
        return "ok"


class ListUsersUseCase(UseCase[str]):
    async def execute(self, **kwargs: Any) -> str:  # type: ignore[override]
        return "ok"


class UncompiledUseCase(UseCase[str]):
    async def execute(self, **kwargs: Any) -> str:  # type: ignore[override]
        return "ok"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _compiler() -> UseCaseCompiler:
    uc = UseCaseCompiler()
    uc.compile(CreateUserUseCase)
    uc.compile(GetUserUseCase)
    uc.compile(ListUsersUseCase)
    return uc


def _rest_compiler(**kwargs: Any) -> RestInterfaceCompiler:
    return RestInterfaceCompiler(_compiler(), **kwargs)


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_compile_single_route() -> None:
    class IFace(RestInterface[str]):
        prefix = "/users"
        routes = (RestRoute(use_case=CreateUserUseCase, method="POST", path="/"),)

    result = _rest_compiler().compile(IFace)
    assert len(result) == 1
    assert result[0].route.use_case is CreateUserUseCase


def test_compile_multiple_routes() -> None:
    class IFace(RestInterface[str]):
        prefix = "/users"
        routes = (
            RestRoute(use_case=CreateUserUseCase, method="POST", path="/"),
            RestRoute(use_case=GetUserUseCase, method="GET", path="/{user_id}"),
            RestRoute(use_case=ListUsersUseCase, method="GET", path="/"),
        )

    result = _rest_compiler().compile(IFace)
    assert len(result) == 3


def test_compile_full_path_assembled() -> None:
    class IFace(RestInterface[str]):
        prefix = "/users"
        routes = (RestRoute(use_case=GetUserUseCase, method="GET", path="/{user_id}"),)

    result = _rest_compiler().compile(IFace)
    assert result[0].full_path == "/users/{user_id}"


def test_compile_prefix_trailing_slash_stripped() -> None:
    class IFace(RestInterface[str]):
        prefix = "/users/"
        routes = (RestRoute(use_case=CreateUserUseCase, method="POST", path="/"),)

    result = _rest_compiler().compile(IFace)
    assert result[0].full_path == "/users/"


def test_compile_is_idempotent() -> None:
    class IFace(RestInterface[str]):
        prefix = "/users"
        routes = (RestRoute(use_case=CreateUserUseCase, method="POST", path="/"),)

    compiler = _rest_compiler()
    first = compiler.compile(IFace)
    second = compiler.compile(IFace)
    assert first is second  # cached


# ---------------------------------------------------------------------------
# Pagination policy: route > interface > global
# ---------------------------------------------------------------------------


def test_pagination_route_level_overrides_all() -> None:
    class IFace(RestInterface[str]):
        prefix = "/items"
        pagination_mode = PaginationMode.OFFSET
        routes = (
            RestRoute(
                use_case=ListUsersUseCase,
                method="GET",
                path="/",
                pagination_mode=PaginationMode.CURSOR,
            ),
        )

    defaults = RestApiDefaults(pagination_mode=PaginationMode.OFFSET)
    result = RestInterfaceCompiler(_compiler(), defaults=defaults).compile(IFace)
    assert result[0].effective_pagination_mode == PaginationMode.CURSOR


def test_pagination_interface_level_overrides_global() -> None:
    class IFace(RestInterface[str]):
        prefix = "/items"
        pagination_mode = PaginationMode.CURSOR
        routes = (RestRoute(use_case=ListUsersUseCase, method="GET", path="/"),)

    defaults = RestApiDefaults(pagination_mode=PaginationMode.OFFSET)
    result = RestInterfaceCompiler(_compiler(), defaults=defaults).compile(IFace)
    assert result[0].effective_pagination_mode == PaginationMode.CURSOR


def test_pagination_global_applied_when_unset() -> None:
    class IFace(RestInterface[str]):
        prefix = "/items"
        routes = (RestRoute(use_case=ListUsersUseCase, method="GET", path="/"),)

    defaults = RestApiDefaults(pagination_mode=PaginationMode.CURSOR)
    result = RestInterfaceCompiler(_compiler(), defaults=defaults).compile(IFace)
    assert result[0].effective_pagination_mode == PaginationMode.CURSOR


# ---------------------------------------------------------------------------
# Profile policy: route > interface > global
# ---------------------------------------------------------------------------


def test_profile_default_route_level() -> None:
    class IFace(RestInterface[str]):
        prefix = "/items"
        profile_default = "summary"
        routes = (
            RestRoute(
                use_case=GetUserUseCase,
                method="GET",
                path="/{id}",
                profile_default="detail",
            ),
        )

    result = _rest_compiler().compile(IFace)
    assert result[0].effective_profile_default == "detail"


def test_profile_default_interface_level() -> None:
    class IFace(RestInterface[str]):
        prefix = "/items"
        profile_default = "summary"
        routes = (RestRoute(use_case=GetUserUseCase, method="GET", path="/{id}"),)

    result = _rest_compiler().compile(IFace)
    assert result[0].effective_profile_default == "summary"


def test_profile_default_global() -> None:
    class IFace(RestInterface[str]):
        prefix = "/items"
        routes = (RestRoute(use_case=GetUserUseCase, method="GET", path="/{id}"),)

    defaults = RestApiDefaults(profile_default="compact")
    result = RestInterfaceCompiler(_compiler(), defaults=defaults).compile(IFace)
    assert result[0].effective_profile_default == "compact"


def test_allowed_profiles_route_level() -> None:
    class IFace(RestInterface[str]):
        prefix = "/items"
        allowed_profiles = ("summary",)
        routes = (
            RestRoute(
                use_case=GetUserUseCase,
                method="GET",
                path="/{id}",
                allowed_profiles=("detail", "full"),
            ),
        )

    result = _rest_compiler().compile(IFace)
    assert result[0].effective_allowed_profiles == ("detail", "full")


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------


def test_compile_missing_prefix_raises() -> None:
    class IFace(RestInterface[str]):
        prefix = ""
        routes = (RestRoute(use_case=CreateUserUseCase, method="POST", path="/"),)

    with pytest.raises(InterfaceCompilationError, match="prefix"):
        _rest_compiler().compile(IFace)


def test_compile_empty_routes_raises() -> None:
    class IFace(RestInterface[str]):
        prefix = "/users"
        routes = ()

    with pytest.raises(InterfaceCompilationError, match="routes"):
        _rest_compiler().compile(IFace)


def test_compile_uncompiled_use_case_raises() -> None:
    class IFace(RestInterface[str]):
        prefix = "/users"
        routes = (RestRoute(use_case=UncompiledUseCase, method="POST", path="/"),)

    with pytest.raises(InterfaceCompilationError, match="not been compiled"):
        _rest_compiler().compile(IFace)


def test_compile_duplicate_method_path_raises() -> None:
    class IFace(RestInterface[str]):
        prefix = "/users"
        routes = (
            RestRoute(use_case=CreateUserUseCase, method="POST", path="/"),
            RestRoute(use_case=CreateUserUseCase, method="POST", path="/"),
        )

    with pytest.raises(InterfaceCompilationError, match="duplicate"):
        _rest_compiler().compile(IFace)


def test_compile_expose_profile_without_allowed_profiles_raises() -> None:
    class IFace(RestInterface[str]):
        prefix = "/users"
        routes = (
            RestRoute(
                use_case=GetUserUseCase,
                method="GET",
                path="/{id}",
                expose_profile=True,
                allowed_profiles=(),  # missing!
            ),
        )

    with pytest.raises(InterfaceCompilationError, match="expose_profile"):
        _rest_compiler().compile(IFace)


def test_compile_expose_profile_with_allowed_profiles_ok() -> None:
    class IFace(RestInterface[str]):
        prefix = "/users"
        routes = (
            RestRoute(
                use_case=GetUserUseCase,
                method="GET",
                path="/{id}",
                expose_profile=True,
                allowed_profiles=("detail",),
            ),
        )

    result = _rest_compiler().compile(IFace)
    assert result[0].route.expose_profile is True
