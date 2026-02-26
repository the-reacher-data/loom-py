"""Unit tests for RestRoute, RestInterface, RestApiDefaults, and PaginationMode."""

from __future__ import annotations

from typing import Any

import pytest

from loom.core.use_case.use_case import UseCase
from loom.rest.model import (
    PaginationMode,
    RestApiDefaults,
    RestInterface,
    RestRoute,
)

# ---------------------------------------------------------------------------
# Dummy use cases
# ---------------------------------------------------------------------------


class CreateUserUseCase(UseCase[Any, str]):
    async def execute(self, **kwargs: Any) -> str:
        return "ok"


class ListUsersUseCase(UseCase[Any, str]):
    async def execute(self, **kwargs: Any) -> str:
        return "ok"


# ---------------------------------------------------------------------------
# RestRoute
# ---------------------------------------------------------------------------


def test_rest_route_required_fields() -> None:
    route = RestRoute(use_case=CreateUserUseCase, method="POST", path="/")
    assert route.use_case is CreateUserUseCase
    assert route.method == "POST"
    assert route.path == "/"


def test_rest_route_defaults() -> None:
    route = RestRoute(use_case=CreateUserUseCase, method="GET", path="/")
    assert route.summary == ""
    assert route.description == ""
    assert route.status_code == 200
    assert route.pagination_mode is None
    assert route.profile_default == ""
    assert route.allowed_profiles == ()
    assert route.expose_profile is False


def test_rest_route_is_frozen() -> None:
    route = RestRoute(use_case=CreateUserUseCase, method="POST", path="/")
    with pytest.raises((AttributeError, TypeError)):
        route.method = "GET"  # type: ignore[misc]


def test_rest_route_with_all_fields() -> None:
    route = RestRoute(
        use_case=CreateUserUseCase,
        method="POST",
        path="/",
        summary="Create user",
        description="Creates a new user account",
        status_code=201,
        pagination_mode=PaginationMode.CURSOR,
        profile_default="detail",
        allowed_profiles=("detail", "summary"),
        expose_profile=True,
    )
    assert route.status_code == 201
    assert route.pagination_mode == PaginationMode.CURSOR
    assert route.expose_profile is True


# ---------------------------------------------------------------------------
# RestInterface
# ---------------------------------------------------------------------------


def test_rest_interface_defaults() -> None:
    class EmptyInterface(RestInterface[str]):
        prefix = "/test"

    assert EmptyInterface.prefix == "/test"
    assert EmptyInterface.tags == ()
    assert EmptyInterface.auto is False
    assert EmptyInterface.include == ()
    assert EmptyInterface.routes == ()
    assert EmptyInterface.pagination_mode is None
    assert EmptyInterface.profile_default == ""
    assert EmptyInterface.allowed_profiles == ()
    assert EmptyInterface.expose_profile is False


def test_rest_interface_subclass_overrides() -> None:
    create = RestRoute(use_case=CreateUserUseCase, method="POST", path="/")
    list_route = RestRoute(use_case=ListUsersUseCase, method="GET", path="/")

    class UserInterface(RestInterface[str]):
        prefix = "/users"
        tags = ("Users",)
        auto = True
        include = ("create", "list")
        routes = (create, list_route)
        pagination_mode = PaginationMode.CURSOR
        profile_default = "summary"
        allowed_profiles = ("summary", "detail")
        expose_profile = True

    assert UserInterface.prefix == "/users"
    assert UserInterface.tags == ("Users",)
    assert UserInterface.auto is True
    assert UserInterface.pagination_mode == PaginationMode.CURSOR
    assert len(UserInterface.routes) == 2
    assert UserInterface.expose_profile is True


def test_rest_interface_is_generic() -> None:
    class TypedInterface(RestInterface[str]):
        prefix = "/items"

    # Just checking no runtime error on instantiation / class access
    assert TypedInterface.prefix == "/items"


# ---------------------------------------------------------------------------
# RestApiDefaults
# ---------------------------------------------------------------------------


def test_rest_api_defaults_default_values() -> None:
    d = RestApiDefaults()
    assert d.pagination_mode == PaginationMode.OFFSET
    assert d.profile_default == "default"
    assert d.allowed_profiles == ()


def test_rest_api_defaults_custom() -> None:
    d = RestApiDefaults(
        pagination_mode=PaginationMode.CURSOR,
        profile_default="summary",
        allowed_profiles=("summary", "detail"),
    )
    assert d.pagination_mode == PaginationMode.CURSOR
    assert d.profile_default == "summary"


def test_rest_api_defaults_is_frozen() -> None:
    d = RestApiDefaults()
    with pytest.raises((AttributeError, TypeError)):
        d.pagination_mode = PaginationMode.CURSOR  # type: ignore[misc]


# ---------------------------------------------------------------------------
# PaginationMode
# ---------------------------------------------------------------------------


def test_pagination_mode_values() -> None:
    assert PaginationMode.OFFSET.value == "offset"
    assert PaginationMode.CURSOR.value == "cursor"
