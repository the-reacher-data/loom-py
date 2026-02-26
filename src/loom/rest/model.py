"""Declarative REST interface model.

Defines the data structures that describe how UseCases are exposed over HTTP.
No FastAPI or transport-specific code lives here — this module is the
declaration layer; :mod:`loom.rest.compiler` validates and compiles it at
startup and :mod:`loom.rest.router_runtime` binds it to FastAPI.

Usage::

    class UserRestInterface(RestInterface[User]):
        prefix = "/users"
        tags = ("Users",)
        auto = True
        include = ("create", "get", "list", "update")
        routes = (
            RestRoute(use_case=CreateUserUseCase, method="POST", path="/"),
            RestRoute(use_case=GetUserUseCase, method="GET", path="/{user_id}"),
            RestRoute(use_case=ListUsersUseCase, method="GET", path="/"),
            RestRoute(
                use_case=UpdateUserUseCase,
                method="PATCH",
                path="/{user_id}",
                summary="Partial update user",
            ),
        )
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

from loom.core.repository.abc.query import PaginationMode
from loom.core.use_case.use_case import UseCase

T = TypeVar("T")

__all__ = [
    "PaginationMode",
    "RestApiDefaults",
    "RestInterface",
    "RestRoute",
]


@dataclass(frozen=True)
class RestRoute:
    """Declaration of a single HTTP endpoint bound to a UseCase.

    All fields except ``use_case``, ``method``, and ``path`` are optional and
    override interface-level or global defaults when set.

    Args:
        use_case: Concrete UseCase class that handles this endpoint.
        method: HTTP method in uppercase (``"GET"``, ``"POST"``, etc.).
        path: Path relative to the interface prefix.  May include path
            parameters (e.g. ``"/{user_id}"``).
        summary: Short OpenAPI summary for the endpoint.
        description: Longer OpenAPI description.
        status_code: Default HTTP success status code.  Defaults to ``200``.
        pagination_mode: Override pagination strategy for this route.
            When ``None``, inherits from the interface or global default.
        profile_default: Default query profile for this route.
            When empty, inherits from the interface or global default.
        allowed_profiles: Profiles that callers may request.  When empty,
            inherits from the interface or global default.
        expose_profile: Whether to expose ``?profile=`` as a public query
            parameter for this route.  Defaults to ``False``.

    Example::

        RestRoute(
            use_case=UpdateUserUseCase,
            method="PATCH",
            path="/{user_id}",
            summary="Partial update user",
            status_code=200,
        )
    """

    use_case: type[UseCase[Any, Any]]
    method: str
    path: str
    summary: str = ""
    description: str = ""
    status_code: int = 200
    pagination_mode: PaginationMode | None = None
    profile_default: str = ""
    allowed_profiles: tuple[str, ...] = ()
    expose_profile: bool = False


class RestInterface(Generic[T]):
    """Base class for declarative REST interface definitions.

    Subclass to expose one or more UseCases under a common HTTP prefix.
    All class attributes have sensible defaults; override only what differs.

    Attributes:
        prefix: URL prefix for all routes in this interface (e.g. ``"/users"``).
        tags: OpenAPI tags applied to every route.
        auto: When ``True``, signals intent to use standard CRUD URL conventions.
        include: Whitelist of CRUD operation names to expose when ``auto=True``.
            Accepted values: ``"create"``, ``"get"``, ``"list"``, ``"update"``,
            ``"delete"``.  Empty tuple means all operations are allowed.
        routes: Explicit route declarations.  Custom routes always override
            auto-CRUD routes with the same ``(method, path)``.
        pagination_mode: Default pagination strategy for list endpoints in this
            interface.  Overridden per-route by :attr:`RestRoute.pagination_mode`.
        profile_default: Default query profile for routes in this interface.
        allowed_profiles: Profiles available to callers of routes in this
            interface.
        expose_profile: Whether this interface publicly accepts
            ``?profile=...`` by default. Can be overridden per-route.

    Example::

        class OrderRestInterface(RestInterface[Order]):
            prefix = "/orders"
            tags = ("Orders",)
            auto = True
            include = ("create", "get", "list")
            routes = (
                RestRoute(use_case=CreateOrderUseCase, method="POST", path="/"),
                RestRoute(use_case=GetOrderUseCase, method="GET", path="/{order_id}"),
                RestRoute(use_case=ListOrdersUseCase, method="GET", path="/"),
            )
    """

    prefix: str = ""
    tags: tuple[str, ...] = ()
    auto: bool = False
    include: tuple[str, ...] = ()
    routes: tuple[RestRoute, ...] = ()
    pagination_mode: PaginationMode | None = None
    profile_default: str = ""
    allowed_profiles: tuple[str, ...] = ()
    expose_profile: bool = False


@dataclass(frozen=True)
class RestApiDefaults:
    """Global REST API defaults applied when interface or route level is unset.

    Args:
        pagination_mode: Default pagination strategy.  Defaults to
            :attr:`PaginationMode.OFFSET`.
        profile_default: Default query profile name.  Defaults to
            ``"default"``.
        allowed_profiles: Globally allowed profiles.  Defaults to empty
            (no restriction imposed at global level).

    Example::

        defaults = RestApiDefaults(
            pagination_mode=PaginationMode.CURSOR,
            profile_default="summary",
        )
    """

    pagination_mode: PaginationMode = PaginationMode.OFFSET
    profile_default: str = "default"
    allowed_profiles: tuple[str, ...] = field(default_factory=tuple)
