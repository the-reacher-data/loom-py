"""Unit tests for RestInterfaceCompiler."""

from __future__ import annotations

from typing import Any

import pytest

from loom.core.engine.compiler import UseCaseCompiler
from loom.core.use_case.use_case import UseCase
from loom.rest.compiler import InterfaceCompilationError, RestInterfaceCompiler
from loom.rest.model import PaginationMode, RestApiDefaults, RestInterface, RestRoute


class CreateUserUseCase(UseCase[Any, str]):
    async def execute(self, **kwargs: Any) -> str:
        return "ok"


class GetUserUseCase(UseCase[Any, str]):
    async def execute(self, **kwargs: Any) -> str:
        return "ok"


class ListUsersUseCase(UseCase[Any, str]):
    async def execute(self, **kwargs: Any) -> str:
        return "ok"


class UncompiledUseCase(UseCase[Any, str]):
    async def execute(self, **kwargs: Any) -> str:
        return "ok"


@pytest.fixture
def use_case_compiler() -> UseCaseCompiler:
    compiler = UseCaseCompiler()
    compiler.compile(CreateUserUseCase)
    compiler.compile(GetUserUseCase)
    compiler.compile(ListUsersUseCase)
    return compiler


@pytest.fixture
def rest_compiler(use_case_compiler: UseCaseCompiler) -> RestInterfaceCompiler:
    return RestInterfaceCompiler(use_case_compiler)


class TestCompile:
    def test_compile_single_route(self, rest_compiler: RestInterfaceCompiler) -> None:
        class IFace(RestInterface[str]):
            prefix = "/users"
            routes = (RestRoute(use_case=CreateUserUseCase, method="POST", path="/"),)

        result = rest_compiler.compile(IFace)
        assert len(result) == 1
        assert result[0].route.use_case is CreateUserUseCase

    def test_compile_multiple_routes(self, rest_compiler: RestInterfaceCompiler) -> None:
        class IFace(RestInterface[str]):
            prefix = "/users"
            routes = (
                RestRoute(use_case=CreateUserUseCase, method="POST", path="/"),
                RestRoute(use_case=GetUserUseCase, method="GET", path="/{user_id}"),
                RestRoute(use_case=ListUsersUseCase, method="GET", path="/"),
            )

        result = rest_compiler.compile(IFace)
        assert len(result) == 3

    def test_compile_full_path_assembled(self, rest_compiler: RestInterfaceCompiler) -> None:
        class IFace(RestInterface[str]):
            prefix = "/users"
            routes = (RestRoute(use_case=GetUserUseCase, method="GET", path="/{user_id}"),)

        result = rest_compiler.compile(IFace)
        assert result[0].full_path == "/users/{user_id}"

    def test_compile_prefix_trailing_slash_stripped(
        self,
        rest_compiler: RestInterfaceCompiler,
    ) -> None:
        class IFace(RestInterface[str]):
            prefix = "/users/"
            routes = (RestRoute(use_case=CreateUserUseCase, method="POST", path="/"),)

        result = rest_compiler.compile(IFace)
        assert result[0].full_path == "/users/"

    def test_compile_is_idempotent(self, rest_compiler: RestInterfaceCompiler) -> None:
        class IFace(RestInterface[str]):
            prefix = "/users"
            routes = (RestRoute(use_case=CreateUserUseCase, method="POST", path="/"),)

        first = rest_compiler.compile(IFace)
        second = rest_compiler.compile(IFace)
        assert first is second


class TestPaginationPolicy:
    def test_route_overrides_interface_and_global(
        self,
        use_case_compiler: UseCaseCompiler,
    ) -> None:
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
        result = RestInterfaceCompiler(use_case_compiler, defaults=defaults).compile(IFace)
        assert result[0].effective_pagination_mode == PaginationMode.CURSOR

    def test_interface_overrides_global(self, use_case_compiler: UseCaseCompiler) -> None:
        class IFace(RestInterface[str]):
            prefix = "/items"
            pagination_mode = PaginationMode.CURSOR
            routes = (RestRoute(use_case=ListUsersUseCase, method="GET", path="/"),)

        defaults = RestApiDefaults(pagination_mode=PaginationMode.OFFSET)
        result = RestInterfaceCompiler(use_case_compiler, defaults=defaults).compile(IFace)
        assert result[0].effective_pagination_mode == PaginationMode.CURSOR

    def test_global_applied_when_unset(self, use_case_compiler: UseCaseCompiler) -> None:
        class IFace(RestInterface[str]):
            prefix = "/items"
            routes = (RestRoute(use_case=ListUsersUseCase, method="GET", path="/"),)

        defaults = RestApiDefaults(pagination_mode=PaginationMode.CURSOR)
        result = RestInterfaceCompiler(use_case_compiler, defaults=defaults).compile(IFace)
        assert result[0].effective_pagination_mode == PaginationMode.CURSOR

    def test_pagination_override_route_level(self, use_case_compiler: UseCaseCompiler) -> None:
        class IFace(RestInterface[str]):
            prefix = "/items"
            allow_pagination_override = True
            routes = (
                RestRoute(
                    use_case=ListUsersUseCase,
                    method="GET",
                    path="/",
                    allow_pagination_override=False,
                ),
            )

        defaults = RestApiDefaults(allow_pagination_override=True)
        result = RestInterfaceCompiler(use_case_compiler, defaults=defaults).compile(IFace)
        assert result[0].effective_allow_pagination_override is False

    def test_pagination_override_interface_level(self, use_case_compiler: UseCaseCompiler) -> None:
        class IFace(RestInterface[str]):
            prefix = "/items"
            allow_pagination_override = False
            routes = (RestRoute(use_case=ListUsersUseCase, method="GET", path="/"),)

        defaults = RestApiDefaults(allow_pagination_override=True)
        result = RestInterfaceCompiler(use_case_compiler, defaults=defaults).compile(IFace)
        assert result[0].effective_allow_pagination_override is False

    def test_pagination_override_global_default(self, use_case_compiler: UseCaseCompiler) -> None:
        class IFace(RestInterface[str]):
            prefix = "/items"
            routes = (RestRoute(use_case=ListUsersUseCase, method="GET", path="/"),)

        defaults = RestApiDefaults(allow_pagination_override=False)
        result = RestInterfaceCompiler(use_case_compiler, defaults=defaults).compile(IFace)
        assert result[0].effective_allow_pagination_override is False


class TestProfilePolicy:
    def test_profile_default_route_level(self, rest_compiler: RestInterfaceCompiler) -> None:
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

        result = rest_compiler.compile(IFace)
        assert result[0].effective_profile_default == "detail"

    def test_profile_default_interface_level(self, rest_compiler: RestInterfaceCompiler) -> None:
        class IFace(RestInterface[str]):
            prefix = "/items"
            profile_default = "summary"
            routes = (RestRoute(use_case=GetUserUseCase, method="GET", path="/{id}"),)

        result = rest_compiler.compile(IFace)
        assert result[0].effective_profile_default == "summary"

    def test_profile_default_global(self, use_case_compiler: UseCaseCompiler) -> None:
        class IFace(RestInterface[str]):
            prefix = "/items"
            routes = (RestRoute(use_case=GetUserUseCase, method="GET", path="/{id}"),)

        defaults = RestApiDefaults(profile_default="compact")
        result = RestInterfaceCompiler(use_case_compiler, defaults=defaults).compile(IFace)
        assert result[0].effective_profile_default == "compact"

    def test_allowed_profiles_route_level(self, rest_compiler: RestInterfaceCompiler) -> None:
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

        result = rest_compiler.compile(IFace)
        assert result[0].effective_allowed_profiles == ("detail", "full")


class TestErrors:
    def test_compile_missing_prefix_raises(self, rest_compiler: RestInterfaceCompiler) -> None:
        class IFace(RestInterface[str]):
            prefix = ""
            routes = (RestRoute(use_case=CreateUserUseCase, method="POST", path="/"),)

        with pytest.raises(InterfaceCompilationError, match="prefix"):
            rest_compiler.compile(IFace)

    def test_compile_empty_routes_raises(self, rest_compiler: RestInterfaceCompiler) -> None:
        class IFace(RestInterface[str]):
            prefix = "/users"
            routes = ()

        with pytest.raises(InterfaceCompilationError, match="routes"):
            rest_compiler.compile(IFace)

    def test_compile_uncompiled_use_case_raises(
        self,
        use_case_compiler: UseCaseCompiler,
    ) -> None:
        class IFace(RestInterface[str]):
            prefix = "/users"
            routes = (RestRoute(use_case=UncompiledUseCase, method="POST", path="/"),)

        with pytest.raises(InterfaceCompilationError, match="not been compiled"):
            RestInterfaceCompiler(use_case_compiler).compile(IFace)

    def test_compile_duplicate_method_path_raises(
        self,
        rest_compiler: RestInterfaceCompiler,
    ) -> None:
        class IFace(RestInterface[str]):
            prefix = "/users"
            routes = (
                RestRoute(use_case=CreateUserUseCase, method="POST", path="/"),
                RestRoute(use_case=CreateUserUseCase, method="POST", path="/"),
            )

        with pytest.raises(InterfaceCompilationError, match="duplicate"):
            rest_compiler.compile(IFace)

    def test_compile_expose_profile_without_allowed_profiles_raises(
        self,
        rest_compiler: RestInterfaceCompiler,
    ) -> None:
        class IFace(RestInterface[str]):
            prefix = "/users"
            routes = (
                RestRoute(
                    use_case=GetUserUseCase,
                    method="GET",
                    path="/{id}",
                    expose_profile=True,
                    allowed_profiles=(),
                ),
            )

        with pytest.raises(InterfaceCompilationError, match="expose_profile"):
            rest_compiler.compile(IFace)

    def test_compile_expose_profile_with_allowed_profiles_ok(
        self,
        rest_compiler: RestInterfaceCompiler,
    ) -> None:
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

        result = rest_compiler.compile(IFace)
        assert result[0].effective_expose_profile is True

    def test_compile_expose_profile_from_interface_with_allowed_profiles_ok(
        self,
        rest_compiler: RestInterfaceCompiler,
    ) -> None:
        class IFace(RestInterface[str]):
            prefix = "/users"
            expose_profile = True
            allowed_profiles = ("detail",)
            routes = (
                RestRoute(
                    use_case=GetUserUseCase,
                    method="GET",
                    path="/{id}",
                ),
            )

        result = rest_compiler.compile(IFace)
        assert result[0].effective_expose_profile is True

    def test_compile_expose_profile_from_interface_without_allowed_profiles_raises(
        self,
        rest_compiler: RestInterfaceCompiler,
    ) -> None:
        class IFace(RestInterface[str]):
            prefix = "/users"
            expose_profile = True
            allowed_profiles = ()
            routes = (
                RestRoute(
                    use_case=GetUserUseCase,
                    method="GET",
                    path="/{id}",
                ),
            )

        with pytest.raises(InterfaceCompilationError, match="profile exposure"):
            rest_compiler.compile(IFace)


class TestReadOnly:
    def test_get_route_is_read_only(self, rest_compiler: RestInterfaceCompiler) -> None:
        class IFace(RestInterface[str]):
            prefix = "/users"
            routes = (RestRoute(use_case=GetUserUseCase, method="GET", path="/{id}"),)

        result = rest_compiler.compile(IFace)
        assert result[0].read_only is True

    def test_post_route_is_not_read_only(self, rest_compiler: RestInterfaceCompiler) -> None:
        class IFace(RestInterface[str]):
            prefix = "/users"
            routes = (RestRoute(use_case=CreateUserUseCase, method="POST", path="/"),)

        result = rest_compiler.compile(IFace)
        assert result[0].read_only is False

    def test_mixed_routes_read_only_per_method(self, rest_compiler: RestInterfaceCompiler) -> None:
        class IFace(RestInterface[str]):
            prefix = "/users"
            routes = (
                RestRoute(use_case=GetUserUseCase, method="GET", path="/{id}"),
                RestRoute(use_case=CreateUserUseCase, method="POST", path="/"),
                RestRoute(use_case=ListUsersUseCase, method="GET", path="/"),
            )

        result = rest_compiler.compile(IFace)
        by_method = {cr.route.method: cr.read_only for cr in result}
        assert by_method["GET"] is True
        assert by_method["POST"] is False

    def test_execution_plan_read_only_from_class_flag(
        self, use_case_compiler: UseCaseCompiler
    ) -> None:
        class _ReadOnlyUC(UseCase[Any, str]):
            read_only = True

            async def execute(self) -> str:
                return "ok"

        plan = use_case_compiler.compile(_ReadOnlyUC)
        assert plan.read_only is True

    def test_execution_plan_read_only_defaults_false(
        self, use_case_compiler: UseCaseCompiler
    ) -> None:
        plan = use_case_compiler.compile(GetUserUseCase)
        assert plan.read_only is False
