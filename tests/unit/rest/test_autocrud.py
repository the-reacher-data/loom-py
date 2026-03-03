"""Unit tests for loom.rest.autocrud."""

from typing import Any

import msgspec
import pytest

from loom.core.engine.compiler import UseCaseCompiler
from loom.core.use_case.use_case import UseCase
from loom.rest.autocrud import _get_or_create, _id_coerce, build_auto_routes
from loom.rest.model import RestRoute

# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------


class _IntItem(msgspec.Struct):
    id: int
    name: str


class _StrItem(msgspec.Struct):
    id: str
    name: str


class _NoIdItem(msgspec.Struct):
    name: str


# ---------------------------------------------------------------------------
# TestBuildAutoRoutes
# ---------------------------------------------------------------------------


class TestBuildAutoRoutes:
    def test_returns_five_routes_when_include_empty(self) -> None:
        routes = build_auto_routes(_IntItem, include=())
        assert len(routes) == 5

    def test_include_filters_ops(self) -> None:
        routes = build_auto_routes(_IntItem, include=("create", "get"))
        assert len(routes) == 2
        methods = {r.method for r in routes}
        assert methods == {"POST", "GET"}

    def test_create_has_status_201(self) -> None:
        routes = build_auto_routes(_IntItem, include=("create",))
        assert routes[0].status_code == 201

    def test_non_create_have_status_200(self) -> None:
        routes = build_auto_routes(_IntItem, include=("get", "list", "update", "delete"))
        for route in routes:
            assert route.status_code == 200

    def test_get_path_is_slash_id(self) -> None:
        routes = build_auto_routes(_IntItem, include=("get",))
        assert routes[0].path == "/{id}"

    def test_list_path_is_slash(self) -> None:
        routes = build_auto_routes(_IntItem, include=("list",))
        assert routes[0].path == "/"

    def test_create_path_is_slash(self) -> None:
        routes = build_auto_routes(_IntItem, include=("create",))
        assert routes[0].path == "/"

    def test_update_path_is_slash_id(self) -> None:
        routes = build_auto_routes(_IntItem, include=("update",))
        assert routes[0].path == "/{id}"

    def test_delete_path_is_slash_id(self) -> None:
        routes = build_auto_routes(_IntItem, include=("delete",))
        assert routes[0].path == "/{id}"

    def test_all_routes_are_rest_route_instances(self) -> None:
        routes = build_auto_routes(_IntItem, include=())
        assert all(isinstance(r, RestRoute) for r in routes)


# ---------------------------------------------------------------------------
# TestAutoCrudCache
# ---------------------------------------------------------------------------


class TestAutoCrudCache:
    def test_same_uc_class_returned_on_repeated_call(self) -> None:
        class _Item(msgspec.Struct):
            id: int

        first = _get_or_create(_Item)
        second = _get_or_create(_Item)
        assert first is second

    def test_different_models_get_different_classes(self) -> None:
        class _A(msgspec.Struct):
            id: int

        class _B(msgspec.Struct):
            id: int

        a_ucs = _get_or_create(_A)
        b_ucs = _get_or_create(_B)
        assert a_ucs["create"] is not b_ucs["create"]


# ---------------------------------------------------------------------------
# TestAutoCrudUseCaseCompilation
# ---------------------------------------------------------------------------


class TestAutoCrudUseCaseCompilation:
    def test_create_uc_compiles_via_use_case_compiler(self) -> None:
        compiler = UseCaseCompiler()
        ucs = _get_or_create(_IntItem)
        plan = compiler.compile(ucs["create"])
        assert plan is not None
        assert plan.input_binding is not None
        assert plan.input_binding.name == "cmd"

    def test_get_uc_compiles_via_use_case_compiler(self) -> None:
        compiler = UseCaseCompiler()
        ucs = _get_or_create(_IntItem)
        plan = compiler.compile(ucs["get"])
        assert plan is not None
        assert any(pb.name == "id" for pb in plan.param_bindings)

    def test_list_uc_compiles_via_use_case_compiler(self) -> None:
        compiler = UseCaseCompiler()
        ucs = _get_or_create(_IntItem)
        plan = compiler.compile(ucs["list"])
        assert plan is not None
        # query is a ParamBinding (not Input), profile is also a ParamBinding
        param_names = {pb.name for pb in plan.param_bindings}
        assert "query" in param_names

    def test_update_uc_compiles_via_use_case_compiler(self) -> None:
        compiler = UseCaseCompiler()
        ucs = _get_or_create(_IntItem)
        plan = compiler.compile(ucs["update"])
        assert plan is not None
        assert plan.input_binding is not None
        assert any(pb.name == "id" for pb in plan.param_bindings)

    def test_delete_uc_compiles_via_use_case_compiler(self) -> None:
        compiler = UseCaseCompiler()
        ucs = _get_or_create(_IntItem)
        plan = compiler.compile(ucs["delete"])
        assert plan is not None
        assert any(pb.name == "id" for pb in plan.param_bindings)

    def test_auto_uc_orig_bases_has_concrete_model(self) -> None:
        """Factory embeds model in __orig_bases__ so UseCaseFactory can infer repo."""
        import typing

        ucs = _get_or_create(_IntItem)
        create_uc = ucs["create"]
        for base in getattr(create_uc, "__orig_bases__", ()):
            if typing.get_origin(base) is UseCase:
                args = typing.get_args(base)
                assert len(args) == 2
                assert args[0] is _IntItem
                return
        pytest.fail("No UseCase[_IntItem, ...] found in __orig_bases__")

    def test_generated_use_cases_have_typed_return_annotations(self) -> None:
        ucs = _get_or_create(_IntItem)
        for use_case in ucs.values():
            return_hint = use_case.execute.__annotations__.get("return")
            assert return_hint is not None
            assert return_hint is not Any


# ---------------------------------------------------------------------------
# TestIdCoerce
# ---------------------------------------------------------------------------


class TestIdCoerce:
    def test_int_model_coerces_to_int(self) -> None:
        coerce = _id_coerce(_IntItem)
        assert coerce("42") == 42
        assert isinstance(coerce("1"), int)

    def test_str_model_returns_str(self) -> None:
        coerce = _id_coerce(_StrItem)
        result = coerce("abc")
        assert result == "abc"
        assert isinstance(result, str)

    def test_model_without_id_field_returns_str(self) -> None:
        coerce = _id_coerce(_NoIdItem)
        assert coerce("hello") == "hello"
