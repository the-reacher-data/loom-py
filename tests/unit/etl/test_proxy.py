"""Tests for the params proxy and ParamExpr."""

from __future__ import annotations

from datetime import date

import pytest

from loom.etl import ETLParams
from loom.etl._predicate import EqPred, InPred
from loom.etl._proxy import ParamExpr, _ParamProxy, params, resolve_param_expr


class RunParams(ETLParams):
    run_date: date
    countries: tuple[str, ...]


def test_params_is_singleton_proxy() -> None:
    assert isinstance(params, _ParamProxy)


def test_params_attr_returns_param_expr() -> None:
    expr = params.run_date
    assert isinstance(expr, ParamExpr)
    assert expr.path == ("run_date",)


def test_chained_attr_extends_path() -> None:
    expr = params.run_date.year
    assert isinstance(expr, ParamExpr)
    assert expr.path == ("run_date", "year")


def test_deep_chain() -> None:
    expr = params.a.b.c.d
    assert expr.path == ("a", "b", "c", "d")


def test_param_expr_eq_produces_eq_pred() -> None:
    expr = params.run_date.year
    pred = expr == 2024
    assert isinstance(pred, EqPred)
    assert pred.left is expr
    assert pred.right == 2024


def test_param_expr_isin_produces_in_pred() -> None:
    pred = params.countries.isin(("ES", "FR"))
    assert isinstance(pred, InPred)


def test_param_expr_repr() -> None:
    assert repr(params.run_date.year) == "params.run_date.year"


def test_resolve_simple_field() -> None:
    p = RunParams(run_date=date(2024, 1, 5), countries=("ES",))
    assert resolve_param_expr(ParamExpr(("run_date",)), p) == date(2024, 1, 5)


def test_resolve_nested_attr() -> None:
    p = RunParams(run_date=date(2024, 1, 5), countries=("ES",))
    assert resolve_param_expr(ParamExpr(("run_date", "year")), p) == 2024
    assert resolve_param_expr(ParamExpr(("run_date", "month")), p) == 1
    assert resolve_param_expr(ParamExpr(("run_date", "day")), p) == 5


def test_resolve_tuple_field() -> None:
    p = RunParams(run_date=date(2024, 1, 5), countries=("ES", "FR"))
    assert resolve_param_expr(ParamExpr(("countries",)), p) == ("ES", "FR")


def test_resolve_missing_field_raises() -> None:
    p = RunParams(run_date=date(2024, 1, 5), countries=("ES",))
    with pytest.raises(AttributeError):
        resolve_param_expr(ParamExpr(("nonexistent",)), p)


def test_private_attr_raises_attribute_error() -> None:
    with pytest.raises(AttributeError):
        _ = params._private


def test_param_expr_hash_stable() -> None:
    e1 = ParamExpr(("run_date", "year"))
    e2 = ParamExpr(("run_date", "year"))
    assert hash(e1) == hash(e2)
