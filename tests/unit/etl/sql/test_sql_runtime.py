"""Tests for SQL runtime helpers: resolve_sql, predicate_to_sql, and StepSQL."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

import polars as pl
import pytest

from loom.etl import col, params
from loom.etl.sql._predicate_sql import predicate_to_sql
from loom.etl.sql._sql import resolve_sql
from loom.etl.sql._step_sql import StepSQL, _extract_stepsql_types


@dataclass(frozen=True)
class _SqlParams:
    run_date: date
    countries: tuple[str, ...]
    active: bool


def _params() -> _SqlParams:
    return _SqlParams(run_date=date(2024, 1, 15), countries=("ES", "FR"), active=True)


def test_resolve_sql_interpolates_nested_params_as_safe_literals() -> None:
    query = (
        "SELECT * FROM orders "
        "WHERE year = {{ params.run_date.year }} "
        "AND country IN ({{ params.countries.0 }})"
    )

    # list index access is intentionally disallowed (non-identifier);
    # only attribute chains are valid by design.
    with pytest.raises(ValueError, match="invalid expression"):
        resolve_sql(query, _params())

    valid = (
        "SELECT * FROM orders "
        "WHERE year = {{ params.run_date.year }} "
        "AND active = {{ params.active }}"
    )
    assert (
        resolve_sql(valid, _params()) == "SELECT * FROM orders WHERE year = 2024 AND active = TRUE"
    )


@pytest.mark.parametrize(
    "sql,exc,match",
    [
        ("SELECT {{ config.env }}", ValueError, "only 'params.x.y'"),
        ("SELECT {{ params.run-date }}", ValueError, "invalid expression"),
        ("SELECT {{ params.missing }}", AttributeError, "missing"),
    ],
)
def test_resolve_sql_errors(sql: str, exc: type[Exception], match: str) -> None:
    with pytest.raises(exc, match=match):
        resolve_sql(sql, _params())


def test_predicate_to_sql_supports_composition_and_param_resolution() -> None:
    node = ((col("year") == params.run_date.year) & (col("active") == params.active)) | (
        col("country").isin(params.countries)
    )
    sql = predicate_to_sql(node, _params())
    assert sql == "((year = 2024) AND (active = TRUE)) OR (country IN ('ES', 'FR'))"


@pytest.mark.parametrize(
    "node,expected",
    [
        (col("amount") > 10, "amount > 10"),
        (col("amount") >= 10, "amount >= 10"),
        (col("amount") < 10, "amount < 10"),
        (col("amount") <= 10, "amount <= 10"),
        (col("status") != "deleted", "status != deleted"),
        (~(col("active") == True), "NOT (active = TRUE)"),  # noqa: E712
    ],
)
def test_predicate_to_sql_binary_and_unary_forms(node: Any, expected: str) -> None:
    assert predicate_to_sql(node, _params()) == expected


def test_predicate_to_sql_raises_on_unsupported_node() -> None:
    with pytest.raises(TypeError, match="unsupported node type"):
        predicate_to_sql(object(), _params())  # type: ignore[arg-type]


class _StaticStep(StepSQL[_SqlParams, pl.LazyFrame]):
    sql = "SELECT id, amount FROM orders"


class _DynamicStep(StepSQL[_SqlParams, pl.LazyFrame]):
    @staticmethod
    def sql(p: _SqlParams) -> str:
        return f"SELECT id, {p.run_date.year} AS run_year FROM orders"


def test_stepsql_generates_execute_for_polars_backend() -> None:
    orders = pl.DataFrame({"id": [1, 2], "amount": [10.0, 20.0]}).lazy()
    out_static = _StaticStep().execute(_params(), orders=orders).collect()
    out_dynamic = _DynamicStep().execute(_params(), orders=orders).collect()

    assert out_static.columns == ["id", "amount"]
    assert out_dynamic.columns == ["id", "run_year"]
    assert out_dynamic["run_year"].to_list() == [2024, 2024]


class _FakeSparkFrame:
    __module__ = "pyspark.sql.dataframe"

    def __init__(self) -> None:
        self.sparkSession = object()


class _RouteStep(StepSQL[_SqlParams, object]):
    sql = "SELECT 1 AS value"


def test_stepsql_routes_to_spark_when_first_frame_is_spark_like(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sentinel = object()

    monkeypatch.setattr("loom.etl.sql._step_sql._run_spark_sql", lambda frames, query: sentinel)
    monkeypatch.setattr("loom.etl.sql._step_sql._run_polars_sql", lambda frames, query: None)

    result = _RouteStep().execute(_params(), frame=_FakeSparkFrame())
    assert result is sentinel


def test_stepsql_routes_to_polars_when_frame_is_not_spark(monkeypatch: pytest.MonkeyPatch) -> None:
    sentinel = object()

    monkeypatch.setattr("loom.etl.sql._step_sql._run_spark_sql", lambda frames, query: None)
    monkeypatch.setattr("loom.etl.sql._step_sql._run_polars_sql", lambda frames, query: sentinel)

    result = _RouteStep().execute(_params(), frame=object())
    assert result is sentinel


def test_stepsql_extract_types_on_non_stepsql_class_returns_none() -> None:
    class _Plain:
        pass

    assert _extract_stepsql_types(_Plain) == (None, None)


def test_base_stepsql_execute_raises_not_implemented() -> None:
    with pytest.raises(NotImplementedError, match="declare a 'sql' ClassVar"):
        StepSQL().execute(_params())  # type: ignore[misc]
