"""Tests for TableRef, ColumnRef, UnboundColumnRef and col()."""

from __future__ import annotations

import pytest

from loom.etl.declarative.expr._params import ParamExpr
from loom.etl.declarative.expr._predicate import EqPred, GtPred, InPred
from loom.etl.declarative.expr._refs import ColumnRef, TableRef, UnboundColumnRef, col


class TestTableRefAndColumns:
    def test_table_ref_and_col_construction(self) -> None:
        table = TableRef("raw.orders")
        unbound = col("year")
        bound = table.c.year

        assert table.ref == "raw.orders"
        assert isinstance(unbound, UnboundColumnRef)
        assert unbound.name == "year"
        assert isinstance(bound, ColumnRef)
        assert bound.table == table
        assert bound.name == "year"

    def test_table_ref_equality_hash_and_repr(self) -> None:
        orders = TableRef("raw.orders")
        same_orders = TableRef("raw.orders")
        assert orders == same_orders
        assert orders != TableRef("raw.customers")
        assert len({TableRef("raw.orders"), TableRef("raw.orders"), TableRef("raw.customers")}) == 2
        assert repr(TableRef("raw.orders")) == "TableRef('raw.orders')"

    def test_column_namespace_private_attr_raises(self) -> None:
        with pytest.raises(AttributeError):
            _ = TableRef("raw.orders").c._private


class TestPredicatesAndHashes:
    @pytest.mark.parametrize(
        "expr,expected_type",
        [
            (col("year") == 2024, EqPred),
            (TableRef("raw.orders").c.year == ParamExpr(("run_date", "year")), EqPred),
            (col("amount") > 0, GtPred),
            (col("country").isin(("ES", "FR")), InPred),
        ],
    )
    def test_predicate_builders(self, expr: object, expected_type: type[object]) -> None:
        assert isinstance(expr, expected_type)

    def test_eq_predicate_payloads(self) -> None:
        literal_pred = col("year") == 2024
        bound_pred = TableRef("raw.orders").c.year == ParamExpr(("run_date", "year"))

        assert isinstance(literal_pred, EqPred)
        assert literal_pred.right == 2024
        assert isinstance(bound_pred, EqPred)
        assert isinstance(bound_pred.left, ColumnRef)

    def test_hash_and_repr_contract(self) -> None:
        """Equality cannot be asserted directly: ``__eq__`` builds a DSL predicate."""
        table = TableRef("raw.orders")
        year, same_year = col("year"), col("year")
        bound_year, same_bound_year = table.c.year, table.c.year
        assert hash(year) == hash(same_year)
        assert hash(year) != hash(col("month"))
        assert hash(bound_year) == hash(same_bound_year)
        assert repr(col("year")) == "col('year')"
