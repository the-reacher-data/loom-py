"""Tests for TableRef, ColumnRef, UnboundColumnRef and col()."""

from __future__ import annotations

import pytest

from loom.etl.pipeline._proxy import ParamExpr
from loom.etl.schema._table import ColumnRef, TableRef, UnboundColumnRef, col
from loom.etl.sql._predicate import EqPred, GtPred, InPred


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
        assert TableRef("raw.orders") == TableRef("raw.orders")
        assert TableRef("raw.orders") != TableRef("raw.customers")
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
        table = TableRef("raw.orders")
        assert hash(col("year")) == hash(col("year"))
        assert hash(col("year")) != hash(col("month"))
        assert hash(table.c.year) == hash(table.c.year)
        assert repr(col("year")) == "col('year')"
