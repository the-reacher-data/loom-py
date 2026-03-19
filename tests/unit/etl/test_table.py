"""Tests for TableRef, ColumnRef, UnboundColumnRef and col()."""

from __future__ import annotations

from loom.etl._predicate import EqPred, GtPred, InPred
from loom.etl._proxy import ParamExpr
from loom.etl._table import ColumnRef, TableRef, UnboundColumnRef, col


def test_col_returns_unbound_ref() -> None:
    ref = col("year")
    assert isinstance(ref, UnboundColumnRef)
    assert ref.name == "year"


def test_table_ref_stores_ref_string() -> None:
    t = TableRef("raw.orders")
    assert t.ref == "raw.orders"


def test_table_ref_c_returns_column_ref() -> None:
    t = TableRef("raw.orders")
    c = t.c.year
    assert isinstance(c, ColumnRef)
    assert c.table == t
    assert c.name == "year"


def test_table_ref_equality() -> None:
    assert TableRef("raw.orders") == TableRef("raw.orders")
    assert TableRef("raw.orders") != TableRef("raw.customers")


def test_table_ref_hashable() -> None:
    s = {TableRef("raw.orders"), TableRef("raw.orders"), TableRef("raw.customers")}
    assert len(s) == 2


def test_unbound_col_eq_literal_produces_eq_pred() -> None:
    pred = col("year") == 2024
    assert isinstance(pred, EqPred)
    assert pred.right == 2024


def test_bound_col_eq_param_expr_produces_eq_pred() -> None:
    t = TableRef("raw.orders")
    pred = t.c.year == ParamExpr(("run_date", "year"))
    assert isinstance(pred, EqPred)
    assert isinstance(pred.left, ColumnRef)


def test_unbound_col_gt() -> None:
    assert isinstance(col("amount") > 0, GtPred)


def test_unbound_col_isin() -> None:
    pred = col("country").isin(("ES", "FR"))
    assert isinstance(pred, InPred)


def test_column_namespace_private_attr_raises() -> None:
    t = TableRef("raw.orders")
    try:
        _ = t.c._private
        raised = False
    except AttributeError:
        raised = True
    assert raised


def test_unbound_col_hash_by_name() -> None:
    assert hash(col("year")) == hash(col("year"))
    assert hash(col("year")) != hash(col("month"))


def test_bound_col_hash() -> None:
    t = TableRef("raw.orders")
    c1 = t.c.year
    c2 = t.c.year
    assert hash(c1) == hash(c2)


def test_col_repr() -> None:
    assert repr(col("year")) == "col('year')"


def test_table_ref_repr() -> None:
    assert repr(TableRef("raw.orders")) == "TableRef('raw.orders')"
