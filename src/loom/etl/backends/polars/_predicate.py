"""Predicate AST → Polars expression converter.

Translates a :data:`~loom.etl._predicate.PredicateNode` tree into a
``polars.Expr`` so it can be passed to ``LazyFrame.filter()``.

Polars' lazy optimizer then pushes the filter into the PyArrow dataset
scanner, which performs:

* **Partition pruning** — entire partition directories whose column values
  do not match the predicate are skipped before any I/O.
* **Row-group pruning** — Parquet row groups whose column statistics
  (min/max) exclude the predicate are skipped within each file.
* **Row-level filtering** — remaining rows are filtered in-memory.

The combined effect means a ``WHERE year = 2024`` predicate on a table
partitioned by ``year`` scans only the relevant partition files — no full
table scan in memory.

See https://delta-io.github.io/delta-rs/api/delta_table/#deltalake.DeltaTable.to_pyarrow_dataset
See https://docs.pola.rs/api/python/stable/reference/lazyframe/api/polars.LazyFrame.filter.html
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import polars as pl

from loom.etl.model._proxy import ParamExpr, resolve_param_expr
from loom.etl.schema._table import UnboundColumnRef
from loom.etl.sql._predicate import (
    AndPred,
    EqPred,
    GePred,
    GtPred,
    InPred,
    LePred,
    LtPred,
    NePred,
    NotPred,
    OrPred,
    PredicateNode,
)


def predicate_to_polars(node: PredicateNode, params_instance: Any) -> pl.Expr:
    """Convert a :data:`~loom.etl._predicate.PredicateNode` to a ``polars.Expr``.

    :class:`~loom.etl._proxy.ParamExpr` leaf nodes are resolved against
    *params_instance* at call time.

    Args:
        node:            Root predicate node from the column / param DSL.
        params_instance: Concrete params used to resolve
                         :class:`~loom.etl._proxy.ParamExpr` leaves.

    Returns:
        Polars expression suitable for :meth:`polars.LazyFrame.filter`.

    Raises:
        TypeError: When an unsupported node type is encountered.

    Example::

        from loom.etl import col, params
        from loom.etl.backends.polars._predicate import predicate_to_polars

        node = (col("year") == params.run_date.year) & (col("month") == params.run_date.month)
        expr = predicate_to_polars(node, DailyParams(run_date=date(2024, 1, 15)))
        # → (pl.col("year") == 2024) & (pl.col("month") == 1)
    """
    return _to_expr(node, params_instance)


_BINARY: dict[type, Callable[[pl.Expr, pl.Expr], pl.Expr]] = {
    EqPred: lambda lhs, r: lhs == r,
    NePred: lambda lhs, r: lhs != r,
    GtPred: lambda lhs, r: lhs > r,
    GePred: lambda lhs, r: lhs >= r,
    LtPred: lambda lhs, r: lhs < r,
    LePred: lambda lhs, r: lhs <= r,
}


def _to_expr(node: Any, params: Any) -> pl.Expr:
    node_type = type(node)

    binary_op = _BINARY.get(node_type)
    if binary_op is not None:
        return binary_op(_as_expr(node.left, params), _as_expr(node.right, params))

    if node_type is AndPred:
        return _to_expr(node.left, params) & _to_expr(node.right, params)

    if node_type is OrPred:
        return _to_expr(node.left, params) | _to_expr(node.right, params)

    if node_type is NotPred:
        return ~_to_expr(node.operand, params)

    if node_type is InPred:
        col_expr = _as_expr(node.ref, params)
        raw = (
            resolve_param_expr(node.values, params)
            if isinstance(node.values, ParamExpr)
            else node.values
        )
        return col_expr.is_in(list(raw))

    raise TypeError(f"predicate_to_polars: unsupported node type {node_type!r}")


def _as_expr(value: Any, params: Any) -> pl.Expr:
    """Convert a predicate leaf to a Polars expression."""
    if isinstance(value, UnboundColumnRef):
        return pl.col(value.name)
    if isinstance(value, ParamExpr):
        return pl.lit(resolve_param_expr(value, params))
    return pl.lit(value)
