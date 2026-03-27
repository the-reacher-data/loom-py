"""Predicate → SQL string serialiser.

Converts a :data:`~loom.etl._predicate.PredicateNode` tree into an ANSI SQL
predicate string suitable for Delta Lake ``replaceWhere``.

:class:`~loom.etl._proxy.ParamExpr` leaf nodes are resolved against a
concrete params instance at serialisation time.  Column name nodes are
emitted as-is.

Internal module — not part of the public API.
"""

from __future__ import annotations

from typing import Any

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


def predicate_to_sql(node: PredicateNode, params_instance: Any) -> str:
    """Serialise *node* to an ANSI SQL predicate string.

    Args:
        node:            Root predicate node produced by the column / param DSL.
        params_instance: Concrete params used to resolve
                         :class:`~loom.etl._proxy.ParamExpr` leaf values.

    Returns:
        SQL predicate string, e.g. ``"year = 2024 AND month = 1"``.

    Raises:
        TypeError: If an unrecognised node type is encountered.

    Example::

        from loom.etl import col, params
        from loom.etl.sql._predicate_sql import predicate_to_sql

        node = (col("year") == params.run_date.year) & (col("month") == params.run_date.month)
        sql  = predicate_to_sql(node, DailyParams(run_date=date(2024, 1, 15)))
        # → "(year = 2024) AND (month = 1)"
    """
    return _to_sql(node, params_instance)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_BINARY_OPS: dict[type, str] = {
    EqPred: "=",
    NePred: "!=",
    GtPred: ">",
    GePred: ">=",
    LtPred: "<",
    LePred: "<=",
}


def _to_sql(node: Any, params_instance: Any) -> str:
    node_type = type(node)

    if node_type in _BINARY_OPS:
        op = _BINARY_OPS[node_type]
        return f"{_side(node.left, params_instance)} {op} {_side(node.right, params_instance)}"

    if node_type is AndPred:
        return (
            f"({_to_sql(node.left, params_instance)}) AND ({_to_sql(node.right, params_instance)})"
        )

    if node_type is OrPred:
        return (
            f"({_to_sql(node.left, params_instance)}) OR ({_to_sql(node.right, params_instance)})"
        )

    if node_type is NotPred:
        return f"NOT ({_to_sql(node.operand, params_instance)})"

    if node_type is InPred:
        col_sql = _side(node.ref, params_instance)
        values = _resolve(node.values, params_instance)
        if not isinstance(values, (list, tuple)):
            values = list(values)
        formatted = ", ".join(_literal(v) for v in values)
        return f"{col_sql} IN ({formatted})"

    raise TypeError(f"predicate_to_sql: unsupported node type {node_type!r}")


def _side(value: Any, params_instance: Any) -> str:
    resolved = _resolve(value, params_instance)
    if isinstance(resolved, str) and not resolved.startswith("'"):
        # Could be a column name from UnboundColumnRef resolution
        return resolved
    return _literal(resolved)


def _resolve(value: Any, params_instance: Any) -> Any:
    if isinstance(value, ParamExpr):
        return resolve_param_expr(value, params_instance)
    if isinstance(value, UnboundColumnRef):
        return value.name
    return value


def _literal(value: Any) -> str:
    if isinstance(value, str):
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    return str(value)
