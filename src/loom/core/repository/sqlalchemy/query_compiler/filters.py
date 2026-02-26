"""FilterSpec → SQLAlchemy WHERE clause compiler."""

from __future__ import annotations

from typing import Any

from sqlalchemy import and_, or_

from loom.core.repository.abc.query import FilterGroup, FilterOp, FilterSpec
from loom.core.repository.sqlalchemy.query_compiler.errors import UnsafeFilterError
from loom.core.repository.sqlalchemy.query_compiler.paths import resolve_column
from loom.core.repository.sqlalchemy.query_compiler.subquery import (
    compile_exists,
    compile_not_exists,
)

_RELATION_OPS = frozenset([FilterOp.EXISTS, FilterOp.NOT_EXISTS])


def compile_filter_group(
    sa_model: type[Any],
    group: FilterGroup,
    allowed_fields: frozenset[str],
) -> Any:
    """Compile a :class:`FilterGroup` into a SQLAlchemy boolean clause.

    Args:
        sa_model: Root SQLAlchemy mapped model class.
        group: Filter group to compile.
        allowed_fields: Permitted field names.  Empty frozenset allows all.

    Returns:
        A SQLAlchemy clause element (``and_`` or ``or_``).

    Raises:
        UnsafeFilterError: If a field is not in ``allowed_fields``.
        FilterPathError: If a field path cannot be resolved.
    """
    clauses = [
        _compile_spec(sa_model, spec, allowed_fields) for spec in group.filters
    ]
    if group.op == "OR":
        return or_(*clauses)
    return and_(*clauses)


def _compile_spec(
    sa_model: type[Any],
    spec: FilterSpec,
    allowed_fields: frozenset[str],
) -> Any:
    root_field = spec.field.split(".")[0]
    if allowed_fields and root_field not in allowed_fields:
        raise UnsafeFilterError(root_field)

    if spec.op == FilterOp.EXISTS:
        return compile_exists(sa_model, spec.field)
    if spec.op == FilterOp.NOT_EXISTS:
        return compile_not_exists(sa_model, spec.field)

    col = resolve_column(sa_model, spec.field)
    return _apply_op(col, spec.op, spec.value)


def _apply_op(col: Any, op: FilterOp, value: Any) -> Any:
    if op == FilterOp.EQ:
        return col == value
    if op == FilterOp.NE:
        return col != value
    if op == FilterOp.GT:
        return col > value
    if op == FilterOp.GTE:
        return col >= value
    if op == FilterOp.LT:
        return col < value
    if op == FilterOp.LTE:
        return col <= value
    if op == FilterOp.IN:
        return col.in_(value)
    if op == FilterOp.LIKE:
        return col.like(value)
    if op == FilterOp.ILIKE:
        return col.ilike(value)
    if op == FilterOp.IS_NULL:
        return col.is_(None)
    raise ValueError(f"Unsupported FilterOp: {op!r}")
