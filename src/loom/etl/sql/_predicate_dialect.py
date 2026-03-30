"""Shared predicate fold with pluggable render dialects.

This module centralizes traversal of :mod:`loom.etl.sql._predicate` nodes.
Backends provide a small ``PredicateDialect`` implementation that defines
how each logical/comparison operation is rendered.
"""

from __future__ import annotations

from functools import singledispatch
from typing import Any, Protocol, TypeVar

from loom.etl.pipeline._proxy import ParamExpr, resolve_param_expr
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

T = TypeVar("T")


class PredicateDialect(Protocol[T]):
    """Backend renderer for predicate fold output type ``T``."""

    def column(self, name: str) -> T:
        """Return representation for a column reference."""
        ...

    def literal(self, value: Any) -> T:
        """Return representation for a literal value."""
        ...

    def eq(self, left: T, right: T) -> T:
        """Return representation for ``left == right``."""
        ...

    def ne(self, left: T, right: T) -> T:
        """Return representation for ``left != right``."""
        ...

    def gt(self, left: T, right: T) -> T:
        """Return representation for ``left > right``."""
        ...

    def ge(self, left: T, right: T) -> T:
        """Return representation for ``left >= right``."""
        ...

    def lt(self, left: T, right: T) -> T:
        """Return representation for ``left < right``."""
        ...

    def le(self, left: T, right: T) -> T:
        """Return representation for ``left <= right``."""
        ...

    def in_(self, ref: T, values: tuple[Any, ...]) -> T:
        """Return representation for ``ref IN values``."""
        ...

    def and_(self, left: T, right: T) -> T:
        """Return representation for ``left AND right``."""
        ...

    def or_(self, left: T, right: T) -> T:
        """Return representation for ``left OR right``."""
        ...

    def not_(self, operand: T) -> T:
        """Return representation for ``NOT operand``."""
        ...


def fold_predicate(
    node: PredicateNode,
    params_instance: Any,
    dialect: PredicateDialect[T],
) -> T:
    """Fold a predicate AST using the supplied *dialect*."""
    return _fold(node, params_instance, dialect)


@singledispatch
def _fold(node: object, params_instance: Any, dialect: PredicateDialect[T]) -> T:
    raise TypeError(f"fold_predicate: unsupported node type {type(node)!r}")


def _term(value: Any, params_instance: Any, dialect: PredicateDialect[T]) -> T:
    if isinstance(value, UnboundColumnRef):
        return dialect.column(value.name)
    if isinstance(value, ParamExpr):
        return dialect.literal(resolve_param_expr(value, params_instance))
    return dialect.literal(value)


def _binary(
    op: str,
    left: Any,
    right: Any,
    params_instance: Any,
    dialect: PredicateDialect[T],
) -> T:
    lhs = _term(left, params_instance, dialect)
    rhs = _term(right, params_instance, dialect)
    match op:
        case "eq":
            return dialect.eq(lhs, rhs)
        case "ne":
            return dialect.ne(lhs, rhs)
        case "gt":
            return dialect.gt(lhs, rhs)
        case "ge":
            return dialect.ge(lhs, rhs)
        case "lt":
            return dialect.lt(lhs, rhs)
        case "le":
            return dialect.le(lhs, rhs)
        case _:
            raise TypeError(f"fold_predicate: unsupported binary operator {op!r}")


@_fold.register(EqPred)
def _(node: EqPred, params_instance: Any, dialect: PredicateDialect[T]) -> T:
    return _binary("eq", node.left, node.right, params_instance, dialect)


@_fold.register(NePred)
def _(node: NePred, params_instance: Any, dialect: PredicateDialect[T]) -> T:
    return _binary("ne", node.left, node.right, params_instance, dialect)


@_fold.register(GtPred)
def _(node: GtPred, params_instance: Any, dialect: PredicateDialect[T]) -> T:
    return _binary("gt", node.left, node.right, params_instance, dialect)


@_fold.register(GePred)
def _(node: GePred, params_instance: Any, dialect: PredicateDialect[T]) -> T:
    return _binary("ge", node.left, node.right, params_instance, dialect)


@_fold.register(LtPred)
def _(node: LtPred, params_instance: Any, dialect: PredicateDialect[T]) -> T:
    return _binary("lt", node.left, node.right, params_instance, dialect)


@_fold.register(LePred)
def _(node: LePred, params_instance: Any, dialect: PredicateDialect[T]) -> T:
    return _binary("le", node.left, node.right, params_instance, dialect)


@_fold.register(InPred)
def _(node: InPred, params_instance: Any, dialect: PredicateDialect[T]) -> T:
    raw_values = (
        resolve_param_expr(node.values, params_instance)
        if isinstance(node.values, ParamExpr)
        else node.values
    )
    values = tuple(raw_values)
    ref = _term(node.ref, params_instance, dialect)
    return dialect.in_(ref, values)


@_fold.register(AndPred)
def _(node: AndPred, params_instance: Any, dialect: PredicateDialect[T]) -> T:
    return dialect.and_(
        _fold(node.left, params_instance, dialect),
        _fold(node.right, params_instance, dialect),
    )


@_fold.register(OrPred)
def _(node: OrPred, params_instance: Any, dialect: PredicateDialect[T]) -> T:
    return dialect.or_(
        _fold(node.left, params_instance, dialect),
        _fold(node.right, params_instance, dialect),
    )


@_fold.register(NotPred)
def _(node: NotPred, params_instance: Any, dialect: PredicateDialect[T]) -> T:
    return dialect.not_(_fold(node.operand, params_instance, dialect))
