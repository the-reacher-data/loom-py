"""Generic in-memory expression evaluator."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from loom.core.expr.nodes import (
    AndExpr,
    BetweenExpr,
    EqExpr,
    GeExpr,
    GtExpr,
    InExpr,
    LeExpr,
    LtExpr,
    NeExpr,
    NotExpr,
    OrExpr,
    PathSegment,
)
from loom.core.expr.refs import PathRef

Evaluator = Callable[[Any, Mapping[str, Any]], Any]


def resolve_path(root: Any, path: tuple[PathSegment, ...]) -> Any:
    """Resolve a path against an object or mapping root.

    Args:
        root: Root object or mapping.
        path: Attribute/item path segments.

    Returns:
        Resolved value.
    """

    current = root
    for segment in path:
        if isinstance(current, Mapping):
            current = current[segment]
        else:
            current = current[segment] if isinstance(segment, int) else getattr(current, segment)
    return current


def evaluate_expr(expr: Any, roots: Mapping[str, Any]) -> Any:
    """Evaluate an expression tree using named runtime roots.

    Args:
        expr: Expression node, :class:`PathRef`, or literal value.
        roots: Runtime roots keyed by root identifier.

    Returns:
        Evaluated value.
    """

    evaluator = _EVALUATORS.get(type(expr))
    return expr if evaluator is None else evaluator(expr, roots)


def _eval_path(expr: PathRef, roots: Mapping[str, Any]) -> Any:
    return resolve_path(roots[expr.root], expr.path)


def _eval_eq(expr: EqExpr, roots: Mapping[str, Any]) -> bool:
    return bool(evaluate_expr(expr.left, roots) == evaluate_expr(expr.right, roots))


def _eval_ne(expr: NeExpr, roots: Mapping[str, Any]) -> bool:
    return bool(evaluate_expr(expr.left, roots) != evaluate_expr(expr.right, roots))


def _eval_gt(expr: GtExpr, roots: Mapping[str, Any]) -> bool:
    return bool(evaluate_expr(expr.left, roots) > evaluate_expr(expr.right, roots))


def _eval_ge(expr: GeExpr, roots: Mapping[str, Any]) -> bool:
    return bool(evaluate_expr(expr.left, roots) >= evaluate_expr(expr.right, roots))


def _eval_lt(expr: LtExpr, roots: Mapping[str, Any]) -> bool:
    return bool(evaluate_expr(expr.left, roots) < evaluate_expr(expr.right, roots))


def _eval_le(expr: LeExpr, roots: Mapping[str, Any]) -> bool:
    return bool(evaluate_expr(expr.left, roots) <= evaluate_expr(expr.right, roots))


def _eval_in(expr: InExpr, roots: Mapping[str, Any]) -> bool:
    return bool(evaluate_expr(expr.ref, roots) in evaluate_expr(expr.values, roots))


def _eval_between(expr: BetweenExpr, roots: Mapping[str, Any]) -> bool:
    value = evaluate_expr(expr.ref, roots)
    return bool(evaluate_expr(expr.low, roots) <= value <= evaluate_expr(expr.high, roots))


def _eval_and(expr: AndExpr, roots: Mapping[str, Any]) -> bool:
    return bool(evaluate_expr(expr.left, roots)) and bool(evaluate_expr(expr.right, roots))


def _eval_or(expr: OrExpr, roots: Mapping[str, Any]) -> bool:
    return bool(evaluate_expr(expr.left, roots)) or bool(evaluate_expr(expr.right, roots))


def _eval_not(expr: NotExpr, roots: Mapping[str, Any]) -> bool:
    return not bool(evaluate_expr(expr.operand, roots))


_EVALUATORS: dict[type[Any], Evaluator] = {
    PathRef: _eval_path,
    EqExpr: _eval_eq,
    NeExpr: _eval_ne,
    GtExpr: _eval_gt,
    GeExpr: _eval_ge,
    LtExpr: _eval_lt,
    LeExpr: _eval_le,
    InExpr: _eval_in,
    BetweenExpr: _eval_between,
    AndExpr: _eval_and,
    OrExpr: _eval_or,
    NotExpr: _eval_not,
}


__all__ = ["evaluate_expr", "resolve_path"]
