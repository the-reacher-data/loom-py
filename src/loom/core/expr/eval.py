"""Generic in-memory expression evaluator."""

from __future__ import annotations

from collections.abc import Mapping
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

    if isinstance(expr, PathRef):
        return resolve_path(roots[expr.root], expr.path)
    if isinstance(expr, EqExpr):
        return evaluate_expr(expr.left, roots) == evaluate_expr(expr.right, roots)
    if isinstance(expr, NeExpr):
        return evaluate_expr(expr.left, roots) != evaluate_expr(expr.right, roots)
    if isinstance(expr, GtExpr):
        return evaluate_expr(expr.left, roots) > evaluate_expr(expr.right, roots)
    if isinstance(expr, GeExpr):
        return evaluate_expr(expr.left, roots) >= evaluate_expr(expr.right, roots)
    if isinstance(expr, LtExpr):
        return evaluate_expr(expr.left, roots) < evaluate_expr(expr.right, roots)
    if isinstance(expr, LeExpr):
        return evaluate_expr(expr.left, roots) <= evaluate_expr(expr.right, roots)
    if isinstance(expr, InExpr):
        return evaluate_expr(expr.ref, roots) in evaluate_expr(expr.values, roots)
    if isinstance(expr, BetweenExpr):
        value = evaluate_expr(expr.ref, roots)
        return evaluate_expr(expr.low, roots) <= value <= evaluate_expr(expr.high, roots)
    if isinstance(expr, AndExpr):
        return bool(evaluate_expr(expr.left, roots)) and bool(evaluate_expr(expr.right, roots))
    if isinstance(expr, OrExpr):
        return bool(evaluate_expr(expr.left, roots)) or bool(evaluate_expr(expr.right, roots))
    if isinstance(expr, NotExpr):
        return not bool(evaluate_expr(expr.operand, roots))
    return expr


__all__ = ["evaluate_expr", "resolve_path"]
