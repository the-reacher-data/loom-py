"""Expression node model shared by declarative Loom DSLs."""

from __future__ import annotations

from typing import Any, TypeAlias

from loom.core.model import LoomFrozenStruct

PathSegment: TypeAlias = str | int


class BoolOps:
    """Boolean composition operators for expression nodes."""

    def __and__(self, other: ExprNode) -> AndExpr:
        return AndExpr(left=self, right=other)

    def __or__(self, other: ExprNode) -> OrExpr:
        return OrExpr(left=self, right=other)

    def __invert__(self) -> NotExpr:
        return NotExpr(operand=self)


class EqExpr(LoomFrozenStruct, BoolOps, frozen=True):
    """Equality expression."""

    left: Any
    right: Any


class NeExpr(LoomFrozenStruct, BoolOps, frozen=True):
    """Inequality expression."""

    left: Any
    right: Any


class GtExpr(LoomFrozenStruct, BoolOps, frozen=True):
    """Greater-than expression."""

    left: Any
    right: Any


class GeExpr(LoomFrozenStruct, BoolOps, frozen=True):
    """Greater-or-equal expression."""

    left: Any
    right: Any


class LtExpr(LoomFrozenStruct, BoolOps, frozen=True):
    """Less-than expression."""

    left: Any
    right: Any


class LeExpr(LoomFrozenStruct, BoolOps, frozen=True):
    """Less-or-equal expression."""

    left: Any
    right: Any


class InExpr(LoomFrozenStruct, BoolOps, frozen=True):
    """Membership expression."""

    ref: Any
    values: Any


class BetweenExpr(LoomFrozenStruct, BoolOps, frozen=True):
    """Inclusive range expression."""

    ref: Any
    low: Any
    high: Any


class AndExpr(LoomFrozenStruct, BoolOps, frozen=True):
    """Conjunction expression."""

    left: Any
    right: Any


class OrExpr(LoomFrozenStruct, BoolOps, frozen=True):
    """Disjunction expression."""

    left: Any
    right: Any


class NotExpr(LoomFrozenStruct, BoolOps, frozen=True):
    """Negation expression."""

    operand: Any


ExprNode: TypeAlias = (
    EqExpr
    | NeExpr
    | GtExpr
    | GeExpr
    | LtExpr
    | LeExpr
    | InExpr
    | BetweenExpr
    | AndExpr
    | OrExpr
    | NotExpr
)


__all__ = [
    "AndExpr",
    "BetweenExpr",
    "BoolOps",
    "EqExpr",
    "ExprNode",
    "GeExpr",
    "GtExpr",
    "InExpr",
    "LeExpr",
    "LtExpr",
    "NeExpr",
    "NotExpr",
    "OrExpr",
    "PathSegment",
]
