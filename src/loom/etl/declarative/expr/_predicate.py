"""Predicate node model for ETL filtering DSL.

All nodes are immutable. Operator overloading on column and param references
produces these nodes at class-definition time; the ETL compiler stores them
in the compiled plan and the executor resolves them at runtime.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Union

PredicateNode = Union[
    "EqPred",
    "NePred",
    "GtPred",
    "GePred",
    "LtPred",
    "LePred",
    "InPred",
    "AndPred",
    "OrPred",
    "NotPred",
]


class _PredCompose:
    """Compose predicate nodes with ``&``, ``|``, ``~`` operators."""

    def __and__(self, other: PredicateNode) -> AndPred:
        return AndPred(left=self, right=other)

    def __or__(self, other: PredicateNode) -> OrPred:
        return OrPred(left=self, right=other)

    def __invert__(self) -> NotPred:
        return NotPred(operand=self)


class _ColOps:
    """Operator mixin for DSL reference types.

    Subclasses define ``__hash__`` because ``__eq__`` is overridden.
    """

    def __eq__(self, other: object) -> EqPred:  # type: ignore[override]
        return EqPred(left=self, right=other)

    def __ne__(self, other: object) -> NePred:  # type: ignore[override]
        return NePred(left=self, right=other)

    def __gt__(self, other: object) -> GtPred:
        return GtPred(left=self, right=other)

    def __ge__(self, other: object) -> GePred:
        return GePred(left=self, right=other)

    def __lt__(self, other: object) -> LtPred:
        return LtPred(left=self, right=other)

    def __le__(self, other: object) -> LePred:
        return LePred(left=self, right=other)

    def isin(self, values: Any) -> InPred:
        """Return membership predicate for ``ref.isin(values)``."""
        return InPred(ref=self, values=values)

    def between(self, low: Any, high: Any) -> AndPred:
        """Return inclusive range predicate ``(ref >= low) & (ref <= high)``."""
        return AndPred(left=GePred(left=self, right=low), right=LePred(left=self, right=high))

    def __and__(self, other: PredicateNode) -> AndPred:
        return AndPred(left=self, right=other)

    def __or__(self, other: PredicateNode) -> OrPred:
        return OrPred(left=self, right=other)

    def __invert__(self) -> NotPred:
        return NotPred(operand=self)


@dataclass(frozen=True)
class EqPred(_PredCompose):
    """Equality predicate ``left == right``."""

    left: Any
    right: Any


@dataclass(frozen=True)
class NePred(_PredCompose):
    """Inequality predicate ``left != right``."""

    left: Any
    right: Any


@dataclass(frozen=True)
class GtPred(_PredCompose):
    """Greater-than predicate ``left > right``."""

    left: Any
    right: Any


@dataclass(frozen=True)
class GePred(_PredCompose):
    """Greater-or-equal predicate ``left >= right``."""

    left: Any
    right: Any


@dataclass(frozen=True)
class LtPred(_PredCompose):
    """Less-than predicate ``left < right``."""

    left: Any
    right: Any


@dataclass(frozen=True)
class LePred(_PredCompose):
    """Less-or-equal predicate ``left <= right``."""

    left: Any
    right: Any


@dataclass(frozen=True)
class InPred(_PredCompose):
    """Membership predicate ``ref IN values``."""

    ref: Any
    values: Any


@dataclass(frozen=True)
class AndPred(_PredCompose):
    """Conjunction predicate ``left AND right``."""

    left: Any
    right: Any


@dataclass(frozen=True)
class OrPred(_PredCompose):
    """Disjunction predicate ``left OR right``."""

    left: Any
    right: Any


@dataclass(frozen=True)
class NotPred(_PredCompose):
    """Negation predicate ``NOT operand``."""

    operand: Any


__all__ = [
    "PredicateNode",
    "_ColOps",
    "EqPred",
    "NePred",
    "GtPred",
    "GePred",
    "LtPred",
    "LePred",
    "InPred",
    "AndPred",
    "OrPred",
    "NotPred",
]
