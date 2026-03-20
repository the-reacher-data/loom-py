"""Predicate node model for ETL source filtering.

All nodes are immutable.  Operator overloading on column and param
references produces these nodes at class-definition time; the ETL
compiler stores them in the compiled plan and the executor resolves
them against the active backend at runtime.

Supported operators (standard Python / dataframe idiom):

* Comparison  : ``==``, ``!=``, ``>``, ``>=``, ``<``, ``<=``
* Membership  : ``.isin(values)``
* Composition : ``&`` (AND), ``|`` (OR), ``~`` (NOT)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Union

if TYPE_CHECKING:
    pass


# ---------------------------------------------------------------------------
# Forward-declared union — resolved after all node classes are defined.
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Mixins
# ---------------------------------------------------------------------------


class _PredCompose:
    """Operator mixin that composes predicate nodes with ``&``, ``|``, ``~``."""

    def __and__(self, other: PredicateNode) -> AndPred:
        return AndPred(left=self, right=other)

    def __or__(self, other: PredicateNode) -> OrPred:
        return OrPred(left=self, right=other)

    def __invert__(self) -> NotPred:
        return NotPred(operand=self)


class _ColOps:
    """Operator mixin for column / param reference types.

    Overrides comparison dunder methods to return :data:`PredicateNode`
    instances instead of ``bool``.  Also exposes :meth:`isin` for
    membership checks since Python does not allow overriding ``in``.

    Subclasses **must** define ``__hash__`` because ``__eq__`` is
    overridden here (Python sets ``__hash__ = None`` otherwise).
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
        """Return an :class:`InPred` for ``col.isin(values)``.

        Args:
            values: Iterable of literals or a :class:`~loom.etl._proxy.ParamExpr`.

        Returns:
            Membership predicate node.
        """
        return InPred(ref=self, values=values)

    def between(self, low: Any, high: Any) -> AndPred:
        """Return ``low <= col <= high`` as an :class:`AndPred`.

        Equivalent to ``(col >= low) & (col <= high)``.

        Args:
            low:  Lower bound (inclusive).
            high: Upper bound (inclusive).

        Returns:
            Conjunction predicate node.
        """
        return AndPred(left=GePred(left=self, right=low), right=LePred(left=self, right=high))

    def __and__(self, other: PredicateNode) -> AndPred:
        return AndPred(left=self, right=other)

    def __or__(self, other: PredicateNode) -> OrPred:
        return OrPred(left=self, right=other)

    def __invert__(self) -> NotPred:
        return NotPred(operand=self)


# ---------------------------------------------------------------------------
# Predicate nodes
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class EqPred(_PredCompose):
    """Equality predicate: ``left == right``."""

    left: Any
    right: Any


@dataclass(frozen=True)
class NePred(_PredCompose):
    """Inequality predicate: ``left != right``."""

    left: Any
    right: Any


@dataclass(frozen=True)
class GtPred(_PredCompose):
    """Greater-than predicate: ``left > right``."""

    left: Any
    right: Any


@dataclass(frozen=True)
class GePred(_PredCompose):
    """Greater-or-equal predicate: ``left >= right``."""

    left: Any
    right: Any


@dataclass(frozen=True)
class LtPred(_PredCompose):
    """Less-than predicate: ``left < right``."""

    left: Any
    right: Any


@dataclass(frozen=True)
class LePred(_PredCompose):
    """Less-or-equal predicate: ``left <= right``."""

    left: Any
    right: Any


@dataclass(frozen=True)
class InPred(_PredCompose):
    """Membership predicate: ``ref.isin(values)``."""

    ref: Any
    values: Any


@dataclass(frozen=True)
class AndPred(_PredCompose):
    """Conjunction predicate: ``left & right``."""

    left: Any
    right: Any


@dataclass(frozen=True)
class OrPred(_PredCompose):
    """Disjunction predicate: ``left | right``."""

    left: Any
    right: Any


@dataclass(frozen=True)
class NotPred(_PredCompose):
    """Negation predicate: ``~operand``."""

    operand: Any
