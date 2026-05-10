"""Predicate node aliases for the ETL declarative DSL.

The boolean AST is owned by :mod:`loom.core.expr.nodes` and re-exported here
so ETL can share the same immutable expression language as streaming.
Operator overloading on column and param references still lives in ETL, but
the node types themselves are framework-common value objects.
"""

from __future__ import annotations

from typing import Any

from loom.core.expr.nodes import (
    AndExpr as AndPred,
)
from loom.core.expr.nodes import (
    EqExpr as EqPred,
)
from loom.core.expr.nodes import (
    ExprNode as PredicateNode,
)
from loom.core.expr.nodes import (
    GeExpr as GePred,
)
from loom.core.expr.nodes import (
    GtExpr as GtPred,
)
from loom.core.expr.nodes import (
    InExpr as InPred,
)
from loom.core.expr.nodes import (
    LeExpr as LePred,
)
from loom.core.expr.nodes import (
    LtExpr as LtPred,
)
from loom.core.expr.nodes import (
    NeExpr as NePred,
)
from loom.core.expr.nodes import (
    NotExpr as NotPred,
)
from loom.core.expr.nodes import (
    OrExpr as OrPred,
)


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
