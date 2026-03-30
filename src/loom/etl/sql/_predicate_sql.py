"""Predicate → SQL string serialiser.

Converts a :data:`~loom.etl._predicate.PredicateNode` tree into an ANSI SQL
predicate string suitable for Delta Lake ``replaceWhere``.

Internal module — not part of the public API.
"""

from __future__ import annotations

from typing import Any

from loom.etl.sql._predicate import PredicateNode
from loom.etl.sql._predicate_dialect import PredicateDialect, fold_predicate


class _SqlPredicateDialect(PredicateDialect[str]):
    """Render predicate nodes as ANSI SQL fragments."""

    def column(self, name: str) -> str:
        return name

    def literal(self, value: Any) -> str:
        # Preserve current behavior for binary predicates:
        # plain string leaves are emitted raw (unquoted).
        # IN-lists use _in_literal() below, which quotes strings.
        if isinstance(value, str):
            return value
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        return str(value)

    def eq(self, left: str, right: str) -> str:
        return f"{left} = {right}"

    def ne(self, left: str, right: str) -> str:
        return f"{left} != {right}"

    def gt(self, left: str, right: str) -> str:
        return f"{left} > {right}"

    def ge(self, left: str, right: str) -> str:
        return f"{left} >= {right}"

    def lt(self, left: str, right: str) -> str:
        return f"{left} < {right}"

    def le(self, left: str, right: str) -> str:
        return f"{left} <= {right}"

    def in_(self, ref: str, values: tuple[Any, ...]) -> str:
        formatted = ", ".join(self._in_literal(v) for v in values)
        return f"{ref} IN ({formatted})"

    def and_(self, left: str, right: str) -> str:
        return f"({left}) AND ({right})"

    def or_(self, left: str, right: str) -> str:
        return f"({left}) OR ({right})"

    def not_(self, operand: str) -> str:
        return f"NOT ({operand})"

    def _in_literal(self, value: Any) -> str:
        if isinstance(value, str):
            escaped = value.replace("'", "''")
            return f"'{escaped}'"
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        return str(value)


_SQL_PREDICATE_DIALECT = _SqlPredicateDialect()


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
    """
    return fold_predicate(node, params_instance, _SQL_PREDICATE_DIALECT)
