"""QuerySpec compilation for MongoDB.

Emits plain filter documents and pymongo-style sort lists; no driver import
is needed here, so the compiler is testable without the ``mongo`` extra.

Null semantics follow SQL three-valued logic so every backend selects the
same rows for one ``QuerySpec``: ``NE`` never matches a ``null`` field, hence
it is emitted with a ``{field: {"$ne": None}}`` guard (MongoDB's bare ``$ne``
would match ``null`` and missing fields).
"""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any, ClassVar

from loom.core.model.introspection import get_column_fields
from loom.core.repository.abc.cursor import Cursor
from loom.core.repository.abc.errors import UnsupportedQuery
from loom.core.repository.abc.query import FilterGroup, FilterOp, FilterSpec, SortSpec

MongoFilter = dict[str, Any]
MongoSort = list[tuple[str, int]]

_ID = "_id"
_ASCENDING = 1
_DESCENDING = -1
_COMPARISON_OPS: dict[FilterOp, str] = {
    FilterOp.GT: "$gt",
    FilterOp.GTE: "$gte",
    FilterOp.LT: "$lt",
    FilterOp.LTE: "$lte",
}
_STEP_OPS: dict[str, str] = {"ASC": "$gt", "DESC": "$lt"}
_WILDCARDS: dict[str, str] = {"%": ".*", "_": "."}


def _like_to_regex(pattern: str) -> str:
    """Translate a SQL ``LIKE`` pattern into an anchored regular expression."""
    body = "".join(_WILDCARDS.get(char) or re.escape(char) for char in pattern)
    return f"^{body}$"


def _like(value: Any) -> MongoFilter:
    return {"$regex": _like_to_regex(str(value))}


def _ilike(value: Any) -> MongoFilter:
    return {"$regex": _like_to_regex(str(value)), "$options": "i"}


def _compare(operator: str) -> Callable[[Any], MongoFilter]:
    return lambda value: {operator: value}


# IS_NULL ignores the spec value, exactly like the SQLAlchemy compiler.
_VALUE_OPS: dict[FilterOp, Callable[[Any], Any]] = {
    FilterOp.EQ: lambda value: value,
    FilterOp.IN: lambda value: {"$in": list(value)},
    FilterOp.LIKE: _like,
    FilterOp.ILIKE: _ilike,
    FilterOp.IS_NULL: lambda _value: None,
    **{op: _compare(key) for op, key in _COMPARISON_OPS.items()},
}


class MongoQueryCompiler:
    """Compiles :class:`QuerySpec` parts into MongoDB filter and sort documents.

    The primary key is stored as ``_id``; every other field keeps its model
    name. Only flat AND/OR groups are supported, per the ``FilterGroup``
    contract.

    Args:
        model: Loom model the collection is bound to; its column fields are
            the only filterable and sortable names.
        id_field: Name of the model's primary-key field, mapped to ``_id``.

    Example::

        compiler = MongoQueryCompiler(Article, "slug")
        compiler.compile_filter(FilterGroup(filters=(FilterSpec("slug", FilterOp.EQ, "a"),)))
        # {"$and": [{"_id": "a"}]}
    """

    backend: ClassVar[str] = "mongo"

    def __init__(self, model: type, id_field: str) -> None:
        self._model_name = model.__qualname__
        self._id_field = id_field
        self._fields = frozenset(get_column_fields(model))

    def compile_filter(self, group: FilterGroup) -> MongoFilter:
        """Compile a filter group into a Mongo filter document.

        Args:
            group: Flat AND/OR group of field conditions.

        Returns:
            ``{"$and": [...]}`` or ``{"$or": [...]}``; ``{}`` for an empty
            group.

        Raises:
            UnsupportedQuery: On an unknown field or a relation operator.
        """
        if not group.filters:
            return {}
        clauses = [self._compile_spec(spec) for spec in group.filters]
        return {"$or" if group.op == "OR" else "$and": clauses}

    def compile_sort(self, sort: tuple[SortSpec, ...]) -> MongoSort:
        """Compile sort directives into pymongo ``(field, direction)`` pairs.

        Args:
            sort: Ordered sort directives.

        Returns:
            List of ``(field, 1 | -1)`` pairs; empty when ``sort`` is empty.

        Raises:
            UnsupportedQuery: On an unknown sort field.
        """
        return [
            (self._column(spec.field), _DESCENDING if spec.direction == "DESC" else _ASCENDING)
            for spec in sort
        ]

    def compile_cursor_filter(self, sort: tuple[SortSpec, ...], cursor: Cursor) -> MongoFilter:
        """Build the keyset predicate positioning a page after ``cursor``.

        The row-value comparison is expanded as an ``$or`` of ``$and``
        branches with ``_id`` as the ascending tie-breaker:
        ``k1 > v1 OR (k1 = v1 AND k2 < v2) OR (... AND _id > vid)``.

        Args:
            sort: Sort directives the page is ordered by.
            cursor: Decoded cursor whose keys match ``sort`` one to one.

        Returns:
            Mongo filter document.

        Raises:
            UnsupportedQuery: If the cursor keys do not match ``sort`` or a
                sort field is unknown.
        """
        if len(cursor.keys) != len(sort):
            raise self._unsupported("cursor token does not match the sort")
        columns = [self._column(spec.field) for spec in sort] + [_ID]
        steps = [_STEP_OPS[spec.direction] for spec in sort] + [_STEP_OPS["ASC"]]
        values = [*cursor.keys, cursor.tie_breaker]
        branches: list[MongoFilter] = []
        for index, (column, step, value) in enumerate(zip(columns, steps, values, strict=True)):
            equal_prefix = [{columns[j]: values[j]} for j in range(index)]
            branches.append({"$and": [*equal_prefix, {column: {step: value}}]})
        return {"$or": branches}

    def _compile_spec(self, spec: FilterSpec) -> MongoFilter:
        column = self._column(spec.field)
        if spec.op is FilterOp.NE:
            return {"$and": [{column: {"$ne": None}}, {column: {"$ne": spec.value}}]}
        build = _VALUE_OPS.get(spec.op)
        if build is None:
            raise self._unsupported(
                f"operator '{spec.op.value}' is not supported by the {self.backend} backend"
            )
        return {column: build(spec.value)}

    def _column(self, field: str) -> str:
        if field == self._id_field:
            return _ID
        if field not in self._fields:
            raise self._unsupported(f"unknown field '{field}'")
        return field

    def _unsupported(self, reason: str) -> UnsupportedQuery:
        return UnsupportedQuery(self.backend, self._model_name, reason)
