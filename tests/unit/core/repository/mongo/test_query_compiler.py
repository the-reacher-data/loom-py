from __future__ import annotations

from datetime import datetime

import pytest

from loom.core.repository.abc import (
    Cursor,
    FilterGroup,
    FilterOp,
    FilterSpec,
    SortSpec,
    UnsupportedQuery,
)
from loom.core.repository.abc.query_compiler import QueryCompiler
from loom.core.repository.mongo.query_compiler import MongoQueryCompiler

from .conftest import Article


def _single(spec: FilterSpec) -> FilterGroup:
    return FilterGroup(filters=(spec,))


class TestFieldMapping:
    def test_primary_key_maps_to_underscore_id(self, compiler: MongoQueryCompiler) -> None:
        group = _single(FilterSpec("slug", FilterOp.EQ, "hello"))

        assert compiler.compile_filter(group) == {"$and": [{"_id": "hello"}]}

    def test_other_fields_keep_their_name(self, compiler: MongoQueryCompiler) -> None:
        group = _single(FilterSpec("published_at", FilterOp.EQ, datetime(2026, 1, 1)))

        assert compiler.compile_filter(group) == {"$and": [{"published_at": datetime(2026, 1, 1)}]}

    def test_unknown_field_is_unsupported(self, compiler: MongoQueryCompiler) -> None:
        with pytest.raises(UnsupportedQuery, match="'author'") as info:
            compiler.compile_filter(_single(FilterSpec("author", FilterOp.EQ, "x")))

        assert info.value.backend == "mongo"
        assert info.value.model == "Article"

    def test_dotted_path_is_unsupported(self, compiler: MongoQueryCompiler) -> None:
        with pytest.raises(UnsupportedQuery, match="'category.name'"):
            compiler.compile_filter(_single(FilterSpec("category.name", FilterOp.EQ, "x")))


class TestOperators:
    def test_eq(self, compiler: MongoQueryCompiler) -> None:
        assert compiler.compile_filter(_single(FilterSpec("views", FilterOp.EQ, 3))) == {
            "$and": [{"views": 3}]
        }

    @pytest.mark.parametrize(
        ("op", "mongo_op"),
        [
            (FilterOp.GT, "$gt"),
            (FilterOp.GTE, "$gte"),
            (FilterOp.LT, "$lt"),
            (FilterOp.LTE, "$lte"),
        ],
    )
    def test_comparisons(self, compiler: MongoQueryCompiler, op: FilterOp, mongo_op: str) -> None:
        assert compiler.compile_filter(_single(FilterSpec("views", op, 3))) == {
            "$and": [{"views": {mongo_op: 3}}]
        }

    def test_ne_guards_null_like_sql(self, compiler: MongoQueryCompiler) -> None:
        group = _single(FilterSpec("published_at", FilterOp.NE, "x"))

        assert compiler.compile_filter(group) == {
            "$and": [{"$and": [{"published_at": {"$ne": None}}, {"published_at": {"$ne": "x"}}]}]
        }

    def test_in_coerces_value_to_list(self, compiler: MongoQueryCompiler) -> None:
        group = _single(FilterSpec("views", FilterOp.IN, (1, 2)))

        assert compiler.compile_filter(group) == {"$and": [{"views": {"$in": [1, 2]}}]}

    def test_like_translates_sql_wildcards_and_escapes_the_rest(
        self, compiler: MongoQueryCompiler
    ) -> None:
        group = _single(FilterSpec("title", FilterOp.LIKE, "a%b_c.d(e"))

        assert compiler.compile_filter(group) == {
            "$and": [{"title": {"$regex": r"^a.*b.c\.d\(e$"}}]
        }

    def test_ilike_adds_case_insensitive_option(self, compiler: MongoQueryCompiler) -> None:
        group = _single(FilterSpec("title", FilterOp.ILIKE, "%loom%"))

        assert compiler.compile_filter(group) == {
            "$and": [{"title": {"$regex": "^.*loom.*$", "$options": "i"}}]
        }

    @pytest.mark.parametrize("value", [None, True, False])
    def test_is_null_ignores_the_value_like_sqlalchemy(
        self, compiler: MongoQueryCompiler, value: object
    ) -> None:
        group = _single(FilterSpec("published_at", FilterOp.IS_NULL, value))

        assert compiler.compile_filter(group) == {"$and": [{"published_at": None}]}

    @pytest.mark.parametrize("op", [FilterOp.EXISTS, FilterOp.NOT_EXISTS])
    def test_relation_operators_are_unsupported(
        self, compiler: MongoQueryCompiler, op: FilterOp
    ) -> None:
        with pytest.raises(UnsupportedQuery, match=f"{op.value}.*mongo|mongo.*{op.value}") as info:
            compiler.compile_filter(_single(FilterSpec("title", op)))

        assert info.value.backend == "mongo"
        assert op.value in info.value.reason


class TestGroups:
    def test_and_group(self, compiler: MongoQueryCompiler) -> None:
        group = FilterGroup(
            filters=(FilterSpec("views", FilterOp.GTE, 1), FilterSpec("views", FilterOp.LTE, 9))
        )

        assert compiler.compile_filter(group) == {
            "$and": [{"views": {"$gte": 1}}, {"views": {"$lte": 9}}]
        }

    def test_or_group(self, compiler: MongoQueryCompiler) -> None:
        group = FilterGroup(
            filters=(FilterSpec("title", FilterOp.EQ, "a"), FilterSpec("slug", FilterOp.EQ, "b")),
            op="OR",
        )

        assert compiler.compile_filter(group) == {"$or": [{"title": "a"}, {"_id": "b"}]}

    def test_empty_group_matches_everything(self, compiler: MongoQueryCompiler) -> None:
        assert compiler.compile_filter(FilterGroup(filters=())) == {}


class TestSort:
    def test_sort_uses_pymongo_pairs_with_pk_mapped(self, compiler: MongoQueryCompiler) -> None:
        sort = (SortSpec("views", "DESC"), SortSpec("slug"))

        assert compiler.compile_sort(sort) == [("views", -1), ("_id", 1)]

    def test_empty_sort(self, compiler: MongoQueryCompiler) -> None:
        assert compiler.compile_sort(()) == []

    def test_unknown_sort_field_is_unsupported(self, compiler: MongoQueryCompiler) -> None:
        with pytest.raises(UnsupportedQuery, match="'rank'"):
            compiler.compile_sort((SortSpec("rank"),))


class TestCursorFilter:
    def test_keyset_predicate_expands_lexicographically(self, compiler: MongoQueryCompiler) -> None:
        sort = (SortSpec("views", "DESC"), SortSpec("title"))
        cursor = Cursor(backend="mongo", keys=(10, "m"), tie_breaker="k")

        assert compiler.compile_cursor_filter(sort, cursor) == {
            "$or": [
                {"$and": [{"views": {"$lt": 10}}]},
                {"$and": [{"views": 10}, {"title": {"$gt": "m"}}]},
                {"$and": [{"views": 10}, {"title": "m"}, {"_id": {"$gt": "k"}}]},
            ]
        }

    def test_without_sort_only_the_tie_breaker_positions(
        self, compiler: MongoQueryCompiler
    ) -> None:
        cursor = Cursor(backend="mongo", keys=(), tie_breaker="k")

        assert compiler.compile_cursor_filter((), cursor) == {
            "$or": [{"$and": [{"_id": {"$gt": "k"}}]}]
        }

    def test_key_count_must_match_sort(self, compiler: MongoQueryCompiler) -> None:
        cursor = Cursor(backend="mongo", keys=(1,), tie_breaker="k")

        with pytest.raises(UnsupportedQuery, match="does not match the sort"):
            compiler.compile_cursor_filter((), cursor)


class TestProtocol:
    def test_mongo_compiler_satisfies_query_compiler(self) -> None:
        assert isinstance(MongoQueryCompiler(Article, "slug"), QueryCompiler)
