from __future__ import annotations

from datetime import datetime
from typing import Any

import pytest
from pymongo.errors import BulkWriteError, DuplicateKeyError

from loom.core.repository.abc import Cursor, FilterGroup, FilterOp, FilterSpec, SortSpec
from loom.core.repository.mongo.query_compiler import MongoQueryCompiler

from ._fake import FakeCollection, FakeMongoClient

_DAY = datetime(2026, 1, 1)
_DOCS: tuple[dict[str, Any], ...] = (
    {"_id": "a", "title": "Loom intro", "views": 10, "published_at": _DAY},
    {"_id": "b", "title": "loom deep dive", "views": 20, "published_at": None},
    {"_id": "c", "title": "Other", "views": 20, "published_at": _DAY},
    {"_id": "d", "title": "Draft_1", "views": 5, "published_at": None},
    {"_id": "e", "title": "Draft.1", "views": 30, "published_at": _DAY},
)


def _single(field: str, op: FilterOp, value: object = None) -> FilterGroup:
    return FilterGroup(filters=(FilterSpec(field, op, value),))


@pytest.fixture
async def collection() -> FakeCollection:
    fake = FakeCollection()
    await fake.insert_many([dict(doc) for doc in _DOCS])
    return fake


async def _ids(collection: FakeCollection, query: dict[str, Any]) -> list[str]:
    return [doc["_id"] for doc in await collection.find(query).sort([("_id", 1)]).to_list()]


class TestFilterEvaluation:
    @pytest.mark.parametrize(
        ("group", "expected"),
        [
            (_single("slug", FilterOp.EQ, "c"), ["c"]),
            (_single("views", FilterOp.NE, 20), ["a", "d", "e"]),
            (_single("published_at", FilterOp.NE, "x"), ["a", "c", "e"]),
            (_single("views", FilterOp.GT, 20), ["e"]),
            (_single("views", FilterOp.GTE, 20), ["b", "c", "e"]),
            (_single("views", FilterOp.LT, 10), ["d"]),
            (_single("views", FilterOp.LTE, 10), ["a", "d"]),
            (_single("slug", FilterOp.IN, ("a", "e", "zz")), ["a", "e"]),
            (_single("title", FilterOp.LIKE, "Draft_1"), ["d", "e"]),
            (_single("title", FilterOp.LIKE, "Draft.1"), ["e"]),
            (_single("title", FilterOp.LIKE, "%oom%"), ["a", "b"]),
            (_single("title", FilterOp.LIKE, "loom%"), ["b"]),
            (_single("title", FilterOp.ILIKE, "loom%"), ["a", "b"]),
            (_single("published_at", FilterOp.IS_NULL), ["b", "d"]),
            (_single("published_at", FilterOp.GT, datetime(2025, 12, 31)), ["a", "c", "e"]),
        ],
        ids=lambda value: value if isinstance(value, list) else "",
    )
    async def test_compiled_filter_selects_expected_documents(
        self,
        collection: FakeCollection,
        compiler: MongoQueryCompiler,
        group: FilterGroup,
        expected: list[str],
    ) -> None:
        assert await _ids(collection, compiler.compile_filter(group)) == expected

    async def test_or_group(self, collection: FakeCollection, compiler: MongoQueryCompiler) -> None:
        group = FilterGroup(
            filters=(FilterSpec("views", FilterOp.LT, 10), FilterSpec("slug", FilterOp.EQ, "e")),
            op="OR",
        )

        assert await _ids(collection, compiler.compile_filter(group)) == ["d", "e"]

    async def test_empty_filter_selects_everything(self, collection: FakeCollection) -> None:
        assert await _ids(collection, {}) == ["a", "b", "c", "d", "e"]

    async def test_unknown_field_operator_is_visible(self, collection: FakeCollection) -> None:
        with pytest.raises(NotImplementedError, match=r"\$exists"):
            await collection.count_documents({"title": {"$exists": True}})

    async def test_unknown_top_level_operator_is_visible(self, collection: FakeCollection) -> None:
        with pytest.raises(NotImplementedError, match=r"\$nor"):
            await collection.count_documents({"$nor": [{"title": "x"}]})


class TestCursorAndPaging:
    async def test_sort_skip_limit(
        self, collection: FakeCollection, compiler: MongoQueryCompiler
    ) -> None:
        sort = compiler.compile_sort((SortSpec("views", "DESC"), SortSpec("slug")))

        page = await collection.find({}).sort(sort).skip(1).limit(2).to_list()

        assert [doc["_id"] for doc in page] == ["b", "c"]

    async def test_nulls_sort_first_ascending(self, collection: FakeCollection) -> None:
        docs = await collection.find({}).sort([("published_at", 1), ("_id", 1)]).to_list()

        assert [doc["_id"] for doc in docs] == ["b", "d", "a", "c", "e"]

    async def test_to_list_length_caps_and_copies(self, collection: FakeCollection) -> None:
        docs = await collection.find({}).sort("views", -1).to_list(length=1)
        docs[0]["views"] = -1

        assert [doc["_id"] for doc in docs] == ["e"]
        assert collection.documents["e"]["views"] == 30

    async def test_keyset_walk_visits_every_document_once(
        self, collection: FakeCollection, compiler: MongoQueryCompiler
    ) -> None:
        sort_spec = (SortSpec("views", "DESC"),)
        sort = compiler.compile_sort(sort_spec) + [("_id", 1)]
        seen: list[str] = []
        cursor: Cursor | None = None
        for _ in range(10):
            query = {} if cursor is None else compiler.compile_cursor_filter(sort_spec, cursor)
            page = await collection.find(query).sort(sort).limit(2).to_list()
            if not page:
                break
            seen.extend(doc["_id"] for doc in page)
            cursor = Cursor(backend="mongo", keys=(page[-1]["views"],), tie_breaker=page[-1]["_id"])

        assert seen == ["e", "b", "c", "a", "d"]


class TestWrites:
    async def test_find_one_and_count(
        self, collection: FakeCollection, compiler: MongoQueryCompiler
    ) -> None:
        query = compiler.compile_filter(_single("views", FilterOp.EQ, 20))

        found = await collection.find_one(query)
        assert found is not None and found["_id"] == "b"
        assert await collection.count_documents(query) == 2
        assert await collection.find_one({"_id": "zz"}) is None

    async def test_update_one_sets_fields_on_the_first_match(
        self, collection: FakeCollection
    ) -> None:
        result = await collection.update_one({"views": 20}, {"$set": {"views": 21}})

        assert result.matched_count == 1
        assert collection.documents["b"]["views"] == 21
        assert collection.documents["c"]["views"] == 20

    async def test_update_one_rejects_other_operators(self, collection: FakeCollection) -> None:
        with pytest.raises(NotImplementedError, match=r"\$inc"):
            await collection.update_one({"_id": "a"}, {"$inc": {"views": 1}})

    async def test_delete_one(self, collection: FakeCollection) -> None:
        result = await collection.delete_one({"_id": "a"})
        missing = await collection.delete_one({"_id": "a"})

        assert (result.deleted_count, missing.deleted_count) == (1, 0)
        assert "a" not in collection.documents

    async def test_insert_one_generates_an_id_and_rejects_duplicates(self) -> None:
        collection = FakeCollection()
        generated = await collection.insert_one({"title": "x"})
        await collection.insert_one({"_id": "a"})

        assert generated.inserted_id in collection.documents
        with pytest.raises(DuplicateKeyError):
            await collection.insert_one({"_id": "a"})

    async def test_insert_many_ordered_keeps_earlier_documents_and_reports_the_index(
        self,
    ) -> None:
        collection = FakeCollection()

        with pytest.raises(BulkWriteError) as info:
            await collection.insert_many([{"_id": 1}, {"_id": 2}, {"_id": 1}, {"_id": 3}])

        assert sorted(collection.documents) == [1, 2]
        assert info.value.details["writeErrors"][0]["index"] == 2
        assert info.value.details["writeErrors"][0]["code"] == 11000


class TestSessions:
    async def test_transaction_commits_on_clean_exit(self) -> None:
        client = FakeMongoClient()

        async with client.start_session() as session, await session.start_transaction():
            await client.collection("articles").insert_one({"_id": 1})

        assert (client.committed, client.aborted) == (1, 0)

    async def test_transaction_aborts_on_error(self) -> None:
        client = FakeMongoClient()

        with pytest.raises(RuntimeError):
            async with client.start_session() as session, await session.start_transaction():
                raise RuntimeError("boom")

        assert (client.committed, client.aborted) == (0, 1)
