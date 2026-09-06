"""In-memory stand-in for pymongo's asynchronous collection and client.

Scope is deliberately narrow (plan decision 10): the calls the Mongo
repository and unit of work issue, over the operators
:class:`~loom.core.repository.mongo.query_compiler.MongoQueryCompiler` emits.
Any other query or update operator raises :class:`NotImplementedError` naming
it, so a drift between compiler and fake is visible instead of silently
matching nothing.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable, Mapping
from types import TracebackType
from typing import Any

from bson import ObjectId
from pymongo.errors import BulkWriteError, DuplicateKeyError
from pymongo.results import DeleteResult, InsertManyResult, InsertOneResult, UpdateResult

_DUPLICATE_KEY = 11000
_Document = dict[str, Any]


def _ordered(value: Any, other: Any, verdict: Callable[[Any, Any], bool]) -> bool:
    """Mongo type bracketing: an ordering comparison never matches ``null``."""
    return value is not None and verdict(value, other)


_FIELD_OPS: dict[str, Callable[[Any, Any], bool]] = {
    "$ne": lambda value, other: value != other,
    "$gt": lambda value, other: _ordered(value, other, lambda a, b: a > b),
    "$gte": lambda value, other: _ordered(value, other, lambda a, b: a >= b),
    "$lt": lambda value, other: _ordered(value, other, lambda a, b: a < b),
    "$lte": lambda value, other: _ordered(value, other, lambda a, b: a <= b),
    "$in": lambda value, other: value in other,
}


def _regex_matches(value: Any, condition: Mapping[str, Any]) -> bool:
    flags = re.IGNORECASE if "i" in condition.get("$options", "") else 0
    return isinstance(value, str) and re.search(condition["$regex"], value, flags) is not None


def _field_matches(value: Any, condition: Any) -> bool:
    if not isinstance(condition, Mapping):
        return value == condition
    for operator in condition:
        if operator == "$options":
            continue
        if operator == "$regex":
            if not _regex_matches(value, condition):
                return False
            continue
        verdict = _FIELD_OPS.get(operator)
        if verdict is None:
            raise NotImplementedError(
                f"Fake collection does not evaluate field operator {operator!r}"
            )
        if not verdict(value, condition[operator]):
            return False
    return True


def _matches(document: _Document, query: Mapping[str, Any]) -> bool:
    for key, condition in query.items():
        if key == "$and":
            if not all(_matches(document, clause) for clause in condition):
                return False
        elif key == "$or":
            if not any(_matches(document, clause) for clause in condition):
                return False
        elif key.startswith("$"):
            raise NotImplementedError(f"Fake collection does not evaluate operator {key!r}")
        elif not _field_matches(document.get(key), condition):
            return False
    return True


def _sort_key(field: str, document: _Document) -> tuple[bool, Any]:
    """Order ``null`` before every other value, as MongoDB does."""
    value = document.get(field)
    return (value is not None, value)


class FakeCursor:
    """Result of :meth:`FakeCollection.find`; evaluated lazily by :meth:`to_list`."""

    def __init__(self, documents: list[_Document]) -> None:
        self._documents = documents
        self._sort: list[tuple[str, int]] = []
        self._skip = 0
        self._limit = 0

    def sort(
        self, key_or_list: str | list[tuple[str, int]], direction: int | None = None
    ) -> FakeCursor:
        if isinstance(key_or_list, str):
            self._sort = [(key_or_list, 1 if direction is None else direction)]
        else:
            self._sort = list(key_or_list)
        return self

    def skip(self, skip: int) -> FakeCursor:
        self._skip = skip
        return self

    def limit(self, limit: int) -> FakeCursor:
        self._limit = limit
        return self

    async def to_list(self, length: int | None = None) -> list[_Document]:
        documents = list(self._documents)
        for field, direction in reversed(self._sort):
            documents.sort(key=lambda doc: _sort_key(field, doc), reverse=direction == -1)
        documents = documents[self._skip :]
        if self._limit:
            documents = documents[: self._limit]
        if length is not None:
            documents = documents[:length]
        return [dict(doc) for doc in documents]


class FakeCollection:
    """In-memory stand-in for :class:`pymongo.asynchronous.collection.AsyncCollection`.

    Documents are keyed by ``_id``; inserting a duplicate raises a real
    :class:`pymongo.errors.DuplicateKeyError` (``insert_one``) or
    :class:`pymongo.errors.BulkWriteError` (``insert_many``, ordered: earlier
    documents stay inserted), exactly as the driver does. ``session``
    arguments are accepted and ignored.
    """

    def __init__(self) -> None:
        self.documents: dict[Any, _Document] = {}

    async def insert_one(
        self, document: _Document, session: object | None = None
    ) -> InsertOneResult:
        return InsertOneResult(self._store(document), acknowledged=True)

    async def insert_many(
        self, documents: Iterable[_Document], ordered: bool = True, session: object | None = None
    ) -> InsertManyResult:
        if not ordered:
            raise NotImplementedError("fake insert_many supports ordered inserts only")
        inserted: list[Any] = []
        for index, document in enumerate(documents):
            try:
                inserted.append(self._store(document))
            except DuplicateKeyError as exc:
                raise self._bulk_error(index, exc, inserted) from exc
        return InsertManyResult(inserted, acknowledged=True)

    async def find_one(
        self, filter: Mapping[str, Any] | None = None, session: object | None = None
    ) -> _Document | None:
        found = await self.find(filter, session).limit(1).to_list()
        return found[0] if found else None

    def find(
        self, filter: Mapping[str, Any] | None = None, session: object | None = None
    ) -> FakeCursor:
        return FakeCursor(self._select(filter or {}))

    async def count_documents(
        self, filter: Mapping[str, Any], session: object | None = None
    ) -> int:
        return len(self._select(filter))

    async def update_one(
        self, filter: Mapping[str, Any], update: Mapping[str, Any], session: object | None = None
    ) -> UpdateResult:
        unknown = [operator for operator in update if operator != "$set"]
        if unknown:
            raise NotImplementedError(
                f"Fake collection does not apply update operator {unknown[0]!r}"
            )
        matched = self._select(filter)[:1]
        for document in matched:
            document.update(update.get("$set", {}))
        return UpdateResult({"n": len(matched), "nModified": len(matched)}, acknowledged=True)

    async def delete_one(
        self, filter: Mapping[str, Any], session: object | None = None
    ) -> DeleteResult:
        matched = self._select(filter)[:1]
        for document in matched:
            del self.documents[document["_id"]]
        return DeleteResult({"n": len(matched)}, acknowledged=True)

    def _select(self, query: Mapping[str, Any]) -> list[_Document]:
        return [doc for doc in self.documents.values() if _matches(doc, query)]

    def _store(self, document: _Document) -> Any:
        document.setdefault("_id", ObjectId())
        key = document["_id"]
        if key in self.documents:
            raise DuplicateKeyError(f"E11000 duplicate key error: _id {key!r}", code=_DUPLICATE_KEY)
        self.documents[key] = document
        return key

    @staticmethod
    def _bulk_error(index: int, exc: DuplicateKeyError, inserted: list[Any]) -> BulkWriteError:
        write_error = {"index": index, "code": _DUPLICATE_KEY, "errmsg": str(exc)}
        return BulkWriteError({"nInserted": len(inserted), "writeErrors": [write_error]})


class FakeTransaction:
    """Block entered via ``async with await session.start_transaction()``.

    Commits on clean exit, aborts when an exception propagates.
    """

    def __init__(self, client: FakeMongoClient) -> None:
        self._client = client

    async def __aenter__(self) -> FakeTransaction:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if exc_type is None:
            self._client.committed += 1
        else:
            self._client.aborted += 1


class FakeSession:
    """Stand-in for :class:`pymongo.asynchronous.client_session.AsyncClientSession`."""

    def __init__(self, client: FakeMongoClient) -> None:
        self._client = client

    async def __aenter__(self) -> FakeSession:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        return None

    async def start_transaction(self) -> FakeTransaction:
        return FakeTransaction(self._client)


class FakeMongoClient:
    """Stand-in for :class:`pymongo.asynchronous.mongo_client.AsyncMongoClient`.

    Mirrors the driver's shapes: ``start_session()`` is synchronous and
    returns an async context manager; ``start_transaction()`` is awaited and
    returns one. Commits and aborts are counted, nothing is rolled back.
    """

    def __init__(self) -> None:
        self.collections: dict[str, FakeCollection] = {}
        self.committed = 0
        self.aborted = 0

    def collection(self, name: str) -> FakeCollection:
        return self.collections.setdefault(name, FakeCollection())

    def start_session(self) -> FakeSession:
        return FakeSession(self)
